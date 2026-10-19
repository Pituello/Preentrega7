"""
Estado de los jobs en Redis: el "buzón" entre la API y el worker.

Claves:
- vw:job:{id}            hash con el estado del job (lo que devuelve GET /tasks/{id})
- vw:job:{id}:historial  lista con cada cambio de estado y cuándo pasó
- vw:cola                lista que funciona como cola (RPUSH para encolar, BLPOP para tomar)
- vw:jobs                sorted set con todos los jobs por fecha (para listarlos)

Ciclo de vida:
    PENDING -> RUNNING -> DONE
                       -> FAILED            (cualquier excepción o timeout)
                       -> WAITING_APPROVAL  (pausa HITL) -> PENDING -> RUNNING -> ...

Dos errores comunes de la clase que esto evita:
- "No persistir el estado inicial": crear() guarda el job y lo encola en una
  sola transacción (MULTI/EXEC): el job existe en Redis antes de que un worker
  pueda tomarlo y antes de que el cliente lo consulte.
- "Ignorar los errores en background": el worker marca FAILED ante cualquier
  excepción (ver worker.py), así el cliente nunca queda en un polling infinito.
"""
import json
import time
import uuid
from datetime import datetime, timezone
from enum import StrEnum

from redis.asyncio import Redis

PREFIJO_JOB = "vw:job:"
COLA = "vw:cola"
INDICE = "vw:jobs"
CAMPOS_JSON = ("simulacion", "resultado", "aprobacion", "decision")


class Estado(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    DONE = "DONE"
    FAILED = "FAILED"


TERMINALES = {Estado.DONE, Estado.FAILED}

# Aprobar es "leer el estado y, si está esperando, cambiarlo y encolar": tiene que ser atómico,
# si no dos aprobaciones simultáneas reanudarían el grafo dos veces. Lua corre de una sola vez.
_LUA_REGISTRAR_DECISION = """
if redis.call('HGET', KEYS[1], 'estado') ~= 'WAITING_APPROVAL' then return 0 end
redis.call('HSET', KEYS[1], 'estado', 'PENDING', 'decision', ARGV[1], 'actualizado_en', ARGV[2])
redis.call('RPUSH', KEYS[2], ARGV[3])
redis.call('RPUSH', KEYS[3], ARGV[4])
return 1
"""


def ahora() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _clave(job_id: str) -> str:
    return f"{PREFIJO_JOB}{job_id}"


def _mensaje(job_id: str, accion: str) -> str:
    return json.dumps({"job_id": job_id, "accion": accion})


def _evento(estado: str, detalle: str = "") -> str:
    return json.dumps({"estado": estado, "en": ahora(), "detalle": detalle}, ensure_ascii=False)


class RepositorioJobs:
    def __init__(self, redis: Redis):
        self.redis = redis
        self._registrar_decision = redis.register_script(_LUA_REGISTRAR_DECISION)

    async def crear(self, tarea: str, simulacion: dict | None = None) -> dict:
        job_id = str(uuid.uuid4())
        marca = ahora()
        job = {
            "job_id": job_id,
            "estado": Estado.PENDING.value,
            "tarea": tarea,
            "simulacion": json.dumps(simulacion or {}),
            "creado_en": marca,
            "creado_ts": f"{time.time():.3f}",
            "actualizado_en": marca,
            "duracion_ejecucion_s": "0",
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(_clave(job_id), mapping=job)
            pipe.rpush(f"{_clave(job_id)}:historial", _evento(Estado.PENDING, "job creado y encolado"))
            pipe.zadd(INDICE, {job_id: time.time()})
            pipe.rpush(COLA, _mensaje(job_id, "iniciar"))  # se encola DESPUÉS de guardar el estado
            await pipe.execute()
        return await self.obtener(job_id)

    async def obtener(self, job_id: str) -> dict | None:
        crudo = await self.redis.hgetall(_clave(job_id))
        if not crudo:
            return None
        job: dict = dict(crudo)
        for campo in CAMPOS_JSON:
            if campo in job:
                job[campo] = json.loads(job[campo])
        job["duracion_ejecucion_s"] = float(job.get("duracion_ejecucion_s", 0))
        if "latencia_total_s" in job:
            job["latencia_total_s"] = float(job["latencia_total_s"])
        job["historial"] = [json.loads(e) for e in await self.redis.lrange(f"{_clave(job_id)}:historial", 0, -1)]
        return job

    async def actualizar(self, job_id: str, estado: Estado | None = None, detalle: str = "", **campos) -> None:
        valores = {"actualizado_en": ahora()}
        for campo, valor in campos.items():
            valores[campo] = json.dumps(valor, ensure_ascii=False) if campo in CAMPOS_JSON else str(valor)
        async with self.redis.pipeline(transaction=True) as pipe:
            if estado is not None:
                valores["estado"] = estado.value
                pipe.rpush(f"{_clave(job_id)}:historial", _evento(estado, detalle))
            pipe.hset(_clave(job_id), mapping=valores)
            await pipe.execute()

    async def sumar_duracion(self, job_id: str, segundos: float) -> None:
        await self.redis.hincrbyfloat(_clave(job_id), "duracion_ejecucion_s", round(segundos, 3))

    async def encolar(self, job_id: str, accion: str) -> None:
        await self.redis.rpush(COLA, _mensaje(job_id, accion))

    async def tomar(self, timeout_s: int = 5) -> dict | None:
        """BLPOP: espera un mensaje sin consumir CPU. El timeout permite cortar el bucle al apagar."""
        item = await self.redis.blpop([COLA], timeout=timeout_s)
        return json.loads(item[1]) if item else None

    async def registrar_decision(self, job_id: str, decision: dict) -> bool:
        detalle = f"{'aprobado' if decision['aprobado'] else 'rechazado'} por {decision.get('revisor', 'anónimo')}"
        resultado = await self._registrar_decision(
            keys=[_clave(job_id), COLA, f"{_clave(job_id)}:historial"],
            args=[json.dumps(decision, ensure_ascii=False), ahora(), _mensaje(job_id, "reanudar"),
                  _evento(Estado.PENDING, f"decisión humana: {detalle}; reanudación encolada")],
        )
        return bool(resultado)

    async def listar(self, cantidad: int = 20) -> list[dict]:
        ids = await self.redis.zrevrange(INDICE, 0, cantidad - 1)
        jobs = [await self.obtener(job_id) for job_id in ids]
        return [j for j in jobs if j is not None]

    async def largo_de_cola(self) -> int:
        return await self.redis.llen(COLA)

    async def recuperar_interrumpidos(self) -> list[str]:
        """
        Jobs que quedaron en RUNNING porque el worker se cortó a mitad de camino
        (reinicio, Ctrl+C). Se vuelven a encolar con la acción "continuar": el
        grafo retoma desde su último checkpoint en Redis, sin repetir los pasos
        que ya había hecho. Supone un solo proceso worker (el de la API, o el
        de python -m app.worker), como en esta entrega.
        """
        recuperados = []
        async for clave in self.redis.scan_iter(match=f"{PREFIJO_JOB}*", count=200):
            if clave.endswith(":historial") or await self.redis.hget(clave, "estado") != Estado.RUNNING:
                continue
            job_id = clave.removeprefix(PREFIJO_JOB)
            await self.actualizar(job_id, Estado.PENDING, "el worker se reinició: se retoma desde el último checkpoint")
            await self.encolar(job_id, "continuar")
            recuperados.append(job_id)
        return recuperados
