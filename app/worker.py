"""
Worker: toma jobs de la cola de Redis, corre el grafo y actualiza el estado.

Patrón de la clase: FastAPI (recepcionista) -> Redis (buzón) -> Worker
(especialista). La API nunca corre el grafo dentro del request: solo encola.

Dos formas de correrlo:
- Dentro de la API (WORKER_EN_PROCESO=true, el default): app/main.py lanza
  WORKERS tareas asyncio en el mismo event loop que uvicorn.
- Como proceso aparte (WORKER_EN_PROCESO=false en la API):
      python -m app.worker
  Así la API y el procesamiento escalan por separado.

Cada worker es una tarea asyncio, no un hilo: el grafo es async de punta a
punta (LLM, herramientas, Redis), así que mientras un job espera al modelo, el
event loop atiende a los demás jobs y a los requests de la API. Ninguna
llamada bloqueante corre en el event loop (error común: "bloquear el event
loop").

Manejo de errores: CUALQUIER excepción del grafo (o un timeout) deja el job
en FAILED con el error, así el cliente que hace polling sabe que terminó.
"""
import asyncio
import time
from typing import Any

from langgraph.graph.state import CompiledStateGraph
from langgraph.types import Command
from loguru import logger

from app.graph import config_de_ejecucion
from app.jobs import TERMINALES, Estado, RepositorioJobs, ahora
from app.observability import span_de_job
from orquestador import config, prompts
from orquestador.state import estado_inicial


def resumir_resultado(valores: dict[str, Any]) -> dict[str, Any]:
    """Lo que devuelve GET /tasks/{id} al terminar: la respuesta y quién decidió qué y por qué."""
    decisiones = valores.get("decisiones", [])
    validaciones = valores.get("validaciones", [])
    reserva = valores.get("resultado_reserva")
    decision_humana = valores.get("decision_humana")
    return {
        "respuesta": valores.get("respuesta_final", ""),
        "intervenciones_de_especialistas": valores.get("pasos", 0),
        "escalado_humano": valores.get("escalado_humano", False),
        "validaciones_fallidas": sum(not v.ok for v in validaciones),
        "decisiones": [
            {"paso": d.paso, "origen": d.origen, "siguiente": d.siguiente, "razon": d.razon} for d in decisiones
        ],
        "decision_humana": decision_humana.model_dump() if decision_humana else None,
        "reserva": reserva.model_dump() if reserva else None,
        "prompts_version": prompts.PROMPTS_VERSION,
    }


async def _entrada_del_grafo(grafo: CompiledStateGraph, job: dict, accion: str, cfg: dict):
    if accion == "reanudar":
        return Command(resume=job["decision"])  # retoma el hilo pausado con la decisión del humano
    if accion == "continuar":
        # El worker se cortó a mitad de camino: si hay checkpoint, None = "seguí desde el último paso guardado".
        guardado = await grafo.aget_state(cfg)
        return None if guardado.values else estado_inicial(job["tarea"])
    return estado_inicial(job["tarea"])


async def procesar(grafo: CompiledStateGraph, jobs: RepositorioJobs, mensaje: dict, nombre: str = "worker") -> None:
    job_id, accion = mensaje["job_id"], mensaje.get("accion", "iniciar")
    job = await jobs.obtener(job_id)
    if job is None:
        logger.warning(f"[{nombre}] job {job_id} no existe: se descarta el mensaje")
        return
    if job["estado"] in TERMINALES:
        logger.warning(f"[{nombre}] job {job_id[:8]} ya está {job['estado']}: se ignora el mensaje duplicado")
        return

    cfg = config_de_ejecucion(job["simulacion"], thread_id=job_id)
    await jobs.actualizar(job_id, Estado.RUNNING, f"{accion} ({nombre})", worker=nombre)
    logger.info(f"[{nombre}] {accion} job {job_id[:8]}: {job['tarea'][:70]}")
    inicio = time.perf_counter()
    try:
        entrada = await _entrada_del_grafo(grafo, job, accion, cfg)
        with span_de_job(job_id, accion, job["tarea"], job["simulacion"]) as span:
            await asyncio.wait_for(grafo.ainvoke(entrada, cfg), timeout=config.timeout_job_s())
            snapshot = await grafo.aget_state(cfg)
            # La salida de la traza en Phoenix: la respuesta, o el pedido de aprobación si quedó en pausa.
            span.set_output(snapshot.interrupts[0].value["pregunta"] if snapshot.interrupts
                            else snapshot.values.get("respuesta_final", ""))
    except TimeoutError:
        await _fallar(jobs, job, f"Timeout: la ejecución superó los {config.timeout_job_s()} s (TIMEOUT_JOB_S)")
        return
    except Exception as error:  # cualquier error del grafo, del LLM o de Redis: el job NO queda colgado
        logger.exception(f"[{nombre}] job {job_id[:8]} falló")
        await _fallar(jobs, job, f"{type(error).__name__}: {error}")
        return
    finally:
        await jobs.sumar_duracion(job_id, time.perf_counter() - inicio)

    if snapshot.interrupts:
        pedido = snapshot.interrupts[0].value
        await jobs.actualizar(job_id, Estado.WAITING_APPROVAL, pedido["pregunta"], aprobacion=pedido)
        logger.info(f"[{nombre}] job {job_id[:8]} en pausa: espera aprobación humana")
        return
    await jobs.actualizar(
        job_id, Estado.DONE, "respuesta lista",
        resultado=resumir_resultado(snapshot.values),
        finalizado_en=ahora(), latencia_total_s=_latencia(job),
    )
    logger.info(f"[{nombre}] job {job_id[:8]} DONE en {time.perf_counter() - inicio:.1f} s")


async def _fallar(jobs: RepositorioJobs, job: dict, error: str) -> None:
    await jobs.actualizar(job["job_id"], Estado.FAILED, error, error=error,
                          finalizado_en=ahora(), latencia_total_s=_latencia(job))


def _latencia(job: dict) -> str:
    """Desde que se creó el job hasta ahora (incluye la espera en la cola y, si hubo, la espera del humano)."""
    return f"{time.time() - float(job['creado_ts']):.3f}"


async def bucle(grafo: CompiledStateGraph, jobs: RepositorioJobs, nombre: str) -> None:
    """Un worker: saca mensajes de la cola para siempre. Un error inesperado no lo mata."""
    logger.info(f"[{nombre}] esperando jobs en la cola de Redis")
    while True:
        try:
            mensaje = await jobs.tomar(timeout_s=5)
            if mensaje is not None:
                await procesar(grafo, jobs, mensaje, nombre)
        except asyncio.CancelledError:
            raise  # apagado ordenado: el job en curso queda RUNNING y se retoma al volver a arrancar
        except Exception:
            logger.exception(f"[{nombre}] error inesperado (¿se cayó Redis?); reintento en 2 s")
            await asyncio.sleep(2)


def lanzar_workers(grafo: CompiledStateGraph, jobs: RepositorioJobs, cantidad: int) -> list[asyncio.Task]:
    return [asyncio.create_task(bucle(grafo, jobs, f"worker-{i + 1}")) for i in range(cantidad)]


async def main() -> None:
    """Worker como proceso separado: python -m app.worker"""
    from redis.asyncio import from_url

    from app.graph import checkpointer_redis, crear_orquestador
    from app.observability import apagar_observabilidad, iniciar_observabilidad

    iniciar_observabilidad("worker")
    redis = from_url(config.redis_url(), decode_responses=True)
    await redis.ping()
    jobs = RepositorioJobs(redis)
    try:
        async with checkpointer_redis(config.redis_url()) as checkpointer:
            grafo = crear_orquestador(checkpointer)
            recuperados = await jobs.recuperar_interrumpidos()
            if recuperados:
                logger.info(f"Se retoman {len(recuperados)} jobs interrumpidos desde su último checkpoint")
            await asyncio.gather(*lanzar_workers(grafo, jobs, config.workers()))
    finally:
        await redis.aclose()
        apagar_observabilidad()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker detenido")
