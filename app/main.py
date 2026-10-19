"""
API REST del orquestador multi-agente (Pre-Entrega 7).

    POST /tasks                  encola una tarea y devuelve el job_id AL INSTANTE (202)
    GET  /tasks/{job_id}         estado del job: PENDING, RUNNING, WAITING_APPROVAL, DONE o FAILED
    GET  /tasks                  últimos jobs (para revisar una prueba de carga)
    POST /tasks/{job_id}/approve aprobación humana de una acción crítica (app/hitl.py)
    GET  /health                 Redis, cola y workers

Ningún endpoint corre el grafo: solo leen y escriben en Redis con el cliente
async (redis.asyncio), así que nunca bloquean el event loop. Un flujo de
agentes tarda decenas de segundos; mantener el request abierto todo ese
tiempo se corta por timeout y satura el servidor. Por eso el cliente recibe
un job_id y consulta el estado (polling).

Levantar:  uvicorn app.main:app --port 8000   (con Redis y Phoenix: docker compose up -d)
Docs:      http://localhost:8000/docs
"""
import asyncio
import inspect
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, status
from loguru import logger
from pydantic import BaseModel, Field
from redis.asyncio import from_url
from redis.exceptions import ConnectionError as ErrorDeConexionRedis

from app.hitl import router as router_hitl
from app.jobs import Estado, RepositorioJobs
from orquestador import config


class PedidoTarea(BaseModel):
    tarea: str = Field(
        min_length=5, max_length=1000,
        examples=["¿Cuántos CV y cuántos Nm más tiene el Virtus Exclusive que el Tera High?"],
    )
    simulacion: dict[str, Any] | None = Field(
        default=None,
        description="Solo para probar caminos de error: romper_grafo, demora_s, fallar_investigador, corromper_analisis",
        examples=[None],
    )


@asynccontextmanager
async def ciclo_de_vida(app: FastAPI):
    from app.graph import checkpointer_redis, crear_orquestador
    from app.observability import apagar_observabilidad, iniciar_observabilidad
    from app.worker import lanzar_workers

    iniciar_observabilidad("api")
    redis = from_url(config.redis_url(), decode_responses=True)
    try:
        await redis.ping()
    except ErrorDeConexionRedis:
        logger.error(f"No se pudo conectar a Redis en {config.redis_url()}. ¿Corriste 'docker compose up -d'?")
        raise
    app.state.jobs = RepositorioJobs(redis)
    app.state.workers = []
    async with AsyncExitStack() as pila:
        if config.worker_en_proceso():
            checkpointer = await pila.enter_async_context(checkpointer_redis(config.redis_url()))
            grafo = crear_orquestador(checkpointer)
            recuperados = await app.state.jobs.recuperar_interrumpidos()
            if recuperados:
                logger.info(f"Se retoman {len(recuperados)} jobs interrumpidos desde su último checkpoint")
            app.state.workers = lanzar_workers(grafo, app.state.jobs, config.workers())
        else:
            logger.info("WORKER_EN_PROCESO=false: la API solo encola; correr aparte 'python -m app.worker'")
        yield
        for tarea in app.state.workers:
            tarea.cancel()
        await asyncio.gather(*app.state.workers, return_exceptions=True)
    await redis.aclose()
    apagar_observabilidad()


def _sin_telemetria_http() -> dict:
    """
    Las versiones nuevas de FastAPI generan una traza por cada request HTTP si
    hay un TracerProvider global (el de Phoenix). Se apaga: si no, los ~5 ms de
    cada GET /tasks/{id} del polling se mezclan con las ejecuciones del grafo
    (decenas de segundos) y el p95 del dashboard deja de medir a los agentes.
    En versiones de FastAPI sin ese parámetro no hace falta (no generan trazas).
    """
    if "telemetry" not in inspect.signature(FastAPI.__init__).parameters:
        return {}
    return {"telemetry": {"tracing": False, "metrics": False, "logs": False, "operation_spans": False}}


def crear_app(ciclo=ciclo_de_vida) -> FastAPI:
    """Fábrica de la app: los tests la crean sin ciclo de vida y le inyectan un Redis falso."""
    app = FastAPI(
        **_sin_telemetria_http(),
        title="Orquestador VW — API de producción",
        description="Sistema multi-agente de la Pre-Entrega 6 expuesto como API asíncrona, con Redis, "
                    "observabilidad en Phoenix y aprobación humana para acciones críticas.",
        version="7.0",
        lifespan=ciclo,
    )
    app.include_router(router_hitl)

    @app.post("/tasks", status_code=status.HTTP_202_ACCEPTED, tags=["tareas"])
    async def crear_tarea(pedido: PedidoTarea, request: Request) -> dict:
        """Guarda el job en Redis (PENDING), lo encola y devuelve el job_id sin esperar al grafo."""
        job = await request.app.state.jobs.crear(pedido.tarea, pedido.simulacion)
        return {"job_id": job["job_id"], "estado": job["estado"], "consultar": f"/tasks/{job['job_id']}"}

    @app.get("/tasks/{job_id}", tags=["tareas"])
    async def estado_de_tarea(job_id: str, request: Request) -> dict:
        job = await request.app.state.jobs.obtener(job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"No existe el job '{job_id}'")
        job.pop("creado_ts", None)
        if job["estado"] == Estado.WAITING_APPROVAL:
            job["siguiente_paso"] = f"POST /tasks/{job_id}/approve con la decisión del revisor"
        return job

    @app.get("/tasks", tags=["tareas"])
    async def listar_tareas(request: Request, cantidad: int = Query(20, ge=1, le=200)) -> list[dict]:
        jobs = await request.app.state.jobs.listar(cantidad)
        return [
            {k: j.get(k) for k in ("job_id", "estado", "tarea", "creado_en", "latencia_total_s", "duracion_ejecucion_s")}
            for j in jobs
        ]

    @app.get("/health", tags=["operación"])
    async def salud(request: Request) -> dict:
        jobs = request.app.state.jobs
        await jobs.redis.ping()
        return {"estado": "ok", "redis": "ok", "en_cola": await jobs.largo_de_cola(),
                "workers_en_proceso": len(getattr(request.app.state, "workers", []))}

    return app


app = crear_app()
