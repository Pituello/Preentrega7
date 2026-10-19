"""
Observabilidad con Arize Phoenix (OpenInference sobre OpenTelemetry).

Qué se envía a Phoenix en cada ejecución de un job:

    orquestador (span raíz AGENT: una traza por ejecución; session.id = job_id)
    └── LangGraph
        ├── supervisor (nodo)
        │   └── agente_supervisor (AGENT) -> ChatOpenAI (LLM: tokens, costo, latencia)
        ├── investigador (nodo)
        │   └── agente_investigador (AGENT) -> ChatOpenAI, consultar_catalogo (TOOL), ChatOpenAI
        ├── validador (nodo, sin LLM)
        ├── analista, reservas, aprobacion_humana, ejecutar_reserva -> reservar_unidad (TOOL)
        └── sintesis (nodo) -> agente_sintesis (AGENT) -> ChatOpenAI

- El instrumentador de LangChain registra solo cada nodo, cada llamada al
  modelo (con tokens de entrada y salida) y cada herramienta. Con esos tokens
  y el nombre del modelo, Phoenix calcula el COSTO de cada traza.
- El decorador @agente de orquestador/trazado.py suma el span de cada
  agente, para leer la jerarquía nodo -> agente -> herramienta / modelo.
- span_de_job() abre el span raíz con metadata del job (mejor práctica de la
  clase: la versión de los prompts va como etiqueta en cada traza).
- Un job con aprobación humana genera DOS trazas (antes y después de la
  pausa) que comparten session.id: en la pestaña Sessions de Phoenix se ven
  juntas.
"""
from collections.abc import Iterator
from contextlib import contextmanager

from loguru import logger
from openinference.instrumentation import OITracer, TraceConfig, using_metadata, using_session, using_tags
from opentelemetry import trace

from orquestador import config, prompts
from orquestador.llm import describir_llm
# Tracer para el span raíz de cada job. Mientras no se registre Phoenix, no hace nada (tests).
tracer = OITracer(trace.get_tracer("orquestador-vw"), config=TraceConfig())
_proveedor = None


def iniciar_observabilidad(servicio: str) -> None:
    """Registra el exportador hacia Phoenix e instrumenta LangChain/LangGraph. Idempotente."""
    global _proveedor
    if _proveedor is not None:
        return
    if not config.phoenix_habilitado():
        logger.warning("Observabilidad desactivada (PHOENIX_HABILITADO=false): no se envían trazas")
        return
    from openinference.instrumentation.langchain import LangChainInstrumentor
    from phoenix.otel import register

    _proveedor = register(
        project_name=config.phoenix_proyecto(),
        endpoint=config.phoenix_endpoint(),
        batch=True,  # exporta en segundo plano: no agrega latencia a los jobs
        set_global_tracer_provider=True,
        verbose=False,
    )
    LangChainInstrumentor().instrument(tracer_provider=_proveedor)
    logger.info(f"Trazas -> Phoenix {config.phoenix_endpoint()} (proyecto '{config.phoenix_proyecto()}', {servicio})")


def apagar_observabilidad() -> None:
    """Envía las trazas que quedaron en el buffer antes de cerrar el proceso."""
    if _proveedor is not None:
        _proveedor.force_flush()
        _proveedor.shutdown()


def _llm_configurado() -> str:
    try:
        return describir_llm()
    except (RuntimeError, ValueError):  # sin API key (por ejemplo, en los tests con agentes falsos)
        return "sin configurar"


def _etiquetas(accion: str, simulacion: dict) -> list[str]:
    etiquetas = [f"prompts:{prompts.PROMPTS_VERSION}", f"accion:{accion}"]
    if simulacion:
        etiquetas.append("simulacion")
    return etiquetas


@contextmanager
def span_de_job(job_id: str, accion: str, tarea: str, simulacion: dict) -> Iterator:
    """Span raíz de una ejecución del grafo: todo lo que pase adentro queda en la misma traza."""
    metadata = {
        "job_id": job_id,
        "accion": accion,
        "llm": _llm_configurado(),
        "prompts_version": prompts.PROMPTS_VERSION,
        "prompts_huella": prompts.huella_prompts(),
        "simulacion": simulacion,
    }
    with using_session(job_id), using_metadata(metadata), using_tags(_etiquetas(accion, simulacion)):
        with tracer.start_as_current_span("orquestador", openinference_span_kind="agent") as span:
            span.set_input(tarea)
            yield span
