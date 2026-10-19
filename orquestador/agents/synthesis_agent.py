"""
Síntesis final: redacta la respuesta al usuario con lo validado. Solo se
ejecuta cuando el supervisor decide FINALIZAR (o cuando un límite lo fuerza).
"""
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from orquestador import prompts
from orquestador.agents.supervisor import resumir_estado
from orquestador.trazado import agente


class Sintetizador:
    def __init__(self, llm: BaseChatModel):
        self._llm = llm

    async def redactar(self, estado: dict) -> str:
        extra = []
        if estado.get("escalado_humano"):
            extra.append("ATENCIÓN: el proceso se escaló a REVISIÓN HUMANA porque un resultado no pasó la "
                         "validación después de los reintentos permitidos.")
        val = estado.get("validacion")
        if val and not val.ok:
            extra.append("La última validación FALLÓ: " + " | ".join(val.errores))
        return await self._redactar(resumir_estado(estado) + ("\n\n" + "\n".join(extra) if extra else ""))

    @agente("sintesis")
    async def _redactar(self, mensaje: str) -> str:
        respuesta = await self._llm.ainvoke([SystemMessage(prompts.SINTESIS), HumanMessage(mensaje)])
        contenido = respuesta.content
        if isinstance(contenido, list):  # Gemini puede devolver bloques en vez de un string
            contenido = "".join(b.get("text", "") for b in contenido if isinstance(b, dict))
        return str(contenido).strip()
