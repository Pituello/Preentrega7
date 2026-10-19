"""
Supervisor: el router del sistema. Decide quién interviene, con qué
instrucción y por qué, usando with_structured_output(DecisionSupervisor):
la salida es un Literal de los nombres de nodo, así el ruteo nunca depende de
interpretar texto libre.

Ve un RESUMEN del estado (datos validados, cálculos, errores), no las salidas
crudas de las herramientas: así su prompt no crece con cada paso.
"""
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from orquestador import prompts
from orquestador.state import DecisionSupervisor
from orquestador.trazado import agente


def resumir_estado(estado: dict) -> str:
    lineas = [f"TAREA: {estado['tarea']}", ""]

    inv = estado.get("investigacion")
    if inv:
        lineas.append(f"INVESTIGACIÓN: {inv.resumen}")
        lineas += [f"  - {d.descripcion}: {d.valor:g} {d.unidad} (fuente: {d.fuente})" for d in inv.datos]
    else:
        lineas.append("INVESTIGACIÓN: (todavía no hay)")

    an = estado.get("analisis")
    if an:
        lineas.append(f"ANÁLISIS: {an.conclusion}")
        lineas += [f"  - {c.descripcion}: {c.expresion} = {c.resultado:g} {c.unidad}" for c in an.calculos]
    else:
        lineas.append("ANÁLISIS: (todavía no hay)")

    propuesta = estado.get("propuesta_reserva")
    if propuesta:
        lineas.append(f"RESERVA PROPUESTA: {propuesta.resumen} (versión {propuesta.version_id}, "
                      f"sucursal {propuesta.sucursal}, cliente {propuesta.cliente})")
    decision = estado.get("decision_humana")
    if decision:
        veredicto = "APROBADA" if decision.aprobado else "RECHAZADA"
        lineas.append(f"REVISIÓN HUMANA: {veredicto} por {decision.revisor}"
                      + (f" — comentario: {decision.comentario}" if decision.comentario else ""))
    resultado = estado.get("resultado_reserva")
    if resultado:
        lineas.append(f"RESERVA EJECUTADA: {'OK' if resultado.ok else 'NO SE PUDO'} — {resultado.detalle}")

    val = estado.get("validacion")
    if val:
        estado_val = "OK" if val.ok else "FALLÓ: " + " | ".join(val.errores)
        lineas.append(f"ÚLTIMA VALIDACIÓN ({val.agente}): {estado_val}")

    lineas.append(f"PASOS USADOS: {estado.get('pasos', 0)}")
    return "\n".join(lineas)


class Supervisor:
    def __init__(self, llm: BaseChatModel):
        self._llm = llm.with_structured_output(DecisionSupervisor)

    async def decidir(self, estado: dict) -> DecisionSupervisor:
        return await self._decidir(resumir_estado(estado))

    @agente("supervisor")
    async def _decidir(self, resumen: str) -> DecisionSupervisor:
        # El span de Phoenix muestra como entrada el resumen que vio el supervisor y como salida su decisión.
        return await self._llm.ainvoke([SystemMessage(prompts.SUPERVISOR), HumanMessage(resumen)])
