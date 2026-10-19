"""
Herramienta del Investigador: búsqueda en las fichas técnicas de los 7
modelos (el mismo dataset de las Pre-Entregas 3 y 4, en data/).

Dos backends (BUSQUEDA_BACKEND en el .env):
- "local" (default): BM25 sobre las fichas, con el tokenizador para español
  de la Pre-Entrega 4. No necesita API keys ni dependencias pesadas.
- "pinecone": combina ese BM25 con la búsqueda vectorial en el índice de la
  Pre-Entrega 4, fusionando los dos rankings con RRF (como el
  EnsembleRetriever de esa entrega). No reingesta nada: solo se conecta.

Cada fragmento lleva antepuesto el nombre del modelo (lección de la
Pre-Entrega 3: un fragmento que no nombra su modelo se confunde con otros).
"""
import asyncio
import os
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

from langchain_core.tools import tool
from pydantic import BaseModel, Field
from rank_bm25 import BM25Okapi

from orquestador import config

TOP_K = 4
_STOPWORDS = set(
    "el la los las un una unos unas de del al a en y o que con por para es son se su sus como cual cuales "
    "cuanto cuantos cuanta cuantas tiene tienen hay le lo me mi te tu mas pero sin sobre entre este esta".split()
)


def tokenizar(texto: str) -> list[str]:
    sin_tildes = "".join(c for c in unicodedata.normalize("NFD", texto.lower()) if unicodedata.category(c) != "Mn")
    return [t for t in re.findall(r"\w+", sin_tildes) if t not in _STOPWORDS]


@lru_cache(maxsize=1)
def _fragmentos() -> list[tuple[str, str]]:
    """[(archivo, texto)]: cada ficha partida por sección '## ', con el título antepuesto."""
    fragmentos = []
    for ruta in sorted(Path(config.DATA_DIR).glob("*.md")):
        texto = ruta.read_text(encoding="utf-8")
        titulo = texto.splitlines()[0].lstrip("# ").strip()
        for seccion in re.split(r"\n(?=## )", texto):
            seccion = seccion.strip()
            if seccion and not seccion.startswith("# "):
                fragmentos.append((ruta.name, f"{titulo}\n{seccion}"))
    if not fragmentos:
        raise RuntimeError(f"No hay fichas .md en '{config.DATA_DIR}'")
    return fragmentos


@lru_cache(maxsize=1)
def _indice_bm25() -> BM25Okapi:
    return BM25Okapi([tokenizar(texto) for _, texto in _fragmentos()])


# Refuerzo para los fragmentos de la ficha del modelo que nombra la consulta.
# Sin esto, "Latin NCAP del Nivus" traía primero la ficha del Tera: su sección
# de Latin NCAP también menciona al Nivus, y la del Nivus es más larga (BM25
# penaliza el largo). Es un refuerzo SUAVE, no un filtro: en la Pre-Entrega 4
# el filtro duro por modelo fallaba en preguntas como "el sedán del Polo",
# cuya respuesta está en la ficha del Virtus.
REFUERZO_MODELO_NOMBRADO = 1.5


def buscar_bm25(consulta: str, k: int = TOP_K) -> list[tuple[str, str]]:
    tokens = tokenizar(consulta)
    puntajes = _indice_bm25().get_scores(tokens)
    nombrados = {archivo for archivo, _ in _fragmentos() if Path(archivo).stem in tokens}
    puntajes = [
        p * REFUERZO_MODELO_NOMBRADO if _fragmentos()[i][0] in nombrados else p for i, p in enumerate(puntajes)
    ]
    orden = sorted(range(len(puntajes)), key=lambda i: puntajes[i], reverse=True)
    return [_fragmentos()[i] for i in orden[:k] if puntajes[i] > 0]


def fusionar_rrf(*rankings: list[tuple[str, str]], k: int = TOP_K, c: int = 60) -> list[tuple[str, str]]:
    """Reciprocal Rank Fusion: combina por POSICIÓN, no por puntaje (escalas distintas)."""
    puntos: dict[tuple[str, str], float] = {}
    for ranking in rankings:
        for posicion, item in enumerate(ranking, start=1):
            puntos[item] = puntos.get(item, 0.0) + 1 / (c + posicion)
    return sorted(puntos, key=puntos.get, reverse=True)[:k]


@lru_cache(maxsize=1)
def _vectorstore_pinecone():
    """Conecta al índice de la Pre-Entrega 4 (requiere requirements-pinecone.txt)."""
    from langchain_huggingface import HuggingFaceEmbeddings
    from langchain_pinecone import PineconeVectorStore

    if not os.getenv("PINECONE_API_KEY"):
        raise RuntimeError("BUSQUEDA_BACKEND=pinecone necesita PINECONE_API_KEY en el .env")
    return PineconeVectorStore(
        index_name=os.getenv("INDEX_NAME", "volkswagen-rag-hibrido"),
        namespace=os.getenv("NAMESPACE", "vw-modelos-dev"),
        embedding=HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2"),
        text_key="text",
    )


def buscar_pinecone(consulta: str, k: int = TOP_K) -> list[tuple[str, str]]:
    docs = _vectorstore_pinecone().similarity_search(consulta, k=k)
    return [(d.metadata.get("source", "?"), d.page_content) for d in docs]


class BuscarEnFichasInput(BaseModel):
    consulta: str = Field(description="Qué buscar, incluyendo el modelo, ej: 'Latin NCAP ocupante adulto Tera'")


@tool(args_schema=BuscarEnFichasInput)
async def buscar_en_fichas(consulta: str) -> str:
    """Busca en las fichas técnicas de los modelos Volkswagen (Tera, Polo, Virtus, Nivus,
    Taos, Vento, Saveiro): seguridad y puntajes Latin NCAP, dimensiones, historia, plantas
    de fabricación, colores, rivales. Incluí el nombre del modelo en la consulta. Para
    versiones, potencia, torque o stock usá consultar_catalogo.
    Devuelve los fragmentos más relevantes, cada uno precedido por [Fuente: archivo]."""
    if config.backend_busqueda() == "pinecone":
        resultados = fusionar_rrf(
            await asyncio.to_thread(buscar_bm25, consulta, TOP_K * 2),
            await asyncio.to_thread(buscar_pinecone, consulta, TOP_K * 2),
        )
    else:
        resultados = buscar_bm25(consulta)
    if not resultados:
        return "ERROR: no se encontró nada relevante en las fichas. Probá con otras palabras."
    return "\n\n".join(f"[Fuente: {fuente}]\n{texto}" for fuente, texto in resultados)
