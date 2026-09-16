from __future__ import annotations

from datetime import date, datetime
from typing import Any

from rag import config
from rag.embeddings import EmbeddingService, get_embedding_service
from rag.vectorstore import VectorStore, get_vector_store


def retrieve(
    query: str,
    top_k: int | None = None,
    source: str | None = None,
    date_from: str | date | datetime | None = None,
    date_to: str | date | datetime | None = None,
    *,
    vector_store: VectorStore | None = None,
    embedder: EmbeddingService | None = None,
) -> list[dict[str, Any]]:
    store = vector_store or get_vector_store()
    embeddings = embedder or get_embedding_service()
    k = top_k if top_k is not None else config.RAG_TOP_K
    query_vector = embeddings.embed_text(query)
    where = _where_filter(source, date_from, date_to)
    return store.query(query_vector, top_k=k, where=where)


def _where_filter(
    source: str | None,
    date_from: str | date | datetime | None,
    date_to: str | date | datetime | None,
) -> dict[str, Any] | None:
    clauses: list[dict[str, Any]] = []
    if source:
        clauses.append({"source": {"$eq": source}})
    start = _as_date(date_from)
    end = _as_date(date_to)
    if start:
        clauses.append({"published_date": {"$gte": start}})
    if end:
        clauses.append({"published_date": {"$lte": end}})
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return {"$and": clauses}


def _as_date(value: str | date | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text[:10] if text else None
