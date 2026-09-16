from __future__ import annotations

from datetime import date, datetime
from typing import Any

from rag import config
from rag.chunker import to_unix_timestamp
from rag.embeddings import EmbeddingService, get_embedding_service
from rag.vectorstore import VectorStore, get_vector_store


def retrieve(
    query: str,
    top_k: int | None = None,
    source: str | None = None,
    date_from: str | date | datetime | int | float | None = None,
    date_to: str | date | datetime | int | float | None = None,
    *,
    vector_store: VectorStore | None = None,
    embedder: EmbeddingService | None = None,
) -> list[dict[str, Any]]:
    store = vector_store or get_vector_store()
    embeddings = embedder or get_embedding_service()
    k = top_k if top_k is not None else config.RAG_TOP_K
    query_vector = embeddings.embed_text(query)
    where = where_filter(source, date_from, date_to)
    return store.query(query_vector, top_k=k, where=where)


def where_filter(
    source: str | None,
    date_from: str | date | datetime | int | float | None,
    date_to: str | date | datetime | int | float | None,
) -> dict[str, Any] | None:
    clauses: list[dict[str, Any]] = []
    if source:
        clauses.append({"source": {"$eq": source}})
    start = to_unix_timestamp(date_from)
    end = to_unix_timestamp(date_to)
    if start is not None:
        clauses.append({"published_timestamp": {"$gte": start}})
    if end is not None:
        clauses.append({"published_timestamp": {"$lte": end}})
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return {"$and": clauses}
