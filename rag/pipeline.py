from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from news_scraper.db import Database
from news_scraper.sources import get_source
from rag import config
from rag.ingestion import IngestStats, ingest_articles
from rag.qa import answer_question
from rag.retriever import retrieve
from rag.vectorstore import get_vector_store

logger = logging.getLogger(__name__)


def ingest() -> IngestStats:
    return ingest_articles()


def status(database: Database | None = None, vector_store=None) -> dict[str, int]:
    db = database or Database()
    db.init()
    counts = db.rag_status_counts()
    store = vector_store if vector_store is not None else get_vector_store()
    try:
        counts["chunks"] = store.count()
    except Exception as exc:
        logger.error("Could not read vector store: %s", exc)
        counts["chunks"] = 0
    return counts


def search(
    query: str,
    *,
    source: str | None = None,
    days: int | None = None,
    top_k: int | None = None,
    **retrieve_kwargs: Any,
) -> list[dict[str, Any]]:
    source_id, date_from, date_to = _filters(source, days)
    limit = top_k if top_k is not None else config.RAG_TOP_K
    hits = retrieve(
        query,
        top_k=max(limit * 4, limit),
        source=source_id,
        date_from=date_from,
        date_to=date_to,
        **retrieve_kwargs,
    )
    return _unique_articles(hits)[:limit]


def ask(
    query: str,
    *,
    source: str | None = None,
    days: int | None = None,
    top_k: int | None = None,
    **qa_kwargs: Any,
) -> str:
    source_id, date_from, date_to = _filters(source, days)
    return answer_question(
        query,
        source=source_id,
        date_from=date_from,
        date_to=date_to,
        top_k=top_k,
        **qa_kwargs,
    )


def chat_loop(
    *,
    source: str | None = None,
    days: int | None = None,
    top_k: int | None = None,
    input_fn=input,
    output_fn=print,
) -> None:
    output_fn("News RAG Assistant")
    output_fn("Type 'exit' to quit.")
    output_fn("")
    while True:
        try:
            question = input_fn("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            output_fn("")
            break
        if question.lower() in {"exit", "quit"}:
            break
        if not question:
            continue
        output_fn("")
        output_fn("Assistant:")
        output_fn(ask(question, source=source, days=days, top_k=top_k))
        output_fn("")


def format_status(counts: dict[str, int]) -> str:
    return "\n".join(
        [
            "RAG Knowledge Base",
            "------------------",
            f"Total articles:     {counts.get('total', 0):5d}",
            f"Indexed articles:   {counts.get('indexed', 0):5d}",
            f"Pending articles:   {counts.get('pending', 0):5d}",
            f"Failed articles:    {counts.get('failed', 0):5d}",
            f"Vector chunks:      {counts.get('chunks', 0):5d}",
        ]
    )


def format_search_results(hits: list[dict[str, Any]]) -> str:
    if not hits:
        return "No matching articles in the local knowledge base."
    lines: list[str] = []
    for index, hit in enumerate(hits, start=1):
        title = hit.get("title") or "Untitled"
        source = hit.get("source") or "unknown"
        published = hit.get("published_at") or "unknown"
        relevance = float(hit.get("relevance") or 0.0)
        url = hit.get("url") or ""
        lines.extend(
            [
                f"{index}. {title}",
                f"   Source: {source}",
                f"   Date: {published}",
                f"   Relevance: {relevance:.3f}",
                f"   {url}" if url else "   URL: (missing)",
                "",
            ]
        )
    return "\n".join(lines).rstrip()


def _filters(source: str | None, days: int | None) -> tuple[str | None, str | None, str | None]:
    source_id = None
    if source:
        source_id = get_source(source).id
    date_from = None
    date_to = None
    if days is not None:
        if days < 0:
            raise ValueError("--days must be zero or positive")
        date_to = datetime.now().date().isoformat()
        date_from = (datetime.now().date() - timedelta(days=days)).isoformat()
    return source_id, date_from, date_to


def _unique_articles(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: list[dict[str, Any]] = []
    seen: set[object] = set()
    for hit in hits:
        key = hit.get("article_id") or hit.get("url") or hit.get("id")
        if key in seen:
            continue
        seen.add(key)
        unique.append(hit)
    return unique
