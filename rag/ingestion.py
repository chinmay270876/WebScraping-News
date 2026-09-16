from __future__ import annotations

import logging
from dataclasses import dataclass

from news_scraper.db import Database
from news_scraper.models import Article
from rag import config
from rag.chunker import chunk_article
from rag.cleaner import clean_article
from rag.embeddings import EmbeddingService, get_embedding_service
from rag.vectorstore import VectorStore, get_vector_store

logger = logging.getLogger(__name__)


@dataclass
class IngestStats:
    processed: int = 0
    skipped: int = 0
    failed: int = 0
    chunks: int = 0


def ingest_articles(
    database: Database | None = None,
    vector_store: VectorStore | None = None,
    embedder: EmbeddingService | None = None,
    chunk_size: int | None = None,
    overlap: int | None = None,
) -> IngestStats:
    """Index new or changed articles. Does not call an LLM."""
    logger.info("RAG ingestion started")
    db = database or Database()
    db.init()
    store = vector_store or get_vector_store()
    embeddings = embedder or get_embedding_service()
    size = chunk_size if chunk_size is not None else config.RAG_CHUNK_SIZE
    overlap_size = overlap if overlap is not None else config.RAG_CHUNK_OVERLAP

    articles = db.fetch_all()
    records = db.fetch_rag_records()
    stats = IngestStats()
    pending = [
        article
        for article in articles
        if article.id is not None and _needs_index(article, records.get(article.id))
    ]
    stats.skipped = len(articles) - len(pending)
    logger.info("Found %s new articles", len(pending))

    for article in articles:
        if article.id is None:
            continue
        record = records.get(article.id)
        if not _needs_index(article, record):
            logger.debug("Article %s already indexed, skipping", article.id)

    for article in pending:
        try:
            created = _index_article(
                article,
                db=db,
                store=store,
                embeddings=embeddings,
                chunk_size=size,
                overlap=overlap_size,
            )
            stats.processed += 1
            stats.chunks += created
        except Exception as exc:
            stats.failed += 1
            logger.error("Failed to index article %s: %s", article.id, exc)
            if article.id is not None:
                db.upsert_rag_ingestion(
                    article.id,
                    content_hash=article.content_hash,
                    status="failed",
                    error=str(exc),
                    chunk_count=0,
                )

    logger.info("RAG ingestion completed")
    return stats


def rebuild_articles(
    database: Database | None = None,
    vector_store: VectorStore | None = None,
    embedder: EmbeddingService | None = None,
    chunk_size: int | None = None,
    overlap: int | None = None,
) -> IngestStats:
    """Recreate the vector collection from news_articles. Does not modify original rows."""
    logger.info("RAG rebuild started")
    db = database or Database()
    db.init()
    store = vector_store or get_vector_store()
    db.clear_rag_ingestion()
    store.reset_collection()
    stats = ingest_articles(
        database=db,
        vector_store=store,
        embedder=embedder,
        chunk_size=chunk_size,
        overlap=overlap,
    )
    logger.info("RAG rebuild completed")
    return stats


def _needs_index(article: Article, record: dict | None) -> bool:
    if record is None:
        return True
    if record.get("status") != "indexed":
        return True
    return record.get("content_hash") != article.content_hash


def _index_article(
    article: Article,
    *,
    db: Database,
    store: VectorStore,
    embeddings: EmbeddingService,
    chunk_size: int,
    overlap: int,
) -> int:
    assert article.id is not None
    logger.info("Processing article %s", article.id)
    cleaned = clean_article(article.description)
    if not cleaned:
        raise ValueError("empty article after cleaning")

    chunks = chunk_article(
        article_id=article.id,
        text=cleaned,
        source=article.website,
        title=article.title,
        url=article.url,
        published_at=article.published_at,
        chunk_size=chunk_size,
        overlap=overlap,
    )
    if not chunks:
        raise ValueError("no chunks produced")

    logger.info("Created %s chunks", len(chunks))
    vectors = embeddings.embed_documents([chunk["text"] for chunk in chunks])
    new_ids = [chunk["id"] for chunk in chunks]
    previous_ids = set(store.article_chunk_ids(article.id))
    store.upsert_chunks(
        ids=new_ids,
        documents=[chunk["text"] for chunk in chunks],
        embeddings=vectors,
        metadatas=[chunk["metadata"] for chunk in chunks],
    )
    leftover = previous_ids - set(new_ids)
    if leftover:
        store.delete_ids(sorted(leftover))
    logger.info("Stored %s embeddings", len(chunks))

    db.upsert_rag_ingestion(
        article.id,
        content_hash=article.content_hash,
        status="indexed",
        error=None,
        chunk_count=len(chunks),
    )
    return len(chunks)
