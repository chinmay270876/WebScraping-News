from __future__ import annotations

from datetime import date, datetime
from typing import Any

from rag import config


def chunk_text(
    text: str,
    chunk_size: int | None = None,
    overlap: int | None = None,
) -> list[str]:
    """Split text on word boundaries with overlap."""
    size = chunk_size if chunk_size is not None else config.RAG_CHUNK_SIZE
    overlap_size = overlap if overlap is not None else config.RAG_CHUNK_OVERLAP
    if size <= 0:
        raise ValueError("chunk_size must be positive")
    if overlap_size < 0:
        overlap_size = 0
    if overlap_size >= size:
        overlap_size = max(size // 4, 0)

    words = text.split()
    if not words:
        return []
    if len(words) <= size:
        return [text.strip()]

    chunks: list[str] = []
    start = 0
    while start < len(words):
        end = min(start + size, len(words))
        chunks.append(" ".join(words[start:end]))
        if end >= len(words):
            break
        start = end - overlap_size
    return chunks


def chunk_article(
    *,
    article_id: int,
    text: str,
    source: str,
    title: str,
    url: str,
    published_at: datetime | str | None = None,
    chunk_size: int | None = None,
    overlap: int | None = None,
) -> list[dict[str, Any]]:
    published = _format_published(published_at)
    timestamp = to_unix_timestamp(published_at)
    chunks = []
    for index, chunk in enumerate(chunk_text(text, chunk_size, overlap)):
        metadata: dict[str, Any] = {
            "article_id": str(article_id),
            "chunk_index": index,
            "source": source,
            "title": title,
            "url": url,
            "published_at": published,
            "published_date": published[:10] if published else "",
        }
        if timestamp is not None:
            metadata["published_timestamp"] = timestamp
        chunks.append(
            {
                "id": chunk_id(article_id, index),
                "text": chunk,
                "metadata": metadata,
            }
        )
    return chunks


def chunk_id(article_id: int, chunk_index: int) -> str:
    return f"article_{article_id}_chunk_{chunk_index}"


def parse_published_at(value: datetime | date | str | None) -> datetime | None:
    """Return a datetime only when the publication date is valid. Never invent one."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "T" not in text:
        text = text.replace(" ", "T", 1)
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def to_unix_timestamp(value: datetime | date | str | int | float | None) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    parsed = parse_published_at(value)
    if parsed is None:
        return None
    try:
        return int(parsed.timestamp())
    except (OSError, OverflowError, ValueError):
        return None


def _format_published(value: datetime | date | str | None) -> str:
    parsed = parse_published_at(value)
    if parsed is not None:
        return parsed.isoformat()
    if value is None:
        return ""
    return str(value).strip()
