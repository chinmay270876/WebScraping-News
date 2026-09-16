from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Sequence

from rag import config

logger = logging.getLogger(__name__)

os.environ.setdefault("ANONYMIZED_TELEMETRY", "False")
os.environ.setdefault("CHROMA_TELEMETRY_IMPLEMENTATION", "none")

_STORE: VectorStore | None = None


class VectorStore:
    """Persistent Chroma collection with caller-supplied embeddings."""

    def __init__(
        self,
        persist_path: str | Path | None = None,
        collection_name: str | None = None,
    ) -> None:
        self.persist_path = Path(persist_path or config.RAG_VECTOR_DB_PATH)
        self.collection_name = collection_name or config.RAG_COLLECTION_NAME
        self.persist_path.mkdir(parents=True, exist_ok=True)
        import chromadb

        self._client = chromadb.PersistentClient(path=str(self.persist_path))
        try:
            self._collection = self._client.get_or_create_collection(
                name=self.collection_name,
                embedding_function=None,
                metadata={"hnsw:space": "cosine"},
            )
        except Exception:
            self._collection = self._client.get_or_create_collection(
                name=self.collection_name,
                embedding_function=None,
            )

    @property
    def collection(self):
        return self._collection

    def count(self) -> int:
        return int(self._collection.count())

    def reset_collection(self) -> None:
        try:
            self._client.delete_collection(self.collection_name)
        except Exception:
            logger.debug("No existing collection %s to delete", self.collection_name)
        try:
            self._collection = self._client.get_or_create_collection(
                name=self.collection_name,
                embedding_function=None,
                metadata={"hnsw:space": "cosine"},
            )
        except Exception:
            self._collection = self._client.get_or_create_collection(
                name=self.collection_name,
                embedding_function=None,
            )

    def upsert_chunks(
        self,
        *,
        ids: Sequence[str],
        documents: Sequence[str],
        embeddings: Sequence[Sequence[float]],
        metadatas: Sequence[dict[str, Any]],
    ) -> None:
        if not ids:
            return
        self._collection.upsert(
            ids=list(ids),
            documents=list(documents),
            embeddings=list(embeddings),
            metadatas=list(metadatas),
        )

    def delete_article(self, article_id: int) -> None:
        self._collection.delete(where={"article_id": str(article_id)})

    def delete_ids(self, ids: Sequence[str]) -> None:
        if ids:
            self._collection.delete(ids=list(ids))

    def article_chunk_ids(self, article_id: int) -> list[str]:
        result = self._collection.get(where={"article_id": str(article_id)})
        return list(result.get("ids") or [])

    def query(
        self,
        embedding: Sequence[float],
        top_k: int = 5,
        where: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {
            "query_embeddings": [list(embedding)],
            "n_results": max(top_k, 1),
            "include": ["documents", "metadatas", "distances"],
        }
        if where:
            kwargs["where"] = where
        try:
            raw = self._collection.query(**kwargs)
        except Exception as exc:
            logger.error("Vector query failed: %s", exc)
            return []

        ids = (raw.get("ids") or [[]])[0]
        docs = (raw.get("documents") or [[]])[0]
        metas = (raw.get("metadatas") or [[]])[0]
        distances = (raw.get("distances") or [[]])[0]
        hits: list[dict[str, Any]] = []
        for chunk_id, document, metadata, distance in zip(ids, docs, metas, distances):
            meta = metadata or {}
            distance_value = float(distance) if distance is not None else 1.0
            hits.append(
                {
                    "id": chunk_id,
                    "text": document or "",
                    "article_id": _as_int(meta.get("article_id")),
                    "chunk_index": meta.get("chunk_index"),
                    "title": meta.get("title") or "",
                    "url": meta.get("url") or "",
                    "source": meta.get("source") or "",
                    "published_at": meta.get("published_at") or "",
                    "distance": distance_value,
                    "relevance": max(0.0, 1.0 - distance_value),
                }
            )
        return hits


def get_vector_store() -> VectorStore:
    global _STORE
    if _STORE is None:
        _STORE = VectorStore()
    return _STORE


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
