from __future__ import annotations

import logging
from typing import Sequence

from rag import config

logger = logging.getLogger(__name__)

_SERVICE: EmbeddingService | None = None


class EmbeddingService:
    """Lazy, reusable sentence-transformers embedding wrapper."""

    def __init__(self, model_name: str | None = None, model: object | None = None) -> None:
        self.model_name = model_name or config.RAG_EMBEDDING_MODEL
        self._model = model

    def _load(self):
        if self._model is None:
            logger.info("Loading embedding model %s", self.model_name)
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(self.model_name)
        return self._model

    def embed_text(self, text: str) -> list[float]:
        return self.embed_documents([text])[0]

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        model = self._load()
        vectors = model.encode(
            list(texts),
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return [vector.tolist() for vector in vectors]


def get_embedding_service() -> EmbeddingService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = EmbeddingService()
    return _SERVICE
