from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


class RagConfigError(Exception):
    """Raised when RAG/LLM configuration is missing or invalid."""


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


RAG_ENABLED = _bool("RAG_ENABLED", True)

_vector_path = Path(os.getenv("RAG_VECTOR_DB_PATH", "./data/chroma"))
RAG_VECTOR_DB_PATH = _vector_path if _vector_path.is_absolute() else ROOT / _vector_path

RAG_COLLECTION_NAME = os.getenv("RAG_COLLECTION_NAME", "news_articles").strip() or "news_articles"
RAG_EMBEDDING_MODEL = os.getenv("RAG_EMBEDDING_MODEL", "all-MiniLM-L6-v2").strip() or "all-MiniLM-L6-v2"
RAG_CHUNK_SIZE = _int("RAG_CHUNK_SIZE", 800)
RAG_CHUNK_OVERLAP = _int("RAG_CHUNK_OVERLAP", 100)
RAG_TOP_K = _int("RAG_TOP_K", 5)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "").strip() or "gpt-4o-mini"


def require_openai_key() -> str:
    if not OPENAI_API_KEY:
        raise RagConfigError(
            "OPENAI_API_KEY is not set. Add it to your .env file to use question answering."
        )
    return OPENAI_API_KEY
