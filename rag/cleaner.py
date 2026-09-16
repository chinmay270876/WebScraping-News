from __future__ import annotations

import re
from html import unescape

_TAG = re.compile(r"<[^>]+>", re.S)
_WHITESPACE = re.compile(r"[ \t\f\v]+")
_MANY_NEWLINES = re.compile(r"\n{3,}")


def clean_article(text: str | None) -> str:
    """Normalize article text for RAG without changing news.db rows."""
    if not text:
        return ""

    cleaned = unescape(str(text))
    cleaned = _TAG.sub(" ", cleaned)
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = "\n".join(_WHITESPACE.sub(" ", line).strip() for line in cleaned.split("\n"))
    cleaned = _MANY_NEWLINES.sub("\n\n", cleaned)
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", cleaned) if part.strip()]
    return "\n\n".join(paragraphs)
