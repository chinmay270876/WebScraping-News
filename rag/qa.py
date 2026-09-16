from __future__ import annotations

import logging
from typing import Any, Callable

from rag.config import OPENAI_MODEL, RagConfigError, require_openai_key
from rag.retriever import retrieve

logger = logging.getLogger(__name__)

NO_CONTEXT_MESSAGE = (
    "I couldn't find enough relevant information in the indexed news articles "
    "to answer that reliably."
)

SYSTEM_PROMPT = """You are a news research assistant.

Answer the user's question using ONLY the supplied article context.

Do not invent facts.

If the supplied context does not contain enough information, clearly say that the available articles do not provide enough information.

Distinguish between facts reported by different sources.

Do not claim that an article says something unless it is supported by the retrieved context.

When possible, cite the article title and source.

Keep answers concise but informative.

Do not invent URLs, publication dates, or sources.
Do not pretend you searched the internet.
Do not claim the scraper found something that is not in the context.

Format your answer as:

Answer
------

<your answer>

Key points:
• ...
• ...
"""


def answer_question(
    query: str,
    *,
    top_k: int | None = None,
    source: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    retrieve_fn: Callable[..., list[dict[str, Any]]] | None = None,
    llm_client: Any | None = None,
    **retrieve_kwargs: Any,
) -> str:
    hits = (retrieve_fn or retrieve)(
        query,
        top_k=top_k,
        source=source,
        date_from=date_from,
        date_to=date_to,
        **retrieve_kwargs,
    )
    useful = [hit for hit in hits if (hit.get("text") or "").strip()]
    if not useful:
        return NO_CONTEXT_MESSAGE

    completion = _complete(query, useful, llm_client=llm_client)
    return format_answer(completion, useful)


def format_answer(answer_text: str, hits: list[dict[str, Any]]) -> str:
    body = (answer_text or "").strip() or NO_CONTEXT_MESSAGE
    sources = _unique_sources(hits)
    lines = [body, "", "Sources", "-------", ""]
    for index, item in enumerate(sources, start=1):
        title = item["title"] or "Untitled"
        source = item["source"] or "Unknown source"
        url = item["url"]
        lines.append(f"{index}. {source} — {title}")
        if url:
            lines.append(f"   {url}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _complete(query: str, hits: list[dict[str, Any]], llm_client: Any | None = None) -> str:
    client = llm_client or _openai_client()
    user_message = _user_message(query, hits)
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
        )
    except RagConfigError:
        raise
    except Exception as exc:
        logger.error("OpenAI request failed: %s", exc)
        raise RagConfigError(
            "The language model request failed. Check OPENAI_API_KEY, OPENAI_MODEL, and network access."
        ) from exc

    try:
        return (response.choices[0].message.content or "").strip()
    except (AttributeError, IndexError) as exc:
        raise RagConfigError("The language model returned an empty response.") from exc


def _openai_client():
    api_key = require_openai_key()
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RagConfigError("The openai package is not installed. Run: pip install openai") from exc
    return OpenAI(api_key=api_key)


def _user_message(query: str, hits: list[dict[str, Any]]) -> str:
    blocks = [f"Question:\n{query.strip()}", "", "Article context:"]
    for index, hit in enumerate(hits, start=1):
        published = hit.get("published_at") or "unknown"
        blocks.extend(
            [
                "",
                f"[{index}] Title: {hit.get('title') or 'Untitled'}",
                f"Source: {hit.get('source') or 'unknown'}",
                f"URL: {hit.get('url') or ''}",
                f"Published: {published}",
                hit.get("text") or "",
            ]
        )
    return "\n".join(blocks)


def _unique_sources(hits: list[dict[str, Any]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    sources: list[dict[str, str]] = []
    for hit in hits:
        url = (hit.get("url") or "").strip()
        key = url or f"{hit.get('source')}|{hit.get('title')}|{hit.get('article_id')}"
        if key in seen:
            continue
        seen.add(key)
        sources.append(
            {
                "title": str(hit.get("title") or ""),
                "source": _source_label(str(hit.get("source") or "")),
                "url": url,
            }
        )
    return sources


def _source_label(source_id: str) -> str:
    if not source_id:
        return "Unknown source"
    try:
        from news_scraper.sources import get_source

        return get_source(source_id).name
    except Exception:
        return source_id
