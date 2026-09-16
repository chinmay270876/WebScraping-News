from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from html import unescape
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from news_scraper.models import Article
from news_scraper.sources import Source, is_article_url

SKIP_SUFFIXES = (
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".svg",
    ".css",
    ".js",
    ".pdf",
    ".zip",
    ".mp4",
    ".mp3",
    ".woff",
    ".woff2",
    ".ico",
)
_WHITESPACE = re.compile(r"\s+")


def normalize_url(url: str) -> str:
    url, _frag = urldefrag(url.strip())
    parsed = urlparse(url)
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    return parsed._replace(scheme=scheme, netloc=netloc, path=path, query="").geturl()


def same_domain(url: str, domain: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host == domain.lower() or host.endswith("." + domain.lower())


def is_skippable(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(suffix) for suffix in SKIP_SUFFIXES)


def collect_links(html: str, base_url: str, source: Source) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    found: list[str] = []
    seen: set[str] = set()
    for tag in soup.find_all("a", href=True):
        href = tag.get("href")
        if not href or href.startswith(("mailto:", "javascript:", "tel:")):
            continue
        absolute = urljoin(base_url, href)
        if not absolute.startswith("http"):
            continue
        if is_skippable(absolute) or not same_domain(absolute, source.domain):
            continue
        canonical = normalize_url(absolute)
        if canonical in seen:
            continue
        seen.add(canonical)
        found.append(canonical)
    return found


def extract_article(html: str, url: str, source: Source) -> Article | None:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        if tag.name == "script" and tag.get("type") == "application/ld+json":
            continue
        tag.decompose()

    ld = _best_ld_article(soup)
    title = _first_text(
        [
            ld.get("headline") if ld else None,
            _meta(soup, "og:title"),
            _heading(soup),
            soup.title.get_text(" ", strip=True) if soup.title else None,
        ],
        as_title=True,
    )
    body = _first_text(
        [
            _ld_body(ld),
            _selected_body(soup, source),
            _main_paragraphs(soup),
        ]
    )
    if not title or not body:
        return None
    if len(title) < 8 or len(body) < 80:
        return None
    if source.keyword and not re.search(
        rf"\b{re.escape(source.keyword)}\b", body, re.I
    ):
        return None

    published = _parse_date(ld.get("datePublished") if ld else None) or _parse_date(
        _meta(soup, "article:published_time")
    )
    canonical = normalize_url(url)
    digest = hashlib.sha256(_WHITESPACE.sub(" ", body).lower().encode("utf-8")).hexdigest()
    return Article(
        title=title[:500],
        description=body,
        url=canonical,
        website=source.id,
        content_hash=digest,
        published_at=published,
    )


def should_store(url: str, source: Source) -> bool:
    return is_article_url(url, source)


def _best_ld_article(soup: BeautifulSoup) -> dict[str, Any]:
    for item in _iter_ld_json(soup):
        types = item.get("@type", "")
        if isinstance(types, list):
            type_names = {str(t).lower() for t in types}
        else:
            type_names = {str(types).lower()}
        if type_names & {"newsarticle", "article", "reportage"}:
            return item
    return {}


def _iter_ld_json(soup: BeautifulSoup) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        items.extend(_flatten_ld(data))
    return items


def _flatten_ld(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        out: list[dict[str, Any]] = []
        for item in data:
            out.extend(_flatten_ld(item))
        return out
    if isinstance(data, dict):
        if "@graph" in data:
            return _flatten_ld(data["@graph"])
        return [data]
    return []


def _selected_body(soup: BeautifulSoup, source: Source) -> str | None:
    for selector in source.body_selectors:
        node = soup.select_one(selector)
        if not isinstance(node, Tag):
            continue
        text = _paragraphs_in(node)
        if text:
            return text
        plain = _clean(node.get_text(" ", strip=True))
        if len(plain) >= 80:
            return plain
    return None


def _main_paragraphs(soup: BeautifulSoup) -> str | None:
    root = soup.find("article") or soup.find("main") or soup.body
    if not isinstance(root, Tag):
        return None
    return _paragraphs_in(root)


def _paragraphs_in(root: Tag) -> str | None:
    chunks: list[str] = []
    for p in root.find_all("p"):
        text = _clean(p.get_text(" ", strip=True))
        if len(text) >= 40:
            chunks.append(text)
    if not chunks:
        return None
    return "\n\n".join(chunks)


def _heading(soup: BeautifulSoup) -> str | None:
    h1 = soup.find("h1")
    if h1:
        return _clean(h1.get_text(" ", strip=True))
    return None


def _meta(soup: BeautifulSoup, property_name: str) -> str | None:
    tag = soup.find("meta", attrs={"property": property_name}) or soup.find(
        "meta", attrs={"name": property_name}
    )
    if tag and tag.get("content"):
        return _clean(str(tag["content"]))
    return None


def _ld_body(ld: dict[str, Any]) -> str | None:
    if not ld:
        return None
    body = ld.get("articleBody") or ld.get("description")
    if isinstance(body, list):
        body = " ".join(str(part) for part in body if part)
    if not body:
        return None
    return str(body)


def _first_text(values: list[Any], as_title: bool = False) -> str | None:
    for value in values:
        if not value:
            continue
        text = _clean_title(str(value)) if as_title else _clean(str(value))
        if text:
            return text
    return None


def _clean(value: str) -> str:
    return unescape(_WHITESPACE.sub(" ", value).strip())


def _clean_title(value: str) -> str:
    text = _clean(value)
    for sep in (" | ", " – ", " — "):
        if sep in text:
            left, right = text.rsplit(sep, 1)
            if len(right) < 40:
                text = left.strip()
    return text


def _parse_date(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None
