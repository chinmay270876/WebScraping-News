from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Source:
    id: str
    name: str
    start_urls: tuple[str, ...]
    domain: str
    article_url_re: re.Pattern[str]
    body_selectors: tuple[str, ...] = ()
    keyword: str | None = None
    aliases: tuple[str, ...] = field(default_factory=tuple)


SOURCES: tuple[Source, ...] = (
    Source(
        id="timesofindia",
        name="Times of India",
        start_urls=("https://timesofindia.indiatimes.com/",),
        domain="timesofindia.indiatimes.com",
        article_url_re=re.compile(r"/articleshow/\d+", re.I),
        body_selectors=("div.artText", "div.Normal", "[itemprop='articleBody']", "article"),
        aliases=("toi",),
    ),
    Source(
        id="economictimes",
        name="Economic Times",
        start_urls=("https://economictimes.indiatimes.com/",),
        domain="economictimes.indiatimes.com",
        article_url_re=re.compile(r"/articleshow/\d+", re.I),
        body_selectors=("div.artText", ".article_content", "[itemprop='articleBody']", "article"),
        aliases=("et",),
    ),
    Source(
        id="hindustantimes",
        name="Hindustan Times",
        start_urls=("https://www.hindustantimes.com/",),
        domain="hindustantimes.com",
        article_url_re=re.compile(r"-\d{8,}\.html(?:$|\?)", re.I),
        body_selectors=("div.storyDetails", "div.detail", "[itemprop='articleBody']", "article"),
        aliases=("ht",),
    ),
    Source(
        id="chemanalyst",
        name="ChemAnalyst",
        start_urls=("https://www.chemanalyst.com/Pricing-data/carbon-black-42",),
        domain="chemanalyst.com",
        article_url_re=re.compile(
            r"/(?:NewsAndDeals|NewsDetails|News|Pricing-data)/",
            re.I,
        ),
        body_selectors=("article", "div.news-detail", "div.content", "main"),
        keyword="carbon",
        aliases=("carbon",),
    ),
)

_BY_KEY: dict[str, Source] = {}
for _source in SOURCES:
    _BY_KEY[_source.id] = _source
    for _alias in _source.aliases:
        _BY_KEY[_alias] = _source


def get_source(key: str) -> Source:
    try:
        return _BY_KEY[key.strip().lower()]
    except KeyError as exc:
        known = ", ".join(sorted(_BY_KEY))
        raise KeyError(f"Unknown source {key!r}. Choose one of: {known}") from exc


def is_article_url(url: str, source: Source) -> bool:
    return bool(source.article_url_re.search(url))
