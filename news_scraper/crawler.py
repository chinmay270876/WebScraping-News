from __future__ import annotations

import logging
import random
import time
from collections import deque
from typing import Callable
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests

from news_scraper import config
from news_scraper.db import Database
from news_scraper.extract import collect_links, extract_article, normalize_url, should_store
from news_scraper.models import Article
from news_scraper.sources import Source

logger = logging.getLogger(__name__)

FetchFn = Callable[[str], str | None]


class HttpFetcher:
    def __init__(self, user_agent: str = config.USER_AGENT, timeout: int = 15) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept-Language": "en"})
        self.timeout = timeout
        self.user_agent = user_agent
        self._robots: dict[str, RobotFileParser | None] = {}

    def allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if origin not in self._robots:
            robots_url = urljoin(origin + "/", "robots.txt")
            parser = RobotFileParser()
            try:
                response = self.session.get(robots_url, timeout=self.timeout)
                if response.status_code >= 400:
                    self._robots[origin] = None
                else:
                    parser.parse(response.text.splitlines())
                    self._robots[origin] = parser
            except requests.RequestException:
                self._robots[origin] = None
        parser = self._robots[origin]
        if parser is None:
            return True
        return parser.can_fetch(self.user_agent, url)

    def __call__(self, url: str) -> str | None:
        if not self.allowed(url):
            logger.info("Skipping %s (robots.txt)", url)
            return None
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            content_type = response.headers.get("Content-Type", "").lower()
            path = urlparse(url).path.lower()
            looks_like_page = path.endswith(("/", ".html", ".htm", ".cms")) or not path.rsplit(".", 1)[-1].isalpha()
            if content_type and "html" not in content_type and "text/" not in content_type and not looks_like_page:
                return None
            return response.text
        except requests.RequestException as exc:
            logger.warning("Failed to fetch %s: %s", url, exc)
            return None


def crawl_source(
    source: Source,
    database: Database | None = None,
    *,
    fetch: FetchFn | None = None,
    max_articles: int = config.MAX_ARTICLES,
    max_pages: int = config.MAX_PAGES,
    max_depth: int = config.MAX_DEPTH,
    delay_range: tuple[float, float] | None = (
        config.REQUEST_DELAY_MIN,
        config.REQUEST_DELAY_MAX,
    ),
    dry_run: bool = False,
) -> list[Article]:
    fetcher = fetch or HttpFetcher()
    db = None if dry_run else (database or Database())
    if db is not None:
        db.init()

    queue: deque[tuple[str, int]] = deque(
        (normalize_url(url), 0) for url in source.start_urls
    )
    visited: set[str] = set()
    stored: list[Article] = []
    pages = 0

    while queue and pages < max_pages and len(stored) < max_articles:
        url, depth = queue.popleft()
        if url in visited or depth > max_depth:
            continue
        visited.add(url)

        if delay_range and pages:
            time.sleep(random.uniform(*delay_range))

        html = fetcher(url)
        pages += 1
        if not html:
            continue

        if should_store(url, source):
            article = extract_article(html, url, source)
            if article:
                if db is None or db.store(article):
                    stored.append(article)
                    logger.info("Stored: %s", article.title)
                else:
                    logger.info("Duplicate: %s", article.url)
            else:
                logger.debug("Not an article or keyword miss: %s", url)

        if depth < max_depth:
            for link in collect_links(html, url, source):
                if link not in visited:
                    queue.append((link, depth + 1))

    logger.info(
        "%s: fetched %s pages, kept %s articles",
        source.id,
        pages,
        len(stored),
    )
    return stored
