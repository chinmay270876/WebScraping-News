from __future__ import annotations

import re

from news_scraper.crawler import crawl_source
from news_scraper.sources import Source

HOME = "https://news.example/home"
ARTICLE = "https://news.example/article/markets-open-higher"
OTHER = "https://news.example/sports"
OFFSITE = "https://ads.example/click"

PAGES = {
    HOME: f"""
        <html><body>
          <a href="{ARTICLE}">Markets</a>
          <a href="{OTHER}">Sports</a>
          <a href="{OFFSITE}">Ad</a>
        </body></html>
    """,
    ARTICLE: """
        <html>
          <head>
            <script type="application/ld+json">
            {
              "@type": "NewsArticle",
              "headline": "Markets open higher on rate-hold news",
              "articleBody": "Equity benchmarks rose at the open after the central bank held rates and traders covered short positions in banks and information technology."
            }
            </script>
          </head>
          <body>
            <h1>Markets open higher on rate-hold news</h1>
            <article>
              <p>Equity benchmarks rose at the open after the central bank held rates and traders covered short positions in banks and information technology.</p>
            </article>
          </body>
        </html>
    """,
    OTHER: """
        <html><body><h1>Sports</h1><p>Scores</p></body></html>
    """,
}

SOURCE = Source(
    id="example",
    name="Example News",
    start_urls=(HOME,),
    domain="news.example",
    article_url_re=re.compile(r"/article/"),
    body_selectors=("article",),
)


def test_bfs_stores_only_article_urls_and_stays_on_domain():
    fetched: list[str] = []

    def fetch(url: str) -> str | None:
        fetched.append(url)
        return PAGES[url]

    articles = crawl_source(
        SOURCE,
        fetch=fetch,
        max_articles=5,
        max_pages=10,
        max_depth=2,
        delay_range=None,
        dry_run=True,
    )
    assert [item.title for item in articles] == ["Markets open higher on rate-hold news"]
    assert ARTICLE in fetched
    assert HOME in fetched
    assert OFFSITE not in fetched


def test_max_pages_caps_fetches():
    fetched: list[str] = []

    def fetch(url: str) -> str | None:
        fetched.append(url)
        return PAGES.get(url, "")

    crawl_source(
        SOURCE,
        fetch=fetch,
        max_articles=25,
        max_pages=1,
        max_depth=2,
        delay_range=None,
        dry_run=True,
    )
    assert fetched == [HOME]
