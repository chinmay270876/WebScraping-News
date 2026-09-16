from __future__ import annotations

from datetime import datetime

from news_scraper.models import Article
from news_scraper.newsletter import render_digest


def test_digest_escapes_html():
    articles = [
        Article(
            title="<script>alert(1)</script>",
            description="Body with <img src=x onerror=alert(1)> tag.",
            url="https://example.com/a?q=1&x=2",
            website="timesofindia",
            content_hash="a" * 64,
            published_at=datetime(2026, 9, 16, 9, 0),
        )
    ]
    html = render_digest(articles)
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert "<img" not in html
    assert "&lt;img" in html
    assert "https://example.com/a?q=1&amp;x=2" in html
    assert "timesofindia" in html
