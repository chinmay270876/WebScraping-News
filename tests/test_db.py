from __future__ import annotations

from news_scraper.db import Database
from news_scraper.models import Article


def test_sqlite_dedupes_url_and_hash(tmp_path, monkeypatch):
    db_path = tmp_path / "news.db"
    monkeypatch.setattr("news_scraper.config.SQLITE_PATH", db_path)
    db = Database("sqlite")
    db.init()
    article = Article(
        title="A unique headline about markets today",
        description="Long enough body " * 20,
        url="https://timesofindia.indiatimes.com/business/articleshow/1.cms",
        website="timesofindia",
        content_hash="b" * 64,
    )
    assert db.store(article) is True
    assert db.store(article) is False

    clone = Article(
        title="Different title same body hash",
        description="other",
        url="https://timesofindia.indiatimes.com/business/articleshow/2.cms",
        website="timesofindia",
        content_hash="b" * 64,
    )
    assert db.store(clone) is False

    today = db.fetch_today()
    assert len(today) == 1
    assert today[0].title == article.title

    assert db.fetch_today("economictimes") == []
    assert db.fetch_today("timesofindia")[0].url == article.url
