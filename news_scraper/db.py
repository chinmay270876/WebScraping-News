from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator, Sequence

from news_scraper import config
from news_scraper.models import Article

SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS news_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT,
    url TEXT NOT NULL UNIQUE,
    published_at TEXT,
    scraped_at TEXT NOT NULL,
    content_hash TEXT NOT NULL UNIQUE,
    website TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_news_website_scraped
    ON news_articles (website, scraped_at);
CREATE TABLE IF NOT EXISTS rag_ingestion (
    article_id INTEGER PRIMARY KEY,
    content_hash TEXT,
    status TEXT NOT NULL,
    indexed_at TEXT,
    error TEXT,
    chunk_count INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_rag_status ON rag_ingestion (status);
"""

MYSQL_DDL = """
CREATE TABLE IF NOT EXISTS news_articles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    description TEXT,
    url VARCHAR(768) NOT NULL,
    published_at DATETIME NULL,
    scraped_at DATETIME NOT NULL,
    content_hash CHAR(64) NOT NULL,
    website VARCHAR(80) NOT NULL,
    UNIQUE KEY uq_news_url (url),
    UNIQUE KEY uq_news_hash (content_hash),
    KEY idx_news_website_scraped (website, scraped_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

MYSQL_RAG_DDL = """
CREATE TABLE IF NOT EXISTS rag_ingestion (
    article_id INT PRIMARY KEY,
    content_hash CHAR(64) NULL,
    status VARCHAR(20) NOT NULL,
    indexed_at DATETIME NULL,
    error TEXT NULL,
    chunk_count INT DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


class Database:
    def __init__(self, engine: str | None = None) -> None:
        self.engine = (engine or config.DB_ENGINE).lower()
        if self.engine not in {"sqlite", "mysql"}:
            raise ValueError(f"Unsupported DB_ENGINE: {self.engine}")
        self.placeholder = "?" if self.engine == "sqlite" else "%s"

    @contextmanager
    def connect(self) -> Iterator:
        if self.engine == "sqlite":
            conn = sqlite3.connect(config.SQLITE_PATH)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
                conn.commit()
            finally:
                conn.close()
            return

        import mysql.connector

        conn = mysql.connector.connect(**config.MYSQL)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            cursor = conn.cursor()
            if self.engine == "sqlite":
                cursor.executescript(SQLITE_DDL)
            else:
                cursor.execute(MYSQL_DDL)
                cursor.execute(MYSQL_RAG_DDL)
            cursor.close()

    def store(self, article: Article) -> bool:
        now = datetime.now().replace(microsecond=0).isoformat(sep=" ")
        published = (
            article.published_at.replace(microsecond=0).isoformat(sep=" ")
            if article.published_at
            else None
        )
        insert = (
            "INSERT OR IGNORE INTO news_articles"
            if self.engine == "sqlite"
            else "INSERT IGNORE INTO news_articles"
        )
        sql = f"""
            {insert}
                (title, description, url, published_at, scraped_at, content_hash, website)
            VALUES ({", ".join([self.placeholder] * 7)})
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                sql,
                (
                    article.title,
                    article.description,
                    article.url,
                    published,
                    now,
                    article.content_hash,
                    article.website,
                ),
            )
            inserted = cursor.rowcount == 1
            cursor.close()
        return inserted

    def fetch_today(self, website: str | None = None) -> list[Article]:
        if self.engine == "sqlite":
            where = "date(scraped_at) = date('now', 'localtime')"
        else:
            where = "DATE(scraped_at) = CURDATE()"
        params: list[str] = []
        if website:
            where += f" AND website = {self.placeholder}"
            params.append(website)
        sql = f"""
            SELECT title, description, url, website, content_hash, published_at
            FROM news_articles
            WHERE {where}
            ORDER BY scraped_at DESC
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            cursor.close()
        return [_row_to_article(row) for row in rows]

    def fetch_all(self) -> list[Article]:
        columns = (
            "id",
            "title",
            "description",
            "url",
            "website",
            "content_hash",
            "published_at",
            "scraped_at",
        )
        sql = f"""
            SELECT {", ".join(columns)}
            FROM news_articles
            ORDER BY id
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()
        return [_row_to_article(row, columns) for row in rows]

    def fetch_rag_records(self) -> dict[int, dict[str, object]]:
        sql = """
            SELECT article_id, content_hash, status, chunk_count
            FROM rag_ingestion
        """
        records: dict[int, dict[str, object]] = {}
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            cursor.close()
        for row in rows:
            mapping = _mapping(row, ("article_id", "content_hash", "status", "chunk_count"))
            article_id = int(mapping["article_id"])
            records[article_id] = {
                "content_hash": mapping.get("content_hash"),
                "status": mapping.get("status"),
                "chunk_count": mapping.get("chunk_count") or 0,
            }
        return records

    def upsert_rag_ingestion(
        self,
        article_id: int,
        *,
        content_hash: str | None,
        status: str,
        error: str | None = None,
        chunk_count: int = 0,
    ) -> None:
        indexed_at = (
            datetime.now().replace(microsecond=0).isoformat(sep=" ")
            if status == "indexed"
            else None
        )
        values = (article_id, content_hash, status, indexed_at, error, chunk_count)
        if self.engine == "sqlite":
            sql = """
                INSERT INTO rag_ingestion
                    (article_id, content_hash, status, indexed_at, error, chunk_count)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(article_id) DO UPDATE SET
                    content_hash = excluded.content_hash,
                    status = excluded.status,
                    indexed_at = excluded.indexed_at,
                    error = excluded.error,
                    chunk_count = excluded.chunk_count
            """
        else:
            sql = """
                INSERT INTO rag_ingestion
                    (article_id, content_hash, status, indexed_at, error, chunk_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    content_hash = VALUES(content_hash),
                    status = VALUES(status),
                    indexed_at = VALUES(indexed_at),
                    error = VALUES(error),
                    chunk_count = VALUES(chunk_count)
            """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, values)
            cursor.close()

    def rag_status_counts(self) -> dict[str, int]:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM news_articles")
            total = _scalar(cursor)
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM news_articles a
                INNER JOIN rag_ingestion r ON r.article_id = a.id
                WHERE r.status = 'indexed' AND r.content_hash = a.content_hash
                """
            )
            indexed = _scalar(cursor)
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM news_articles a
                INNER JOIN rag_ingestion r ON r.article_id = a.id
                WHERE r.status = 'failed' AND r.content_hash = a.content_hash
                """
            )
            failed = _scalar(cursor)
            cursor.close()
        pending = max(total - indexed - failed, 0)
        return {
            "total": total,
            "indexed": indexed,
            "pending": pending,
            "failed": failed,
        }


def _scalar(cursor) -> int:
    row = cursor.fetchone()
    if row is None:
        return 0
    return int(row[0])


def _mapping(row: Sequence, columns: Sequence[str] | None = None) -> dict:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    if columns is not None:
        return dict(zip(columns, row))
    return {
        "title": row[0],
        "description": row[1],
        "url": row[2],
        "website": row[3],
        "content_hash": row[4],
        "published_at": row[5],
    }


def _row_to_article(row: Sequence, columns: Sequence[str] | None = None) -> Article:
    mapping = _mapping(row, columns)
    published = mapping.get("published_at")
    parsed = None
    if isinstance(published, datetime):
        parsed = published
    elif isinstance(published, str) and published:
        try:
            parsed = datetime.fromisoformat(published)
        except ValueError:
            parsed = None
    scraped = mapping.get("scraped_at")
    if isinstance(scraped, datetime):
        scraped = scraped.replace(microsecond=0).isoformat(sep=" ")
    article_id = mapping.get("id")
    return Article(
        title=mapping["title"],
        description=mapping["description"] or "",
        url=mapping["url"],
        website=mapping["website"],
        content_hash=mapping["content_hash"],
        published_at=parsed,
        id=int(article_id) if article_id is not None else None,
        scraped_at=str(scraped) if scraped else None,
    )
