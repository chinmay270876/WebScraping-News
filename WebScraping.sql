CREATE DATABASE IF NOT EXISTS news
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE news;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS rag_ingestion (
    article_id INT PRIMARY KEY,
    content_hash CHAR(64) NULL,
    status VARCHAR(20) NOT NULL,
    indexed_at DATETIME NULL,
    error TEXT NULL,
    chunk_count INT DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
