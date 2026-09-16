from __future__ import annotations

import logging
import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape

from news_scraper import config
from news_scraper.db import Database
from news_scraper.models import Article

logger = logging.getLogger(__name__)


def fetch_articles(database: Database | None = None, website: str | None = None) -> list[Article]:
    db = database or Database()
    db.init()
    return db.fetch_today(website)


def render_digest(articles: list[Article], website: str | None = None) -> str:
    heading = "Daily News Newsletter"
    if website:
        heading += f" ({escape(website)})"
    parts = [
        "<html><body>",
        f"<h2>{heading} — {escape(str(date.today()))}</h2>",
    ]
    for article in articles:
        snippet = article.description
        if len(snippet) > 400:
            snippet = snippet[:400].rsplit(" ", 1)[0] + "…"
        published = (
            article.published_at.strftime("%Y-%m-%d %H:%M")
            if article.published_at
            else "unknown"
        )
        parts.extend(
            [
                f"<h3>{escape(article.title)}</h3>",
                f"<p>{escape(snippet)}</p>",
                f"<p><a href=\"{escape(article.url, quote=True)}\">Read more</a></p>",
                f"<p>Source: {escape(article.website)} | Published: {escape(published)}</p>",
                "<hr>",
            ]
        )
    parts.append("</body></html>")
    return "".join(parts)


def send_newsletter(
    articles: list[Article],
    to_emails: list[str] | None = None,
    website: str | None = None,
) -> bool:
    if not articles:
        logger.info("No new articles to send.")
        return False

    recipients = to_emails or _recipients()
    body = render_digest(articles, website)
    if not config.EMAIL_USER or not config.EMAIL_PASSWORD or not recipients:
        logger.info("Email is not configured; printing digest instead.")
        print(body)
        return False

    subject = f"Daily News Newsletter - {date.today()}"
    if website:
        subject += f" ({website})"

    message = MIMEMultipart()
    message["From"] = config.EMAIL_USER
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject
    message.attach(MIMEText(body, "html"))

    with smtplib.SMTP_SSL(config.SMTP_HOST, config.SMTP_PORT) as server:
        server.login(config.EMAIL_USER, config.EMAIL_PASSWORD)
        server.sendmail(config.EMAIL_USER, recipients, message.as_string())
    logger.info("Newsletter sent to %s", ", ".join(recipients))
    return True


def _recipients() -> list[str]:
    return [part.strip() for part in config.RECIPIENT_EMAIL.split(",") if part.strip()]
