from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


DB_ENGINE = os.getenv("DB_ENGINE", "sqlite").strip().lower()
SQLITE_PATH = Path(os.getenv("SQLITE_PATH", str(ROOT / "news.db")))
if not SQLITE_PATH.is_absolute():
    SQLITE_PATH = ROOT / SQLITE_PATH

MYSQL = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": _int("DB_PORT", 3306),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "news"),
}

MAX_ARTICLES = _int("MAX_ARTICLES", 25)
MAX_PAGES = _int("MAX_PAGES", 40)
MAX_DEPTH = _int("MAX_DEPTH", 2)
REQUEST_DELAY_MIN = _float("REQUEST_DELAY_MIN", 1.5)
REQUEST_DELAY_MAX = _float("REQUEST_DELAY_MAX", 3.0)

USER_AGENT = os.getenv(
    "USER_AGENT",
    "NewsDigest/1.0 (+https://github.com/chinmay270876/WebScraping-News)",
)

EMAIL_USER = os.getenv("EMAIL_USER", "").strip()
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL", "").strip()
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = _int("SMTP_PORT", 465)
