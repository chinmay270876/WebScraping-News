from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Article:
    title: str
    description: str
    url: str
    website: str
    content_hash: str
    published_at: datetime | None = None
    id: int | None = None
    scraped_at: str | None = None
