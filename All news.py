"""Compatibility entry point. Prefer: python -m news_scraper run"""

from news_scraper.cli import main

if __name__ == "__main__":
    raise SystemExit(main(["run"]))
