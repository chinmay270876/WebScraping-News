"""Compatibility entry point. Prefer: python -m news_scraper crawl --source chemanalyst"""

from news_scraper.cli import main

if __name__ == "__main__":
    raise SystemExit(main(["crawl", "--source", "chemanalyst"]))
