from __future__ import annotations

import argparse
import logging
import sys

from news_scraper.crawler import crawl_source
from news_scraper.db import Database
from news_scraper.newsletter import fetch_articles, send_newsletter
from news_scraper.sources import SOURCES, get_source


def main(argv: list[str] | None = None) -> int:
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Debug logging",
    )
    parser = argparse.ArgumentParser(
        prog="news_scraper",
        description="Crawl a small set of news sites into one table, then email a digest.",
        parents=[shared],
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", parents=[shared], help="Create the news_articles table")

    crawl = sub.add_parser("crawl", parents=[shared], help="Crawl one source or all sources")
    crawl.add_argument("--source", help="Source id or alias (toi, et, ht, carbon)")
    crawl.add_argument("--max-articles", type=int)
    crawl.add_argument("--max-pages", type=int)
    crawl.add_argument("--max-depth", type=int)
    crawl.add_argument("--dry-run", action="store_true", help="Extract without writing to the database")

    mail = sub.add_parser("newsletter", parents=[shared], help="Email (or print) today's stored articles")
    mail.add_argument("--source", help="Limit the digest to one source id")

    run = sub.add_parser("run", parents=[shared], help="Crawl all sources, then send today's digest")
    run.add_argument("--source", help="Limit the run to one source")
    run.add_argument("--max-articles", type=int)
    run.add_argument("--dry-run", action="store_true")

    sub.add_parser("ingest", parents=[shared], help="Index newly scraped articles into the vector database")
    sub.add_parser("rebuild-rag", parents=[shared], help="Rebuild the vector index from news_articles")
    sub.add_parser("rag-status", parents=[shared], help="Show RAG index coverage")

    ask = sub.add_parser("ask", parents=[shared], help="Answer a question from indexed articles")
    ask.add_argument("question")
    _add_rag_filters(ask)

    search = sub.add_parser("search", parents=[shared], help="Semantic search without calling an LLM")
    search.add_argument("query")
    _add_rag_filters(search)

    chat = sub.add_parser("chat", parents=[shared], help="Interactive RAG assistant")
    _add_rag_filters(chat)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    if not args.verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("huggingface_hub").setLevel(logging.WARNING)
        logging.getLogger("chromadb").setLevel(logging.WARNING)

    if args.command == "init-db":
        Database().init()
        print("Database ready.")
        return 0

    if args.command == "newsletter":
        articles = fetch_articles(website=args.source)
        send_newsletter(articles, website=args.source)
        return 0

    if args.command == "ingest":
        return _command_ingest()

    if args.command == "rebuild-rag":
        return _command_rebuild()

    if args.command == "rag-status":
        return _command_rag_status()

    if args.command == "ask":
        return _command_ask(args)

    if args.command == "search":
        return _command_search(args)

    if args.command == "chat":
        return _command_chat(args)

    sources = [get_source(args.source)] if getattr(args, "source", None) else list(SOURCES)
    kwargs = {}
    if getattr(args, "max_articles", None) is not None:
        kwargs["max_articles"] = args.max_articles
    if getattr(args, "max_pages", None) is not None:
        kwargs["max_pages"] = args.max_pages
    if getattr(args, "max_depth", None) is not None:
        kwargs["max_depth"] = args.max_depth
    kwargs["dry_run"] = bool(getattr(args, "dry_run", False))

    collected = []
    for source in sources:
        logging.info("Crawling %s", source.name)
        collected.extend(crawl_source(source, **kwargs))

    if args.command == "run" and not args.dry_run:
        _ingest_after_crawl()
        website = sources[0].id if getattr(args, "source", None) else None
        send_newsletter(fetch_articles(website=website), website=website)
    elif args.dry_run:
        for article in collected:
            print(f"{article.website}\t{article.title}\t{article.url}")

    return 0


def _add_rag_filters(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source", help="Limit retrieval to one source id or alias")
    parser.add_argument("--days", type=int, help="Only use articles from the last N days")
    parser.add_argument("--top-k", type=int, dest="top_k", help="Number of chunks to retrieve")


def _command_ingest() -> int:
    try:
        from rag.config import RAG_ENABLED
        from rag.pipeline import ingest

        if not RAG_ENABLED:
            logging.info("RAG ingestion skipped because RAG_ENABLED is false")
            print("RAG ingestion is disabled (RAG_ENABLED=false).")
            return 0
        stats = ingest()
        print(
            f"Indexed {stats.processed} articles ({stats.chunks} chunks). "
            f"Skipped {stats.skipped}. Failed {stats.failed}."
        )
        return 0 if stats.failed == 0 else 1
    except Exception:
        logging.exception("RAG ingestion failed")
        print("RAG ingestion failed. See logs for details.", file=sys.stderr)
        return 1


def _command_rebuild() -> int:
    try:
        from rag.config import RAG_ENABLED
        from rag.pipeline import rebuild

        if not RAG_ENABLED:
            logging.info("RAG rebuild skipped because RAG_ENABLED is false")
            print("RAG rebuild is disabled (RAG_ENABLED=false).")
            return 0
        stats = rebuild()
        print(
            f"Rebuilt {stats.processed} articles ({stats.chunks} chunks). "
            f"Failed {stats.failed}."
        )
        return 0 if stats.failed == 0 else 1
    except Exception:
        logging.exception("RAG rebuild failed")
        print("RAG rebuild failed. See logs for details.", file=sys.stderr)
        return 1


def _command_rag_status() -> int:
    try:
        from rag.pipeline import format_status, status

        print(format_status(status()))
        return 0
    except Exception:
        logging.exception("Could not read RAG status")
        print("Could not read RAG status. See logs for details.", file=sys.stderr)
        return 1


def _command_ask(args: argparse.Namespace) -> int:
    from rag.config import RagConfigError
    from rag.pipeline import ask

    try:
        print(ask(args.question, source=args.source, days=args.days, top_k=args.top_k))
        return 0
    except (RagConfigError, KeyError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception:
        logging.exception("Question answering failed")
        print("Question answering failed. See logs for details.", file=sys.stderr)
        return 1


def _command_search(args: argparse.Namespace) -> int:
    from rag.pipeline import format_search_results, search

    try:
        print(format_search_results(search(args.query, source=args.source, days=args.days, top_k=args.top_k)))
        return 0
    except (KeyError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception:
        logging.exception("Search failed")
        print("Search failed. See logs for details.", file=sys.stderr)
        return 1


def _command_chat(args: argparse.Namespace) -> int:
    from rag.config import RagConfigError
    from rag.pipeline import chat_loop

    try:
        chat_loop(source=args.source, days=args.days, top_k=args.top_k)
        return 0
    except (RagConfigError, KeyError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception:
        logging.exception("Chat failed")
        print("Chat failed. See logs for details.", file=sys.stderr)
        return 1


def _ingest_after_crawl() -> None:
    try:
        from rag.config import RAG_ENABLED
        from rag.pipeline import ingest

        if not RAG_ENABLED:
            logging.info("RAG ingestion skipped because RAG_ENABLED is false")
            return
        stats = ingest()
        logging.info(
            "RAG ingest summary: processed=%s skipped=%s failed=%s chunks=%s",
            stats.processed,
            stats.skipped,
            stats.failed,
            stats.chunks,
        )
    except Exception:
        logging.exception(
            "RAG ingestion failed; scrape results were kept and newsletter will continue"
        )


if __name__ == "__main__":
    sys.exit(main())
