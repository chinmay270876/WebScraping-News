# WebScraping-News

Continuously collect news from a small, explicit list of sites, store the original articles, incrementally index new articles, run semantic search, and generate source-grounded answers.

```text
TOI / Economic Times / Hindustan Times / ChemAnalyst
            ↓
       BFS crawler
            ↓
       news_articles  (SQLite/MySQL — source of truth)
            ↓
   Incremental ingestion
            ↓
       Clean + chunk
            ↓
        Embeddings
            ↓
       ChromaDB
            ↓
       Retrieval
            ↓
        OpenAI LLM
            ↓
    Answer + citations
```

The crawler, extraction rules, and newsletter are unchanged. RAG is an additional layer: original rows stay in `news.db`, and chunk embeddings live in a local Chroma database.

## Project overview

This project:

1. Crawls Times of India, Economic Times, Hindustan Times, and ChemAnalyst.
2. Stores original articles in `news_articles`.
3. Incrementally indexes only new or changed articles.
4. Retrieves relevant chunks with semantic search.
5. Answers questions using only retrieved article context, with real source URLs.

## Architecture

```text
TOI / ET / HT / ChemAnalyst
            ↓
       BFS Crawler
            ↓
       news_articles
            ↓
   Incremental Ingestion
            ↓
       Clean + Chunk
            ↓
        Embeddings
            ↓
       ChromaDB
            ↓
       Retrieval
            ↓
        OpenAI LLM
            ↓
    Answer + Citations
```

Two stores are used on purpose:

- **SQLite/MySQL** holds the original article (`id`, title, body, URL, `content_hash`, source, timestamps).
- **ChromaDB** holds chunks, embeddings, and metadata that point back to that article id.

## Installation

Python 3.10 or newer.

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

macOS / Linux:

```bash
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

The first RAG ingest downloads the local embedding model (`all-MiniLM-L6-v2` by default).

## Environment setup

Copy `.env.example` to `.env` and fill in secrets locally. Never commit `.env`.

SQLite is the default (`news.db` in the project root). To use MySQL, set `DB_ENGINE=mysql`, create the database, and run `WebScraping.sql`.

RAG settings:

| Variable | Purpose |
| --- | --- |
| `RAG_ENABLED` | When `false`, `run` still crawls and sends the newsletter but skips indexing |
| `RAG_VECTOR_DB_PATH` | Local Chroma path (`./data/chroma`) |
| `RAG_COLLECTION_NAME` | Chroma collection name |
| `RAG_EMBEDDING_MODEL` | sentence-transformers model |
| `RAG_CHUNK_SIZE` / `RAG_CHUNK_OVERLAP` | Word-based chunking |
| `RAG_TOP_K` | Chunks retrieved per question |
| `OPENAI_API_KEY` | Required only for `ask` / `chat` |
| `OPENAI_MODEL` | Chat model, default `gpt-4o-mini` |

Crawl, database, and newsletter still work if `OPENAI_API_KEY` is empty.

## Database setup

```bash
python -m news_scraper init-db
```

This creates `news_articles` and the RAG tracking table `rag_ingestion`.

## Scraping

```bash
python -m news_scraper run
```

`run` now does:

```text
crawl all sources
       ↓
update news.db
       ↓
incremental RAG ingestion
       ↓
newsletter
```

If embedding/Chroma fails, the error is logged and the crawl/newsletter still complete.

Other scrape commands:

```bash
python -m news_scraper crawl --source toi --max-articles 10
python -m news_scraper crawl --dry-run --source ht
python -m news_scraper newsletter
python -m news_scraper -v run --source carbon
```

Sources: `timesofindia` (`toi`), `economictimes` (`et`), `hindustantimes` (`ht`), `chemanalyst` (`carbon`).

If email is not configured, the HTML digest is printed instead of sent.

Compatibility wrappers still work:

```bash
python TOI.py
python "Economic Times.py"
python "Hindustan Times.py"
python carbon.py
python "All news.py"
```

## RAG ingestion

```bash
python -m news_scraper ingest
```

Only articles that are missing from the index, previously failed, or whose `content_hash` changed are embedded. Running ingest twice does not duplicate vectors.

## Ask questions

```bash
python -m news_scraper ask "What are the latest developments in carbon black?"
```

Optional filters:

```bash
python -m news_scraper ask "What happened to carbon black prices recently?" --source carbon --days 7 --top-k 5
```

Answers are generated from the local knowledge base only. URLs always come from stored metadata.

## Semantic search (no LLM)

```bash
python -m news_scraper search "carbon black pricing"
```

Useful for checking retrieval without calling OpenAI.

## RAG status

```bash
python -m news_scraper rag-status
```

Example:

```text
RAG Knowledge Base
------------------
Total articles:       182
Indexed articles:     176
Pending articles:       6
Failed articles:        0
Vector chunks:        731
```

## Interactive mode

```bash
python -m news_scraper chat
```

```text
News RAG Assistant
Type 'exit' to quit.

You: What are the latest developments in carbon black?

Assistant:
...
```

## Continuous operation

Schedule the existing `run` command. A scheduler only needs to start:

```bash
python -m news_scraper run
```

That crawls, stores new rows, indexes new articles, and builds the newsletter.

### Windows Task Scheduler

1. Create a Basic Task.
2. Trigger: daily (or every few hours).
3. Action: Start a program.
4. Program: the venv Python, for example `C:\Users\<you>\WebScraping-News\.venv\Scripts\python.exe`.
5. Arguments: `-m news_scraper run`
6. Start in: `C:\Users\<you>\WebScraping-News`

### cron

```cron
0 */6 * * * cd /path/to/WebScraping-News && .venv/bin/python -m news_scraper run
```

## How a crawl works

1. Start from the source homepage (ChemAnalyst starts at the carbon-black pricing page).
2. Honour `robots.txt` for this project's user agent.
3. Fetch with a delay (1.5–3s by default), cap `MAX_PAGES` and `MAX_DEPTH`.
4. Follow in-domain links only; skip binaries and other hosts.
5. Store a page only if the URL looks like an article (or ChemAnalyst news/pricing) **and** extraction finds a real headline and body.
6. ChemAnalyst also requires the word `carbon` in the body.
7. Deduplicate on canonical URL and SHA-256 of the body.

Extraction prefers JSON-LD `NewsArticle`, then `og:title` / article selectors, then `<p>` tags inside `<article>` or `<main>`.

## How incremental ingestion works

```text
news_articles
  → compare ids and content_hash with rag_ingestion
  → skip already indexed articles
  → clean → chunk → embed → upsert into Chroma
  → record status in rag_ingestion
```

Vector ids are deterministic (`article_123_chunk_0`), so reruns overwrite rather than duplicate. If body content changes, old leftover chunks are removed and the article is re-indexed.

Ingestion never calls the LLM. OpenAI is used only for `ask` and `chat`.

## Tests

```bash
pytest
```

Tests use fixture HTML and fake embeddings. They do not hit live news sites or require an OpenAI API key.

## Limits

News sites change markup and often restrict automated access. This project is for personal/educational use against sites you are allowed to collect. If a source returns nothing, check `robots.txt`, try `-v`, and inspect whether article URLs still match the patterns in `news_scraper/sources.py`.

Question answering cannot browse the live web. If an article was not scraped and indexed, the assistant will say there is not enough information.
