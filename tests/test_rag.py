from __future__ import annotations

import hashlib
from datetime import datetime
from types import SimpleNamespace

from news_scraper.db import Database
from news_scraper.models import Article
from rag.chunker import chunk_article, chunk_text
from rag.cleaner import clean_article
from rag.ingestion import ingest_articles
from rag.pipeline import format_status, status
from rag.qa import NO_CONTEXT_MESSAGE, answer_question
from rag.retriever import retrieve
from rag.vectorstore import VectorStore


class FakeEmbedder:
    dim = 8

    def embed_text(self, text: str) -> list[float]:
        return self.embed_documents([text])[0]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        vectors = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            vectors.append([digest[index] / 255.0 for index in range(self.dim)])
        return vectors


def _db(tmp_path, monkeypatch) -> Database:
    db_path = tmp_path / "news.db"
    monkeypatch.setattr("news_scraper.config.SQLITE_PATH", db_path)
    database = Database("sqlite")
    database.init()
    return database


def _article(**overrides) -> Article:
    values = dict(
        title="Carbon black prices hold in Asia",
        description="Carbon black feedstock costs were little changed this week as tyre makers delayed spot purchases in China and India.",
        url="https://www.chemanalyst.com/News/carbon-black-1",
        website="chemanalyst",
        content_hash="a" * 64,
        published_at=datetime(2026, 9, 16, 10, 0),
    )
    values.update(overrides)
    return Article(**values)


def _store_article(database: Database, article: Article) -> Article:
    assert database.store(article) is True
    stored = database.fetch_all()
    assert stored
    return stored[-1]


def test_cleaner_removes_html_and_extra_whitespace():
    raw = "<p>Hello   world</p>\n\n\n<p>Next   line</p>"
    cleaned = clean_article(raw)
    assert "<p>" not in cleaned
    assert "Hello world" in cleaned
    assert "Next line" in cleaned
    assert "\n\n\n" not in cleaned


def test_cleaner_handles_empty_article():
    assert clean_article("") == ""
    assert clean_article(None) == ""
    assert clean_article("   \n\n  ") == ""


def test_chunker_creates_overlapping_chunks():
    words = [f"word{i}" for i in range(50)]
    chunks = chunk_text(" ".join(words), chunk_size=20, overlap=5)
    assert len(chunks) > 1
    first = chunks[0].split()
    second = chunks[1].split()
    assert first[-5:] == second[:5]
    assert all(" " not in token for chunk in chunks for token in chunk.split())


def test_chunker_small_article_stays_one_chunk():
    text = "short carbon black note"
    assert chunk_text(text, chunk_size=800, overlap=100) == [text]


def test_chunker_large_article_splits_without_breaking_words():
    text = " ".join(f"token{i}" for i in range(2000))
    chunks = chunk_text(text, chunk_size=800, overlap=100)
    assert len(chunks) > 1
    assert sum(len(chunk.split()) for chunk in chunks) > 2000
    packed = chunk_article(
        article_id=7,
        text=text,
        source="toi",
        title="Example headline",
        url="https://example.com/article",
        published_at="2026-09-16T10:00:00",
        chunk_size=800,
        overlap=100,
    )
    assert packed[0]["id"] == "article_7_chunk_0"
    assert packed[0]["metadata"]["article_id"] == "7"
    assert packed[0]["metadata"]["source"] == "toi"


def test_incremental_ingestion_skips_and_reindexes(tmp_path, monkeypatch):
    database = _db(tmp_path, monkeypatch)
    store = VectorStore(persist_path=tmp_path / "chroma")
    embedder = FakeEmbedder()
    first = _store_article(database, _article())

    result = ingest_articles(database=database, vector_store=store, embedder=embedder)
    assert result.processed == 1
    assert result.failed == 0
    assert result.chunks >= 1
    first_count = store.count()
    assert first_count == result.chunks

    again = ingest_articles(database=database, vector_store=store, embedder=embedder)
    assert again.processed == 0
    assert again.skipped == 1
    assert store.count() == first_count
    assert len(store.article_chunk_ids(first.id)) == first_count

    with database.connect() as conn:
        conn.execute(
            "UPDATE news_articles SET description = ?, content_hash = ? WHERE id = ?",
            (
                "Updated carbon black prices jumped after a feedstock shortage across tyre plants in India.",
                "b" * 64,
                first.id,
            ),
        )

    updated = ingest_articles(database=database, vector_store=store, embedder=embedder)
    assert updated.processed == 1
    assert store.count() == updated.chunks
    ids = store.article_chunk_ids(first.id)
    assert all(item.startswith(f"article_{first.id}_chunk_") for item in ids)
    documents = store.collection.get(ids=ids)["documents"]
    assert any("jumped after a feedstock shortage" in (doc or "") for doc in documents)

    counts = status(database=database, vector_store=store)
    assert counts["total"] == 1
    assert counts["indexed"] == 1
    assert counts["pending"] == 0
    assert counts["failed"] == 0
    rendered = format_status(counts)
    assert "Indexed articles:" in rendered


def test_new_article_is_indexed_without_duplicating_old_ones(tmp_path, monkeypatch):
    database = _db(tmp_path, monkeypatch)
    store = VectorStore(persist_path=tmp_path / "chroma")
    embedder = FakeEmbedder()
    _store_article(database, _article())
    ingest_articles(database=database, vector_store=store, embedder=embedder)
    _store_article(
        database,
        _article(
            title="Second carbon black note",
            description="A second carbon black report covers freight rates and Asian offers.",
            url="https://www.chemanalyst.com/News/carbon-black-2",
            content_hash="c" * 64,
        ),
    )
    result = ingest_articles(database=database, vector_store=store, embedder=embedder)
    assert result.processed == 1
    assert result.skipped == 1
    assert store.count() >= 2


def test_retriever_returns_article_metadata(tmp_path, monkeypatch):
    database = _db(tmp_path, monkeypatch)
    store = VectorStore(persist_path=tmp_path / "chroma")
    embedder = FakeEmbedder()
    stored = _store_article(database, _article())
    ingest_articles(database=database, vector_store=store, embedder=embedder)
    query = clean_article(stored.description)
    hits = retrieve(
        query,
        top_k=3,
        source="chemanalyst",
        vector_store=store,
        embedder=embedder,
    )
    assert hits
    assert hits[0]["title"] == stored.title
    assert hits[0]["url"] == stored.url
    assert hits[0]["source"] == "chemanalyst"
    assert hits[0]["article_id"] == stored.id


def test_qa_uses_retrieved_context_and_real_urls(tmp_path, monkeypatch):
    hits = [
        {
            "text": "ChemAnalyst said carbon black prices rose in India.",
            "title": "Carbon Black Market Update",
            "url": "https://www.chemanalyst.com/News/carbon-black-1",
            "source": "chemanalyst",
            "article_id": 1,
            "published_at": "2026-09-16T10:00:00",
        }
    ]

    class FakeCompletions:
        def create(self, **kwargs):
            messages = kwargs["messages"]
            assert "ONLY the supplied article context" in messages[0]["content"]
            assert "ChemAnalyst said carbon black prices rose in India." in messages[1]["content"]
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            content=(
                                "Answer\n------\n\n"
                                "Carbon black prices rose in India according to ChemAnalyst.\n\n"
                                "Key points:\n"
                                "• Prices rose in India\n"
                            )
                        )
                    )
                ]
            )

    client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))
    answer = answer_question(
        "What happened to carbon black prices?",
        retrieve_fn=lambda *args, **kwargs: hits,
        llm_client=client,
    )
    assert "Carbon black prices rose in India" in answer
    assert "https://www.chemanalyst.com/News/carbon-black-1" in answer
    assert "ChemAnalyst" in answer


def test_qa_without_hits_does_not_call_llm():
    called = {"llm": False}

    class FakeCompletions:
        def create(self, **kwargs):
            called["llm"] = True
            raise AssertionError("LLM should not be called")

    client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))
    answer = answer_question(
        "What happened?",
        retrieve_fn=lambda *args, **kwargs: [],
        llm_client=client,
    )
    assert answer == NO_CONTEXT_MESSAGE
    assert called["llm"] is False


def test_empty_article_is_marked_failed(tmp_path, monkeypatch):
    database = _db(tmp_path, monkeypatch)
    store = VectorStore(persist_path=tmp_path / "chroma")
    _store_article(
        database,
        _article(
            title="Empty body placeholder headline",
            description="<p>   </p>",
            url="https://www.chemanalyst.com/News/empty",
            content_hash="d" * 64,
        ),
    )
    result = ingest_articles(database=database, vector_store=store, embedder=FakeEmbedder())
    assert result.processed == 0
    assert result.failed == 1
    counts = database.rag_status_counts()
    assert counts["failed"] == 1
    assert counts["indexed"] == 0
