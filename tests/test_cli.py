from __future__ import annotations

import pytest

from news_scraper.cli import main


def test_help_exits_zero():
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0


def test_help_lists_rag_commands(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    output = capsys.readouterr().out
    assert "ingest" in output
    assert "ask" in output
    assert "rag-status" in output
    assert "search" in output
    assert "chat" in output
    assert "rebuild-rag" in output


def test_verbose_flag_is_accepted_after_subcommand():
    assert main(["init-db", "-v"]) == 0


def test_ask_without_api_key_is_a_clear_error(monkeypatch, capsys):
    monkeypatch.setattr("rag.config.OPENAI_API_KEY", "")
    monkeypatch.setattr(
        "rag.qa.retrieve",
        lambda *args, **kwargs: [
            {
                "text": "Carbon black prices held steady.",
                "title": "Carbon black update",
                "url": "https://www.chemanalyst.com/News/carbon-black",
                "source": "chemanalyst",
                "article_id": 1,
                "published_at": "2026-09-16T10:00:00",
            }
        ],
    )
    assert main(["ask", "What happened to carbon black?"]) == 1
    assert "OPENAI_API_KEY" in capsys.readouterr().err
