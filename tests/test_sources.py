from __future__ import annotations

import re

from news_scraper.sources import get_source, is_article_url


def test_aliases_resolve_to_the_same_source():
    assert get_source("toi").id == "timesofindia"
    assert get_source("et").id == "economictimes"
    assert get_source("ht").id == "hindustantimes"
    assert get_source("carbon").id == "chemanalyst"


def test_article_url_patterns():
    toi = get_source("toi")
    assert is_article_url(
        "https://timesofindia.indiatimes.com/business/india-business/foo/articleshow/12345678.cms",
        toi,
    )
    assert not is_article_url("https://timesofindia.indiatimes.com/business", toi)

    ht = get_source("ht")
    assert is_article_url(
        "https://www.hindustantimes.com/india-news/headline-here-101743000123.html",
        ht,
    )
    assert not is_article_url("https://www.hindustantimes.com/india-news/", ht)


def test_unknown_source_lists_options():
    try:
        get_source("bbc")
    except KeyError as exc:
        assert "timesofindia" in str(exc)
    else:
        raise AssertionError("expected KeyError")


def test_source_domains_are_explicit():
    assert get_source("et").domain == "economictimes.indiatimes.com"
    assert isinstance(get_source("carbon").article_url_re, re.Pattern)
