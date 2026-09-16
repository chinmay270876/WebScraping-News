from __future__ import annotations

from news_scraper.extract import extract_article, normalize_url
from news_scraper.sources import get_source

ARTICLE_HTML = """
<html>
  <head>
    <title>Markets rally after rate pause | Times of India</title>
    <meta property="og:title" content="Markets rally after rate pause">
    <script type="application/ld+json">
    {
      "@type": "NewsArticle",
      "headline": "Markets rally after rate pause",
      "datePublished": "2026-09-16T08:00:00+05:30",
      "articleBody": "The benchmark indices closed higher on Wednesday as investors welcomed the central bank decision to hold rates. Bond yields eased and banking stocks led the advance across the session."
    }
    </script>
  </head>
  <body>
    <h1>Markets rally after rate pause</h1>
    <article>
      <p>The benchmark indices closed higher on Wednesday as investors welcomed the central bank decision to hold rates.</p>
      <p>Bond yields eased and banking stocks led the advance across the session while traders booked profits in metals.</p>
    </article>
  </body>
</html>
"""

NAV_HTML = """
<html>
  <head><title>Home | Times of India</title></head>
  <body>
    <h1>Latest news</h1>
    <p>Sports</p>
    <p>Business</p>
  </body>
</html>
"""

XSS_HTML = """
<html>
  <head>
    <script type="application/ld+json">
    {
      "@type": "NewsArticle",
      "headline": "Alert <b>stolen</b> in headline",
      "articleBody": "Investigators said the advisory was a drill and that no customer data was moved off-site during the window under review."
    }
    </script>
  </head>
  <body></body>
</html>
"""

CARBON_HTML = """
<html>
  <body>
    <h1>Carbon Black prices hold steady in Asia</h1>
    <main>
      <p>Carbon black feedstock costs were little changed this week as tyre makers delayed spot purchases in China and India while watching crude moves.</p>
      <p>Suppliers left offers unchanged pending clearer demand from the automotive sector heading into the next quarter.</p>
    </main>
  </body>
</html>
"""

NO_CARBON_HTML = """
<html>
  <body>
    <h1>Polyethylene prices climb on outage news</h1>
    <main>
      <p>Spot polyethylene cargoes were offered higher after a cracker outage reduced supply in the region and converters covered nearby needs.</p>
      <p>Traders said the increase may fade if operating rates recover before month end and inventories remain comfortable.</p>
    </main>
  </body>
</html>
"""


def test_extracts_json_ld_article():
    source = get_source("toi")
    article = extract_article(
        ARTICLE_HTML,
        "https://timesofindia.indiatimes.com/business/markets/articleshow/123.cms",
        source,
    )
    assert article is not None
    assert article.title == "Markets rally after rate pause"
    assert "bond yields eased" in article.description.lower()
    assert article.website == "timesofindia"
    assert article.published_at is not None
    assert len(article.content_hash) == 64


def test_skips_thin_navigation_pages():
    source = get_source("toi")
    article = extract_article(
        NAV_HTML,
        "https://timesofindia.indiatimes.com/",
        source,
    )
    assert article is None


def test_keeps_raw_title_text_for_escaping_later():
    source = get_source("toi")
    article = extract_article(
        XSS_HTML,
        "https://timesofindia.indiatimes.com/india/articleshow/9.cms",
        source,
    )
    assert article is not None
    assert "<b>stolen</b>" in article.title


def test_carbon_keyword_filter():
    source = get_source("carbon")
    kept = extract_article(
        CARBON_HTML,
        "https://www.chemanalyst.com/Pricing-data/carbon-black-42",
        source,
    )
    skipped = extract_article(
        NO_CARBON_HTML,
        "https://www.chemanalyst.com/Pricing-data/polyethylene-1",
        source,
    )
    assert kept is not None
    assert skipped is None


def test_normalize_url_strips_fragment_and_query():
    assert (
        normalize_url("https://TimesOfIndia.indiatimes.com/india/articleshow/1.cms?utm=1#top")
        == "https://timesofindia.indiatimes.com/india/articleshow/1.cms"
    )
