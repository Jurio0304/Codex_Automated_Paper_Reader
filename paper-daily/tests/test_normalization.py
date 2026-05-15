import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from fetch_arxiv import normalize_arxiv_abs_html, normalize_arxiv_entry, parse_arxiv_entries_xml, parse_arxiv_recent_ids
from fetch_openreview import normalize_openreview_note


class Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def get(self, key, default=None):
        return getattr(self, key, default)


def test_arxiv_schema_normalization():
    entry = Obj(
        id="https://arxiv.org/abs/2605.12345v1",
        title=" Sensor Forecasting with Sequence Modeling ",
        summary="We forecast sensor signals with temporal representations.",
        published="2026-05-14T00:00:00Z",
        updated="2026-05-14T01:00:00Z",
        authors=[{"name": "Alice"}, {"name": "Bob"}],
        links=[
            {"rel": "alternate", "href": "https://arxiv.org/abs/2605.12345v1"},
            {"type": "application/pdf", "href": "https://arxiv.org/pdf/2605.12345v1"},
        ],
        tags=[{"term": "cs.LG"}, {"term": "q-bio.NC"}],
    )

    paper = normalize_arxiv_entry(entry)

    assert paper["source"] == "arxiv"
    assert paper["id"] == "2605.12345v1"
    assert paper["title"] == "Sensor Forecasting with Sequence Modeling"
    assert paper["authors"] == ["Alice", "Bob"]
    assert paper["pdf_url"].endswith("2605.12345v1")
    assert paper["categories"] == ["cs.LG", "q-bio.NC"]


def test_openreview_schema_normalization():
    note = Obj(
        id="abc123",
        forum="forum123",
        cdate=1778716800000,
        mdate=1778720400000,
        content={
            "title": {"value": "Language Model Assisted Time-Series Forecasting"},
            "authors": {"value": ["Carol", "Dave"]},
            "abstract": {"value": "A method for adaptive sequence forecasting."},
            "keywords": {"value": ["time series", "sequence modeling"]},
        },
    )

    paper = normalize_openreview_note(note, "ICLR")

    assert paper["source"] == "openreview"
    assert paper["id"] == "abc123"
    assert paper["venue"] == "ICLR"
    assert paper["authors"] == ["Carol", "Dave"]
    assert "openreview.net/forum" in paper["url"]
    assert "time series" in paper["categories"]


def test_arxiv_stdlib_xml_parser_supports_real_feed_shape():
    feed = """<?xml version="1.0" encoding="UTF-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
      <entry>
        <id>http://arxiv.org/abs/2605.12345v1</id>
        <updated>2026-05-14T01:00:00Z</updated>
        <published>2026-05-14T00:00:00Z</published>
        <title>Sequence Modeling for Sensor Forecasting</title>
        <summary>We forecast sensor signals.</summary>
        <author><name>Alice</name></author>
        <arxiv:primary_category term="q-bio.NC" />
        <category term="cs.LG" />
        <link href="http://arxiv.org/abs/2605.12345v1" rel="alternate" type="text/html" />
        <link href="http://arxiv.org/pdf/2605.12345v1" rel="related" type="application/pdf" title="pdf" />
      </entry>
    </feed>
    """

    entries = parse_arxiv_entries_xml(feed)
    paper = normalize_arxiv_entry(entries[0])

    assert paper["id"] == "2605.12345v1"
    assert paper["authors"] == ["Alice"]
    assert paper["categories"] == ["q-bio.NC", "cs.LG"]
    assert paper["pdf_url"].endswith("2605.12345v1")


def test_arxiv_html_fallback_parsers_support_recent_and_abs_pages():
    recent_html = """
    <dl id='articles'>
      <dt><a href ="/abs/2605.15188" title="Abstract">arXiv:2605.15188</a></dt>
      <dt><a href="/abs/2605.15188" title="Abstract">duplicate</a></dt>
      <dt><a href="/abs/2605.15183v2" title="Abstract">arXiv:2605.15183</a></dt>
    </dl>
    """
    assert parse_arxiv_recent_ids(recent_html) == ["2605.15188", "2605.15183"]

    abs_html = """
    <meta name="citation_title" content="A Test Paper" />
    <meta name="citation_author" content="Alice" />
    <meta name="citation_author" content="Bob" />
    <meta name="citation_date" content="2026/05/14" />
    <meta name="citation_pdf_url" content="https://arxiv.org/pdf/2605.15188" />
    <meta name="citation_abstract" content="An abstract about neural time series." />
    <td class="tablecell subjects">
      <span class="primary-subject">Machine Learning (cs.LG)</span>; Signal Processing (eess.SP)
    </td>
    """
    paper = normalize_arxiv_abs_html("2605.15188", abs_html)
    assert paper["title"] == "A Test Paper"
    assert paper["authors"] == ["Alice", "Bob"]
    assert paper["categories"] == ["cs.LG", "eess.SP"]
    assert paper["published_at"].startswith("2026-05-14")


def test_openreview_dict_note_normalization_for_http_api_fallback():
    note = {
        "id": "dict123",
        "forum": "forum456",
        "cdate": 1778716800000,
        "mdate": 1778720400000,
        "content": {
            "title": {"value": "Adaptive Sensor Forecasting"},
            "authors": {"value": ["Eve"]},
            "abstract": {"value": "OpenReview HTTP API shape."},
            "keywords": {"value": ["time series", "forecasting"]},
        },
    }

    paper = normalize_openreview_note(note, "TMLR")

    assert paper["id"] == "dict123"
    assert paper["title"] == "Adaptive Sensor Forecasting"
    assert paper["authors"] == ["Eve"]
    assert "time series" in paper["categories"]
