"""OpenReview retrieval and normalization."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request

from utils import http_open, isoformat_or_empty, normalize_whitespace, parse_datetime, validate_paper_schema


OPENREVIEW_FORUM_URL = "https://openreview.net/forum?id={paper_id}"
OPENREVIEW_PDF_URL = "https://openreview.net/pdf?id={paper_id}"


def fetch_openreview(
    source_config: dict[str, Any],
    target_date: date,
    lookback_days: int,
    logger,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Fetch OpenReview notes from configured venues with best-effort fallbacks."""

    warnings: list[str] = []
    if not source_config.get("enabled", True):
        return [], warnings

    client = None
    try:
        import openreview

        client = _make_client(openreview, warnings)
    except ImportError:
        logger.warning("openreview-py is not installed; using OpenReview HTTP API fallback")

    if client is None and warnings:
        for warning in warnings:
            logger.warning(warning)

    venues = source_config.get("venues", [])
    max_results = int(source_config.get("max_results", 100))
    start_dt, end_dt = lookback_window(target_date, lookback_days)
    all_papers: list[dict[str, Any]] = []

    for venue in venues:
        venue_papers: list[dict[str, Any]] = []
        venue_errors: list[str] = []
        for invitation in candidate_venue_ids(str(venue), target_date.year):
            try:
                if client is not None:
                    try:
                        notes = _get_notes(client, invitation=invitation, limit=max_results)
                    except Exception as exc_client:
                        # Fall back to the no-proxy HTTP path when openreview-py
                        # inherits a broken proxy environment.
                        try:
                            notes = _get_notes_http(invitation=invitation, limit=max_results)
                        except Exception as exc_http:
                            raise RuntimeError(f"client={exc_client}; http={exc_http}") from exc_http
                else:
                    notes = _get_notes_http(invitation=invitation, limit=max_results)
            except Exception as exc:
                venue_errors.append(f"{invitation}: {exc}")
                continue

            if not notes:
                continue

            for note in notes[:max_results]:
                paper = normalize_openreview_note(note, str(venue))
                paper_dt = parse_datetime(paper.get("published_at")) or parse_datetime(paper.get("updated_at"))
                # Keep undated notes because some OpenReview venues expose sparse public metadata.
                if paper_dt and not (start_dt <= paper_dt <= end_dt):
                    continue
                venue_papers.append(paper)
            break

        if venue_errors and not venue_papers:
            warning = f"OpenReview venue {venue} fetch failed or returned no notes. Tried: {'; '.join(venue_errors[:3])}"
            warnings.append(warning)
            logger.warning(warning)
        else:
            logger.info("Fetched %s OpenReview papers for %s", len(venue_papers), venue)
        all_papers.extend(venue_papers)

    return all_papers, warnings


def _make_client(openreview_module: Any, warnings: list[str]) -> Any | None:
    """Create an OpenReview client compatible with old and new API packages."""

    try:
        return openreview_module.api.OpenReviewClient(baseurl="https://api2.openreview.net")
    except Exception as exc_api2:
        try:
            return openreview_module.Client(baseurl="https://api.openreview.net")
        except Exception as exc_api1:
            warnings.append(f"OpenReview client initialization failed: api2={exc_api2}; api1={exc_api1}")
            return None


def _get_notes(client: Any, invitation: str, limit: int) -> list[Any]:
    """Call get_notes while tolerating client signature differences."""

    try:
        return list(client.get_notes(invitation=invitation, limit=limit))
    except TypeError:
        return list(client.get_notes(invitation=invitation))


def _get_notes_http(invitation: str, limit: int) -> list[dict[str, Any]]:
    """Query OpenReview notes through public HTTP APIs without openreview-py."""

    errors = []
    for base_url in ("https://api2.openreview.net/notes", "https://api.openreview.net/notes"):
        params = urlencode({"invitation": invitation, "limit": limit})
        url = f"{base_url}?{params}"
        try:
            request = Request(url, headers={"User-Agent": "paper-daily-mvp/0.1"})
            with http_open(request, timeout=30) as response:
                payload = response.read().decode("utf-8", errors="replace")
            import json

            data = json.loads(payload)
            return list(data.get("notes", []))
        except Exception as exc:
            errors.append(f"{base_url}: {exc}")
    raise RuntimeError("; ".join(errors))


def candidate_venue_ids(venue_name: str, target_year: int) -> list[str]:
    """Return likely OpenReview invitation IDs for common ML venues."""

    canonical = venue_name.strip().lower()
    years = [target_year, target_year - 1]
    if canonical == "tmlr":
        return [
            "TMLR/-/Submission",
            "TMLR/Submission",
            "TMLR/-/Paper",
            "TMLR/-/Accepted_Certification",
        ]

    venue_prefix = {
        "iclr": "ICLR.cc",
        "neurips": "NeurIPS.cc",
        "nips": "NeurIPS.cc",
        "icml": "ICML.cc",
        "colm": "colmweb.org",
    }.get(canonical, venue_name)

    candidates: list[str] = []
    for year in years:
        candidates.extend(
            [
                f"{venue_prefix}/{year}/Conference/-/Submission",
                f"{venue_prefix}/{year}/Conference/Submission",
                f"{venue_prefix}/{year}/Conference/-/Paper",
                f"{venue_prefix}/{year}/Conference/Paper",
            ]
        )
    return candidates


def normalize_openreview_note(note: Any, venue_name: str = "") -> dict[str, Any]:
    """Normalize an OpenReview note into the project paper schema."""

    content = _note_field(note, "content", {}) or {}
    note_id = str(_note_field(note, "id", "") or "")
    forum_id = str(_note_field(note, "forum", "") or note_id)

    title = extract_openreview_field(content, "title")
    authors = extract_openreview_field(content, "authors", default=[])
    if isinstance(authors, str):
        authors = [authors]
    abstract = extract_openreview_field(content, "abstract")
    venue = (
        extract_openreview_field(content, "venue")
        or extract_openreview_field(content, "venueid")
        or venue_name
    )
    keywords = extract_openreview_field(content, "keywords", default=[])
    subject_areas = extract_openreview_field(content, "subject_areas", default=[])
    categories = _as_list(keywords) + _as_list(subject_areas)

    pdf_url = extract_openreview_field(content, "pdf")
    if pdf_url and str(pdf_url).startswith("/"):
        pdf_url = "https://openreview.net" + str(pdf_url)
    if not pdf_url and forum_id:
        pdf_url = OPENREVIEW_PDF_URL.format(paper_id=forum_id)

    cdate = _note_field(note, "pdate", None) or _note_field(note, "cdate", None)
    mdate = _note_field(note, "mdate", None) or _note_field(note, "tcdate", None)

    paper = {
        "id": note_id,
        "source": "openreview",
        "title": title,
        "authors": authors,
        "abstract": abstract,
        "url": OPENREVIEW_FORUM_URL.format(paper_id=forum_id) if forum_id else "",
        "pdf_url": pdf_url,
        "published_at": isoformat_or_empty(cdate),
        "updated_at": isoformat_or_empty(mdate),
        "venue": venue,
        "categories": categories,
    }
    return validate_paper_schema(paper)


def extract_openreview_field(content: dict[str, Any], key: str, default: Any = "") -> Any:
    """Extract a field from OpenReview v1/v2 content dictionaries."""

    if not isinstance(content, dict):
        return default
    value = content.get(key, default)
    if isinstance(value, dict) and "value" in value:
        return value.get("value", default)
    return value


def _note_field(note: Any, key: str, default: Any = None) -> Any:
    if isinstance(note, dict):
        return note.get(key, default)
    return getattr(note, key, default)


def _as_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [normalize_whitespace(str(item)) for item in value if normalize_whitespace(str(item))]
    return [normalize_whitespace(str(value))]


def lookback_window(target_date: date, lookback_days: int) -> tuple[datetime, datetime]:
    days = max(1, int(lookback_days))
    start_date = target_date - timedelta(days=days - 1)
    start_dt = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(target_date, time.max, tzinfo=timezone.utc)
    return start_dt, end_dt
