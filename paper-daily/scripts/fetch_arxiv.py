"""arXiv retrieval and normalization."""

from __future__ import annotations

import time as time_module
import xml.etree.ElementTree as ET
import re
from datetime import date, datetime, time, timedelta, timezone
from html import unescape
from typing import Any
from urllib.parse import urlencode

from utils import isoformat_or_empty, normalize_whitespace, parse_datetime, requests_get, validate_paper_schema


ARXIV_API_URL = "https://export.arxiv.org/api/query"
ARXIV_HTML_LIST_URL = "https://arxiv.org/list/{category}/recent?show={show}"
ARXIV_ABS_URL = "https://arxiv.org/abs/{paper_id}"


def fetch_arxiv(
    source_config: dict[str, Any],
    research_profile: dict[str, Any],
    target_date: date,
    lookback_days: int,
    logger,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Fetch recent arXiv papers. Network errors are returned as warnings."""

    warnings: list[str] = []
    if not source_config.get("enabled", True):
        return [], warnings

    max_results = int(source_config.get("max_results", 100))
    logger.info("Fetching arXiv papers, max_results=%s", max_results)

    papers: list[dict[str, Any]] = []
    keyword_chunks = chunk_keywords(research_profile.get("positive_keywords", []), chunk_size=12)
    api_failure_count = 0
    max_api_failures = int(source_config.get("max_api_failures_before_html", 1))
    for chunk in keyword_chunks:
        query = build_arxiv_query(
            research_profile.get("arxiv_categories", []),
            chunk,
            target_date,
            lookback_days,
        )
        try:
            chunk_papers = fetch_arxiv_query(
                query,
                max_results,
                target_date,
                lookback_days,
                retries=int(source_config.get("retries", 2)),
                retry_after_seconds=int(source_config.get("retry_after_seconds", 60)),
            )
            papers.extend(chunk_papers)
        except Exception as exc:
            api_failure_count += 1
            warning = f"arXiv keyword chunk fetch failed: {exc}"
            warnings.append(warning)
            logger.warning(warning)
            # 429 usually means the API is throttling us; stop chunking and try one
            # short fallback query instead of hammering the endpoint.
            if "429" in str(exc) or (
                source_config.get("html_fallback_enabled", True) and api_failure_count >= max_api_failures
            ):
                break
        if len(papers) >= max_results:
            break
        time_module.sleep(3)

    if not papers:
        # Topic-specific matches can be sparse; fall back to a broader
        # category/date query so the rule ranker can inspect recent papers.
        fallback_query = build_arxiv_query(
            research_profile.get("arxiv_categories", []),
            [],
            target_date,
            lookback_days,
        )
        logger.info("arXiv keyword query returned 0 papers; retrying category/date-only query")
        try:
            papers = fetch_arxiv_query(
                fallback_query,
                max_results,
                target_date,
                lookback_days,
                retries=int(source_config.get("retries", 2)),
                retry_after_seconds=int(source_config.get("retry_after_seconds", 60)),
            )
        except Exception as exc:
            warning = f"arXiv fallback query failed: {exc}"
            warnings.append(warning)
            logger.warning(warning)

    if not papers and source_config.get("html_fallback_enabled", True):
        logger.info("arXiv API returned 0 papers; trying arxiv.org HTML recent-list fallback")
        html_papers, html_warnings = fetch_arxiv_html_recent(
            source_config,
            research_profile.get("arxiv_categories", []),
            target_date,
            lookback_days,
            max_results,
            logger,
        )
        papers.extend(html_papers)
        warnings.extend(html_warnings)

    papers = _dedupe_arxiv_results(papers)[:max_results]
    logger.info("Fetched %s arXiv papers after date filtering", len(papers))
    return papers, warnings


def chunk_keywords(keywords: list[str], chunk_size: int = 12) -> list[list[str]]:
    clean = [normalize_whitespace(keyword) for keyword in keywords if normalize_whitespace(keyword)]
    if not clean:
        return [[]]
    return [clean[idx : idx + chunk_size] for idx in range(0, len(clean), chunk_size)]


def _dedupe_arxiv_results(papers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for paper in papers:
        key = str(paper.get("id") or paper.get("url") or paper.get("title"))
        if key in seen:
            continue
        seen.add(key)
        result.append(paper)
    return result


def fetch_arxiv_query(
    query: str,
    max_results: int,
    target_date: date,
    lookback_days: int,
    retries: int = 2,
    retry_after_seconds: int = 60,
) -> list[dict[str, Any]]:
    params = {
        "search_query": query,
        "start": 0,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    url = f"{ARXIV_API_URL}?{urlencode(params)}"
    feed_text = http_get_text(url, retries=retries, retry_after_seconds=retry_after_seconds)
    start_dt, end_dt = lookback_window(target_date, lookback_days)
    papers: list[dict[str, Any]] = []

    for entry in parse_arxiv_entries(feed_text):
        paper = normalize_arxiv_entry(entry)
        paper_dt = parse_datetime(paper.get("published_at")) or parse_datetime(paper.get("updated_at"))
        if paper_dt and not (start_dt <= paper_dt <= end_dt):
            continue
        papers.append(paper)
    return papers


def http_get_text(url: str, retries: int = 2, retry_after_seconds: int = 60) -> str:
    """Fetch URL text while bypassing broken env proxies by default."""

    last_response = None
    for attempt in range(max(1, retries + 1)):
        response = requests_get(url, timeout=30)
        last_response = response
        if response.status_code != 429 or attempt >= retries:
            response.raise_for_status()
            return response.text

        retry_after = parse_retry_after(response.headers.get("Retry-After"), retry_after_seconds)
        time_module.sleep(retry_after)

    assert last_response is not None
    last_response.raise_for_status()
    return last_response.text


def parse_retry_after(value: str | None, default_seconds: int) -> int:
    if value and value.isdigit():
        return max(1, int(value))
    return max(1, int(default_seconds))


def fetch_arxiv_html_recent(
    source_config: dict[str, Any],
    categories: list[str],
    target_date: date,
    lookback_days: int,
    max_results: int,
    logger,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Fetch recent arXiv metadata through arxiv.org HTML when export API is throttled."""

    warnings: list[str] = []
    per_category = normalize_arxiv_show_count(int(source_config.get("html_fallback_per_category", 50)))
    total_limit = min(max_results, int(source_config.get("html_fallback_max_results", max_results)))
    sleep_seconds = float(source_config.get("html_fallback_sleep_seconds", 0.25))
    start_dt, end_dt = lookback_window(target_date, lookback_days)

    paper_ids: list[str] = []
    seen_ids: set[str] = set()
    for category in categories:
        if len(paper_ids) >= total_limit:
            break
        url = ARXIV_HTML_LIST_URL.format(category=category, show=per_category)
        try:
            html = http_get_text(url, retries=0)
            ids = parse_arxiv_recent_ids(html)
        except Exception as exc:
            warning = f"arXiv HTML list fetch failed for {category}: {exc}"
            warnings.append(warning)
            logger.warning(warning)
            continue
        for paper_id in ids:
            if paper_id not in seen_ids:
                seen_ids.add(paper_id)
                paper_ids.append(paper_id)
            if len(paper_ids) >= total_limit:
                break

    papers: list[dict[str, Any]] = []
    for paper_id in paper_ids:
        try:
            html = http_get_text(ARXIV_ABS_URL.format(paper_id=paper_id), retries=0)
            paper = normalize_arxiv_abs_html(paper_id, html)
        except Exception as exc:
            warning = f"arXiv HTML abs fetch failed for {paper_id}: {exc}"
            warnings.append(warning)
            logger.warning(warning)
            continue

        paper_dt = parse_datetime(paper.get("published_at")) or parse_datetime(paper.get("updated_at"))
        if paper_dt and not (start_dt <= paper_dt <= end_dt):
            continue
        papers.append(paper)
        if len(papers) >= total_limit:
            break
        if sleep_seconds > 0:
            time_module.sleep(sleep_seconds)

    logger.info("Fetched %s arXiv papers through HTML fallback", len(papers))
    return papers, warnings


def parse_arxiv_recent_ids(html: str) -> list[str]:
    ids = re.findall(r"href\s*=\s*['\"]/abs/(\d{4}\.\d{4,5})(?:v\d+)?['\"]", html)
    return unique_preserve_order_local(ids)


def normalize_arxiv_show_count(value: int) -> int:
    """arxiv.org recent-list pages accept a small set of show counts."""

    for allowed in (25, 50, 100, 250, 500, 1000, 2000):
        if value <= allowed:
            return allowed
    return 2000


def normalize_arxiv_abs_html(paper_id: str, html: str) -> dict[str, Any]:
    title = meta_content(html, "citation_title") or parse_html_title(html)
    authors = meta_contents(html, "citation_author")
    abstract = meta_content(html, "citation_abstract") or parse_html_abstract(html)
    citation_date = meta_content(html, "citation_date")
    published_at = isoformat_or_empty(citation_date.replace("/", "-") if citation_date else "")
    pdf_url = meta_content(html, "citation_pdf_url") or f"https://arxiv.org/pdf/{paper_id}"
    categories = parse_arxiv_html_categories(html)

    paper = {
        "id": paper_id,
        "source": "arxiv",
        "title": title,
        "authors": authors,
        "abstract": abstract,
        "url": ARXIV_ABS_URL.format(paper_id=paper_id),
        "pdf_url": pdf_url,
        "published_at": published_at,
        "updated_at": published_at,
        "venue": categories[0] if categories else "arXiv",
        "categories": categories,
    }
    return validate_paper_schema(paper)


def meta_content(html: str, name: str) -> str:
    values = meta_contents(html, name)
    return values[0] if values else ""


def meta_contents(html: str, name: str) -> list[str]:
    pattern = rf"<meta\s+name=['\"]{re.escape(name)}['\"]\s+content=['\"](.*?)['\"]\s*/?>"
    return [normalize_whitespace(unescape(value)) for value in re.findall(pattern, html, flags=re.S)]


def parse_html_title(html: str) -> str:
    match = re.search(r"<h1[^>]*class=['\"]title[^>]*>\s*<span[^>]*>Title:</span>(.*?)</h1>", html, re.S)
    return normalize_whitespace(strip_tags(match.group(1))) if match else ""


def parse_html_abstract(html: str) -> str:
    match = re.search(
        r"<blockquote[^>]*class=['\"]abstract[^>]*>\s*<span[^>]*>Abstract:</span>(.*?)</blockquote>",
        html,
        re.S,
    )
    return normalize_whitespace(strip_tags(match.group(1))) if match else ""


def parse_arxiv_html_categories(html: str) -> list[str]:
    match = re.search(r"<td[^>]*class=['\"]tablecell subjects['\"][^>]*>(.*?)</td>", html, re.S)
    if not match:
        return []
    return unique_preserve_order_local(re.findall(r"\(([a-z-]+(?:\.[A-Z]+)+)\)", match.group(1)))


def strip_tags(html: str) -> str:
    return unescape(re.sub(r"<[^>]+>", " ", html))


def unique_preserve_order_local(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        value = normalize_whitespace(str(value))
        key = value.lower()
        if value and key not in seen:
            seen.add(key)
            result.append(value)
    return result


def parse_arxiv_entries(feed_text: str) -> list[Any]:
    """Parse arXiv Atom entries with feedparser if present, else stdlib XML."""

    try:
        import feedparser

        feed = feedparser.parse(feed_text)
        return list(feed.entries)
    except ImportError:
        return parse_arxiv_entries_xml(feed_text)


def parse_arxiv_entries_xml(feed_text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(feed_text)
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "arxiv": "http://arxiv.org/schemas/atom",
    }
    entries: list[dict[str, Any]] = []
    for elem in root.findall("atom:entry", ns):
        links = []
        for link in elem.findall("atom:link", ns):
            links.append(dict(link.attrib))
        authors = []
        for author in elem.findall("atom:author", ns):
            name = author.findtext("atom:name", default="", namespaces=ns)
            if name:
                authors.append({"name": name})
        tags = []
        primary_category = elem.find("arxiv:primary_category", ns)
        if primary_category is not None and primary_category.attrib.get("term"):
            tags.append({"term": primary_category.attrib["term"]})
        for category in elem.findall("atom:category", ns):
            term = category.attrib.get("term")
            if term and {"term": term} not in tags:
                tags.append({"term": term})
        entries.append(
            {
                "id": elem.findtext("atom:id", default="", namespaces=ns),
                "title": elem.findtext("atom:title", default="", namespaces=ns),
                "summary": elem.findtext("atom:summary", default="", namespaces=ns),
                "published": elem.findtext("atom:published", default="", namespaces=ns),
                "updated": elem.findtext("atom:updated", default="", namespaces=ns),
                "authors": authors,
                "links": links,
                "tags": tags,
            }
        )
    return entries


def build_arxiv_query(
    categories: list[str],
    keywords: list[str],
    target_date: date,
    lookback_days: int,
) -> str:
    """Build an arXiv API query with category, keyword, and date filters."""

    category_query = " OR ".join(f"cat:{category}" for category in categories)
    keyword_terms = []
    for keyword in keywords:
        keyword = normalize_whitespace(keyword)
        if not keyword:
            continue
        if " " in keyword:
            keyword_terms.append(f'all:"{keyword}"')
        else:
            keyword_terms.append(f"all:{keyword}")
    keyword_query = " OR ".join(keyword_terms)

    start_dt, end_dt = lookback_window(target_date, lookback_days)
    date_query = (
        f"submittedDate:[{start_dt.strftime('%Y%m%d%H%M')} "
        f"TO {end_dt.strftime('%Y%m%d%H%M')}]"
    )

    parts = []
    if category_query:
        parts.append(f"({category_query})")
    if keyword_query:
        parts.append(f"({keyword_query})")
    parts.append(date_query)
    return " AND ".join(parts)


def lookback_window(target_date: date, lookback_days: int) -> tuple[datetime, datetime]:
    days = max(1, int(lookback_days))
    start_date = target_date - timedelta(days=days - 1)
    start_dt = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(target_date, time.max, tzinfo=timezone.utc)
    return start_dt, end_dt


def normalize_arxiv_entry(entry: Any) -> dict[str, Any]:
    """Normalize a feedparser arXiv entry into the project paper schema."""

    entry_id = str(getattr(entry, "id", "") or entry.get("id", ""))
    arxiv_id = entry_id.rstrip("/").rsplit("/", 1)[-1] if entry_id else ""
    links = getattr(entry, "links", None) or entry.get("links", [])
    pdf_url = ""
    abs_url = entry_id
    for link in links:
        href = _get_field(link, "href", "")
        rel = _get_field(link, "rel", "")
        link_type = _get_field(link, "type", "")
        title = _get_field(link, "title", "")
        if rel == "alternate" and href:
            abs_url = href
        if href and (link_type == "application/pdf" or title == "pdf" or "/pdf/" in href):
            pdf_url = href

    authors_raw = getattr(entry, "authors", None) or entry.get("authors", [])
    authors = [_get_field(author, "name", "") for author in authors_raw]
    tags = getattr(entry, "tags", None) or entry.get("tags", [])
    categories = [_get_field(tag, "term", "") for tag in tags]

    paper = {
        "id": arxiv_id,
        "source": "arxiv",
        "title": getattr(entry, "title", None) or entry.get("title", ""),
        "authors": authors,
        "abstract": getattr(entry, "summary", None) or entry.get("summary", ""),
        "url": abs_url,
        "pdf_url": pdf_url,
        "published_at": isoformat_or_empty(getattr(entry, "published", None) or entry.get("published", "")),
        "updated_at": isoformat_or_empty(getattr(entry, "updated", None) or entry.get("updated", "")),
        "venue": categories[0] if categories else "arXiv",
        "categories": categories,
    }
    return validate_paper_schema(paper)


def _get_field(obj: Any, key: str, default: str = "") -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)
