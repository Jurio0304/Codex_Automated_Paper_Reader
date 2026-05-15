"""Shared utilities for the daily paper pipeline."""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import time as time_module
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.request import Request, ProxyHandler, build_opener


REQUIRED_PAPER_FIELDS = [
    "id",
    "source",
    "title",
    "authors",
    "abstract",
    "url",
    "pdf_url",
    "published_at",
    "updated_at",
    "venue",
    "categories",
]

DEFAULT_USER_AGENT = "paper-daily-mvp/0.1"
TRUTHY_VALUES = {"1", "true", "yes", "y", "on"}


class NetworkPreflightError(RuntimeError):
    """Raised when core literature-source URLs are not reachable."""

    def __init__(self, failures: list[dict[str, str]]) -> None:
        self.failures = failures
        details = "; ".join(f"{item['url']} -> {item['error']}" for item in failures)
        super().__init__(f"Network preflight failed: {details}")


def load_config(config_path: str | Path) -> dict[str, Any]:
    """Load YAML configuration from disk."""

    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - exercised only without deps.
        raise RuntimeError(
            "PyYAML is required to read config.yaml. Install with: pip install -r requirements.txt"
        ) from exc

    path = Path(config_path)
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a YAML mapping: {path}")
    return data


def resolve_output_paths(config_path: str | Path, config: dict[str, Any]) -> dict[str, Path]:
    """Resolve output paths relative to the config file directory."""

    base_dir = Path(config_path).resolve().parent
    output = config.get("output", {})
    data_dir = _resolve_path(base_dir, output.get("data_dir", "data"))
    report_dir = _resolve_path(base_dir, output.get("report_dir", "reports"))
    log_dir = _resolve_path(base_dir, output.get("log_dir", "logs"))
    return {
        "base_dir": base_dir,
        "data_dir": data_dir,
        "raw_dir": data_dir / "raw",
        "processed_dir": data_dir / "processed",
        "report_dir": report_dir,
        "log_dir": log_dir,
    }


def _resolve_path(base_dir: Path, value: str | os.PathLike[str]) -> Path:
    path = Path(value)
    return path if path.is_absolute() else base_dir / path


def ensure_dirs(paths: dict[str, Path] | Iterable[Path]) -> None:
    """Create output directories if needed."""

    if isinstance(paths, dict):
        iterable = paths.values()
    else:
        iterable = paths
    for path in iterable:
        if isinstance(path, Path) and path.name:
            if path.suffix:
                path.parent.mkdir(parents=True, exist_ok=True)
            else:
                path.mkdir(parents=True, exist_ok=True)


def parse_target_date(value: str | None) -> date:
    """Parse 'today' or an ISO date into a date object."""

    if value is None or value.lower() == "today":
        return date.today()
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("--date must be 'today' or YYYY-MM-DD") from exc


def setup_logger(log_path: Path) -> logging.Logger:
    """Create a per-run logger with file and console handlers."""

    ensure_dirs([log_path])
    logger = logging.getLogger(f"paper_daily.{log_path.stem}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def write_json_atomic(path: Path, data: Any) -> None:
    """Write JSON atomically where supported, with a Windows-safe fallback."""

    ensure_dirs([path])
    if _direct_json_write_enabled():
        with path.open("w", encoding="utf-8") as f:
            _dump_json(f, data)
        return

    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            _dump_json(f, data)
        try:
            os.replace(tmp_name, path)
        except PermissionError:
            # Some sandboxed Windows runners allow direct file writes but block
            # rename/replace. Fall back so the daily job can still complete.
            with path.open("w", encoding="utf-8") as f:
                _dump_json(f, data)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.unlink(tmp_name)
            except PermissionError:
                pass


def _direct_json_write_enabled() -> bool:
    """Avoid temp-file rename/delete issues in Windows sandboxed runners."""

    return os.name == "nt" and os.environ.get("PAPER_DAILY_FORCE_ATOMIC_JSON") != "1"


def _dump_json(file_obj: Any, data: Any) -> None:
    json.dump(data, file_obj, ensure_ascii=False, indent=2, default=json_default)
    file_obj.write("\n")


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def should_use_env_proxy() -> bool:
    """Use env proxies only when explicitly requested.

    The Codex desktop sandbox can expose dead loopback proxy variables.
    Bypassing env proxies by default avoids false network failures.
    Set PAPER_DAILY_USE_ENV_PROXY=1 when a real proxy is required.
    """

    return os.environ.get("PAPER_DAILY_USE_ENV_PROXY", "").strip().lower() in TRUTHY_VALUES


def requests_get(url: str, timeout: int = 30, headers: dict[str, str] | None = None, **kwargs: Any) -> Any:
    """GET a URL with requests while ignoring broken env proxies by default."""

    import requests

    session = requests.Session()
    session.trust_env = should_use_env_proxy()
    return session.get(
        url,
        timeout=timeout,
        headers=headers or {"User-Agent": DEFAULT_USER_AGENT},
        **kwargs,
    )


def http_open(request: Request | str, timeout: int = 30) -> Any:
    """Open a URL/request with urllib while ignoring env proxies by default."""

    if should_use_env_proxy():
        opener = build_opener()
    else:
        opener = build_opener(ProxyHandler({}))
    return opener.open(request, timeout=timeout)


def network_preflight_urls(config: dict[str, Any], enabled_sources: list[str]) -> list[str]:
    """Return core source URLs to check before fetching candidates."""

    network_config = config.get("network", {}) if isinstance(config, dict) else {}
    if network_config.get("preflight_enabled", True) is False:
        return []
    configured = network_config.get("preflight_urls")
    if configured:
        return [str(url) for url in configured if str(url).strip()]

    urls: list[str] = []
    if "arxiv" in enabled_sources:
        urls.extend(["https://export.arxiv.org/", "https://arxiv.org/"])
    if "openreview" in enabled_sources:
        urls.extend(["https://openreview.net/", "https://api2.openreview.net/"])
    return unique_preserve_order(urls)


def check_network_preflight(urls: list[str], timeout: int = 20, retries: int = 1) -> None:
    """Fail fast if core URLs are unreachable before creating empty outputs."""

    failures: list[dict[str, str]] = []
    for url in urls:
        last_error = ""
        for attempt in range(max(1, retries + 1)):
            try:
                response = requests_get(
                    url,
                    timeout=timeout,
                    headers={"User-Agent": DEFAULT_USER_AGENT},
                    stream=True,
                )
                try:
                    status_code = int(getattr(response, "status_code", 0) or 0)
                    if status_code < 500:
                        last_error = ""
                        break
                    last_error = f"HTTP {status_code}"
                finally:
                    response.close()
            except ImportError:
                try:
                    request = Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
                    response = http_open(request, timeout=timeout)
                    response.close()
                    last_error = ""
                    break
                except Exception as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"

            if attempt < retries:
                time_module.sleep(1)
        if last_error:
            failures.append({"url": url, "error": last_error})

    if failures:
        raise NetworkPreflightError(failures)


def normalize_whitespace(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_title(title: str | None) -> str:
    """Normalize titles for duplicate detection."""

    text = unicodedata.normalize("NFKC", title or "").lower()
    text = re.sub(r"\barxiv:\d{4}\.\d{4,5}(v\d+)?\b", " ", text)
    text = re.sub(r"\bv\d+\b", " ", text)
    text = re.sub(r"[\[\]{}()<>]", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return normalize_whitespace(text)


def stable_source_key(paper: dict[str, Any]) -> str | None:
    source = normalize_whitespace(str(paper.get("source", ""))).lower()
    paper_id = normalize_whitespace(str(paper.get("id", ""))).lower()
    if not source or not paper_id:
        return None
    return f"{source}:{paper_id}"


def validate_paper_schema(paper: dict[str, Any]) -> dict[str, Any]:
    """Return a copy that contains all required fields with safe defaults."""

    normalized = dict(paper)
    for field in REQUIRED_PAPER_FIELDS:
        if field not in normalized or normalized[field] is None:
            normalized[field] = [] if field in {"authors", "categories"} else ""
    if isinstance(normalized["authors"], str):
        normalized["authors"] = [normalized["authors"]]
    if isinstance(normalized["categories"], str):
        normalized["categories"] = [normalized["categories"]]
    normalized["title"] = normalize_whitespace(normalized.get("title"))
    normalized["abstract"] = normalize_whitespace(normalized.get("abstract"))
    normalized["venue"] = normalize_whitespace(normalized.get("venue"))
    normalized["authors"] = [
        normalize_whitespace(str(author))
        for author in normalized.get("authors", [])
        if normalize_whitespace(str(author))
    ]
    normalized["categories"] = [
        normalize_whitespace(str(category))
        for category in normalized.get("categories", [])
        if normalize_whitespace(str(category))
    ]
    return normalized


def dedupe_papers(papers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate by source id and normalized title."""

    deduped: list[dict[str, Any]] = []
    by_source: dict[str, int] = {}
    by_title: dict[str, int] = {}

    for raw_paper in papers:
        paper = validate_paper_schema(raw_paper)
        source_key = stable_source_key(paper)
        title_key = normalize_title(paper.get("title"))

        existing_idx = None
        if source_key and source_key in by_source:
            existing_idx = by_source[source_key]
        elif title_key and title_key in by_title:
            existing_idx = by_title[title_key]

        if existing_idx is None:
            deduped.append(paper)
            idx = len(deduped) - 1
            if source_key:
                by_source[source_key] = idx
            if title_key:
                by_title[title_key] = idx
            continue

        deduped[existing_idx] = merge_duplicate_papers(deduped[existing_idx], paper)
        merged = deduped[existing_idx]
        merged_source_key = stable_source_key(merged)
        merged_title_key = normalize_title(merged.get("title"))
        if merged_source_key:
            by_source[merged_source_key] = existing_idx
        if source_key:
            by_source[source_key] = existing_idx
        if merged_title_key:
            by_title[merged_title_key] = existing_idx
        if title_key:
            by_title[title_key] = existing_idx

    return deduped


def merge_duplicate_papers(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    """Merge duplicate records while preserving the richer metadata."""

    merged = dict(existing)
    aliases = set(merged.get("duplicate_source_ids", []))
    for paper in (existing, incoming):
        source_key = stable_source_key(paper)
        if source_key:
            aliases.add(source_key)
    if aliases:
        merged["duplicate_source_ids"] = sorted(aliases)

    for field in ("url", "pdf_url", "published_at", "updated_at", "venue"):
        if not merged.get(field) and incoming.get(field):
            merged[field] = incoming[field]

    if len(str(incoming.get("abstract", ""))) > len(str(merged.get("abstract", ""))):
        merged["abstract"] = incoming.get("abstract", "")

    merged["authors"] = unique_preserve_order(
        list(merged.get("authors", [])) + list(incoming.get("authors", []))
    )
    merged["categories"] = unique_preserve_order(
        list(merged.get("categories", [])) + list(incoming.get("categories", []))
    )
    return validate_paper_schema(merged)


def unique_preserve_order(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = normalize_whitespace(str(value))
        key = text.lower()
        if text and key not in seen:
            seen.add(key)
            result.append(text)
    return result


def parse_datetime(value: Any) -> datetime | None:
    """Parse common date representations into a timezone-aware datetime."""

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000.0
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    else:
        text = str(value).strip()
        if text.isdigit():
            return parse_datetime(int(text))
        try:
            from dateutil import parser as date_parser

            dt = date_parser.parse(text)
        except Exception:
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except Exception:
                return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def isoformat_or_empty(value: Any) -> str:
    dt = parse_datetime(value)
    return dt.isoformat() if dt else ""


def paper_display_date(paper: dict[str, Any]) -> datetime | None:
    return parse_datetime(paper.get("published_at")) or parse_datetime(paper.get("updated_at"))


def split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]
