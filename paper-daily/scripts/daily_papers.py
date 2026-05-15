"""Fetch candidate papers for Codex-assisted daily literature review.

This script intentionally stops at candidate retrieval. It does not choose the
final Top 10 and does not write a Markdown report; Codex should do that after
reading and scoring the candidate pool.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fetch_arxiv import fetch_arxiv
from fetch_openreview import fetch_openreview
from rank_papers import build_candidate_pool
from utils import (
    dedupe_papers,
    ensure_dirs,
    check_network_preflight,
    load_config,
    network_preflight_urls,
    NetworkPreflightError,
    parse_target_date,
    read_json,
    resolve_output_paths,
    setup_logger,
    split_csv,
    write_json_atomic,
)


class FetchStageError(RuntimeError):
    """Raised when source fetches fail and writing empty candidates would be misleading."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch paper candidates for Codex review.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--date", default="today", help="'today' or YYYY-MM-DD")
    parser.add_argument(
        "--stage",
        default="fetch",
        choices=["fetch"],
        help="Only 'fetch' is supported; final reports are written by Codex, not this script.",
    )
    parser.add_argument("--lookback-days", type=int, default=None, help="Override source lookback days")
    parser.add_argument("--force", action="store_true", help="Overwrite existing candidate outputs")
    parser.add_argument("--sources", default=None, help="Comma-separated source list, e.g. arxiv,openreview")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        result = run_pipeline(args)
    except (NetworkPreflightError, FetchStageError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    print(f"Candidates: {result['candidates_path']}")


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    if getattr(args, "stage", "fetch") != "fetch":
        raise ValueError("Only --stage fetch is supported. Final reports must be written by Codex.")

    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    report_date = parse_target_date(args.date)
    paths = resolve_output_paths(config_path, config)
    ensure_dirs([paths["raw_dir"], paths["processed_dir"], paths["report_dir"], paths["log_dir"]])

    date_str = report_date.isoformat()
    raw_path = paths["raw_dir"] / f"{date_str}.json"
    candidates_path = paths["processed_dir"] / f"{date_str}_candidates.json"
    log_path = paths["log_dir"] / f"{date_str}.log"
    logger = setup_logger(log_path)
    enabled_sources = resolve_enabled_sources(args, config)

    if candidates_path.exists() and not args.force:
        logger.info("Candidate file already exists and --force was not set: %s", candidates_path)
        return {
            "skipped": True,
            "raw_path": str(raw_path),
            "candidates_path": str(candidates_path),
            "log_path": str(log_path),
        }

    run_network_preflight(config, enabled_sources, logger)

    logger.info("Starting fetch-only candidate pipeline for %s", date_str)
    research_profile = config.get("research_profile", {})

    source_counts: dict[str, int] = {}
    source_warnings: dict[str, list[str]] = {}
    all_papers: list[dict[str, Any]] = []

    if "arxiv" in enabled_sources:
        source_config = config.get("sources", {}).get("arxiv", {})
        lookback = args.lookback_days or int(source_config.get("lookback_days", 3))
        papers, warnings = fetch_arxiv(source_config, research_profile, report_date, lookback, logger)
        all_papers.extend(papers)
        source_counts["arxiv"] = len(papers)
        source_warnings["arxiv"] = warnings

    if "openreview" in enabled_sources:
        source_config = config.get("sources", {}).get("openreview", {})
        lookback = args.lookback_days or int(source_config.get("lookback_days", 3))
        papers, warnings = fetch_openreview(source_config, report_date, lookback, logger)
        all_papers.extend(papers)
        source_counts["openreview"] = len(papers)
        source_warnings["openreview"] = warnings

    deduped = dedupe_papers(all_papers)
    logger.info("Total papers fetched=%s, deduped=%s", len(all_papers), len(deduped))

    if not deduped and source_had_failures(source_warnings):
        existing_count = existing_candidate_count(candidates_path)
        if existing_count > 0:
            message = (
                "All source fetches failed or returned zero papers; "
                f"preserving existing candidate file with {existing_count} papers: {candidates_path}"
            )
            logger.error(message)
            raise FetchStageError(message)
        message = (
            "All enabled source fetches failed or returned zero papers with warnings; "
            "aborting before writing an empty candidate file. Check network/proxy and source warnings in "
            f"{log_path}"
        )
        logger.error(message)
        raise FetchStageError(message)

    raw_payload = {
        "date": date_str,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stage": "fetch",
        "sources": list(enabled_sources),
        "source_counts": source_counts,
        "warnings": source_warnings,
        "paper_count": len(deduped),
        "papers": deduped,
    }
    write_json_atomic(raw_path, raw_payload)

    candidate_limit = int(config.get("retrieval", {}).get("candidate_limit", 80))
    candidates = build_candidate_pool(
        deduped,
        research_profile,
        report_date,
        candidate_limit=candidate_limit,
    )
    write_json_atomic(candidates_path, candidates)
    logger.info("Candidate pool written with %s papers: %s", len(candidates), candidates_path)
    logger.info("Fetch stage complete; no report was generated by the script.")

    return {
        "skipped": False,
        "raw_path": str(raw_path),
        "candidates_path": str(candidates_path),
        "log_path": str(log_path),
        "candidate_count": len(candidates),
        "source_counts": source_counts,
    }


def resolve_enabled_sources(args: argparse.Namespace, config: dict[str, Any]) -> list[str]:
    if args.sources:
        requested = split_csv(args.sources)
    else:
        requested = [
            name
            for name, source_config in config.get("sources", {}).items()
            if source_config.get("enabled", True)
        ]
    supported = {"arxiv", "openreview"}
    return [source for source in requested if source in supported]


def run_network_preflight(config: dict[str, Any], enabled_sources: list[str], logger: Any) -> None:
    urls = network_preflight_urls(config, enabled_sources)
    if not urls:
        logger.info("Network preflight skipped; no enabled network sources.")
        return

    timeout = int(config.get("network", {}).get("preflight_timeout_seconds", 20))
    retries = int(config.get("network", {}).get("preflight_retries", 1))
    logger.info("Running network preflight for %s", ", ".join(urls))
    try:
        check_network_preflight(urls, timeout=timeout, retries=retries)
    except NetworkPreflightError as exc:
        logger.error("Network preflight failed; aborting before fetch. %s", exc)
        raise
    logger.info("Network preflight passed.")


def source_had_failures(source_warnings: dict[str, list[str]]) -> bool:
    return any(bool(warnings) for warnings in source_warnings.values())


def existing_candidate_count(path: Path) -> int:
    try:
        data = read_json(path)
    except Exception:
        return 0
    return len(data) if isinstance(data, list) else 0


if __name__ == "__main__":
    main()
