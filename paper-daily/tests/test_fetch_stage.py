import argparse
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from daily_papers import run_pipeline
from utils import NetworkPreflightError, write_json_atomic


def test_fetch_stage_generates_candidates_not_report(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
research_profile:
  language: zh
  positive_keywords:
    - time series
    - sequence modeling
  negative_keywords:
    - image generation
  arxiv_categories:
    - cs.LG
sources:
  arxiv:
    enabled: false
    lookback_days: 3
    max_results: 100
  openreview:
    enabled: false
    venues: []
    max_results: 100
output:
  report_dir: reports
  data_dir: data
  log_dir: logs
retrieval:
  candidate_limit: 80
""",
        encoding="utf-8",
    )

    args = argparse.Namespace(
        config=str(config_path),
        date="2026-05-14",
        stage="fetch",
        lookback_days=None,
        force=True,
        sources=None,
    )
    result = run_pipeline(args)

    candidates_path = Path(result["candidates_path"])
    assert candidates_path.exists()
    assert (tmp_path / "data" / "raw" / "2026-05-14.json").exists()
    assert not (tmp_path / "reports" / "2026-05-14.md").exists()
    assert not (tmp_path / "data" / "processed" / "2026-05-14_top10.json").exists()


def test_write_json_atomic_falls_back_when_replace_is_blocked(tmp_path, monkeypatch):
    target = tmp_path / "out.json"

    def deny_replace(*args, **kwargs):
        raise PermissionError("rename blocked by sandbox")

    monkeypatch.setattr("utils._direct_json_write_enabled", lambda: False)
    monkeypatch.setattr("utils.os.replace", deny_replace)

    write_json_atomic(target, {"ok": True})

    assert json.loads(target.read_text(encoding="utf-8")) == {"ok": True}


def test_network_preflight_failure_exits_before_writing_empty_candidates(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
research_profile:
  positive_keywords: []
  negative_keywords: []
  arxiv_categories:
    - cs.LG
sources:
  arxiv:
    enabled: true
    lookback_days: 3
    max_results: 1
  openreview:
    enabled: false
output:
  report_dir: reports
  data_dir: data
  log_dir: logs
retrieval:
  candidate_limit: 80
""",
        encoding="utf-8",
    )

    def fail_preflight(urls, timeout=10, retries=1):
        raise NetworkPreflightError([{"url": urls[0], "error": "blocked"}])

    monkeypatch.setattr("daily_papers.check_network_preflight", fail_preflight)
    args = argparse.Namespace(
        config=str(config_path),
        date="2026-05-14",
        stage="fetch",
        lookback_days=None,
        force=True,
        sources=None,
    )

    with pytest.raises(NetworkPreflightError):
        run_pipeline(args)

    assert not (tmp_path / "data" / "processed" / "2026-05-14_candidates.json").exists()
