# paper-daily

`paper-daily` is the candidate-retrieval component of CAPR.

It fetches recent papers, normalizes metadata, deduplicates records, and writes a candidate JSON file for Codex to read. It intentionally does not write the final literature review.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell example:

```powershell
python -m venv .venv-win
.\.venv-win\Scripts\python.exe -m pip install -r requirements.txt
```

## Fetch Candidates

```bash
python scripts/daily_papers.py --config config.yaml --date today --stage fetch --force
```

Common options:

```bash
python scripts/daily_papers.py --config config.yaml --date 2026-05-15 --stage fetch --force
python scripts/daily_papers.py --config config.yaml --date today --stage fetch --lookback-days 3
python scripts/daily_papers.py --config config.yaml --date today --stage fetch --sources arxiv,openreview
```

Outputs:

```text
data/raw/YYYY-MM-DD.json
data/processed/YYYY-MM-DD_candidates.json
logs/YYYY-MM-DD.log
```

Runtime outputs are ignored by Git.

## Network Preflight

Before fetching, the script checks core source URLs for arXiv and OpenReview. If these checks fail, the script exits before writing an empty candidate file.

CAPR ignores environment proxy variables by default for source requests, because some automation sandboxes expose broken local proxy settings. If you need to use your system proxy, set:

```bash
PAPER_DAILY_USE_ENV_PROXY=1
```

## Candidate Schema

Each candidate includes:

```json
{
  "id": "...",
  "source": "arxiv",
  "title": "...",
  "authors": ["..."],
  "abstract": "...",
  "url": "...",
  "pdf_url": "...",
  "published_at": "...",
  "updated_at": "...",
  "venue": "...",
  "categories": ["..."],
  "keyword_score": 0.0,
  "retrieval_reason": "why the retrieval script kept this candidate",
  "matched_keywords": [],
  "negative_matches": [],
  "coarse_retrieval_score": 0.0
}
```

The retrieval scores are only hints. Codex should still perform semantic reading and scoring before writing the final report.

## Tests

```bash
python -m pytest tests
```
