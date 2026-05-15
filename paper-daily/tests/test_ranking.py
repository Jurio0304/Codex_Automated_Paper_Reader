import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from rank_papers import score_candidate_rules


PROFILE = {
    "positive_keywords": [
        "representation learning",
        "self-supervised learning",
        "state space model",
        "sequence modeling",
        "test-time adaptation",
        "uncertainty estimation",
        "calibration",
        "reranking",
        "language model",
    ],
    "negative_keywords": ["protein folding", "recommender system"],
    "arxiv_categories": ["cs.LG", "cs.CV", "cs.CL", "stat.ML"],
}


def test_method_scoring_prefers_transferable_sequence_method_over_domain_keyword():
    method_paper = {
        "id": "1",
        "source": "arxiv",
        "title": "State Space Model for Sequence Modeling with Test-Time Adaptation",
        "abstract": "A self-supervised representation learning method for long-context temporal modeling under distribution shift.",
        "categories": ["cs.LG", "stat.ML"],
        "published_at": "2026-05-14T00:00:00Z",
    }
    domain_only_paper = {
        "id": "2",
        "source": "arxiv",
        "title": "A Narrow Domain Dataset Report",
        "abstract": "A descriptive dataset note with no new modeling method.",
        "categories": ["cs.DB"],
        "published_at": "2026-05-14T00:00:00Z",
    }

    method_score = score_candidate_rules(method_paper, PROFILE, date(2026, 5, 14))
    domain_score = score_candidate_rules(domain_only_paper, PROFILE, date(2026, 5, 14))

    assert method_score["coarse_retrieval_score"] > domain_score["coarse_retrieval_score"]
    assert method_score["keyword_score"] > 0
    assert "state space model" in [item.lower() for item in method_score["matched_keywords"]]


def test_freshness_score_decreases_for_older_paper():
    fresh = {
        "id": "1",
        "source": "arxiv",
        "title": "sequence modeling with uncertainty estimation",
        "abstract": "",
        "categories": [],
        "published_at": "2026-05-14T00:00:00Z",
    }
    older = dict(fresh, id="2", published_at="2026-05-12T00:00:00Z")

    fresh_score = score_candidate_rules(fresh, PROFILE, date(2026, 5, 14))
    older_score = score_candidate_rules(older, PROFILE, date(2026, 5, 14))

    assert fresh_score["freshness_score"] > older_score["freshness_score"]


def test_acronym_matching_does_not_match_inside_words():
    paper = {
        "id": "3",
        "source": "arxiv",
        "title": "Few-shot action recognition with language models",
        "abstract": "A computer vision benchmark for recognition.",
        "categories": ["cs.CV"],
        "published_at": "2026-05-14T00:00:00Z",
    }

    scored = score_candidate_rules(paper, PROFILE, date(2026, 5, 14))

    assert "ECoG" not in scored["matched_keywords"]
    assert scored["retrieval_penalty"] == 0
