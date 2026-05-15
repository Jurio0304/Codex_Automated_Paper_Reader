"""Coarse candidate scoring for retrieval only.

The scores in this module are retrieval hints, not final paper judgments. Codex
must read the candidate file and perform semantic scoring before writing a
daily report.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from utils import paper_display_date, validate_paper_schema


KEYWORD_WEIGHTS = {
    "representation learning": 4.0,
    "disentangled representation": 4.0,
    "invariant representation": 3.5,
    "equivariant representation": 3.5,
    "contrastive learning": 3.5,
    "self-supervised learning": 4.0,
    "masked modeling": 3.5,
    "masked autoencoder": 3.5,
    "latent variable model": 3.0,
    "prototype learning": 3.0,
    "metric learning": 3.0,
    "sequence modeling": 4.0,
    "temporal modeling": 4.0,
    "long-context modeling": 3.5,
    "state space model": 4.0,
    "recurrent model": 3.5,
    "temporal attention": 3.5,
    "alignment modeling": 3.5,
    "ctc": 3.5,
    "transducer": 3.5,
    "sequence-to-sequence": 3.0,
    "uncertainty estimation": 4.0,
    "aleatoric uncertainty": 3.5,
    "epistemic uncertainty": 3.5,
    "calibration": 3.5,
    "conformal prediction": 3.5,
    "robust learning": 3.0,
    "out-of-distribution": 3.0,
    "noisy label learning": 3.0,
    "confidence estimation": 3.5,
    "online adaptation": 4.0,
    "test-time adaptation": 4.0,
    "continual learning": 4.0,
    "domain adaptation": 3.5,
    "domain generalization": 3.5,
    "distribution shift": 3.5,
    "representation drift": 3.5,
    "non-stationary learning": 3.5,
    "language model integration": 3.0,
    "retrieval-augmented generation": 3.0,
    "reranking": 3.5,
    "sequence scoring": 3.5,
    "decoding strategy": 3.5,
    "preference optimization": 3.0,
    "weak supervision": 3.0,
    "posterior inference": 3.5,
    "self-training": 3.5,
    "pseudo-labeling": 3.0,
    "diffusion model": 2.5,
    "multimodal alignment": 3.5,
    "cross-modal representation learning": 4.0,
    "attention mechanism": 2.5,
    "tokenization": 2.5,
    "spatiotemporal modeling": 4.0,
    "time series": 3.0,
    "irregular time series": 4.0,
    "foundation model": 3.0,
    "scientific machine learning": 3.0,
    "language model": 1.5,
}

METHOD_CONTEXT_TERMS = [
    "method",
    "model",
    "learning",
    "training",
    "adaptation",
    "decoding",
    "decoder",
    "representation",
    "alignment",
    "uncertainty",
    "calibration",
    "sequence",
    "temporal",
    "time series",
    "state space",
    "diffusion",
    "transformer",
    "attention",
    "loss",
    "inference",
    "reranking",
]


def build_candidate_pool(
    papers: list[dict[str, Any]],
    research_profile: dict[str, Any],
    target_date: date,
    candidate_limit: int = 80,
) -> list[dict[str, Any]]:
    """Return a 50-100 item candidate pool with retrieval metadata."""

    scored = [
        score_candidate_rules(validate_paper_schema(paper), research_profile, target_date)
        for paper in papers
    ]
    scored.sort(
        key=lambda paper: (
            float(paper.get("keyword_score", 0.0)),
            float(paper.get("coarse_retrieval_score", 0.0)),
            paper.get("published_at", ""),
        ),
        reverse=True,
    )
    return [candidate_schema(paper) for paper in scored[:candidate_limit]]


def score_candidate_rules(
    paper: dict[str, Any],
    research_profile: dict[str, Any],
    target_date: date,
) -> dict[str, Any]:
    """Compute coarse retrieval hints without making final relevance claims."""

    text = scoring_text(paper)
    positive_keywords = research_profile.get("positive_keywords", [])
    negative_keywords = research_profile.get("negative_keywords", [])

    matched = []
    keyword_score = 0.0
    method_context = has_any_term(text, METHOD_CONTEXT_TERMS)

    for keyword in positive_keywords:
        keyword_l = str(keyword).lower()
        if not keyword_l or not contains_keyword(text, keyword_l):
            continue
        if keyword_l == "language model" and not method_context:
            continue
        matched.append(str(keyword))
        keyword_score += KEYWORD_WEIGHTS.get(keyword_l, 2.0)

    negative_matches = []
    penalty = 0.0
    for keyword in negative_keywords:
        keyword_l = str(keyword).lower()
        if keyword_l and contains_keyword(text, keyword_l):
            negative_matches.append(str(keyword))
            penalty -= 3.0

    category_score = sum(
        0.25
        for category in paper.get("categories", [])
        if category in set(research_profile.get("arxiv_categories", []))
    )
    freshness_score = compute_freshness_score(paper, target_date)
    combo_bonus = compute_topic_combo_bonus(text)
    coarse_score = keyword_score + category_score + freshness_score + combo_bonus + penalty

    annotated = dict(paper)
    annotated.update(
        {
            "keyword_score": round(keyword_score, 3),
            "coarse_retrieval_score": round(coarse_score, 3),
            "freshness_score": round(freshness_score, 3),
            "category_score": round(category_score, 3),
            "topic_combo_bonus": round(combo_bonus, 3),
            "retrieval_penalty": round(penalty, 3),
            "matched_keywords": matched,
            "negative_matches": negative_matches,
            "retrieval_reason": build_retrieval_reason(paper, matched, negative_matches),
        }
    )
    return annotated


def candidate_schema(paper: dict[str, Any]) -> dict[str, Any]:
    """Keep candidate files focused on metadata Codex needs for review."""

    fields = [
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
        "keyword_score",
        "retrieval_reason",
        "matched_keywords",
        "negative_matches",
        "coarse_retrieval_score",
    ]
    return {field: paper.get(field, [] if field in {"authors", "categories"} else "") for field in fields}


def build_retrieval_reason(
    paper: dict[str, Any],
    matched_keywords: list[str],
    negative_matches: list[str],
) -> str:
    parts = []
    if matched_keywords:
        parts.append("matched method-transfer terms: " + ", ".join(matched_keywords[:8]))
    else:
        parts.append("included as a broad recent candidate from configured sources/categories")

    categories = paper.get("categories", [])
    if categories:
        parts.append("categories: " + ", ".join(categories[:5]))
    venue = paper.get("venue")
    if venue:
        parts.append(f"venue/source label: {venue}")
    if negative_matches:
        parts.append("possible off-topic signals: " + ", ".join(negative_matches[:4]))
    parts.append("requires Codex semantic review before any recommendation")
    return "; ".join(parts)


def scoring_text(paper: dict[str, Any]) -> str:
    pieces = [
        paper.get("title", ""),
        paper.get("abstract", ""),
        paper.get("venue", ""),
        " ".join(paper.get("categories", [])),
    ]
    return " ".join(str(piece).lower() for piece in pieces if piece)


def contains_keyword(text: str, keyword: str) -> bool:
    """Match keywords as terms, avoiding acronym false positives like ECoG in recognition."""

    escaped = re.escape(keyword.lower()).replace(r"\ ", r"[\s-]+")
    if re.fullmatch(r"[a-z0-9]+", keyword.lower()):
        pattern = rf"(?<![a-z0-9]){escaped}(?![a-z0-9])"
    else:
        pattern = rf"(?<![a-z0-9]){escaped}s?(?![a-z0-9])"
    return re.search(pattern, text) is not None


def has_any_term(text: str, terms: list[str]) -> bool:
    return any(contains_keyword(text, term) for term in terms)


def compute_freshness_score(paper: dict[str, Any], target_date: date) -> float:
    dt = paper_display_date(paper)
    if not dt:
        return 0.25
    delta_days = (target_date - dt.date()).days
    if delta_days <= 0:
        return 1.0
    if delta_days == 1:
        return 0.75
    if delta_days == 2:
        return 0.5
    return 0.0


def compute_topic_combo_bonus(text: str) -> float:
    bonus = 0.0
    if has_any_term(text, ["sequence modeling", "temporal modeling", "state space model", "recurrent model"]) and has_any_term(
        text, ["time series", "long-context", "spatiotemporal", "irregular"]
    ):
        bonus += 2.0
    if has_any_term(text, ["test-time adaptation", "online adaptation", "continual learning"]) and has_any_term(
        text, ["distribution shift", "domain adaptation", "domain generalization", "non-stationary", "drift"]
    ):
        bonus += 2.0
    if has_any_term(text, ["uncertainty estimation", "confidence estimation", "calibration", "conformal prediction"]) and has_any_term(
        text, ["robust", "out-of-distribution", "missing", "noisy"]
    ):
        bonus += 1.5
    if has_any_term(text, ["masked modeling", "self-supervised learning", "contrastive learning"]) and has_any_term(
        text, ["representation", "time series", "multimodal", "sequence"]
    ):
        bonus += 1.5
    if has_any_term(text, ["reranking", "sequence scoring", "decoding strategy", "posterior inference"]) and has_any_term(
        text, ["language model", "sequence", "token", "generation"]
    ):
        bonus += 1.5
    if has_any_term(text, ["multimodal alignment", "cross-modal representation learning"]) and has_any_term(
        text, ["alignment", "representation", "tokenization"]
    ):
        bonus += 1.0
    return bonus
