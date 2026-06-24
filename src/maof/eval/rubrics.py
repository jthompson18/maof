"""Rubric primitives.

Default criteria (factual accuracy, completeness, citation/source quality, tool
efficiency) plus helpers for end-state and adopter-defined rubrics. ``end_state``
rubrics grade the final state, not prescribed steps.
"""

from __future__ import annotations

from maof.types import Rubric

DEFAULT_CRITERIA = ["factual_accuracy", "completeness", "citation_quality", "tool_efficiency"]


def default_rubric(name: str = "default") -> Rubric:
    return Rubric(name=name, criteria=list(DEFAULT_CRITERIA))


def make_rubric(
    name: str,
    *,
    criteria: list[str] | None = None,
    weights: dict[str, float] | None = None,
    pass_threshold: float = 0.7,
    end_state: bool = True,
) -> Rubric:
    return Rubric(
        name=name,
        criteria=list(criteria) if criteria else list(DEFAULT_CRITERIA),
        weights=dict(weights) if weights else {},
        pass_threshold=pass_threshold,
        end_state=end_state,
    )


def weighted_overall(scores: dict[str, float], rubric: Rubric) -> float:
    """Weighted (or, absent weights, mean) score over the rubric's criteria."""
    if rubric.weights:
        total_weight = sum(rubric.weights.get(c, 0.0) for c in rubric.criteria)
        if total_weight > 0:
            return (
                sum(scores.get(c, 0.0) * rubric.weights.get(c, 0.0) for c in rubric.criteria)
                / total_weight
            )
    values = [scores.get(c, 0.0) for c in rubric.criteria]
    return sum(values) / len(values) if values else 0.0


__all__ = ["DEFAULT_CRITERIA", "default_rubric", "make_rubric", "weighted_overall"]
