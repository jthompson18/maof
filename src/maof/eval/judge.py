"""LLM-as-judge rubric scorer.

Scores an output 0.0–1.0 per criterion + a weighted overall + pass/fail. The judge
asks the configured LLM for a JSON score object and is robust to minor noise.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from maof.eval.rubrics import weighted_overall
from maof.types import JudgeResult

if TYPE_CHECKING:
    from maof.models.base import LLMProvider
    from maof.types import Rubric

_JUDGE_SYSTEM = (
    "You are a strict, fair evaluator. Score each named criterion from 0.0 to 1.0 and "
    "respond with a single JSON object mapping each criterion to its score, plus a "
    '"rationale" string. Output JSON only.'
)


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _extract_json(raw: str) -> dict[str, object]:
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end < start:
        return {}
    try:
        parsed: dict[str, object] = json.loads(raw[start : end + 1])
        return parsed
    except json.JSONDecodeError:
        return {}


@runtime_checkable
class Judge(Protocol):
    async def score(self, *, output: str, reference: str | None, rubric: Rubric) -> JudgeResult: ...


class LLMJudge:
    def __init__(self, llm: LLMProvider) -> None:
        self._llm = llm

    async def score(self, *, output: str, reference: str | None, rubric: Rubric) -> JudgeResult:
        raw = await self._llm.generate(
            self._build_prompt(output, reference, rubric), system=_JUDGE_SYSTEM
        )
        data = _extract_json(raw)
        scores = {c: _clamp(float(data.get(c, 0.0) or 0.0)) for c in rubric.criteria}  # type: ignore[arg-type]
        overall = weighted_overall(scores, rubric)
        return JudgeResult(
            scores=scores,
            overall=overall,
            passed=overall >= rubric.pass_threshold,
            rationale=str(data.get("rationale", "")),
        )

    @staticmethod
    def _build_prompt(output: str, reference: str | None, rubric: Rubric) -> str:
        lines = [
            f"Criteria to score: {', '.join(rubric.criteria)}",
            "",
            "OUTPUT TO EVALUATE:",
            output,
        ]
        if reference is not None:
            lines += ["", "REFERENCE (ground truth):", reference]
        if rubric.end_state:
            lines += ["", "Grade the final state, not the steps taken."]
        return "\n".join(lines)


__all__ = ["Judge", "LLMJudge"]
