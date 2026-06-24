"""Evaluation harness: LLM-as-judge, rubric weighting, dataset runner + CI gate."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from maof.eval.judge import LLMJudge
from maof.eval.rubrics import default_rubric, make_rubric, weighted_overall
from maof.eval.runner import CallableHarness, DefaultEvalRunner, assert_eval_gate, load_dataset
from maof.types import EvalCase, EvalDataset, EvalReport, JudgeResult, Rubric


class JsonLLM:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = json.dumps(payload)

    async def generate(
        self, prompt: str, *, system: Any = None, json_schema: Any = None, **opts: Any
    ) -> str:
        return self._payload


async def test_llm_judge_scores_and_passes() -> None:
    judge = LLMJudge(
        JsonLLM(
            {
                "factual_accuracy": 0.9,
                "completeness": 0.8,
                "citation_quality": 0.7,
                "tool_efficiency": 1.0,
                "rationale": "solid",
            }
        )
    )
    result = await judge.score(output="o", reference="ref", rubric=default_rubric())
    assert result.passed
    assert 0.8 < result.overall < 0.9  # mean(0.9,0.8,0.7,1.0) = 0.85
    assert result.rationale == "solid"


async def test_llm_judge_fails_low_scores() -> None:
    judge = LLMJudge(
        JsonLLM(
            {
                "factual_accuracy": 0.2,
                "completeness": 0.1,
                "citation_quality": 0.0,
                "tool_efficiency": 0.3,
            }
        )
    )
    result = await judge.score(output="o", reference=None, rubric=default_rubric())
    assert not result.passed and result.overall < 0.7


async def test_llm_judge_extracts_json_from_noise() -> None:
    class NoisyLLM:
        async def generate(
            self, prompt: str, *, system: Any = None, json_schema: Any = None, **opts: Any
        ) -> str:
            return (
                'Here is the evaluation: {"factual_accuracy":1.0,"completeness":1.0,'
                '"citation_quality":1.0,"tool_efficiency":1.0} -- done.'
            )

    result = await LLMJudge(NoisyLLM()).score(output="o", reference=None, rubric=default_rubric())
    assert result.overall == 1.0 and result.passed


def test_weighted_overall() -> None:
    rubric = make_rubric("w", criteria=["a", "b"], weights={"a": 3, "b": 1})
    assert weighted_overall({"a": 1.0, "b": 0.0}, rubric) == 0.75  # (1*3 + 0*1) / 4


async def test_eval_runner_and_gate() -> None:
    class ContentJudge:
        async def score(self, *, output: str, reference: Any, rubric: Rubric) -> JudgeResult:
            ok = "honored" in output
            return JudgeResult(scores={}, overall=1.0 if ok else 0.0, passed=ok)

    dataset = EvalDataset(
        name="liability",
        cases=[
            EvalCase(id="c1", input="plan honored the liability chain"),
            EvalCase(id="c2", input="plan overcommitted beyond cleared funds"),
        ],
    )

    async def harness_fn(case: EvalCase) -> str:
        return case.input

    runner = DefaultEvalRunner(ContentJudge())
    report = await runner.run_dataset(dataset, CallableHarness(harness_fn))
    assert report.total == 2 and report.passed == 1 and report.pass_rate == 0.5
    assert runner.gate(report, min_pass_rate=0.5)
    assert not runner.gate(report, min_pass_rate=0.8)


def test_assert_eval_gate() -> None:
    assert_eval_gate(EvalReport(dataset="d", passed=8, total=10, pass_rate=0.8), min_pass_rate=0.8)
    with pytest.raises(AssertionError):
        assert_eval_gate(
            EvalReport(dataset="d", passed=5, total=10, pass_rate=0.5), min_pass_rate=0.8
        )


def test_load_dataset(tmp_path: Path) -> None:
    data = {"name": "ds", "cases": [{"id": "c1", "input": "x", "reference": "y"}]}
    path = tmp_path / "ds.json"
    path.write_text(json.dumps(data))
    dataset = load_dataset(path)
    assert dataset.name == "ds"
    assert len(dataset.cases) == 1 and dataset.cases[0].id == "c1"
