"""Eval dataset runner + CI regression gate.

Runs each case through the system-under-test (a ``RunHarness``), scores the output
with a Judge, and gates CI on the pass-rate. End-state evaluation: grade the final
output, not prescribed steps. ``assert_eval_gate`` is the pytest helper adopters
call to fail a build below ``EVAL_MIN_PASS_RATE``.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from maof.eval.rubrics import default_rubric
from maof.types import EvalCase, EvalDataset, EvalReport

if TYPE_CHECKING:
    from maof.eval.judge import Judge
    from maof.types import Rubric, RunHarness


@runtime_checkable
class EvalRunner(Protocol):
    async def run_dataset(self, dataset: EvalDataset, harness: RunHarness) -> EvalReport: ...

    def gate(self, report: EvalReport, *, min_pass_rate: float) -> bool: ...


class CallableHarness:
    """Wraps an ``async (EvalCase) -> str`` callable as a RunHarness."""

    def __init__(self, fn: Callable[[EvalCase], Awaitable[str]]) -> None:
        self._fn = fn

    async def run(self, case: EvalCase) -> str:
        return await self._fn(case)


class DefaultEvalRunner:
    def __init__(self, judge: Judge, *, rubric: Rubric | None = None) -> None:
        self._judge = judge
        self._rubric = rubric or default_rubric()

    async def run_dataset(self, dataset: EvalDataset, harness: RunHarness) -> EvalReport:
        results = []
        for case in dataset.cases:
            output = await harness.run(case)
            results.append(
                await self._judge.score(
                    output=output, reference=case.reference, rubric=self._rubric
                )
            )
        passed = sum(1 for r in results if r.passed)
        total = len(results)
        return EvalReport(
            dataset=dataset.name,
            results=results,
            passed=passed,
            total=total,
            pass_rate=(passed / total if total else 0.0),
        )

    def gate(self, report: EvalReport, *, min_pass_rate: float) -> bool:
        return report.pass_rate >= min_pass_rate


def load_dataset(path: str | Path) -> EvalDataset:
    return EvalDataset.model_validate(json.loads(Path(path).read_text(encoding="utf-8")))


def assert_eval_gate(report: EvalReport, *, min_pass_rate: float) -> None:
    """pytest helper: fail the test if the eval pass-rate is below the threshold."""
    if report.pass_rate < min_pass_rate:
        raise AssertionError(
            f"eval gate failed for {report.dataset!r}: pass_rate "
            f"{report.pass_rate:.2f} < min {min_pass_rate:.2f} "
            f"({report.passed}/{report.total} passed)"
        )


__all__ = [
    "EvalRunner",
    "DefaultEvalRunner",
    "CallableHarness",
    "load_dataset",
    "assert_eval_gate",
]
