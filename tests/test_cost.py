"""Cost ledger: price-table costing + aggregation."""

from __future__ import annotations

from uuid import uuid4

import pytest

from maof.cost.accounting import RepoCostLedger
from maof.types import CostSummary


class FakeCostRepo:
    """In-memory CostRepo for offline ledger tests."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, int, int, float]] = []

    async def record(
        self, run_id: str, model: str, in_tokens: int, out_tokens: int, cost_usd: float
    ) -> None:
        self.records.append((run_id, model, in_tokens, out_tokens, cost_usd))

    async def total(self, run_id: str) -> CostSummary | None:
        rows = [r for r in self.records if r[0] == run_id]
        if not rows:
            return None
        summary = CostSummary(run_id=run_id)
        for _, model, in_tok, out_tok, cost in rows:
            summary.in_tokens += in_tok
            summary.out_tokens += out_tok
            summary.cost_usd += cost
            summary.by_model[model] = summary.by_model.get(model, 0) + in_tok + out_tok
        summary.total_tokens = summary.in_tokens + summary.out_tokens
        return summary


async def test_ledger_records_with_price_table() -> None:
    repo = FakeCostRepo()
    ledger = RepoCostLedger(repo, prices={"gpt-4o": 5.0})  # usd per 1k tokens
    await ledger.record("r1", model="gpt-4o", in_tokens=100, out_tokens=100)
    assert repo.records[0][4] == pytest.approx(1.0)  # 200/1000 * 5.0
    total = await ledger.total("r1")
    assert total.total_tokens == 200
    assert total.cost_usd == pytest.approx(1.0)


async def test_ledger_unknown_model_is_free() -> None:
    repo = FakeCostRepo()
    ledger = RepoCostLedger(repo)
    await ledger.record("r1", model="mystery", in_tokens=50, out_tokens=50)
    assert repo.records[0][4] == 0.0


async def test_ledger_total_empty_returns_zero_summary() -> None:
    ledger = RepoCostLedger(FakeCostRepo())
    total = await ledger.total("missing")
    assert total.run_id == "missing"
    assert total.total_tokens == 0
    assert total.cost_usd == 0.0


async def test_ledger_against_postgres(db) -> None:  # type: ignore[no-untyped-def]
    from maof.persistence.postgres import PostgresCostRepo

    ledger = RepoCostLedger(PostgresCostRepo(db), prices={"m": 2.0})
    run_id = f"run-{uuid4()}"
    await ledger.record(run_id, model="m", in_tokens=500, out_tokens=500)
    total = await ledger.total(run_id)
    assert total.total_tokens == 1000
    assert total.cost_usd == pytest.approx(2.0)
