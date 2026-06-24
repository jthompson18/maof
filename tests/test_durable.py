"""Durable execution: checkpoint/resume, idempotency replay, artifact round-trips."""

from __future__ import annotations

import dataclasses
from uuid import uuid4

import pytest

from maof.persistence.postgres import (
    Database,
    PostgresArtifactRepo,
    PostgresCheckpointRepo,
)
from maof.runs.artifacts import InMemoryArtifactStore, PostgresArtifactStore
from maof.runs.checkpoint import InMemoryCheckpointer, PostgresCheckpointer
from maof.runs.idempotency import (
    InMemoryIdempotencyGuard,
    PostgresIdempotencyGuard,
    make_idempotency_key,
)
from maof.types import Plan, Stage, StageContext, TenantContext

STEPS = ["chat", "intent_synthesis", "action_plan", "publish"]


async def _run(cp: InMemoryCheckpointer, executed: list[str], *, crash_after: str | None) -> None:
    sc = await cp.resume("run-1")
    if sc is None:
        sc = StageContext(run_id="run-1", tenant=TenantContext(tenant_id="t"), goal="g")
    done = list(sc.extras.get("done", []))
    for step in STEPS:
        if step in done:
            continue
        executed.append(step)
        done.append(step)
        sc = dataclasses.replace(sc, stage=Stage(step), extras={"done": list(done)})
        await cp.save("run-1", step, sc)
        if step == crash_after:
            raise RuntimeError("killed mid-run")


async def test_checkpoint_resume_skips_completed_steps() -> None:
    cp = InMemoryCheckpointer()
    executed: list[str] = []
    with pytest.raises(RuntimeError):
        await _run(cp, executed, crash_after="intent_synthesis")
    assert executed == ["chat", "intent_synthesis"]

    await _run(cp, executed, crash_after=None)  # resume
    # chat + intent_synthesis are NOT re-run on resume
    assert executed == ["chat", "intent_synthesis", "action_plan", "publish"]


async def test_idempotency_guard_runs_side_effect_once() -> None:
    guard = InMemoryIdempotencyGuard()
    calls = {"n": 0}

    async def commit() -> dict[str, object]:
        calls["n"] += 1
        return {"committed": True, "amount": 250000}

    r1 = await guard.once("buy-key", commit)
    r2 = await guard.once("buy-key", commit)
    assert calls["n"] == 1  # the side effect fired exactly once
    assert r1 == r2 == {"committed": True, "amount": 250000}


def test_make_idempotency_key_is_deterministic_and_order_independent() -> None:
    k1 = make_idempotency_key("run1", "step1", "funds_commit", {"a": 1, "b": 2})
    k2 = make_idempotency_key("run1", "step1", "funds_commit", {"b": 2, "a": 1})
    assert k1 == k2
    assert len(k1) == 64
    assert make_idempotency_key("run1", "step1", "funds_commit", {"a": 1, "b": 3}) != k1


async def test_in_memory_artifact_store() -> None:
    store = InMemoryArtifactStore()
    ref = await store.put("run1", "plan.json", b'{"x": 1}', "application/json")
    assert await store.get(ref) == b'{"x": 1}'
    with pytest.raises(KeyError):
        await store.get("mem://nope")


# Postgres-backed variants
async def test_postgres_idempotency_guard(db: Database) -> None:
    guard = PostgresIdempotencyGuard(db)
    key = f"key-{uuid4()}"
    calls = {"n": 0}

    async def fx() -> dict[str, bool]:
        calls["n"] += 1
        return {"ok": True}

    r1 = await guard.once(key, fx)
    r2 = await guard.once(key, fx)
    assert calls["n"] == 1
    assert r1 == r2 == {"ok": True}


async def test_postgres_artifact_store(db: Database) -> None:
    store = PostgresArtifactStore(PostgresArtifactRepo(db))
    ref = await store.put("run1", "a.bin", b"\x00\x01\x02", "application/octet-stream")
    assert await store.get(ref) == b"\x00\x01\x02"
    with pytest.raises(KeyError):
        await store.get(str(uuid4()))


async def test_postgres_checkpointer_round_trip(db: Database) -> None:
    cp = PostgresCheckpointer(PostgresCheckpointRepo(db))
    run_id = f"run-{uuid4()}"
    assert await cp.resume(run_id) is None

    sc = StageContext(
        run_id=run_id,
        tenant=TenantContext(tenant_id="t"),
        goal="launch",
        stage=Stage.ACTION_PLAN,
        dialogue=["a", "b"],
        plan=Plan(task_types=["funds_commit"]),
    )
    await cp.save(run_id, "action_plan", sc)
    resumed = await cp.resume(run_id)
    assert resumed is not None
    assert resumed.run_id == run_id
    assert resumed.stage is Stage.ACTION_PLAN
    assert resumed.dialogue == ["a", "b"]
    assert resumed.plan is not None and resumed.plan.task_types == ["funds_commit"]


# guard must be race-free under concurrent consumers
async def test_postgres_guard_exactly_once_under_concurrency(db: Database) -> None:
    """Two consumers racing the same key must execute the side effect ONCE.

    The pre-fix guard did SELECT -> fn() -> INSERT (check-then-act), so concurrent
    racers both missed and both fired. The sleep widens the race window."""
    import asyncio

    guard = PostgresIdempotencyGuard(db)
    key = f"race-{uuid4()}"
    calls = {"n": 0}

    async def commit() -> dict[str, bool]:
        calls["n"] += 1
        await asyncio.sleep(0.1)
        return {"ok": True}

    results = await asyncio.gather(*(guard.once(key, commit) for _ in range(5)))
    assert calls["n"] == 1
    assert all(r == {"ok": True} for r in results)


async def test_in_memory_guard_exactly_once_under_concurrency() -> None:
    import asyncio

    guard = InMemoryIdempotencyGuard()
    calls = {"n": 0}

    async def commit() -> dict[str, bool]:
        calls["n"] += 1
        await asyncio.sleep(0.05)
        return {"ok": True}

    results = await asyncio.gather(*(guard.once("k", commit) for _ in range(5)))
    assert calls["n"] == 1
    assert all(r == {"ok": True} for r in results)
