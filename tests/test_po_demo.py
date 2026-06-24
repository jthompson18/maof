"""po_demo auxiliary scenarios + the distributed entry.

The lifecycle headline (signed workflow, clamp, exactly-once, two-party
approval, post_result quarantine, cancellation, semantic search) lives in
``tests/test_scenario.py``. This file proves the remaining bullets on the
SAME reference system — both coordination modes, declared context delegation,
the autonomous loop, the eval gate — and the embedded/distributed entry
running the full workflow on Postgres-backed durability.
"""

from __future__ import annotations

from examples.po_demo import demo
from examples.po_demo.scenario import build_scenario


async def test_both_coordination_modes() -> None:
    ns = await build_scenario()
    modes = await demo.run_both_coordination_modes(ns)
    assert modes["queued_status"] == "dispatched"
    # independent serve -> queue (mode a), routed via Fulfillment's REGISTRY queue
    assert modes["order_placement_queue_depth"] == 1
    assert modes["queue_name"] == "suppliers.fulfillment.v1"
    assert modes["in_process_summary"]  # interdependent -> in-process (mode b)


async def test_context_delegation() -> None:
    ns = await build_scenario()
    env = await demo.run_context_delegation(ns)
    assert [dp.alias for dp in env.data_pointers] == [
        "purchase_plan"
    ]  # rate_card/po_template de-duped
    assert "rate_card" not in env.semantic_model
    assert env.extras["delegated_context"][0]["agent"] == "commitments"
    assert any(e.event_type == "context_delegated" for e in ns.sink.events)


async def test_autonomous_loop() -> None:
    ns = await build_scenario()
    subresults = await demo.run_autonomous_loop(ns)
    assert len(subresults) == 2  # >= 2 subagents under delegation contracts
    assert all(s["artifacts"] for s in subresults)  # distilled + artifact refs


async def test_eval_gate() -> None:
    report, passed = await demo.run_eval_gate(min_pass_rate=0.6)
    assert report.total == 3
    assert passed  # 2/3 honored the spend-policy chain
    _, strict = await demo.run_eval_gate(min_pass_rate=0.9)
    assert not strict  # the gate discriminates


async def test_main_distributed_embedded_mode(monkeypatch, db) -> None:  # type: ignore[no-untyped-def]
    """Embedded mode: EMBEDDED_L2=true runs the WHOLE reference workflow
    orchestrator, vendor workers, result collector, waker poller — in ONE process
    over the in-memory broker, with Postgres-backed durability (run store,
    checkpoints, idempotency, results, wakeups, registry, workflows)."""
    import os

    dsn = os.getenv("MAOF_TEST_DATABASE_URL", "postgresql://maof:maof@127.0.0.1:55432/maof")
    monkeypatch.setenv("DB_URL", dsn)
    monkeypatch.setenv("EMBEDDED_L2", "true")
    monkeypatch.setenv("MSG_SIGNING_SECRET", "demo-secret")
    # the registry-search embedder must match the dimension the DB was migrated with
    from tests.conftest import TEST_EMBED_DIM

    monkeypatch.setenv("EMBED_DIMENSION", str(TEST_EMBED_DIM))

    from examples.po_demo.main_distributed import run_distributed

    out = await run_distributed()
    assert out["status"] == "completed"
    assert out["commits"] == 1  # the reserve committed exactly once
    assert out["committed_usd"] == 250_000
    assert out["invoice_open"] is True  # the workflow ran through to the invoice
