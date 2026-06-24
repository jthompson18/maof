"""Execution lifecycle: result envelopes, waits/joins,
timers, external events, cancellation, run ops."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from maof.agents.base import BaseL2Agent
from maof.agents.registry_runtime import AgentRegistry
from maof.orchestrator.l1 import DefaultL1
from maof.orchestrator.lifecycle import (
    InMemoryResultStore,
    InMemoryRunWaker,
    NeedsWait,
    ResultCollector,
    ResultEnvelope,
    WakeCondition,
)
from maof.orchestrator.messages import parse_message
from maof.orchestrator.pipeline import Pipeline
from maof.persistence.postgres import Database
from maof.runs.checkpoint import InMemoryCheckpointer
from maof.runs.idempotency import InMemoryIdempotencyGuard
from maof.runs.store import InMemoryRunStore
from maof.transport.fake import InMemoryBroker
from maof.transport.signing import Signer
from maof.types import (
    L2Context,
    RunStatus,
    StageContext,
    TaskResult,
    TenantContext,
)
from maof.workers.pool import WorkerPool


def _task_message(
    run_id: str, *, step_ref: str = "reserve:0", key: str = "k1", tenant: str = "t"
) -> dict[str, Any]:
    return {
        "envelope": {"run_id": run_id, "tenant_id": tenant, "intent_id": None, "stage": "publish"},
        "task": {
            "task_id": "t1",
            "task_type": "funds_commit",
            "priority": 5,
            "description": "buy",
            "idempotency_key": key,
            "step_ref": step_ref,
        },
        "policy_flags": {},
        "toolset": [],
        "data_pointers": {},
        "semantic_model": {},
        "timestamp": "now",
    }


class EchoAgent(BaseL2Agent):
    name = "echo"
    accepted_task_types = ["funds_commit"]

    async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
        return TaskResult(status="ok", task_id=task["task_id"], output={"po_number": "PO-9"})


async def _worker_system() -> tuple[InMemoryBroker, Signer, WorkerPool, AgentRegistry]:
    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    registry = AgentRegistry()
    registry.register_agent(EchoAgent())
    pool = WorkerPool(
        broker,
        signer,
        registry,
        idempotency_guard=InMemoryIdempotencyGuard(),
        result_queue="results",
    )
    return broker, signer, pool, registry


# the worker always publishes a signed result envelope
async def test_worker_publishes_signed_result_envelope() -> None:
    broker, signer, pool, _ = await _worker_system()
    import json

    body = json.dumps(_task_message("run-1")).encode()
    await broker.publish(
        "tasks.funds_commit",
        body,
        headers=signer.headers(body),
        message_id="k1",
        correlation_id="c",
    )
    await pool.consume("tasks.funds_commit")

    assert broker.depth("results") == 1
    raw, headers, message_id, correlation_id = broker.peek("results")[0]
    signer.verify(raw, headers)  # the envelope is signed
    assert message_id == "result:k1"  # deterministic result id
    assert correlation_id == "run-1"
    envelope = ResultEnvelope.model_validate(parse_message(raw))
    assert envelope.run_id == "run-1"
    assert envelope.step_ref == "reserve:0"
    assert envelope.result.output == {"po_number": "PO-9"}


# collector: persist + wake (join of N, exactly once)
async def test_collector_persists_and_join_resumes_exactly_once() -> None:
    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    results = InMemoryResultStore()
    waker = InMemoryRunWaker(results)  # join checks count against the result store
    resumed: list[str] = []

    async def resume(run_id: str) -> None:
        resumed.append(run_id)

    collector = ResultCollector(
        broker, signer, results=results, waker=waker, resume=resume, queue="results"
    )
    await waker.schedule(
        "run-1", WakeCondition(kind="results_ready", step_ref="traffic", expected=2)
    )

    def _envelope(i: int) -> ResultEnvelope:
        return ResultEnvelope(
            run_id="run-1",
            step_ref="traffic",
            task_id=f"t{i}",
            idempotency_key=f"key-{i}",
            tenant_id="t",
            result=TaskResult(status="ok", task_id=f"t{i}", output={"i": i}),
        )

    from maof.orchestrator.lifecycle import publish_result

    await publish_result(broker, signer, queue="results", envelope=_envelope(1))
    await collector.drain()
    assert resumed == []  # join incomplete (1 of 2)

    await publish_result(broker, signer, queue="results", envelope=_envelope(2))
    await collector.drain()
    assert resumed == ["run-1"]  # join complete -> resumed once
    assert len(await results.list("run-1", "traffic")) == 2

    # duplicate redelivery of an already-seen result must NOT re-resume
    await publish_result(broker, signer, queue="results", envelope=_envelope(2))
    await collector.drain()
    assert resumed == ["run-1"]
    assert len(await results.list("run-1", "traffic")) == 2  # deduped


# timers + external events
async def test_timer_and_event_wakeups_fire_exactly_once() -> None:
    waker = InMemoryRunWaker()
    past = (datetime.now(UTC) - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    await waker.schedule("run-t", WakeCondition(kind="timer", at=past))
    await waker.schedule("run-e", WakeCondition(kind="external_event", event_key="shipment:ok"))

    assert await waker.due_timers() == ["run-t"]
    assert await waker.due_timers() == []  # claimed

    assert await waker.fire_event("shipment:ok") == ["run-e"]
    assert await waker.fire_event("shipment:ok") == []  # claimed


async def test_poller_sweeps_joins_satisfied_before_schedule() -> None:
    """Distributed race: a fast worker's result lands BEFORE the run parks and
    schedules its join wakeup, so the collector's satisfy_results finds nothing.
    The WakerPoller must sweep already-satisfied joins."""
    from maof.orchestrator.lifecycle import WakerPoller

    results = InMemoryResultStore()
    waker = InMemoryRunWaker(results)
    envelope = ResultEnvelope(
        run_id="r-race",
        step_ref="reserve",
        task_id="t1",
        idempotency_key="k-race",
        tenant_id="t",
        result=TaskResult(status="ok", task_id="t1", output={}),
    )
    await results.save(envelope)  # the result arrives FIRST...
    await waker.schedule(  # ...and only then does the run park on the join
        "r-race", WakeCondition(kind="results_ready", step_ref="reserve", expected=1)
    )

    resumed: list[str] = []

    async def resume(run_id: str) -> None:
        resumed.append(run_id)

    poller = WakerPoller(waker, resume)
    assert "r-race" in await poller.tick()  # the sweeper fires the stale join
    assert resumed == ["r-race"]
    assert await poller.tick() == []  # claimed exactly once


async def test_postgres_waker_sweeps_stale_join(db: Database) -> None:
    from maof.orchestrator.lifecycle import PostgresResultStore, PostgresRunWaker

    waker = PostgresRunWaker(db)
    results = PostgresResultStore(db)
    run_id = f"run-{uuid4()}"
    envelope = ResultEnvelope(
        run_id=run_id,
        step_ref="reserve",
        task_id="t1",
        idempotency_key=f"key-{uuid4()}",
        tenant_id="t",
        result=TaskResult(status="ok", task_id="t1", output={}),
    )
    await results.save(envelope)  # result first, wakeup second (the race)
    await waker.schedule(
        run_id, WakeCondition(kind="results_ready", step_ref="reserve", expected=1)
    )
    assert run_id in await waker.due_joins()
    assert run_id not in await waker.due_joins()  # claimed


async def test_postgres_waker_round_trip(db: Database) -> None:
    from maof.orchestrator.lifecycle import PostgresResultStore, PostgresRunWaker

    waker = PostgresRunWaker(db)
    results = PostgresResultStore(db)
    run_id = f"run-{uuid4()}"
    await waker.schedule(
        run_id, WakeCondition(kind="results_ready", step_ref="reserve", expected=1)
    )
    envelope = ResultEnvelope(
        run_id=run_id,
        step_ref="reserve",
        task_id="t1",
        idempotency_key=f"key-{uuid4()}",
        tenant_id="t",
        result=TaskResult(status="ok", task_id="t1", output={"ok": True}),
    )
    assert await results.save(envelope) is True
    assert await results.save(envelope) is False  # idempotent
    assert await waker.satisfy_results(run_id, "reserve") is True  # join complete, claimed
    assert await waker.satisfy_results(run_id, "reserve") is False  # already fired

    past = (datetime.now(UTC) - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    await waker.schedule(run_id, WakeCondition(kind="timer", at=past))
    assert run_id in await waker.due_timers()
    assert run_id not in await waker.due_timers()


# NeedsWait: run parks WAITING, resume completes without re-running
async def test_needs_wait_then_resume_completes() -> None:
    results = InMemoryResultStore()
    waker = InMemoryRunWaker()
    executed: list[str] = []

    class PlanStage:
        name = "chat"

        async def execute(self, sc: StageContext) -> StageContext:
            executed.append("chat")
            return sc

    class JoinStage:
        name = "action_plan"

        async def execute(self, sc: StageContext) -> StageContext:
            executed.append("join-attempt")
            if len(await results.list(sc.run_id, "reserve")) < 1:
                raise NeedsWait(WakeCondition(kind="results_ready", step_ref="reserve", expected=1))
            sc.extras["joined"] = True
            return sc

    store = InMemoryRunStore()
    l1 = DefaultL1(
        Pipeline([PlanStage(), JoinStage()]),
        run_store=store,
        checkpointer=InMemoryCheckpointer(),
        waker=waker,
    )
    out = await l1.run("goal", TenantContext(tenant_id="t"))
    assert out.status == "waiting"
    assert (await store.get_state(out.run_id)).status is RunStatus.WAITING
    assert waker.scheduled and waker.scheduled[0][0] == out.run_id

    await results.save(
        ResultEnvelope(
            run_id=out.run_id,
            step_ref="reserve",
            task_id="t1",
            idempotency_key="k",
            tenant_id="t",
            result=TaskResult(status="ok", task_id="t1"),
        )
    )
    final = await l1.resume_run(out.run_id)
    assert final.status == "completed"
    assert executed == ["chat", "join-attempt", "join-attempt"]  # chat NOT re-run


# cancellation
async def test_cancel_stops_run_before_next_stage() -> None:
    store = InMemoryRunStore()
    executed: list[str] = []

    class S1:
        name = "chat"

        async def execute(self, sc: StageContext) -> StageContext:
            executed.append("s1")
            await store.request_cancel(sc.run_id)  # ops cancels mid-run
            return sc

    class S2:
        name = "publish"

        async def execute(self, sc: StageContext) -> StageContext:
            executed.append("s2")
            return sc

    l1 = DefaultL1(Pipeline([S1(), S2()]), run_store=store)
    out = await l1.run("goal", TenantContext(tenant_id="t"))
    assert out.status == "cancelled"
    assert executed == ["s1"]  # s2 never ran
    assert (await store.get_state(out.run_id)).status is RunStatus.CANCELLED


async def test_worker_skips_tasks_for_cancelled_runs() -> None:
    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    registry = AgentRegistry()
    handled: list[str] = []

    class Probe(BaseL2Agent):
        name = "probe"
        accepted_task_types = ["funds_commit"]

        async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
            handled.append(task["task_id"])
            return TaskResult(status="ok", task_id=task["task_id"])

    registry.register_agent(Probe())
    store = InMemoryRunStore()
    run_id = await store.create(TenantContext(tenant_id="t"), "g")
    await store.request_cancel(run_id)

    pool = WorkerPool(broker, signer, registry, run_store=store, result_queue=None)
    import json

    body = json.dumps(_task_message(run_id)).encode()
    await broker.publish(
        "tasks.funds_commit", body, headers=signer.headers(body), message_id="k", correlation_id="c"
    )
    await pool.consume("tasks.funds_commit")
    assert handled == []  # side effect never fired for the cancelled run


# run ops
async def test_run_ops_list_show_cancel_wake(db: Database) -> None:
    from maof.orchestrator.lifecycle import PostgresRunWaker
    from maof.runs.ops import RunOps
    from maof.runs.store import PostgresRunStore

    store = PostgresRunStore(db)
    waker = PostgresRunWaker(db)
    tenant = TenantContext(tenant_id=f"t-{uuid4()}")
    run_id = await store.create(tenant, "reference goal")
    await store.set_state(run_id, status=RunStatus.WAITING)
    await waker.schedule(run_id, WakeCondition(kind="external_event", event_key="funds:cleared"))

    ops = RunOps(db, waker=waker)
    listed = await ops.list_runs(tenant_id=tenant.tenant_id)
    assert any(r["run_id"] == run_id for r in listed)
    shown = await ops.show(run_id)
    assert shown is not None and shown["status"] == "waiting"

    woken = await ops.wake("funds:cleared")
    assert run_id in woken

    await ops.cancel(run_id)
    state = await store.get_state(run_id)
    assert state.cancel_requested is True
    assert state.status is RunStatus.CANCELLED  # WAITING runs cancel immediately
