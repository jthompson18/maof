"""Workflow-as-data: YAML DAG definitions, signed lifecycle,
topological execution on the result path with joins, templates, per-step approvals."""

from __future__ import annotations

import asyncio
import textwrap
from typing import Any
from uuid import uuid4

import pytest

from maof.agents.base import BaseL2Agent
from maof.agents.registry_runtime import AgentRegistry
from maof.errors import RegistryTrustError
from maof.orchestrator.coordinator import DefaultCoordinator, InProcessSubagent, QueueDispatcher
from maof.orchestrator.l1 import DefaultL1
from maof.orchestrator.lifecycle import (
    InMemoryResultStore,
    InMemoryRunWaker,
    ResultCollector,
    ResultEnvelope,
)
from maof.orchestrator.pipeline import Pipeline
from maof.runs.checkpoint import InMemoryCheckpointer
from maof.runs.idempotency import InMemoryIdempotencyGuard
from maof.runs.store import InMemoryRunStore
from maof.transport.fake import InMemoryBroker
from maof.transport.signing import Signer
from maof.types import L2Context, RunStatus, TaskResult, TenantContext
from maof.workers.pool import WorkerPool
from maof.workflows.definition import (
    WorkflowDefinition,
    bind_templates,
    load_workflow_yaml,
)
from maof.workflows.executor import WorkflowExecutor, WorkflowStage
from maof.workflows.promote import promote_run, to_yaml
from maof.workflows.store import InMemoryWorkflowRepo, WorkflowStore

WORKFLOW_YAML = textwrap.dedent("""
    name: po-cycle
    version: 1
    description: plan -> parallel [east, west] -> actualize
    steps:
      - id: plan
        task_type: purchase_plan
        input: {budget: "{{ context.budget }}"}
      - id: east
        task_type: order_placement
        depends_on: [plan]
        input: {region: east, plan_ref: "{{ steps.plan.output.plan_id }}"}
      - id: west
        task_type: order_placement
        depends_on: [plan]
        input: {region: west, plan_ref: "{{ steps.plan.output.plan_id }}"}
      - id: actualize
        task_type: reconciliation
        depends_on: [east, west]
        input: {regions: "2"}
    """)


# definition validation
def test_yaml_workflow_parses_and_validates() -> None:
    wf = load_workflow_yaml(WORKFLOW_YAML)
    assert wf.name == "po-cycle"
    assert [s.id for s in wf.steps] == ["plan", "east", "west", "actualize"]
    assert wf.steps[3].depends_on == ["east", "west"]


def test_workflow_rejects_cycles_unknown_deps_dup_ids() -> None:
    with pytest.raises(ValueError, match="cycle"):
        WorkflowDefinition.model_validate(
            {
                "name": "w",
                "version": 1,
                "steps": [
                    {"id": "a", "task_type": "t", "depends_on": ["b"]},
                    {"id": "b", "task_type": "t", "depends_on": ["a"]},
                ],
            }
        )
    with pytest.raises(ValueError, match="unknown"):
        WorkflowDefinition.model_validate(
            {
                "name": "w",
                "version": 1,
                "steps": [{"id": "a", "task_type": "t", "depends_on": ["zz"]}],
            }
        )
    with pytest.raises(ValueError, match="duplicate"):
        WorkflowDefinition.model_validate(
            {
                "name": "w",
                "version": 1,
                "steps": [{"id": "a", "task_type": "t"}, {"id": "a", "task_type": "t"}],
            }
        )


def test_bind_templates() -> None:
    bound = bind_templates(
        {"budget": "{{ context.budget }}", "ref": "{{ steps.plan.output.plan_id }}", "n": 3},
        context={"budget": "250000"},
        outputs={"plan": {"plan_id": "PLAN-7"}},
    )
    assert bound == {"budget": "250000", "ref": "PLAN-7", "n": 3}


# signed lifecycle
async def test_workflow_store_lifecycle_and_trust() -> None:
    signer = Signer({"default": "secret"})
    store = WorkflowStore(InMemoryWorkflowRepo(), signer)
    wf = load_workflow_yaml(WORKFLOW_YAML)

    await store.submit(wf)
    with pytest.raises(RegistryTrustError):
        await store.load("po-cycle")  # pending is not executable

    await store.approve("po-cycle", 1)
    loaded = await store.load("po-cycle")
    assert loaded.version == 1

    # tamper: swap the stored definition without re-signing
    entry = await store.repo.get("po-cycle", 1)
    assert entry is not None
    tampered = wf.model_copy(update={"description": "EVIL"})
    await store.repo.put(entry.model_copy(update={"definition": tampered}))
    with pytest.raises(RegistryTrustError):
        await store.load("po-cycle")

    # restore, then revoke destroys the signature
    await store.repo.put(entry)
    assert (await store.load("po-cycle")).name == "po-cycle"
    await store.revoke("po-cycle", 1)
    with pytest.raises(RegistryTrustError):
        await store.load("po-cycle")


async def test_workflow_approve_gated_and_audits_approver() -> None:
    from maof.authz import SCOPE_WORKFLOW_APPROVE
    from maof.errors import AuthzError
    from maof.identity import Principal

    class CaptureSink:
        def __init__(self) -> None:
            self.events: list[Any] = []

        async def emit(self, event: Any) -> None:
            self.events.append(event)

    sink = CaptureSink()
    store = WorkflowStore(InMemoryWorkflowRepo(), Signer({"default": "secret"}), event_sink=sink)
    await store.submit(load_workflow_yaml(WORKFLOW_YAML))

    # a principal without the approve scope cannot sign the definition
    with pytest.raises(AuthzError):
        await store.approve("po-cycle", 1, principal=Principal(id="mallory", scopes=[]))
    with pytest.raises(RegistryTrustError):
        await store.load("po-cycle")  # still pending: the denied approve never signed

    # an authorized principal approves, and the approver is recorded in the audit log
    approver = Principal(id="alice", roles=["wf-admin"], scopes=[SCOPE_WORKFLOW_APPROVE])
    await store.approve("po-cycle", 1, principal=approver)
    assert (await store.load("po-cycle")).name == "po-cycle"
    approved = [e for e in sink.events if e.event_type == "workflow_approved"]
    assert len(approved) == 1
    assert approved[0].actor is not None and approved[0].actor["id"] == "alice"


async def test_workflow_store_postgres(db) -> None:  # type: ignore[no-untyped-def]
    from maof.workflows.store import PostgresWorkflowRepo

    signer = Signer({"default": "secret"})
    store = WorkflowStore(PostgresWorkflowRepo(db), signer)
    name = f"wf-{uuid4()}"
    wf = load_workflow_yaml(WORKFLOW_YAML).model_copy(update={"name": name})
    await store.submit(wf)
    await store.approve(name, 1)
    assert (await store.load(name)).name == name
    await store.revoke(name, 1)
    with pytest.raises(RegistryTrustError):
        await store.load(name)


# execution on the result path
class EchoAgent(BaseL2Agent):
    """Echoes its payload back; emits a plan_id for the plan step."""

    name = "echo"
    accepted_task_types = ["purchase_plan", "order_placement", "reconciliation"]

    def __init__(self) -> None:
        super().__init__()
        self.handled: list[dict[str, Any]] = []

    async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
        self.handled.append(task)
        output = dict(task.get("payload", {}))
        if task["task_type"] == "purchase_plan":
            output["plan_id"] = "PLAN-7"
        return TaskResult(status="ok", task_id=task["task_id"], output=output)


async def _pump(
    worker: WorkerPool, collector: ResultCollector, queues: list[str], run_store: Any, run_id: str
) -> None:
    """Drive worker + collector until the run completes (embedded test pump)."""
    for _ in range(12):
        for queue in queues:
            await worker.consume(queue)
        await collector.drain()
        state = await run_store.get_state(run_id)
        if state.status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED):
            return


async def test_workflow_executes_dag_with_joins_and_templates() -> None:
    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    guard = InMemoryIdempotencyGuard()
    results = InMemoryResultStore()
    waker = InMemoryRunWaker(results)
    run_store = InMemoryRunStore()
    agent = EchoAgent()
    registry = AgentRegistry()
    registry.register_agent(agent)

    wf = load_workflow_yaml(WORKFLOW_YAML)
    executor = WorkflowExecutor(
        DefaultCoordinator(queue=QueueDispatcher(broker, signer, idempotency_guard=guard)),
        results=results,
        default_mode="queue",
    )
    l1 = DefaultL1(
        Pipeline([WorkflowStage(executor, wf, context={"budget": "250000"})]),
        run_store=run_store,
        checkpointer=InMemoryCheckpointer(),
        waker=waker,
    )
    collector_holder: dict[str, Any] = {}

    async def resume(run_id: str) -> None:
        await l1.resume_run(run_id)
        # after resume the run may be WAITING again with new dispatches queued

    collector = ResultCollector(
        broker, signer, results=results, waker=waker, resume=resume, queue="results"
    )
    collector_holder["c"] = collector

    out = await l1.run("launch po", TenantContext(tenant_id="t", attributes={}))
    assert out.status == "waiting"  # parked on the plan step's result
    run_id = out.run_id

    worker = WorkerPool(
        broker,
        signer,
        registry,
        idempotency_guard=guard,
        run_store=run_store,
        result_queue="results",
    )
    queues = ["tasks.purchase_plan", "tasks.order_placement", "tasks.reconciliation"]
    await _pump(worker, collector, queues, run_store, run_id)

    state = await run_store.get_state(run_id)
    assert state.status is RunStatus.COMPLETED

    # every step dispatched exactly once (replay-safe step identity)
    types_handled = sorted(t["task_type"] for t in agent.handled)
    assert types_handled == [
        "order_placement",
        "order_placement",
        "purchase_plan",
        "reconciliation",
    ]
    # templates bound prior outputs into dependent steps
    east_task = next(t for t in agent.handled if t.get("payload", {}).get("region") == "east")
    assert east_task["payload"]["plan_ref"] == "PLAN-7"
    # context templates bound
    plan_task = next(t for t in agent.handled if t["task_type"] == "purchase_plan")
    assert plan_task["payload"]["budget"] == "250000"


async def test_promoted_workflow_runs_with_new_goal() -> None:
    """The headline loop: a successful run is promoted into a signed workflow that
    re-executes the same shape under a brand-new goal (DESIGN.md §14.7)."""
    # 1. a prior run whose process worked: two queue-dispatched steps, recorded as results
    tenant = TenantContext(tenant_id="t", attributes={})
    prior_runs = InMemoryRunStore()
    prior_results = InMemoryResultStore()
    prior_id = await prior_runs.create(tenant, "the original purchase")
    await prior_runs.set_state(prior_id, status=RunStatus.COMPLETED)
    for i, (step_ref, task_type) in enumerate(
        [("plan", "purchase_plan"), ("order", "order_placement")]
    ):
        await prior_results.save(
            ResultEnvelope(
                run_id=prior_id,
                step_ref=step_ref,
                task_id=f"t{i}",
                task_type=task_type,
                idempotency_key=f"k{i}",
                tenant_id="t",
                result=TaskResult(status="ok", task_id=f"t{i}", output={}),
            )
        )

    # 2. promote -> draft -> signed, loadable workflow
    definition = await promote_run(
        prior_id, run_store=prior_runs, result_store=prior_results, name="reusable-cycle"
    )
    wf_store = WorkflowStore(InMemoryWorkflowRepo(), Signer({"default": "s"}))
    await wf_store.submit(load_workflow_yaml(to_yaml(definition)))
    await wf_store.approve("reusable-cycle", 1)
    loaded = await wf_store.load("reusable-cycle")  # approved + signature-valid

    # 3. execute the signed, promoted workflow under a brand-new goal
    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    guard = InMemoryIdempotencyGuard()
    results = InMemoryResultStore()
    waker = InMemoryRunWaker(results)
    run_store = InMemoryRunStore()
    agent = EchoAgent()
    registry = AgentRegistry()
    registry.register_agent(agent)

    executor = WorkflowExecutor(
        DefaultCoordinator(queue=QueueDispatcher(broker, signer, idempotency_guard=guard)),
        results=results,
        default_mode="queue",
    )
    l1 = DefaultL1(
        Pipeline([WorkflowStage(executor, loaded, context={})]),
        run_store=run_store,
        checkpointer=InMemoryCheckpointer(),
        waker=waker,
    )

    async def resume(run_id: str) -> None:
        await l1.resume_run(run_id)

    collector = ResultCollector(
        broker, signer, results=results, waker=waker, resume=resume, queue="results"
    )
    out = await l1.run("a brand-new purchase with different parameters", tenant)
    run_id = out.run_id

    worker = WorkerPool(
        broker,
        signer,
        registry,
        idempotency_guard=guard,
        run_store=run_store,
        result_queue="results",
    )
    await _pump(
        worker, collector, ["tasks.purchase_plan", "tasks.order_placement"], run_store, run_id
    )

    state = await run_store.get_state(run_id)
    assert state.status is RunStatus.COMPLETED
    # same pinned shape (plan -> order), re-executed under the new goal
    assert sorted(t["task_type"] for t in agent.handled) == ["order_placement", "purchase_plan"]


async def test_workflow_gate_step_parks_on_timer_then_passes_on_resume() -> None:
    """A ``kind: gate`` step (e.g. wait for flight start) parks the run WAITING on
    a timer; when the waker fires and the run resumes, the gate passes and
    downstream steps execute."""

    class MockLLM:
        async def generate(self, prompt: str, **kw: Any) -> str:
            return "done"

    class CaptureWaker:
        def __init__(self) -> None:
            self.scheduled: list[Any] = []

        async def schedule(self, run_id: str, condition: Any) -> None:
            self.scheduled.append((run_id, condition))

    wf = WorkflowDefinition.model_validate(
        {
            "name": "flighted",
            "version": 1,
            "steps": [
                {"id": "plan", "task_type": "purchase_plan", "coordination_mode": "in_process"},
                {
                    "id": "window_open",
                    "kind": "gate",
                    "depends_on": ["plan"],
                    "input": {"wait": "timer", "delay_s": 60},
                },
                {
                    "id": "traffic",
                    "task_type": "order_placement",
                    "coordination_mode": "in_process",
                    "depends_on": ["window_open"],
                },
            ],
        }
    )
    waker = CaptureWaker()
    executor = WorkflowExecutor(
        DefaultCoordinator(in_process=InProcessSubagent(MockLLM())),
        results=InMemoryResultStore(),
    )
    run_store = InMemoryRunStore()
    l1 = DefaultL1(
        Pipeline([WorkflowStage(executor, wf)]),
        run_store=run_store,
        checkpointer=InMemoryCheckpointer(),
        waker=waker,
    )

    out = await l1.run("goal", TenantContext(tenant_id="t"))
    assert out.status == "waiting"
    assert (await run_store.get_state(out.run_id)).status is RunStatus.WAITING
    ((run_id, condition),) = waker.scheduled
    assert run_id == out.run_id
    assert condition.kind == "timer" and condition.at is not None  # scheduled wake time

    # the waker fires -> resume: the gate passes and traffic executes
    resumed = await l1.resume_run(out.run_id)
    assert resumed.status == "completed"
    state = await run_store.get_state(out.run_id)
    assert state.status is RunStatus.COMPLETED


async def test_workflow_per_step_approval_blocks_then_proceeds() -> None:
    from maof.approval.service import ApprovalGate

    results = InMemoryResultStore()
    gate = ApprovalGate(timeout=5.0)
    wf = WorkflowDefinition.model_validate(
        {
            "name": "gated",
            "version": 1,
            "steps": [
                {
                    "id": "commit",
                    "task_type": "funds_commit",
                    "coordination_mode": "in_process",
                    "approval": {"required": True},
                    "input": {},
                }
            ],
        }
    )

    class MockLLM:
        async def generate(self, prompt: str, **kw: Any) -> str:
            return "committed"

    executor = WorkflowExecutor(
        DefaultCoordinator(in_process=InProcessSubagent(MockLLM())),
        results=results,
        approval_gate=gate,
    )
    run_store = InMemoryRunStore()
    l1 = DefaultL1(
        Pipeline([WorkflowStage(executor, wf)]),
        run_store=run_store,
        checkpointer=InMemoryCheckpointer(),
    )
    run_task = asyncio.create_task(l1.run("goal", TenantContext(tenant_id="t")))
    for _ in range(100):
        await asyncio.sleep(0.01)
        if gate._pending:  # noqa: SLF001
            break
    assert not run_task.done()  # blocked on the step approval
    await gate.resolve(next(iter(gate._pending)), approved=True)  # noqa: SLF001
    out = await asyncio.wait_for(run_task, timeout=5.0)
    assert out.status == "completed"
