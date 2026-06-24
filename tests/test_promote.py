"""Promote a successful run into a draft WorkflowDefinition (workflow-as-data on-ramp).

Covers deriving steps from the authoritative result store (step_ref + task_type),
gating on a COMPLETED run, a conservative linear dependency chain, and auto-filling
agent_version pins from the identifier-only dispatch trace. The remaining lossy parts
(input templates, parallelism) are left for human review before the existing
submit -> approve(sign) -> run pipeline takes over.
"""

from __future__ import annotations

import pytest

from maof.orchestrator.coordinator import QueueDispatcher
from maof.orchestrator.delegation import DelegationContract
from maof.orchestrator.lifecycle import InMemoryResultStore, ResultEnvelope
from maof.registry.models import AgentManifest
from maof.runs.store import InMemoryRunStore
from maof.transport.fake import InMemoryBroker
from maof.transport.signing import Signer
from maof.types import RunStatus, StageContext, TaskResult, TenantContext, TraceEntry
from maof.workflows.definition import WorkflowDefinition, load_workflow_yaml
from maof.workflows.promote import PromotionError, promote_run, to_yaml
from maof.workflows.store import InMemoryWorkflowRepo, WorkflowStore


async def _seed_run(
    run_store: InMemoryRunStore,
    result_store: InMemoryResultStore,
    *,
    steps: list[tuple[str, str, dict[str, object]]],
    status: RunStatus = RunStatus.COMPLETED,
    tenant_id: str = "t1",
    goal: str = "buy 100 widgets",
    agent_versions: dict[str, str] | None = None,
) -> str:
    tenant = TenantContext(tenant_id=tenant_id)
    run_id = await run_store.create(tenant, goal)
    if status is not RunStatus.PENDING:
        await run_store.set_state(run_id, status=status)
    for i, (step_ref, task_type, output) in enumerate(steps):
        await result_store.save(
            ResultEnvelope(
                run_id=run_id,
                step_ref=step_ref,
                task_id=f"task-{i}",
                task_type=task_type,
                idempotency_key=f"key-{i}",
                tenant_id=tenant_id,
                result=TaskResult(status="ok", task_id=f"task-{i}", output=output),
            )
        )
    for step_ref, version in (agent_versions or {}).items():
        await run_store.append_trace(
            run_id,
            TraceEntry(
                run_id=run_id,
                seq=0,
                kind="delegation_dispatched",
                step=step_ref,
                data={
                    "mode": "queue",
                    "task_type": "x",
                    "objective": "o",
                    "agent_id": f"agent-{step_ref}",
                    "agent_version": version,
                },
            ),
        )
    return run_id


async def test_promote_builds_linear_definition() -> None:
    run_store = InMemoryRunStore()
    result_store = InMemoryResultStore()
    run_id = await _seed_run(
        run_store,
        result_store,
        steps=[
            ("plan", "purchase_plan", {"plan_id": "P1"}),
            ("reserve", "funds_commit", {"amount_usd": 500}),
            ("order", "order_placement", {"order_id": "O1"}),
        ],
    )

    definition = await promote_run(
        run_id, run_store=run_store, result_store=result_store, name="po-cycle"
    )

    assert isinstance(definition, WorkflowDefinition)
    assert definition.name == "po-cycle"
    assert definition.version == 1
    assert [s.id for s in definition.steps] == ["plan", "reserve", "order"]
    assert [s.task_type for s in definition.steps] == [
        "purchase_plan",
        "funds_commit",
        "order_placement",
    ]
    # conservative linear chain: each step depends on the previous one
    assert definition.steps[0].depends_on == []
    assert definition.steps[1].depends_on == ["plan"]
    assert definition.steps[2].depends_on == ["reserve"]
    # everything observed on the result path is queue-dispatched
    assert all(s.coordination_mode == "queue" for s in definition.steps)
    # phase 1 infers no version pins (added in phase 2)
    assert all(s.pins == {} for s in definition.steps)


async def test_promote_requires_completed_run() -> None:
    run_store = InMemoryRunStore()
    result_store = InMemoryResultStore()
    run_id = await _seed_run(
        run_store,
        result_store,
        steps=[("plan", "purchase_plan", {})],
        status=RunStatus.PENDING,
    )
    with pytest.raises(PromotionError):
        await promote_run(run_id, run_store=run_store, result_store=result_store, name="x")


async def test_promote_empty_results_raises() -> None:
    run_store = InMemoryRunStore()
    result_store = InMemoryResultStore()
    run_id = await _seed_run(run_store, result_store, steps=[])
    with pytest.raises(PromotionError):
        await promote_run(run_id, run_store=run_store, result_store=result_store, name="x")


async def test_promote_unknown_run_raises() -> None:
    run_store = InMemoryRunStore()
    result_store = InMemoryResultStore()
    with pytest.raises(PromotionError):
        await promote_run("nope", run_store=run_store, result_store=result_store, name="x")


async def test_promote_dedupes_repeated_step_ref() -> None:
    run_store = InMemoryRunStore()
    result_store = InMemoryResultStore()
    run_id = await _seed_run(
        run_store,
        result_store,
        steps=[
            ("plan", "purchase_plan", {"plan_id": "P1"}),
            ("plan", "purchase_plan", {"plan_id": "P1-retry"}),
            ("order", "order_placement", {}),
        ],
    )
    definition = await promote_run(run_id, run_store=run_store, result_store=result_store, name="x")
    assert [s.id for s in definition.steps] == ["plan", "order"]


async def test_to_yaml_roundtrips_through_loader() -> None:
    run_store = InMemoryRunStore()
    result_store = InMemoryResultStore()
    run_id = await _seed_run(
        run_store,
        result_store,
        steps=[("plan", "purchase_plan", {}), ("order", "order_placement", {})],
    )
    definition = await promote_run(
        run_id, run_store=run_store, result_store=result_store, name="po-cycle"
    )

    reloaded = load_workflow_yaml(to_yaml(definition))

    assert reloaded.name == definition.name
    assert reloaded.version == definition.version
    assert [s.id for s in reloaded.steps] == [s.id for s in definition.steps]
    assert [s.task_type for s in reloaded.steps] == [s.task_type for s in definition.steps]
    assert [s.depends_on for s in reloaded.steps] == [s.depends_on for s in definition.steps]


async def test_promote_fills_agent_version_pins() -> None:
    run_store = InMemoryRunStore()
    result_store = InMemoryResultStore()
    run_id = await _seed_run(
        run_store,
        result_store,
        steps=[
            ("plan", "purchase_plan", {}),
            ("reserve", "funds_commit", {}),
            ("order", "order_placement", {}),
        ],
        agent_versions={"reserve": "v3", "order": "v1"},
    )

    definition = await promote_run(run_id, run_store=run_store, result_store=result_store, name="x")

    pins = {s.id: s.pins for s in definition.steps}
    assert pins["plan"] == {}  # no dispatch trace recorded -> no pin inferred
    assert pins["reserve"] == {"agent_version": "v3"}
    assert pins["order"] == {"agent_version": "v1"}


async def test_dispatch_records_resolved_agent_in_trace() -> None:
    manifest = AgentManifest(
        id="catalog",
        kind="l2_agent",
        name="Catalog",
        version="v2",
        endpoint="python:demo:Catalog",
        accepted_task_types=["purchase_plan"],
        queue="tasks.purchase_plan",
    )

    class _Loader:
        async def agents_for_task_type(
            self, task_type: str, *, tenant: TenantContext
        ) -> list[AgentManifest]:
            return [manifest] if task_type == "purchase_plan" else []

    run_store = InMemoryRunStore()
    dispatcher = QueueDispatcher(
        InMemoryBroker(), Signer({"default": "s"}), registry_loader=_Loader()
    )
    sc = StageContext(
        run_id="r1", tenant=TenantContext(tenant_id="t"), goal="g", run_store=run_store
    )

    await dispatcher.dispatch(
        DelegationContract(
            objective="plan it",
            output_format="t",
            task_type="purchase_plan",
            step_ref="plan",
        ),
        sc,
    )

    dispatched = [e for e in await run_store.read_trace("r1") if e.kind == "delegation_dispatched"]
    assert len(dispatched) == 1
    entry = dispatched[0]
    assert entry.step == "plan"
    assert entry.data["agent_id"] == "catalog"
    assert entry.data["agent_version"] == "v2"
    # identifier-only: the sensitive endpoint must never leak into the trace
    assert "endpoint" not in entry.data


async def test_promoted_draft_passes_signed_lifecycle() -> None:
    run_store = InMemoryRunStore()
    result_store = InMemoryResultStore()
    run_id = await _seed_run(
        run_store,
        result_store,
        steps=[("plan", "purchase_plan", {}), ("order", "order_placement", {})],
        agent_versions={"plan": "v1"},
    )
    definition = await promote_run(
        run_id, run_store=run_store, result_store=result_store, name="reusable"
    )

    # the draft round-trips through YAML and clears the signed-workflow trust pipeline
    store = WorkflowStore(InMemoryWorkflowRepo(), Signer({"default": "s"}))
    await store.submit(load_workflow_yaml(to_yaml(definition)))
    await store.approve("reusable", 1)
    loaded = await store.load("reusable")  # raises unless approved + signature-valid

    assert loaded.name == "reusable"
    assert [s.id for s in loaded.steps] == ["plan", "order"]
    assert loaded.steps[0].pins == {"agent_version": "v1"}  # pins survive the round-trip


def test_cli_parses_runs_promote() -> None:
    from maof.cli import _build_parser

    args = _build_parser().parse_args(
        ["runs", "promote", "run-123", "--name", "w", "-o", "draft.yaml"]
    )
    assert args.command == "runs"
    assert args.runs_command == "promote"
    assert args.run_id == "run-123"
    assert args.name == "w"
    assert args.out == "draft.yaml"


async def test_promote_enforces_author_scope_when_principal_present() -> None:
    from maof.authz import SCOPE_WORKFLOW_AUTHOR
    from maof.errors import AuthzError
    from maof.identity import Principal

    run_store = InMemoryRunStore()
    result_store = InMemoryResultStore()
    run_id = await _seed_run(run_store, result_store, steps=[("plan", "purchase_plan", {})])

    # a principal lacking the scope is denied
    with pytest.raises(AuthzError):
        await promote_run(
            run_id,
            run_store=run_store,
            result_store=result_store,
            name="x",
            principal=Principal(id="mallory", scopes=[]),
        )

    # the scope grants access
    ok = await promote_run(
        run_id,
        run_store=run_store,
        result_store=result_store,
        name="x",
        principal=Principal(id="alice", scopes=[SCOPE_WORKFLOW_AUTHOR]),
    )
    assert ok.name == "x"

    # no principal == trusted in-process caller (RBAC not engaged) -> still works
    trusted = await promote_run(run_id, run_store=run_store, result_store=result_store, name="x")
    assert trusted.name == "x"
