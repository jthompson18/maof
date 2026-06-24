"""Agents, registry, both orchestration modes, coordinator routing, and the worker pool."""

from __future__ import annotations

from typing import Any

import pytest

from maof.agents.base import BaseL2Agent
from maof.agents.registry_runtime import AgentRegistry, register_l2_agent
from maof.orchestrator.coordinator import DefaultCoordinator, InProcessSubagent, QueueDispatcher
from maof.orchestrator.delegation import DelegationContract
from maof.orchestrator.l1 import DefaultL1
from maof.orchestrator.loop import OrchestratorLoop
from maof.orchestrator.pipeline import Pipeline
from maof.orchestrator.stages import (
    ActionPlanStage,
    ApprovalStage,
    ChatStage,
    IntentStage,
    PublishStage,
)
from maof.policy.engine import NativePolicyEngine
from maof.runs.artifacts import InMemoryArtifactStore
from maof.runs.checkpoint import InMemoryCheckpointer
from maof.runs.idempotency import InMemoryIdempotencyGuard
from maof.runs.store import InMemoryRunStore
from maof.schemas.registry import SchemaRegistry
from maof.transport.fake import InMemoryBroker
from maof.transport.signing import Signer
from maof.types import EffortBudget, L2Context, Plan, StageContext, Task, TaskResult, TenantContext
from maof.workers.pool import WorkerPool

FUNDS_COMMIT_SCHEMA = {
    "type": "object",
    "required": ["task_type", "description", "priority"],
    "properties": {
        "task_type": {"const": "funds_commit"},
        "description": {"type": "string", "minLength": 1},
        "priority": {"type": "integer", "minimum": 1, "maximum": 9},
    },
}


class MockLLM:
    def __init__(self, output: str = "subagent result") -> None:
        self._output = output

    async def generate(
        self, prompt: str, *, system: Any = None, json_schema: Any = None, **opts: Any
    ) -> str:
        return self._output


# registry
def test_agent_registry_lookup() -> None:
    reg = AgentRegistry()

    class Commitments(BaseL2Agent):
        name = "commitments"
        accepted_task_types = ["funds_commit", "reconciliation"]

        async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
            return TaskResult(status="ok", task_id=task["task_id"])

    agent = Commitments()
    reg.register_agent(agent)
    assert reg.agent("commitments") is agent
    assert reg.agent_for_task_type("funds_commit") is agent
    assert reg.agent_for_task_type("reconciliation") is agent
    assert reg.agent_for_task_type("unknown") is None


def test_register_decorator() -> None:
    reg = AgentRegistry()

    @register_l2_agent(reg)
    class Fulfillment(BaseL2Agent):
        name = "fulfillment"
        accepted_task_types = ["order_placement"]

        async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
            return TaskResult(status="ok", task_id=task["task_id"])

    assert reg.agent_for_task_type("order_placement") is not None


# pipeline editing
def test_pipeline_insert_and_replace() -> None:
    class S:
        def __init__(self, name: str) -> None:
            self.name = name

        async def execute(self, sc: StageContext) -> StageContext:
            return sc

    pipeline = Pipeline([S("chat"), S("publish")])
    pipeline.insert_before("publish", S("action_plan"))
    assert pipeline.stage_names == ["chat", "action_plan", "publish"]
    pipeline.replace("action_plan", S("intent_synthesis"))
    assert pipeline.stage_names == ["chat", "intent_synthesis", "publish"]
    with pytest.raises(Exception):  # noqa: B017 - MAOFError for missing stage
        pipeline.insert_before("nope", S("x"))


# workflow end-to-end
async def test_workflow_end_to_end() -> None:
    broker = InMemoryBroker()
    signer = Signer({"default": "secret"})
    schemas = SchemaRegistry()
    schemas.register("funds_commit.v1", FUNDS_COMMIT_SCHEMA)

    received: list[dict[str, Any]] = []

    class Commitments(BaseL2Agent):
        name = "commitments"
        accepted_task_types = ["funds_commit"]

        async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
            received.append(task)
            return TaskResult(status="ok", task_id=task["task_id"])

    registry = AgentRegistry()
    registry.register_agent(Commitments())

    async def planner(sc: StageContext) -> Plan:
        return Plan(
            tasks=[
                Task(
                    task_id="t1",
                    task_type="funds_commit",
                    description="Commit next-quarter east-region buy",
                    idempotency_key="seed",
                )
            ],
            task_types=["funds_commit"],
        )

    pipeline = Pipeline(
        [
            ChatStage(),
            IntentStage(task_types=["funds_commit"]),
            ActionPlanStage(planner),
            ApprovalStage(hitl_enabled=False),
            PublishStage(
                broker,
                signer,
                schema_registry=schemas,
                idempotency_guard=InMemoryIdempotencyGuard(),
            ),
        ]
    )
    l1 = DefaultL1(pipeline, run_store=InMemoryRunStore(), checkpointer=InMemoryCheckpointer())
    result = await l1.run("run the replenishment cycle", TenantContext(tenant_id="buyer-1"))

    assert result.status == "completed"
    assert broker.depth("tasks.funds_commit") == 1  # a signed, schema-valid task is queued

    worker = WorkerPool(broker, signer, registry, schema_registry=schemas)
    await worker.consume("tasks.funds_commit")  # verify sig -> validate schema -> dispatch
    assert len(received) == 1
    assert received[0]["task_type"] == "funds_commit"
    assert broker.depth("tasks.funds_commit") == 0


async def test_worker_rejects_tampered_message() -> None:
    broker = InMemoryBroker()
    signer = Signer({"default": "secret"})
    registry = AgentRegistry()

    class Commitments(BaseL2Agent):
        name = "commitments"
        accepted_task_types = ["funds_commit"]

        async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
            return TaskResult(status="ok", task_id=task["task_id"])

    registry.register_agent(Commitments())
    await broker.ensure_topology(_dlq_spec())
    # publish with a valid signature, then tamper the stored body
    await broker.publish(
        "tasks.funds_commit",
        b'{"envelope":{"tenant_id":"t","intent_id":null},"task":{"task_type":"funds_commit"}}',
        headers=signer.headers(b"different-body"),  # signature won't match the body
        message_id="m1",
        correlation_id="c1",
    )
    worker = WorkerPool(broker, signer, registry, require_signature=True)
    await worker.consume("tasks.funds_commit")
    # exhausted retries -> dead-lettered (handler kept raising SignatureError)
    assert broker.depth("tasks.funds_commit.dlq") == 1


def _dlq_spec() -> list[Any]:
    from maof.types import QueueSpec

    return [
        QueueSpec(name="tasks.funds_commit", dlq_name="tasks.funds_commit.dlq", retry_steps=["1s"])
    ]


# coordinator routing
async def test_coordinator_routes_both_modes() -> None:
    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    coordinator = DefaultCoordinator(
        queue=QueueDispatcher(broker, signer),
        in_process=InProcessSubagent(MockLLM("in-process answer")),
    )
    sc = StageContext(
        run_id="r1", tenant=TenantContext(tenant_id="t"), goal="g", run_store=InMemoryRunStore()
    )

    independent = DelegationContract(
        objective="serve ads on east-region",
        output_format="order_placement.v1",
        coordination_mode="queue",
        task_type="order_placement",
    )
    queued = await coordinator.dispatch(independent, sc)
    assert queued.status == "dispatched"
    assert broker.depth("tasks.order_placement") == 1  # independent -> queue (mode a)

    interdependent = DelegationContract(
        objective="reconcile plan against commitments",
        output_format="text",
        coordination_mode="in_process",
    )
    shared = await coordinator.dispatch(interdependent, sc)
    assert shared.summary == "in-process answer"  # interdependent -> in-process (mode b)


# autonomous loop
async def test_orchestrator_loop_spawns_subagents_with_artifact_refs() -> None:
    artifacts = InMemoryArtifactStore()
    coordinator = DefaultCoordinator(
        in_process=InProcessSubagent(MockLLM("x" * 5000), artifacts=artifacts, summary_chars=100)
    )
    calls = {"n": 0}

    async def planner(sc: StageContext, subresults: list[Any]) -> list[DelegationContract]:
        if calls["n"] == 0:
            calls["n"] += 1
            return [
                DelegationContract(objective="research region mix", output_format="text"),
                DelegationContract(objective="estimate unit cost", output_format="text"),
            ]
        return []

    loop = OrchestratorLoop(
        MockLLM(),
        coordinator,
        EffortBudget(max_subagents=5),
        NativePolicyEngine(),
        planner=planner,
        max_iterations=3,
    )
    sc = StageContext(
        run_id="r1",
        tenant=TenantContext(tenant_id="t"),
        goal="open-ended research",
        run_store=InMemoryRunStore(),
    )
    out = await loop.run(sc)
    subresults = out.extras["subresults"]
    assert len(subresults) == 2  # >= 2 subagents under delegation contracts
    assert all(s["artifacts"] for s in subresults)  # large outputs -> artifact refs
    assert all(len(s["summary"]) <= 100 for s in subresults)  # distilled summaries


# queue-dispatch step identity must be replay-stable
async def test_queue_dispatch_key_stable_across_dialogue_mutation() -> None:
    """The idempotency key for the same logical delegation must not change when
    sc.dialogue grows between replays (pre-fix step_id used len(sc.dialogue))."""
    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    dispatcher = QueueDispatcher(broker, signer)
    sc = StageContext(
        run_id="r1", tenant=TenantContext(tenant_id="t"), goal="g", run_store=InMemoryRunStore()
    )
    delegation = DelegationContract(
        objective="serve ads", output_format="order_placement.v1", task_type="order_placement"
    )

    await dispatcher.dispatch(delegation, sc)
    sc.dialogue.append("noise appended between replays")
    await dispatcher.dispatch(delegation, sc)

    keys = {message_id for _, _, message_id, _ in broker.peek("tasks.order_placement")}
    assert len(keys) == 1  # identical key -> a guard would dedupe the replay
