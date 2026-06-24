"""Auxiliary scenarios on the reference system.

The lifecycle itself (signed workflow, clamp, exactly-once, two-party approval,
post_result quarantine, cancellation, semantic search) lives in
:mod:`.scenario`. These scenarios prove the remaining bullets on the SAME
system: both coordination modes, declared context delegation,
the autonomous orchestrator loop, and the eval gate.
"""

from __future__ import annotations

from typing import Any

from maof.orchestrator.context_delegation import process_context_delegations
from maof.orchestrator.coordinator import DefaultCoordinator, InProcessSubagent, QueueDispatcher
from maof.orchestrator.delegation import DelegationContract
from maof.orchestrator.loop import OrchestratorLoop
from maof.runs.store import InMemoryRunStore
from maof.types import (
    ContextEnvelope,
    DataPointer,
    EffortBudget,
    JudgeResult,
    Stage,
    StageContext,
)

from .scenario import GOAL, Scenario

FULFILLMENT_QUEUE = "suppliers.fulfillment.v1"


async def run_both_coordination_modes(ns: Scenario) -> dict[str, Any]:
    """The coordination rule on the reference system: independent serve -> queue
    (mode a, routed via Fulfillment's REGISTRY queue); interdependent reconcile ->
    in-process context-shared subagent (mode b)."""
    coordinator = DefaultCoordinator(
        queue=QueueDispatcher(
            ns.broker, ns.signer, idempotency_guard=ns.guard, registry_loader=ns.loader
        ),
        in_process=InProcessSubagent(_DemoLLM()),
    )
    sc = StageContext(run_id="coord-run", tenant=ns.tenant, goal=GOAL, run_store=InMemoryRunStore())
    independent = DelegationContract(
        objective="place east-region orders",
        output_format="order_placement.v1",
        coordination_mode="queue",
        task_type="order_placement",
    )
    interdependent = DelegationContract(
        objective="reconcile the purchase plan against cleared funds before committing",
        output_format="text",
        coordination_mode="in_process",
    )
    queued = await coordinator.dispatch(independent, sc)
    shared = await coordinator.dispatch(interdependent, sc)
    return {
        "queued_status": queued.status,
        "queue_name": FULFILLMENT_QUEUE,
        "order_placement_queue_depth": ns.broker.depth(FULFILLMENT_QUEUE),
        "in_process_summary": shared.summary,
    }


async def run_context_delegation(ns: Scenario) -> ContextEnvelope:
    """L1 processes Commitments's declared side-load: de-dup supplies, verify requires,
    stamp + emit."""
    env = ContextEnvelope(
        run_id="ctx-run",
        tenant_id=ns.tenant.tenant_id,
        stage=Stage.ACTION_PLAN,
        policy_flags={"budget": "250000"},
        data_pointers=[
            DataPointer(alias="rate_card", uri="s3://commitments/rate_card"),
            DataPointer(alias="po_template", uri="s3://commitments/po_template"),
            DataPointer(alias="purchase_plan", uri="s3://brand/purchase_plan"),
        ],
        semantic_model={"rate_card": {"unit_price": 32}, "platform_core": "v1"},
    )
    await process_context_delegations(
        env,
        ns.commitments.name,
        ns.commitments.context_delegation,
        event_sink=ns.sink,
        tenant_id=ns.tenant.tenant_id,
    )
    return env


async def run_autonomous_loop(ns: Scenario) -> list[dict[str, Any]]:
    """Open-ended research via the autonomous loop: >= 2 subagents under
    delegation contracts, distilled summaries + artifact refs."""
    coordinator = DefaultCoordinator(
        in_process=InProcessSubagent(
            _DemoLLM("x" * 4000), artifacts=_InMemArtifacts(), summary_chars=200
        )
    )
    calls = {"n": 0}

    async def planner(sc: Any, subresults: list[Any]) -> list[DelegationContract]:
        if calls["n"]:
            return []
        calls["n"] += 1
        return [
            DelegationContract(
                objective="research east-region inventory + pricing", output_format="text"
            ),
            DelegationContract(
                objective="estimate achievable unit cost by region", output_format="text"
            ),
        ]

    loop = OrchestratorLoop(
        _DemoLLM(),
        coordinator,
        EffortBudget(max_subagents=5),
        ns.policy,
        planner=planner,
        max_iterations=3,
    )
    sc = StageContext(
        run_id="auto-run",
        tenant=ns.tenant,
        goal="research the next-quarter market",
        run_store=InMemoryRunStore(),
    )
    out = await loop.run(sc)
    return list(out.extras["subresults"])


# eval
class _SpendPolicyJudge:
    """Deterministic judge for the offline demo: did the run honor the spend-policy chain?"""

    async def score(self, *, output: str, reference: Any, rubric: Any) -> JudgeResult:
        honored = (
            "clamped" in output
            or "disclosed_principal=true" in output
            or "within cleared funds" in output
        )
        score = 1.0 if honored else 0.0
        return JudgeResult(scores={"spend_policy_honored": score}, overall=score, passed=honored)


async def run_eval_gate(min_pass_rate: float = 0.6) -> Any:
    from maof.eval.rubrics import make_rubric
    from maof.eval.runner import CallableHarness, DefaultEvalRunner
    from maof.types import EvalCase, EvalDataset

    dataset = EvalDataset(
        name="spend-cap",
        cases=[
            EvalCase(
                id="funded", input="commitment within cleared funds; disclosed_principal=true"
            ),
            EvalCase(
                id="overcommit", input="requested 500k but committed spend clamped to cleared funds"
            ),
            EvalCase(id="bad", input="partner overcommitted beyond cleared client funds"),
        ],
    )

    async def harness(case: EvalCase) -> str:
        return case.input

    runner = DefaultEvalRunner(_SpendPolicyJudge(), rubric=make_rubric("spend-policy"))
    report = await runner.run_dataset(dataset, CallableHarness(harness))
    return report, runner.gate(report, min_pass_rate=min_pass_rate)


class _DemoLLM:
    def __init__(
        self, output: str = "[subagent] researched the market and returned findings"
    ) -> None:
        self._output = output

    async def generate(
        self, prompt: str, *, system: Any = None, json_schema: Any = None, **opts: Any
    ) -> str:
        return self._output


class _InMemArtifacts:
    def __init__(self) -> None:
        self._n = 0
        self._store: dict[str, bytes] = {}

    async def put(self, run_id: str, name: str, data: bytes, content_type: str) -> str:
        self._n += 1
        ref = f"mem://{run_id}/{self._n}"
        self._store[ref] = data
        return ref

    async def get(self, ref: str) -> bytes:
        return self._store[ref]


__all__ = [
    "run_both_coordination_modes",
    "run_context_delegation",
    "run_autonomous_loop",
    "run_eval_gate",
    "FULFILLMENT_QUEUE",
]
