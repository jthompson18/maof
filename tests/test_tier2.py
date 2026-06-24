"""RBAC enforcement, approval fallback, cost wiring, builder compaction,
CLI completion, and embedded mode."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

import pytest

from maof.config import Settings
from maof.orchestrator.delegation import DelegationContract
from maof.orchestrator.l1 import DefaultL1
from maof.orchestrator.pipeline import Pipeline
from maof.orchestrator.stages import ApprovalStage
from maof.registry.loader import RegistryLoader
from maof.registry.models import AgentManifest
from maof.registry.store import InMemoryRegistryRepo, RegistryStore
from maof.runs.store import InMemoryRunStore
from maof.transport.signing import Signer
from maof.types import (
    ContextEnvelope,
    CostProjection,
    CostSummary,
    EffortBudget,
    StageContext,
    TenantContext,
)


class FakeLedger:
    def __init__(self, cost_usd: float = 0.0) -> None:
        self._cost = cost_usd

    async def record(self, run_id: str, *, model: str, in_tokens: int, out_tokens: int) -> None:
        return None

    async def total(self, run_id: str) -> CostSummary:
        return CostSummary(run_id=run_id, cost_usd=self._cost)


class MockLLM:
    async def generate(
        self, prompt: str, *, system: Any = None, json_schema: Any = None, **opts: Any
    ) -> str:
        return "ok"


def _manifest(entry_id: str, *, scopes: list[str], task_types: list[str]) -> AgentManifest:
    return AgentManifest(
        id=entry_id,
        kind="l2_agent",
        name=entry_id,
        version="v1",
        endpoint=f"python://{entry_id}",
        accepted_task_types=task_types,
        rbac_scopes=scopes,
        tenancy="tenant",
    )


# RBAC scopes are enforced at the L1 routing path
async def test_loader_enforces_rbac_scopes() -> None:
    signer = Signer({"default": "secret"})
    repo = InMemoryRegistryRepo()
    store = RegistryStore(repo, signer)
    loader = RegistryLoader(repo, signer)

    scoped_agent = _manifest("commitments", scopes=["buy:commit"], task_types=["funds_commit"])
    free_agent = _manifest("reporter", scopes=[], task_types=["funds_commit"])
    for manifest in (scoped_agent, free_agent):
        await store.submit(manifest)
        await store.approve(manifest.id)

    granted = TenantContext(tenant_id="t1", attributes={"scopes": "buy:commit,measure:read"})
    unscoped = TenantContext(tenant_id="t2")

    granted_ids = {m.id for m in await loader.agents_for_task_type("funds_commit", tenant=granted)}
    assert granted_ids == {"commitments", "reporter"}

    unscoped_ids = {
        m.id for m in await loader.agents_for_task_type("funds_commit", tenant=unscoped)
    }
    assert unscoped_ids == {"reporter"}  # scope-requiring agent filtered out

    no_tenant = {m.id for m in await loader.agents_for_task_type("funds_commit")}
    assert no_tenant == {"commitments", "reporter"}  # no tenant -> no RBAC engagement


# approval_fallback (default deny = fail closed)
class _RequireApprovalStage:
    name = "action_plan"

    async def execute(self, sc: StageContext) -> StageContext:
        sc.extras["policy"] = {"require_approval": True, "approval_reason": "over cap"}
        return sc


async def test_approval_required_without_gate_fails_closed() -> None:
    """Default fallback: an approval-requiring plan with HITL off is DENIED."""
    l1 = DefaultL1(
        Pipeline([_RequireApprovalStage(), ApprovalStage(hitl_enabled=False)]),
        run_store=InMemoryRunStore(),
    )
    result = await l1.run("goal", TenantContext(tenant_id="t"))
    assert result.status == "denied"
    assert "approval required" in result.summary.lower()


async def test_approval_fallback_allow_is_advisory() -> None:
    l1 = DefaultL1(
        Pipeline([_RequireApprovalStage(), ApprovalStage(hitl_enabled=False, fallback="allow")]),
        run_store=InMemoryRunStore(),
    )
    result = await l1.run("goal", TenantContext(tenant_id="t"))
    assert result.status == "completed"


def test_settings_approval_fallback_defaults_deny() -> None:
    assert Settings().approval_fallback == "deny"


# prospective cost gating + ledger wiring
async def test_worth_it_gate_prices_projected_tokens() -> None:
    from maof.cost.accounting import DefaultWorthItGate

    gate = DefaultWorthItGate(FakeLedger(), cost_cap_usd=1.0, action="deny", price_per_1k=0.01)
    sc = StageContext(run_id="r", tenant=TenantContext(tenant_id="t"), goal="g")
    decision = await gate.check(sc, CostProjection(projected_subagents=1, projected_tokens=200_000))
    assert decision.denied  # 200k tokens * $0.01/1k = $2 > $1 cap


async def test_loop_projects_budget_tokens_into_gate() -> None:
    from maof.cost.accounting import DefaultWorthItGate
    from maof.orchestrator.coordinator import DefaultCoordinator, InProcessSubagent
    from maof.orchestrator.loop import OrchestratorLoop
    from maof.policy.engine import NativePolicyEngine

    gate = DefaultWorthItGate(FakeLedger(), cost_cap_usd=1.0, action="deny", price_per_1k=0.01)

    async def planner(sc: StageContext, subresults: list[Any]) -> list[DelegationContract]:
        if not subresults:
            return [DelegationContract(objective="big task", output_format="text")]
        return []

    loop = OrchestratorLoop(
        MockLLM(),
        DefaultCoordinator(in_process=InProcessSubagent(MockLLM())),
        EffortBudget(max_subagents=5, max_tokens=500_000),  # projected $5 > $1 cap
        NativePolicyEngine(),
        planner=planner,
        worth_it_gate=gate,
        max_iterations=2,
    )
    sc = StageContext(run_id="r", tenant=TenantContext(tenant_id="t"), goal="g")
    out = await loop.run(sc)
    assert out.extras["subresults"] == []  # halted prospectively, before any spend


async def test_l1_wires_cost_ledger_into_stage_context() -> None:
    ledger = FakeLedger()
    seen: list[Any] = []

    class Probe:
        name = "chat"

        async def execute(self, sc: StageContext) -> StageContext:
            seen.append(sc.cost_ledger)
            return sc

    l1 = DefaultL1(Pipeline([Probe()]), run_store=InMemoryRunStore(), cost_ledger=ledger)
    await l1.run("g", TenantContext(tenant_id="t"))
    assert seen == [ledger]


# compaction reachable from the builder + injectable counter
async def test_builder_compacts_before_blunt_trim() -> None:
    from maof.context.budget import TokenBudgeter
    from maof.context.builder import ContextBuilder

    class StubCompactor:
        async def compact(self, sc: StageContext, *, target_tokens: int) -> StageContext:
            return dataclasses.replace(sc, dialogue=["DIGEST: decisions preserved"])

    builder = ContextBuilder([], budgeter=TokenBudgeter(), max_tokens=30, compactor=StubCompactor())
    sc = StageContext(
        run_id="r",
        tenant=TenantContext(tenant_id="t"),
        goal="g",
        dialogue=["x" * 400, "y" * 400, "z" * 400],
    )
    env = builder  # placate linters about unused var patterns
    env = await builder.build(sc)
    assert any("DIGEST" in line for line in env.dialogue)  # compacted, not just trimmed
    assert TokenBudgeter().count(env) <= 30


def test_budgeter_injectable_counter() -> None:
    from maof.context.budget import TokenBudgeter

    words = TokenBudgeter(counter=lambda text: len(text.split()))
    env = ContextEnvelope(run_id="r", tenant_id="t", stage="chat", goal="five words in this goal")
    assert words.count(env) == 5


# CLI completion + embedded mode
def test_build_broker_embedded_overrides_kind() -> None:
    from maof.transport.factory import build_broker
    from maof.transport.fake import InMemoryBroker

    broker = build_broker(Settings(broker_kind="rabbitmq", embedded_l2=True))
    assert isinstance(broker, InMemoryBroker)


def test_cli_run_orchestrator_runs_module() -> None:
    from maof.cli import main

    assert main(["run-orchestrator", "--module", "examples.po_demo.main"]) == 0


def test_cli_eval_run_scores_and_gates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from maof.cli import main
    from maof.models.base import BaseLLMProvider, register_llm_provider

    scores = {
        "factual_accuracy": 1.0,
        "completeness": 1.0,
        "citation_quality": 1.0,
        "tool_efficiency": 1.0,
        "rationale": "perfect",
    }

    class JudgeProvider(BaseLLMProvider):
        async def _complete(
            self, prompt: str, *, system: Any = None, json_schema: Any = None, **opts: Any
        ) -> tuple[str, int, int]:
            return json.dumps(scores), 1, 1

    register_llm_provider("tier2-judge", lambda s, ledger: JudgeProvider(s.model_name))
    monkeypatch.setenv("MODEL_PROVIDER", "tier2-judge")

    dataset = tmp_path / "ds.json"
    dataset.write_text(
        json.dumps({"name": "ds", "cases": [{"id": "c1", "input": "the output to grade"}]})
    )

    monkeypatch.setenv("EVAL_MIN_PASS_RATE", "0.8")
    assert main(["eval", "run", str(dataset)]) == 0  # 1.0 pass rate -> gate passes

    scores.update({k: 0.0 for k in list(scores) if k != "rationale"})
    assert main(["eval", "run", str(dataset)]) == 1  # all-zero scores -> gate fails
