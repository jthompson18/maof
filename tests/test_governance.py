"""HITL approval gate + tokens, tenancy modes, and the worth-it cost gate."""

from __future__ import annotations

import asyncio
import importlib.util
from typing import Any

import pytest

from maof.approval.service import ApprovalGate
from maof.approval.tokens import mint_approval_token, verify_approval_token
from maof.config import Settings
from maof.cost.accounting import DefaultWorthItGate
from maof.errors import ApprovalRequired, SignatureError, TenancyError
from maof.orchestrator.coordinator import DefaultCoordinator, InProcessSubagent
from maof.orchestrator.delegation import DelegationContract
from maof.orchestrator.loop import OrchestratorLoop
from maof.policy.engine import NativePolicyEngine
from maof.tenancy import resolve_tenant
from maof.types import CostProjection, CostSummary, EffortBudget, StageContext, TenantContext

_HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None


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


# tenancy
def test_single_tenant_defaults() -> None:
    tenant = resolve_tenant(Settings(tenancy_mode="single"))
    assert tenant.tenant_id == "default"
    assert tenant.multi_tenant is False


def test_multi_tenant_requires_id() -> None:
    with pytest.raises(TenancyError):
        resolve_tenant(Settings(tenancy_mode="multi"))
    tenant = resolve_tenant(Settings(tenancy_mode="multi"), tenant_id="brand-1")
    assert tenant.tenant_id == "brand-1" and tenant.multi_tenant is True


# approval tokens
def test_approval_token_round_trip() -> None:
    token = mint_approval_token("appr-1", "secret")
    assert verify_approval_token(token, "secret") == "appr-1"


def test_approval_token_tamper_rejected() -> None:
    token = mint_approval_token("appr-1", "secret")
    with pytest.raises(SignatureError):
        verify_approval_token(token, "wrong-secret")
    with pytest.raises(SignatureError):
        verify_approval_token("garbage", "secret")


# approval gate
async def test_approval_gate_approve_unblocks() -> None:
    gate = ApprovalGate()
    approval_id = await gate.request("brand-1", "run1", "commit exceeds funds")

    async def approver() -> None:
        await gate.resolve(approval_id, approved=True)

    asyncio.create_task(approver())
    assert await gate.wait_for(approval_id) is True


async def test_approval_gate_deny_raises_in_stage_path() -> None:
    gate = ApprovalGate(timeout=2.0)
    sc = StageContext(run_id="run1", tenant=TenantContext(tenant_id="brand-1"), goal="g")

    async def waiter() -> None:
        await gate.wait(sc, reason="over cap")

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0)  # let the gate register the request
    approval_id = sc.extras["approval_id"]
    await gate.resolve(approval_id, approved=False)
    with pytest.raises(ApprovalRequired):
        await task


# worth-it gate
async def test_worth_it_gate_under_budget_ok() -> None:
    gate = DefaultWorthItGate(FakeLedger(), fanout_cap=10, cost_cap_usd=10.0)
    sc = StageContext(run_id="r", tenant=TenantContext(tenant_id="t"), goal="g")
    decision = await gate.check(sc, CostProjection(projected_subagents=3, projected_usd=1.0))
    assert not decision.denied and not decision.require_approval


async def test_worth_it_gate_over_fanout_requires_approval() -> None:
    gate = DefaultWorthItGate(FakeLedger(), fanout_cap=5, action="require_approval")
    sc = StageContext(run_id="r", tenant=TenantContext(tenant_id="t"), goal="g")
    decision = await gate.check(sc, CostProjection(projected_subagents=20))
    assert decision.require_approval and "subagents" in decision.approval_reason


async def test_worth_it_gate_over_cost_denies() -> None:
    gate = DefaultWorthItGate(FakeLedger(cost_usd=9.0), cost_cap_usd=10.0, action="deny")
    sc = StageContext(run_id="r", tenant=TenantContext(tenant_id="t"), goal="g")
    decision = await gate.check(sc, CostProjection(projected_subagents=1, projected_usd=5.0))
    assert decision.denied  # 9 + 5 > 10


async def test_worth_it_gate_caps_loop_fanout() -> None:
    coordinator = DefaultCoordinator(in_process=InProcessSubagent(MockLLM()))
    gate = DefaultWorthItGate(FakeLedger(), fanout_cap=2, action="cap")

    async def planner(sc: StageContext, subresults: list[Any]) -> list[DelegationContract]:
        if not subresults:
            return [
                DelegationContract(objective=f"task {i}", output_format="text") for i in range(5)
            ]
        return []

    loop = OrchestratorLoop(
        MockLLM(),
        coordinator,
        EffortBudget(max_subagents=10),
        NativePolicyEngine(),
        planner=planner,
        worth_it_gate=gate,
        max_iterations=2,
    )
    sc = StageContext(run_id="r1", tenant=TenantContext(tenant_id="t"), goal="g")
    out = await loop.run(sc)
    # gate caps at 2 subagents even though 5 were planned and the budget allowed 10
    assert len(out.extras["subresults"]) == 2


@pytest.mark.skipif(not _HAS_FASTAPI, reason="fastapi (api extra) not installed")
async def test_fastapi_approval_service() -> None:
    from fastapi.testclient import TestClient

    from maof.approval.service import create_approval_app

    gate = ApprovalGate()
    approval_id = await gate.request("brand-1", "run1", "over cap")
    client = TestClient(create_approval_app(gate))
    resp = client.post(f"/approvals/{approval_id}/approve")
    assert resp.status_code == 200
    assert resp.json()["status"] == "approved"
    assert await gate.wait_for(approval_id) is True


# ApprovalRequired must yield a clean result, not an exception
async def test_approval_denial_yields_clean_result_and_failed_status() -> None:
    from maof.orchestrator.l1 import DefaultL1
    from maof.orchestrator.pipeline import Pipeline
    from maof.orchestrator.stages import ApprovalStage
    from maof.runs.store import InMemoryRunStore
    from maof.types import RunStatus

    class RequireApprovalStage:
        name = "action_plan"

        async def execute(self, sc: StageContext) -> StageContext:
            sc.extras["policy"] = {"require_approval": True, "approval_reason": "over cap"}
            return sc

    class DenyGate:
        async def wait(self, sc: StageContext, *, reason: str, **kw: Any) -> None:
            raise ApprovalRequired(reason or "denied")

    store = InMemoryRunStore()
    l1 = DefaultL1(
        Pipeline([RequireApprovalStage(), ApprovalStage(gate=DenyGate(), hitl_enabled=True)]),
        run_store=store,
    )
    result = await l1.run("goal", TenantContext(tenant_id="t"))  # must NOT raise
    assert result.status == "approval_denied"
    assert "over cap" in result.summary
    state = await store.get_state(result.run_id)
    assert state.status is RunStatus.FAILED  # not stuck RUNNING


# approval resolved in ANOTHER process must unblock the run
async def test_cross_process_approval_unblocks_pipeline(db) -> None:  # type: ignore[no-untyped-def]
    """Full-pipeline path: run blocks at approval; a SECOND gate instance sharing
    only the Postgres repo (simulating the approval-service container) resolves it;
    the run unblocks and publishes exactly once."""
    from maof.orchestrator.l1 import DefaultL1
    from maof.orchestrator.pipeline import Pipeline
    from maof.orchestrator.stages import ApprovalStage
    from maof.persistence.postgres import PostgresApprovalRepo
    from maof.runs.store import InMemoryRunStore

    repo = PostgresApprovalRepo(db)
    orch_gate = ApprovalGate(repo=repo, poll_interval=0.05, timeout=10.0)
    svc_gate = ApprovalGate(repo=repo)  # separate instance = separate process

    published: list[str] = []
    tenant_id = f"t-{__import__('uuid').uuid4()}"

    class RequireApprovalStage:
        name = "action_plan"

        async def execute(self, sc: StageContext) -> StageContext:
            sc.extras["policy"] = {"require_approval": True, "approval_reason": "over cap"}
            return sc

    class RecordingPublishStage:
        name = "publish"

        async def execute(self, sc: StageContext) -> StageContext:
            published.append(sc.run_id)
            return sc

    l1 = DefaultL1(
        Pipeline(
            [
                RequireApprovalStage(),
                ApprovalStage(gate=orch_gate, hitl_enabled=True),
                RecordingPublishStage(),
            ]
        ),
        run_store=InMemoryRunStore(),
    )

    run_task = asyncio.create_task(l1.run("goal", TenantContext(tenant_id=tenant_id)))
    # find the pending approval row like the approval service would
    approval_id = None
    for _ in range(100):
        await asyncio.sleep(0.05)
        row = await db.fetchrow(
            "SELECT approval_id FROM approvals WHERE tenant_id = $1 AND status = 'pending'",
            tenant_id,
        )
        if row is not None:
            approval_id = row["approval_id"]
            break
    assert approval_id is not None, "run never requested an approval"
    assert not run_task.done()  # the run is genuinely blocked

    await svc_gate.resolve(approval_id, approved=True)  # other-process approval
    result = await asyncio.wait_for(run_task, timeout=10.0)
    assert result.status == "completed"
    assert published == [result.run_id]  # published exactly once after approval
