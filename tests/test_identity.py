"""Principal identity + multi-party approvals."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from maof.approval.service import ApprovalGate
from maof.config import Settings
from maof.errors import ApprovalRequired, TenancyError
from maof.identity import Principal, resolve_identity
from maof.types import StageContext, TenantContext


class FakeSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def emit(self, event: Any) -> None:
        self.events.append(event)


def _principal(*, org: str = "partner", roles: list[str] | None = None) -> Principal:
    return Principal(
        id="user-7",
        kind="user",
        org=org,
        roles=roles if roles is not None else ["partner-ops"],
        scopes=["buy:commit"],
    )


# identity resolution
def test_resolve_identity_threads_principal() -> None:
    tenant, principal = resolve_identity(
        Settings(tenancy_mode="multi"), tenant_id="brand-1", principal=_principal()
    )
    assert tenant.tenant_id == "brand-1"
    assert principal.org == "partner"


def test_resolve_identity_defaults_service_principal() -> None:
    tenant, principal = resolve_identity(Settings(tenancy_mode="single"))
    assert principal.kind == "service"
    assert principal.id  # a stable default service identity exists


def test_resolve_identity_multi_tenant_requires_id() -> None:
    with pytest.raises(TenancyError):
        resolve_identity(Settings(tenancy_mode="multi"))


# actor on audit events
def test_audit_event_carries_actor() -> None:
    from maof.observability.events import AuditEvent

    event = AuditEvent(
        tenant_id="t",
        intent_id=None,
        event_type="run_started",
        actor={"id": "user-7", "org": "partner", "roles": ["partner-ops"]},
    )
    assert event.actor is not None and event.actor["org"] == "partner"


def test_stage_context_carries_principal() -> None:
    sc = StageContext(
        run_id="r",
        tenant=TenantContext(tenant_id="t"),
        goal="g",
        principal=_principal(),
    )
    assert sc.principal is not None and sc.principal.roles == ["partner-ops"]


# principal-scope RBAC + tool stripping
def test_principal_scopes_gate_registry() -> None:
    from maof.registry.loader import _scopes_granted
    from maof.registry.models import AgentManifest

    manifest = AgentManifest(
        id="commitments",
        kind="l2_agent",
        name="Commitments",
        version="v1",
        endpoint="x",
        rbac_scopes=["buy:commit"],
        tenancy="tenant",
    )
    scoped_principal = _principal()  # scopes=["buy:commit"]
    unscoped_tenant = TenantContext(tenant_id="t")
    assert _scopes_granted(manifest, unscoped_tenant, principal=scoped_principal)
    assert not _scopes_granted(manifest, unscoped_tenant, principal=None)


async def test_worker_strips_unauthorized_tools() -> None:
    import json

    from maof.agents.base import BaseL2Agent
    from maof.agents.registry_runtime import AgentRegistry
    from maof.transport.fake import InMemoryBroker
    from maof.transport.signing import Signer
    from maof.types import L2Context, TaskResult
    from maof.workers.pool import WorkerPool

    seen_tools: list[list[str]] = []

    class Probe(BaseL2Agent):
        name = "probe"
        accepted_task_types = ["funds_commit"]

        async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
            seen_tools.append([t.name for t in ctx.toolset])
            return TaskResult(status="ok", task_id=task["task_id"])

    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    registry = AgentRegistry()
    registry.register_agent(Probe())
    pool = WorkerPool(broker, signer, registry, result_queue=None)

    message = {
        "envelope": {
            "run_id": "r",
            "tenant_id": "t",
            "intent_id": None,
            "stage": "publish",
            "actor": {
                "id": "u",
                "kind": "user",
                "org": "partner",
                "roles": [],
                "scopes": ["buy:commit"],
            },
        },
        "task": {
            "task_id": "t1",
            "task_type": "funds_commit",
            "priority": 5,
            "description": "buy",
            "idempotency_key": "k",
        },
        "policy_flags": {},
        "toolset": [
            {"name": "commitments", "rbac": "buy:commit"},
            {"name": "billing", "rbac": "billing:write"},
            {"name": "open_tool"},
        ],
        "data_pointers": {},
        "semantic_model": {},
        "timestamp": "now",
    }
    body = json.dumps(message).encode()
    await broker.publish(
        "tasks.funds_commit", body, headers=signer.headers(body), message_id="k", correlation_id="c"
    )
    await pool.consume("tasks.funds_commit")
    # billing:write is NOT held by the actor -> stripped; scoped + open tools remain
    assert seen_tools == [["commitments", "open_tool"]]


# multi-party role-bound approvals
async def test_two_party_role_bound_approval() -> None:
    gate = ApprovalGate(timeout=5.0)
    sc = StageContext(run_id="r1", tenant=TenantContext(tenant_id="t"), goal="g")

    waiter = asyncio.create_task(
        gate.wait(
            sc,
            reason="over cap",
            required_roles=["buyer-finance", "partner-ops"],
            parties=2,
        )
    )
    await asyncio.sleep(0)
    approval_id = sc.extras["approval_id"]

    # wrong-role resolution is rejected and does not count
    with pytest.raises(PermissionError):
        await gate.resolve(
            approval_id,
            approved=True,
            principal=Principal(id="x", kind="user", org="other", roles=["intern"]),
        )
    assert not waiter.done()

    await gate.resolve(
        approval_id,
        approved=True,
        principal=Principal(id="fin-1", kind="user", org="buyer", roles=["buyer-finance"]),
    )
    await asyncio.sleep(0.01)
    assert not waiter.done()  # 1 of 2 parties

    await gate.resolve(
        approval_id,
        approved=True,
        principal=Principal(id="ops-1", kind="user", org="partner", roles=["partner-ops"]),
    )
    await asyncio.wait_for(waiter, timeout=5.0)  # both parties -> proceeds

    record = gate.resolutions(approval_id)
    assert {r["principal_id"] for r in record} == {"fin-1", "ops-1"}  # attributed


async def test_multi_party_deny_fails_fast() -> None:
    gate = ApprovalGate(timeout=5.0)
    sc = StageContext(run_id="r1", tenant=TenantContext(tenant_id="t"), goal="g")
    waiter = asyncio.create_task(
        gate.wait(sc, reason="over cap", required_roles=["buyer-finance"], parties=2)
    )
    await asyncio.sleep(0)
    await gate.resolve(
        sc.extras["approval_id"],
        approved=False,
        principal=Principal(id="fin-1", kind="user", org="buyer", roles=["buyer-finance"]),
    )
    with pytest.raises(ApprovalRequired):  # one qualified deny fails the approval
        await waiter


async def test_same_principal_cannot_count_twice() -> None:
    gate = ApprovalGate(timeout=1.0)
    sc = StageContext(run_id="r1", tenant=TenantContext(tenant_id="t"), goal="g")
    waiter = asyncio.create_task(
        gate.wait(sc, reason="over cap", required_roles=["partner-ops"], parties=2)
    )
    await asyncio.sleep(0)
    principal = Principal(id="ops-1", kind="user", org="partner", roles=["partner-ops"])
    await gate.resolve(sc.extras["approval_id"], approved=True, principal=principal)
    await gate.resolve(sc.extras["approval_id"], approved=True, principal=principal)  # dup
    with pytest.raises((ApprovalRequired, TimeoutError)):  # still 1 distinct party -> expiry
        await waiter
