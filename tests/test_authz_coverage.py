"""Completed authz coverage (Option A): every admin mutation is scope-gated.

Asserting a scopeless principal must be denied; the right scope allows it. Trusted
in-process callers (no principal) keep working.
"""

from __future__ import annotations

from typing import Any

import pytest

from maof.authz import (
    SCOPE_REGISTRY_APPROVE,
    SCOPE_REGISTRY_AUTHOR,
    SCOPE_RUNS_WRITE,
    SCOPE_WORKFLOW_APPROVE,
    SCOPE_WORKFLOW_AUTHOR,
)
from maof.errors import AuthzError
from maof.identity import Principal
from maof.registry.models import AgentManifest
from maof.registry.store import InMemoryRegistryRepo, RegistryStore
from maof.runs.ops import RunOps
from maof.transport.signing import Signer
from maof.workflows.definition import WorkflowDefinition, WorkflowStep
from maof.workflows.store import InMemoryWorkflowRepo, WorkflowStore

SECRET = {"default": "authz-coverage-secret-0123456789abcd"}


def _principal(*scopes: str) -> Principal:
    return Principal(id="op", scopes=list(scopes))


def _manifest(entry_id: str = "agent-1") -> AgentManifest:
    return AgentManifest(
        id=entry_id,
        kind="l2_agent",
        name="Agent",
        version="v1",
        endpoint="python://agent",
        capabilities=[],
        accepted_task_types=["t"],
        provided_schemas=[],
        rbac_scopes=[],
        tenancy="tenant",
        side_loaded_context=[],
    )


async def test_registry_submit_requires_author_scope() -> None:
    store = RegistryStore(InMemoryRegistryRepo(), Signer(SECRET))
    with pytest.raises(AuthzError):
        await store.submit(_manifest(), principal=_principal())
    assert (
        await store.submit(_manifest(), principal=_principal(SCOPE_REGISTRY_AUTHOR))
    ).status == ("pending")


async def test_registry_approve_and_revoke_require_approve_scope() -> None:
    store = RegistryStore(InMemoryRegistryRepo(), Signer(SECRET))
    await store.submit(_manifest())  # trusted in-process: no principal
    with pytest.raises(AuthzError):  # wrong scope
        await store.approve("agent-1", principal=_principal(SCOPE_REGISTRY_AUTHOR))
    assert (
        await store.approve("agent-1", principal=_principal(SCOPE_REGISTRY_APPROVE))
    ).status == ("approved")
    with pytest.raises(AuthzError):
        await store.revoke("agent-1", principal=_principal())
    assert (await store.revoke("agent-1", principal=_principal(SCOPE_REGISTRY_APPROVE))).status == (
        "revoked"
    )


def _wf() -> WorkflowDefinition:
    return WorkflowDefinition(name="wf", version=1, steps=[WorkflowStep(id="s", task_type="t")])


async def test_workflow_submit_and_revoke_are_gated() -> None:
    store = WorkflowStore(InMemoryWorkflowRepo(), Signer(SECRET))
    with pytest.raises(AuthzError):
        await store.submit(_wf(), principal=_principal())
    await store.submit(_wf(), principal=_principal(SCOPE_WORKFLOW_AUTHOR))
    await store.approve("wf", 1, principal=_principal(SCOPE_WORKFLOW_APPROVE))
    with pytest.raises(AuthzError):  # revoke needs workflow:approve, not author
        await store.revoke("wf", 1, principal=_principal(SCOPE_WORKFLOW_AUTHOR))
    assert (await store.revoke("wf", 1, principal=_principal(SCOPE_WORKFLOW_APPROVE))).status == (
        "revoked"
    )


class _DB:
    """Minimal duck-typed Database for RunOps cancel/wake."""

    def __init__(self, owner: str = "t1") -> None:
        self._owner = owner
        self.executed: list[str] = []

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return self._owner

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        return None

    async def execute(self, sql: str, *args: Any) -> None:
        self.executed.append(sql.split()[0])


class _Waker:
    async def fire_event(self, event_key: str) -> list[str]:
        return ["r1"]


async def test_runs_cancel_requires_write_scope() -> None:
    db = _DB()
    ops = RunOps(db)  # type: ignore[arg-type]
    with pytest.raises(AuthzError):
        await ops.cancel("r1", principal=_principal())
    await ops.cancel("r1", principal=_principal(SCOPE_RUNS_WRITE))
    assert any(s == "UPDATE" for s in db.executed)


async def test_runs_wake_requires_write_scope() -> None:
    ops = RunOps(_DB(), waker=_Waker())  # type: ignore[arg-type]
    with pytest.raises(AuthzError):
        await ops.wake("evt", principal=_principal())
    assert await ops.wake("evt", principal=_principal(SCOPE_RUNS_WRITE)) == ["r1"]


def test_registry_admin_api_denies_without_scope() -> None:
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from maof.registry.admin_api import create_registry_admin_app
    from maof.registry.loader import RegistryLoader

    repo = InMemoryRegistryRepo()
    signer = Signer(SECRET)

    async def resolver(authorization: str | None) -> Principal:
        return _principal()  # scopeless ⇒ mutations must 403

    client = TestClient(
        create_registry_admin_app(
            RegistryStore(repo, signer), RegistryLoader(repo, signer), principal_resolver=resolver
        )
    )
    assert client.post("/registry/agent-1/approve").status_code == 403
