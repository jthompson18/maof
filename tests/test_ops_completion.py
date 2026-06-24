"""Ops completion: lifecycle events, trajectory, prompt audit,
retention pruning, Ed25519 registry signing."""

from __future__ import annotations

import importlib.util
from typing import Any
from uuid import uuid4

import pytest

from maof.observability.trajectory import TrajectoryRecorder
from maof.orchestrator.l1 import DefaultL1
from maof.orchestrator.pipeline import Pipeline
from maof.runs.store import InMemoryRunStore
from maof.types import StageContext, TenantContext

_HAS_CRYPTO = importlib.util.find_spec("cryptography") is not None


class FakeSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def emit(self, event: Any) -> None:
        self.events.append(event)

    def types(self) -> list[str]:
        return [e.event_type for e in self.events]


class Ok:
    def __init__(self, name: str) -> None:
        self.name = name

    async def execute(self, sc: StageContext) -> StageContext:
        return sc


# run lifecycle events + trajectory
async def test_l1_emits_run_lifecycle_events_with_actor() -> None:
    from maof.identity import Principal

    sink = FakeSink()
    trajectory = TrajectoryRecorder()
    l1 = DefaultL1(
        Pipeline([Ok("chat"), Ok("publish")]),
        run_store=InMemoryRunStore(),
        event_sink=sink,
        trajectory=trajectory,
    )
    principal = Principal(id="user-7", kind="user", org="partner", roles=["partner-ops"])
    result = await l1.run("goal", TenantContext(tenant_id="t"), principal=principal)
    assert result.status == "completed"

    types = sink.types()
    assert types[0] == "run_started"
    assert types[-1] == "run_completed"
    assert types.count("run_checkpointed") == 2  # one per stage
    assert sink.events[0].actor is not None and sink.events[0].actor["id"] == "user-7"

    structure = trajectory.structure()
    assert structure["counts"]["stage"] == 2  # decision structure captured


async def test_l1_emits_failed_and_cancelled_events() -> None:
    from maof.errors import PolicyDenied

    class Deny:
        name = "action_plan"

        async def execute(self, sc: StageContext) -> StageContext:
            raise PolicyDenied("no")

    sink = FakeSink()
    l1 = DefaultL1(Pipeline([Deny()]), run_store=InMemoryRunStore(), event_sink=sink)
    await l1.run("goal", TenantContext(tenant_id="t"))
    assert "run_failed" in sink.types()

    store = InMemoryRunStore()

    class CancelSelf:
        name = "chat"

        async def execute(self, sc: StageContext) -> StageContext:
            await store.request_cancel(sc.run_id)
            return sc

    sink2 = FakeSink()
    l1 = DefaultL1(Pipeline([CancelSelf(), Ok("publish")]), run_store=store, event_sink=sink2)
    await l1.run("goal", TenantContext(tenant_id="t"))
    assert "run_cancelled" in sink2.types()


# automatic (redacted) prompt audit
async def test_llm_provider_records_redacted_prompt_audit() -> None:
    from maof.models.base import BaseLLMProvider

    recorded: list[tuple[str, str, str, str]] = []

    class FakeAuditRepo:
        async def record(
            self, tenant: TenantContext, run_id: str, prompt: str, response: str
        ) -> None:
            recorded.append((tenant.tenant_id, run_id, prompt, response))

    class Stub(BaseLLMProvider):
        async def _complete(
            self, prompt: str, *, system: Any = None, json_schema: Any = None, **opts: Any
        ) -> tuple[str, int, int]:
            return "reply to alice@example.com", 1, 1

    provider = Stub("m", prompt_audit=FakeAuditRepo())
    await provider.generate(
        "contact alice@example.com about next-quarter", run_id="r1", tenant_id="t1"
    )

    assert len(recorded) == 1
    tenant_id, run_id, prompt, response = recorded[0]
    assert (tenant_id, run_id) == ("t1", "r1")
    assert "alice@example.com" not in prompt  # PII redacted before persistence
    assert "alice@example.com" not in response


# retention pruning
async def test_retention_prunes_old_rows(db: Any) -> None:
    from maof.runs.retention import prune

    run_id = f"run-{uuid4()}"
    await db.execute(
        "INSERT INTO run_trace (run_id, seq, kind, ts) "
        "VALUES ($1, 1, 'old', now() - interval '90 days')",
        run_id,
    )
    await db.execute(
        "INSERT INTO run_trace (run_id, seq, kind, ts) VALUES ($1, 2, 'new', now())", run_id
    )
    key = f"prune-{uuid4()}"
    await db.execute(
        "INSERT INTO idempotency_keys (key, created_at) VALUES ($1, now() - interval '2 days')",
        key,
    )

    summary = await prune(
        db, trace_retention_days=30, audit_retention_days=30, idempotency_ttl_s=3600
    )
    assert summary["run_trace"] >= 1
    remaining = await db.fetch("SELECT kind FROM run_trace WHERE run_id = $1", run_id)
    assert [r["kind"] for r in remaining] == ["new"]
    assert await db.fetchval("SELECT count(*) FROM idempotency_keys WHERE key = $1", key) == 0


# Ed25519 (asymmetric) registry signing
@pytest.mark.skipif(not _HAS_CRYPTO, reason="cryptography (crypto extra) not installed")
async def test_ed25519_signer_round_trip_and_registry_lifecycle() -> None:
    from maof.registry.loader import RegistryLoader
    from maof.registry.models import AgentManifest
    from maof.registry.store import InMemoryRegistryRepo, RegistryStore
    from maof.transport.signing import Ed25519Signer, generate_ed25519_keypair

    private_pem, public_pem = generate_ed25519_keypair()
    signer = Ed25519Signer(
        private_key_pem=private_pem, public_keys={"reg": public_pem}, active_kid="reg"
    )

    body = b"registry payload"
    headers = signer.headers(body)
    signer.verify(body, headers)
    assert not signer.is_valid(body + b"x", headers)  # tamper detected

    # verify-only party (no private key) can validate but not sign
    verifier = Ed25519Signer(private_key_pem=None, public_keys={"reg": public_pem})
    verifier.verify(body, headers)

    repo = InMemoryRegistryRepo()
    store = RegistryStore(repo, signer)
    loader = RegistryLoader(repo, verifier)
    manifest = AgentManifest(
        id="commitments",
        kind="l2_agent",
        name="Commitments",
        version="v1",
        endpoint="x",
        tenancy="tenant",
    )
    await store.submit(manifest)
    await store.approve("commitments")
    assert [m.id for m in await loader.manifests()] == ["commitments"]  # asymmetric trust chain
