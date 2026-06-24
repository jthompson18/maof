"""Discovery registry: signing, lifecycle, loader trust, A2A, MCP, context delegation."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from maof.agents.mcp_adapter import MCPAgentAdapter
from maof.errors import RegistryTrustError
from maof.orchestrator.context_delegation import ContextDelegationError, process_context_delegations
from maof.persistence.postgres import Database, PostgresRegistryRepo
from maof.registry.a2a import agent_card_to_manifest, manifest_to_agent_card
from maof.registry.loader import RegistryLoader
from maof.registry.models import AgentManifest, ContextDeclaration, RegistryEntry
from maof.registry.signing import sign_entry, verify_entry
from maof.registry.store import InMemoryRegistryRepo, RegistryStore
from maof.transport.signing import Signer
from maof.types import ContextEnvelope, DataPointer, Stage


def _manifest(**kw: Any) -> AgentManifest:
    return AgentManifest(
        id=kw.get("id", "commitments"),
        kind=kw.get("kind", "l2_agent"),
        name=kw.get("name", "Commitments"),
        version="v1",
        endpoint="python://commitments",
        capabilities=["media"],
        accepted_task_types=kw.get("att", ["funds_commit"]),
        provided_schemas=["funds_commit.v1"],
        rbac_scopes=["buy:commit"],
        tenancy="tenant",
        side_loaded_context=kw.get("sld", []),
    )


class FakeSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def emit(self, event: Any) -> None:
        self.events.append(event)


# signing
def test_sign_and_verify_entry() -> None:
    signer = Signer({"default": "secret"})
    signed = sign_entry(RegistryEntry(manifest=_manifest(), status="pending"), signer)
    assert signed.status == "approved" and signed.signature and signed.kid == "default"
    verify_entry(signed, signer)


def test_verify_unsigned_and_tampered() -> None:
    signer = Signer({"default": "secret"})
    with pytest.raises(RegistryTrustError):
        verify_entry(RegistryEntry(manifest=_manifest(), status="approved"), signer)
    signed = sign_entry(RegistryEntry(manifest=_manifest(), status="pending"), signer)
    tampered = signed.model_copy(update={"manifest": _manifest(name="EvilCorp")})
    with pytest.raises(RegistryTrustError):
        verify_entry(tampered, signer)


# lifecycle + loader
async def test_registry_lifecycle_and_loader() -> None:
    signer = Signer({"default": "secret"})
    repo = InMemoryRegistryRepo()
    sink = FakeSink()
    store = RegistryStore(repo, signer, event_sink=sink)
    loader = RegistryLoader(repo, signer, event_sink=sink)
    manifest = _manifest()

    await store.submit(manifest)
    assert await loader.manifests() == []  # pending is not loadable

    await store.approve(manifest.id)
    loaded = await loader.manifests()
    assert [m.id for m in loaded] == [manifest.id]  # approved + signed
    assert await loader.agents_for_task_type("funds_commit")

    await store.revoke(manifest.id)
    assert await loader.manifests() == []  # revoked excluded

    event_types = {e.event_type for e in sink.events}
    assert {"registry_submitted", "registry_approved", "registry_revoked"} <= event_types


async def test_loader_excludes_tampered_entry_with_event() -> None:
    signer = Signer({"default": "secret"})
    repo = InMemoryRegistryRepo()
    sink = FakeSink()
    store = RegistryStore(repo, signer)
    loader = RegistryLoader(repo, signer, event_sink=sink)
    manifest = _manifest()
    await store.submit(manifest)
    await store.approve(manifest.id)

    stored = await repo.get(manifest.id)
    assert stored is not None
    await repo.put(stored.model_copy(update={"manifest": _manifest(name="Tampered")}))

    assert await loader.manifests() == []  # signature no longer matches -> excluded
    assert any(e.details.get("rejected") for e in sink.events)


# A2A
def test_a2a_card_round_trip() -> None:
    manifest = _manifest(att=["funds_commit", "reconciliation"])
    card = manifest_to_agent_card(manifest)
    assert card["name"] == "Commitments"
    assert {s["id"] for s in card["skills"]} == {"funds_commit", "reconciliation"}
    back = agent_card_to_manifest(card)
    assert back.id == manifest.id
    assert set(back.accepted_task_types) == set(manifest.accepted_task_types)
    assert back.rbac_scopes == manifest.rbac_scopes


# MCP adapter
async def test_mcp_agent_adapter_maps_task_to_tool_call() -> None:
    class FakeMCP:
        async def call_tool(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
            return {"tool": name, "echo": args.get("description")}

    adapter = MCPAgentAdapter(_manifest(kind="mcp_server", att=["funds_commit"]), FakeMCP())
    assert adapter.accepted_task_types == ["funds_commit"]
    result = await adapter.handle(
        {"task_id": "t1", "task_type": "funds_commit", "description": "buy"}, ctx=None  # type: ignore[arg-type]
    )
    assert result.status == "ok"
    assert result.output["result"]["echo"] == "buy"


# context delegation
async def test_context_delegation_dedup_verify_stamp_emit() -> None:
    decl = ContextDeclaration(
        id="vendor_mappings",
        kind="yaml_config",
        description="Commitments rate cards + PO templates",
        scope="tenant",
        supplies=["rate_card"],
        requires_from_l1=["budget"],
        source_ref="pkg://commitments/maps.yaml",
    )
    env = ContextEnvelope(
        run_id="r",
        tenant_id="t",
        stage=Stage.ACTION_PLAN,
        policy_flags={"budget": "100000"},
        data_pointers=[
            DataPointer(alias="rate_card", uri="s3://rc"),
            DataPointer(alias="po_template", uri="s3://io"),
        ],
        semantic_model={"rate_card": {"cpm": 10}, "regions": ["east"]},
    )
    sink = FakeSink()
    await process_context_delegations(env, "commitments", [decl], event_sink=sink, tenant_id="t")

    assert [dp.alias for dp in env.data_pointers] == ["po_template"]  # rate_card de-duplicated
    assert "rate_card" not in env.semantic_model
    assert "regions" in env.semantic_model
    stamped = env.extras["delegated_context"][0]
    assert stamped["agent"] == "commitments" and stamped["id"] == "vendor_mappings"
    assert sink.events[0].event_type == "context_delegated"


async def test_context_delegation_missing_requires_raises() -> None:
    decl = ContextDeclaration(
        id="x",
        kind="yaml",
        description="",
        scope="tenant",
        supplies=[],
        requires_from_l1=["budget"],
    )
    env = ContextEnvelope(run_id="r", tenant_id="t", stage=Stage.ACTION_PLAN)
    with pytest.raises(ContextDelegationError):
        await process_context_delegations(env, "agent", [decl])


# Postgres-backed lifecycle
async def test_registry_lifecycle_postgres(db: Database) -> None:
    signer = Signer({"default": "secret"})
    repo = PostgresRegistryRepo(db)
    store = RegistryStore(repo, signer)
    loader = RegistryLoader(repo, signer)
    manifest = _manifest(id=f"commitments-{uuid4()}")

    await store.submit(manifest)
    await store.approve(manifest.id)
    assert any(m.id == manifest.id for m in await loader.manifests())
    await store.revoke(manifest.id)
    assert not any(m.id == manifest.id for m in await loader.manifests())


# revocation must not be resurrectable by a DB writer
async def test_status_flip_cannot_resurrect_revoked_entry() -> None:
    """Revoke, then simulate an attacker with DB write access flipping the status
    back to 'approved' without touching the manifest. The loader must still
    exclude the entry (revocation destroys the signature; status is signed)."""
    signer = Signer({"default": "secret"})
    repo = InMemoryRegistryRepo()
    store = RegistryStore(repo, signer)
    loader = RegistryLoader(repo, signer)
    manifest = _manifest()

    await store.submit(manifest)
    await store.approve(manifest.id)
    assert await loader.manifests()  # trusted while approved

    await store.revoke(manifest.id)
    revoked = await repo.get(manifest.id)
    assert revoked is not None
    # attacker flips the row's status back without the signing key
    await repo.put(revoked.model_copy(update={"status": "approved"}))

    assert await loader.manifests() == []  # still excluded


def test_signature_binds_status() -> None:
    """A signed-approved entry whose status field is altered must fail verification."""
    signer = Signer({"default": "secret"})
    signed = sign_entry(RegistryEntry(manifest=_manifest(), status="pending"), signer)
    verify_entry(signed, signer)  # baseline ok
    flipped = signed.model_copy(update={"status": "revoked"})
    with pytest.raises(RegistryTrustError):
        verify_entry(flipped, signer)
