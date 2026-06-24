"""Registry intelligence: semantic capability search,
eval-gated certification on approve, registry-driven routing + canary cohorts."""

from __future__ import annotations

from typing import Any

import pytest

from maof.errors import RegistryTrustError
from maof.models.base import HashingEmbeddingProvider
from maof.orchestrator.coordinator import QueueDispatcher
from maof.orchestrator.delegation import DelegationContract
from maof.registry.loader import RegistryLoader
from maof.registry.models import AgentManifest
from maof.registry.search import RegistrySearch
from maof.registry.store import InMemoryRegistryRepo, RegistryStore
from maof.runs.store import InMemoryRunStore
from maof.transport.fake import InMemoryBroker
from maof.transport.signing import Signer
from maof.types import MemorySnippet, StageContext, TenantContext


class FakeVectorStore:
    def __init__(self) -> None:
        self.items: list[tuple[str, MemorySnippet]] = []

    async def upsert(self, tenant: TenantContext, items: list[MemorySnippet]) -> None:
        self.items.extend((tenant.tenant_id, i) for i in items)

    async def query(
        self, tenant: TenantContext, embedding: list[float], top_k: int
    ) -> list[MemorySnippet]:
        # cosine over stored embeddings (small N — fine for tests)
        def dot(a: list[float], b: list[float]) -> float:
            return sum(x * y for x, y in zip(a, b, strict=False))

        scored = [
            (dot(embedding, i.embedding or []), i) for t, i in self.items if t == tenant.tenant_id
        ]
        scored.sort(key=lambda pair: pair[0], reverse=True)
        return [i.model_copy(update={"score": s, "embedding": None}) for s, i in scored[:top_k]]


def _manifest(entry_id: str, description: str, **kw: Any) -> AgentManifest:
    return AgentManifest(
        id=entry_id,
        kind=kw.get("kind", "l2_agent"),
        name=entry_id,
        version=kw.get("version", "v1"),
        endpoint=f"mcp://{entry_id}",
        accepted_task_types=kw.get("att", ["expediting"]),
        rbac_scopes=kw.get("scopes", []),
        tenancy="tenant",
        description=description,
        queue=kw.get("queue"),
        certification=kw.get("certification"),
        canary_pct=kw.get("canary_pct", 0.0),
    )


async def _store(
    *, search: RegistrySearch | None = None, certifier: Any | None = None
) -> tuple[RegistryStore, RegistryLoader, InMemoryRegistryRepo]:
    signer = Signer({"default": "secret"})
    repo = InMemoryRegistryRepo()
    store = RegistryStore(repo, signer, search=search, certifier=certifier)
    return store, RegistryLoader(repo, signer), repo


# semantic capability search
async def test_semantic_search_finds_agent_by_description() -> None:
    search = RegistrySearch(FakeVectorStore(), HashingEmbeddingProvider(dimension=64))
    store, loader, _ = await _store(search=search)

    await store.submit(
        _manifest(
            "expediter", "optimizes carrier selection and expedited routing for in-flight orders"
        )
    )
    await store.submit(
        _manifest("invoicer", "creates open invoices and reconciles billing actuals")
    )
    await store.approve("expediter")
    await store.approve("invoicer")

    hits = await search.search(
        "tune purchase cycle pacing and bid strategy", loader, tenant=TenantContext(tenant_id="t")
    )
    assert hits and hits[0].id == "expediter"


async def test_semantic_search_is_rbac_filtered() -> None:
    search = RegistrySearch(FakeVectorStore(), HashingEmbeddingProvider(dimension=64))
    store, loader, _ = await _store(search=search)
    await store.submit(
        _manifest("expediter", "optimizes purchase cycle pacing", scopes=["optimize:run"])
    )
    await store.approve("expediter")

    unscoped = await search.search(
        "purchase cycle pacing", loader, tenant=TenantContext(tenant_id="t")
    )
    assert unscoped == []  # scope-gated out

    scoped = await search.search(
        "purchase cycle pacing",
        loader,
        tenant=TenantContext(tenant_id="t", attributes={"scopes": "optimize:run"}),
    )
    assert [m.id for m in scoped] == ["expediter"]


# certification-gated approval
async def test_certification_gate_blocks_failing_agent() -> None:
    async def certifier(manifest: AgentManifest) -> tuple[bool, float]:
        # the certification suite outcome (eval harness wired in production)
        return ("good" in manifest.id, 0.9 if "good" in manifest.id else 0.2)

    store, loader, _ = await _store(certifier=certifier)
    await store.submit(
        _manifest(
            "good-agent",
            "x",
            certification={"dataset_ref": "cert.json", "min_pass_rate": 0.8},
        )
    )
    await store.submit(
        _manifest(
            "bad-agent",
            "x",
            certification={"dataset_ref": "cert.json", "min_pass_rate": 0.8},
        )
    )

    await store.approve("good-agent")  # passes certification
    with pytest.raises(RegistryTrustError, match="certification"):
        await store.approve("bad-agent")

    approved = {m.id for m in await loader.manifests()}
    assert approved == {"good-agent"}


# registry-driven routing + canary
async def test_dispatch_routes_via_manifest_queue() -> None:
    store, loader, _ = await _store()
    await store.submit(
        _manifest("expediter", "x", att=["expediting"], queue="suppliers.expediter.v1")
    )
    await store.approve("expediter")

    broker = InMemoryBroker()
    dispatcher = QueueDispatcher(broker, Signer({"default": "s"}), registry_loader=loader)
    sc = StageContext(
        run_id="r1", tenant=TenantContext(tenant_id="t"), goal="g", run_store=InMemoryRunStore()
    )
    await dispatcher.dispatch(
        DelegationContract(objective="optimize pacing", output_format="t", task_type="expediting"),
        sc,
    )
    assert broker.depth("suppliers.expediter.v1") == 1  # manifest queue, not tasks.expediting
    assert broker.depth("tasks.expediting") == 0


async def test_dispatch_falls_back_to_naming_convention() -> None:
    store, loader, _ = await _store()
    broker = InMemoryBroker()
    dispatcher = QueueDispatcher(broker, Signer({"default": "s"}), registry_loader=loader)
    sc = StageContext(
        run_id="r1", tenant=TenantContext(tenant_id="t"), goal="g", run_store=InMemoryRunStore()
    )
    await dispatcher.dispatch(
        DelegationContract(objective="serve", output_format="t", task_type="order_placement"), sc
    )
    assert broker.depth("tasks.order_placement") == 1


async def test_canary_cohort_is_deterministic_per_run() -> None:
    from maof.registry.search import in_canary_cohort

    assert in_canary_cohort("run-A", 0.0) is False
    assert in_canary_cohort("run-A", 100.0) is True
    assert in_canary_cohort("run-A", 37.5) == in_canary_cohort("run-A", 37.5)  # stable

    cohort = {run for run in (f"run-{i}" for i in range(200)) if in_canary_cohort(run, 50.0)}
    assert 60 <= len(cohort) <= 140  # roughly half, deterministic split


async def test_version_pin_mismatch_refuses_dispatch() -> None:
    store, loader, _ = await _store()
    await store.submit(_manifest("expediter", "x", att=["expediting"], version="v2"))
    await store.approve("expediter")

    broker = InMemoryBroker()
    dispatcher = QueueDispatcher(broker, Signer({"default": "s"}), registry_loader=loader)
    sc = StageContext(
        run_id="r1", tenant=TenantContext(tenant_id="t"), goal="g", run_store=InMemoryRunStore()
    )
    with pytest.raises(RegistryTrustError, match="pin"):
        await dispatcher.dispatch(
            DelegationContract(
                objective="optimize",
                output_format="t",
                task_type="expediting",
                pins={"agent_version": "v1"},  # workflow pinned v1; registry serves v2
            ),
            sc,
        )
