"""Registry intelligence: semantic capability search + canary cohorts.

On approval, a manifest's capability description is embedded into the configured
VectorStore (kind ``registry_capability``); planners then select third-party
systems by *describing the need* rather than matching exact task-type strings.
Hits are RBAC-filtered per tenant/principal before they reach the planner.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.policy.rulesets import in_canary
from maof.registry.loader import _scopes_granted
from maof.types import MemorySnippet, TenantContext

if TYPE_CHECKING:
    from maof.memory.base import VectorStore
    from maof.models.base import EmbeddingProvider
    from maof.registry.loader import RegistryLoader
    from maof.registry.models import AgentManifest

#: Manifest embeddings are global (manifests are not tenant data); RBAC filtering
#: happens per query against the asking tenant/principal.
REGISTRY_TENANT = TenantContext(tenant_id="__registry__")


def in_canary_cohort(run_id: str, canary_pct: float) -> bool:
    """Deterministic per-run canary membership — same hash-bucket routing
    as policy ruleset canaries, keyed by run_id."""
    return in_canary(run_id, canary_pct)


class RegistrySearch:
    def __init__(self, vector_store: VectorStore, embedder: EmbeddingProvider) -> None:
        self._vector = vector_store
        self._embedder = embedder

    @staticmethod
    def _capability_text(manifest: AgentManifest) -> str:
        parts = [manifest.description, *manifest.capabilities, *manifest.accepted_task_types]
        return " ".join(p for p in parts if p)

    async def index(self, manifest: AgentManifest) -> None:
        """Embed the manifest's capability text (called by the store on approve)."""
        text = self._capability_text(manifest)
        if not text:
            return
        embedding = (await self._embedder.embed([text]))[0]
        await self._vector.upsert(
            REGISTRY_TENANT,
            [
                MemorySnippet(
                    kind="registry_capability",
                    content=text,
                    prov=manifest.id,
                    embedding=embedding,
                )
            ],
        )

    async def search(
        self,
        query: str,
        loader: RegistryLoader,
        *,
        tenant: TenantContext | None = None,
        principal: object | None = None,
        top_k: int = 5,
    ) -> list[AgentManifest]:
        """Top-k approved manifests by capability-description similarity, RBAC-filtered."""
        embedding = (await self._embedder.embed([query]))[0]
        hits = await self._vector.query(REGISTRY_TENANT, embedding, top_k * 3)
        by_id = {m.id: m for m in await loader.manifests()}
        ranked: list[AgentManifest] = []
        for hit in hits:
            manifest = by_id.get(hit.prov)
            if manifest is None:  # revoked/tampered entries fall out via the loader
                continue
            if not _scopes_granted(manifest, tenant, principal=principal):
                continue
            ranked.append(manifest)
            if len(ranked) >= top_k:
                break
        return ranked


__all__ = ["RegistrySearch", "in_canary_cohort", "REGISTRY_TENANT"]
