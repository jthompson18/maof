"""L1-facing registry loader.

Returns only entries that are ``approved`` AND whose signature verifies. Tampered,
unsigned, or revoked entries are ignored and an event is emitted. The L1 consults
this to decide routable task targets, MCP L2 agents, and context sources.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.errors import RegistryTrustError
from maof.observability.events import AuditEvent
from maof.registry.signing import verify_entry

if TYPE_CHECKING:
    from maof.observability.events import EventSink
    from maof.persistence.base import RegistryRepo
    from maof.registry.models import AgentManifest, RegistryEntry
    from maof.transport.signing import Signer
    from maof.types import TenantContext


def _tenant_scopes(tenant: TenantContext) -> set[str]:
    """Tenant RBAC scopes, comma-separated in ``attributes["scopes"]``."""
    raw = tenant.attributes.get("scopes", "")
    return {scope.strip() for scope in raw.split(",") if scope.strip()}


def _scopes_granted(
    manifest: AgentManifest,
    tenant: TenantContext | None,
    *,
    principal: object | None = None,
) -> bool:
    """Every use is gated by rbac_scopes. Principal scopes are checked first,
    tenant scopes as fallback. ``tenant=None`` with no principal means the
        caller is not engaging RBAC (trusted internal path)."""
    if not manifest.rbac_scopes:
        return True
    if tenant is None and principal is None:
        return True
    held: set[str] = set()
    if principal is not None:
        held |= set(getattr(principal, "scopes", []) or [])
    if tenant is not None:
        held |= _tenant_scopes(tenant)
    return set(manifest.rbac_scopes) <= held


class RegistryLoader:
    def __init__(
        self, repo: RegistryRepo, signer: Signer, *, event_sink: EventSink | None = None
    ) -> None:
        self._repo = repo
        self._signer = signer
        self._event_sink = event_sink

    async def approved_entries(self) -> list[RegistryEntry]:
        trusted: list[RegistryEntry] = []
        for entry in await self._repo.list_approved():
            try:
                verify_entry(entry, self._signer)
            except RegistryTrustError as exc:
                await self._emit_rejected(entry, str(exc))
                continue
            trusted.append(entry)
        return trusted

    async def manifests(self) -> list[AgentManifest]:
        return [e.manifest for e in await self.approved_entries()]

    async def agents_for_task_type(
        self, task_type: str, *, tenant: TenantContext | None = None
    ) -> list[AgentManifest]:
        return [
            m
            for m in await self.manifests()
            if m.kind in ("l2_agent", "mcp_server")
            and task_type in m.accepted_task_types
            and _scopes_granted(m, tenant)
        ]

    async def context_sources(self, *, tenant: TenantContext | None = None) -> list[AgentManifest]:
        return [
            m
            for m in await self.manifests()
            if m.kind == "context_source" and _scopes_granted(m, tenant)
        ]

    async def _emit_rejected(self, entry: RegistryEntry, reason: str) -> None:
        if self._event_sink is None:
            return
        await self._event_sink.emit(
            AuditEvent(
                tenant_id="",
                intent_id=None,
                event_type="registry_revoked",
                severity="warning",
                envelope={"entry_id": entry.manifest.id},
                details={"reason": reason, "rejected": True},
            )
        )


__all__ = ["RegistryLoader"]
