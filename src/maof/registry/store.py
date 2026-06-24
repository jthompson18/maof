"""Discovery registry store + lifecycle.

submit -> pending; approve -> sign + approved; revoke -> revoked. Every transition
emits an audit event. Backed by any RegistryRepo (Postgres default; in-memory for
tests/embedded).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.authz import SCOPE_REGISTRY_APPROVE, SCOPE_REGISTRY_AUTHOR, require_scopes
from maof.errors import RegistryTrustError
from maof.observability.events import AuditEvent
from maof.registry.models import RegistryEntry
from maof.registry.signing import sign_entry

if TYPE_CHECKING:
    from maof.identity import Principal
    from maof.observability.events import EventSink
    from maof.persistence.base import RegistryRepo
    from maof.registry.models import AgentManifest
    from maof.transport.signing import Signer


class InMemoryRegistryRepo:
    def __init__(self) -> None:
        self._entries: dict[str, RegistryEntry] = {}

    async def put(self, entry: RegistryEntry) -> None:
        self._entries[entry.manifest.id] = entry

    async def get(self, entry_id: str) -> RegistryEntry | None:
        return self._entries.get(entry_id)

    async def list_approved(self) -> list[RegistryEntry]:
        return [e for e in self._entries.values() if e.status == "approved"]


class RegistryStore:
    def __init__(
        self,
        repo: RegistryRepo,
        signer: Signer,
        *,
        event_sink: EventSink | None = None,
        search: object | None = None,
        certifier: object | None = None,
    ) -> None:
        self._repo = repo
        self._signer = signer
        self._event_sink = event_sink
        self._search = search  # RegistrySearch: indexes descriptions on approve
        # async (AgentManifest) -> (passed, pass_rate): wire the eval harness here
        # (run manifest.certification["dataset_ref"] and gate on min_pass_rate).
        self._certifier = certifier

    async def submit(
        self, manifest: AgentManifest, *, principal: Principal | None = None
    ) -> RegistryEntry:
        if principal is not None:
            require_scopes(principal, {SCOPE_REGISTRY_AUTHOR})
        entry = RegistryEntry(manifest=manifest, status="pending")
        await self._repo.put(entry)
        await self._emit("registry_submitted", entry, principal=principal)
        return entry

    async def approve(self, entry_id: str, *, principal: Principal | None = None) -> RegistryEntry:
        if principal is not None:
            require_scopes(principal, {SCOPE_REGISTRY_APPROVE})
        entry = await self._repo.get(entry_id)
        if entry is None:
            raise KeyError(f"registry entry not found: {entry_id!r}")
        if entry.manifest.certification is not None and self._certifier is not None:
            # Certification gate: the agent must pass its eval suite first.
            passed, pass_rate = await self._certifier(entry.manifest)  # type: ignore[operator]
            min_rate = float(entry.manifest.certification.get("min_pass_rate", 1.0))
            if not passed or pass_rate < min_rate:
                await self._emit("registry_certification_failed", entry, principal=principal)
                raise RegistryTrustError(
                    f"registry entry {entry_id!r} failed certification "
                    f"(pass_rate={pass_rate:.2f} < min {min_rate:.2f})"
                )
        signed = sign_entry(entry, self._signer)
        await self._repo.put(signed)
        if self._search is not None:
            await self._search.index(signed.manifest)  # type: ignore[attr-defined]
        await self._emit("registry_approved", signed, principal=principal)
        return signed

    async def revoke(self, entry_id: str, *, principal: Principal | None = None) -> RegistryEntry:
        if principal is not None:
            require_scopes(principal, {SCOPE_REGISTRY_APPROVE})
        entry = await self._repo.get(entry_id)
        if entry is None:
            raise KeyError(f"registry entry not found: {entry_id!r}")
        # Destroy the signature: a DB writer flipping status back to "approved"
        # cannot resurrect the entry without re-signing (needs the key).
        revoked = entry.model_copy(update={"status": "revoked", "signature": None, "kid": None})
        await self._repo.put(revoked)
        await self._emit("registry_revoked", revoked, principal=principal)
        return revoked

    async def _emit(
        self, event_type: str, entry: RegistryEntry, *, principal: Principal | None = None
    ) -> None:
        if self._event_sink is None:
            return
        await self._event_sink.emit(
            AuditEvent(
                tenant_id="",
                intent_id=None,
                event_type=event_type,
                actor=principal.as_actor() if principal is not None else None,
                envelope={"entry_id": entry.manifest.id, "kind": entry.manifest.kind},
                details={"status": entry.status},
            )
        )


__all__ = ["RegistryStore", "InMemoryRegistryRepo"]
