"""Signed workflow store — the trust lifecycle for definitions.

submit -> pending; approve -> sign over {definition, status, approved_at};
load -> only approved + signature-valid; revoke -> destroys the signature (a DB
writer cannot resurrect a revoked workflow). Workflows move money — they get the
same trust treatment as registry entries.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pydantic import BaseModel

from maof.authz import SCOPE_WORKFLOW_APPROVE, SCOPE_WORKFLOW_AUTHOR, require_scopes
from maof.errors import RegistryTrustError, SignatureError
from maof.observability.events import AuditEvent
from maof.types import utcnow
from maof.workflows.definition import WorkflowDefinition

if TYPE_CHECKING:
    from maof.identity import Principal
    from maof.observability.events import EventSink
    from maof.persistence.postgres import Database
    from maof.transport.signing import Signer


class WorkflowEntry(BaseModel):
    definition: WorkflowDefinition
    status: str = "pending"  # pending | approved | revoked
    signature: str | None = None
    kid: str | None = None
    submitted_at: str = ""
    approved_at: str | None = None


def canonical_workflow_bytes(
    definition: WorkflowDefinition, *, status: str, approved_at: str | None
) -> bytes:
    payload = {
        "definition": definition.model_dump(),
        "status": status,
        "approved_at": approved_at,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


class InMemoryWorkflowRepo:
    def __init__(self) -> None:
        self._entries: dict[tuple[str, int], WorkflowEntry] = {}

    async def put(self, entry: WorkflowEntry) -> None:
        self._entries[(entry.definition.name, entry.definition.version)] = entry

    async def get(self, name: str, version: int) -> WorkflowEntry | None:
        return self._entries.get((name, version))

    async def latest_approved(self, name: str) -> WorkflowEntry | None:
        candidates = [
            e for (n, _), e in self._entries.items() if n == name and e.status == "approved"
        ]
        return max(candidates, key=lambda e: e.definition.version, default=None)

    async def list_entries(self) -> list[WorkflowEntry]:
        return list(self._entries.values())


class PostgresWorkflowRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def put(self, entry: WorkflowEntry) -> None:
        from maof.persistence.postgres import _parse_rfc3339

        await self._db.execute(
            """
            INSERT INTO workflows (name, version, status, definition, signature, kid, approved_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (name, version) DO UPDATE
              SET status = EXCLUDED.status, definition = EXCLUDED.definition,
                  signature = EXCLUDED.signature, kid = EXCLUDED.kid,
                  approved_at = EXCLUDED.approved_at
            """,
            entry.definition.name,
            entry.definition.version,
            entry.status,
            entry.definition.model_dump(),
            entry.signature,
            entry.kid,
            _parse_rfc3339(entry.approved_at),
        )

    @staticmethod
    def _to_entry(row: object) -> WorkflowEntry:
        from maof.persistence.postgres import _canonical_rfc3339

        r: dict[str, object] = dict(row)  # type: ignore[call-overload]
        return WorkflowEntry(
            definition=WorkflowDefinition.model_validate(r["definition"]),
            status=str(r["status"]),
            signature=r["signature"],  # type: ignore[arg-type]
            kid=r["kid"],  # type: ignore[arg-type]
            submitted_at=str(r["submitted_at"]),
            approved_at=_canonical_rfc3339(r["approved_at"]),  # type: ignore[arg-type]
        )

    async def get(self, name: str, version: int) -> WorkflowEntry | None:
        row = await self._db.fetchrow(
            "SELECT * FROM workflows WHERE name = $1 AND version = $2", name, version
        )
        return self._to_entry(row) if row is not None else None

    async def latest_approved(self, name: str) -> WorkflowEntry | None:
        row = await self._db.fetchrow(
            """
            SELECT * FROM workflows WHERE name = $1 AND status = 'approved'
            ORDER BY version DESC LIMIT 1
            """,
            name,
        )
        return self._to_entry(row) if row is not None else None

    async def list_entries(self) -> list[WorkflowEntry]:
        rows = await self._db.fetch("SELECT * FROM workflows ORDER BY name, version")
        return [self._to_entry(r) for r in rows]


class WorkflowStore:
    def __init__(
        self, repo: object, signer: Signer, *, event_sink: EventSink | None = None
    ) -> None:
        self.repo = repo
        self._signer = signer
        self._event_sink = event_sink

    async def submit(
        self, definition: WorkflowDefinition, *, principal: Principal | None = None
    ) -> WorkflowEntry:
        if principal is not None:
            require_scopes(principal, {SCOPE_WORKFLOW_AUTHOR})
        entry = WorkflowEntry(definition=definition, status="pending", submitted_at=utcnow())
        await self.repo.put(entry)  # type: ignore[attr-defined]
        return entry

    async def approve(
        self, name: str, version: int, *, principal: Principal | None = None
    ) -> WorkflowEntry:
        # Authoring authz: signing a money-bearing definition requires workflow:approve
        # when a principal is asserted (bring-your-own RBAC). Checked before any work.
        if principal is not None:
            require_scopes(principal, {SCOPE_WORKFLOW_APPROVE})
        entry: WorkflowEntry | None = await self.repo.get(name, version)  # type: ignore[attr-defined]
        if entry is None:
            raise KeyError(f"workflow not found: {name} v{version}")
        approved_at = utcnow()
        headers = self._signer.headers(
            canonical_workflow_bytes(entry.definition, status="approved", approved_at=approved_at)
        )
        signed = entry.model_copy(
            update={
                "status": "approved",
                "signature": headers["sig"],
                "kid": headers["kid"],
                "approved_at": approved_at,
            }
        )
        await self.repo.put(signed)  # type: ignore[attr-defined]
        if self._event_sink is not None:
            # Record the approver in both actor and details — the Postgres sink
            # persists details (JSONB) but not the actor column.
            await self._event_sink.emit(
                AuditEvent(
                    tenant_id="",
                    intent_id=None,
                    event_type="workflow_approved",
                    kind="governance",
                    actor=principal.as_actor() if principal is not None else None,
                    details={
                        "name": name,
                        "version": version,
                        "approver": principal.id if principal is not None else None,
                    },
                )
            )
        return signed

    async def revoke(
        self, name: str, version: int, *, principal: Principal | None = None
    ) -> WorkflowEntry:
        if principal is not None:
            require_scopes(principal, {SCOPE_WORKFLOW_APPROVE})
        entry: WorkflowEntry | None = await self.repo.get(name, version)  # type: ignore[attr-defined]
        if entry is None:
            raise KeyError(f"workflow not found: {name} v{version}")
        revoked = entry.model_copy(update={"status": "revoked", "signature": None, "kid": None})
        await self.repo.put(revoked)  # type: ignore[attr-defined]
        if self._event_sink is not None:
            await self._event_sink.emit(
                AuditEvent(
                    tenant_id="",
                    intent_id=None,
                    event_type="workflow_revoked",
                    kind="governance",
                    actor=principal.as_actor() if principal is not None else None,
                    details={"name": name, "version": version},
                )
            )
        return revoked

    async def load(self, name: str, version: int | None = None) -> WorkflowDefinition:
        """Return the executable definition — approved AND signature-valid only."""
        entry: WorkflowEntry | None
        if version is not None:
            entry = await self.repo.get(name, version)  # type: ignore[attr-defined]
        else:
            entry = await self.repo.latest_approved(name)  # type: ignore[attr-defined]
        if entry is None:
            raise RegistryTrustError(f"no approved workflow {name!r}")
        if entry.status != "approved" or not entry.signature or not entry.kid:
            raise RegistryTrustError(
                f"workflow {name!r} v{entry.definition.version} is not executable"
            )
        try:
            self._signer.verify(
                canonical_workflow_bytes(
                    entry.definition, status=entry.status, approved_at=entry.approved_at
                ),
                {"kid": entry.kid, "sig": entry.signature},
            )
        except SignatureError as exc:
            raise RegistryTrustError(
                f"workflow {name!r} v{entry.definition.version} failed verification"
            ) from exc
        return entry.definition


__all__ = [
    "WorkflowEntry",
    "WorkflowStore",
    "InMemoryWorkflowRepo",
    "PostgresWorkflowRepo",
    "canonical_workflow_bytes",
]
