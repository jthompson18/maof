"""HITL approval gate + FastAPI approval service (toggleable).

The gate is what the ``approval`` stage blocks on; the FastAPI service is how a
human resolves it (approve/deny). Both are toggleable: disable HITL and the gate
is never consulted. The FastAPI app requires the ``api`` extra and is constructed
lazily so this module imports without it.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from maof.errors import ApprovalRequired
from maof.observability.events import AuditEvent

if TYPE_CHECKING:
    from maof.observability.events import EventSink
    from maof.persistence.base import ApprovalRepo
    from maof.types import StageContext


@dataclass
class _Pending:
    event: asyncio.Event = field(default_factory=asyncio.Event)
    approved: bool = False
    reason: str = ""
    # multi-party role-bound approvals
    required_roles: list[str] = field(default_factory=list)
    parties: int = 1
    resolutions: list[dict[str, Any]] = field(default_factory=list)


class ApprovalGate:
    """Coordinates a blocking approval. ``wait`` requests an approval and blocks the
    stage; ``resolve`` (called by the FastAPI service or a test) unblocks it."""

    def __init__(
        self,
        *,
        repo: ApprovalRepo | None = None,
        event_sink: EventSink | None = None,
        timeout: float | None = None,
        poll_interval: float = 0.5,
        auto_approve: bool = False,
    ) -> None:
        self._repo = repo
        self._event_sink = event_sink
        self._timeout = timeout
        self._poll_interval = poll_interval
        self._auto_approve = auto_approve
        self._pending: dict[str, _Pending] = {}
        self._counter = 0

    async def request(
        self,
        tenant_id: str,
        run_id: str,
        reason: str,
        *,
        required_roles: list[str] | None = None,
        parties: int = 1,
    ) -> str:
        if self._repo is not None:
            from maof.types import TenantContext as _TC

            approval_id = await self._repo.create(_TC(tenant_id=tenant_id), run_id, reason)
        else:
            self._counter += 1
            approval_id = f"appr-{run_id}-{self._counter}"
        self._pending[approval_id] = _Pending(
            required_roles=list(required_roles or []), parties=parties
        )
        await self._emit("approval_requested", tenant_id, run_id, approval_id, reason)
        return approval_id

    def resolutions(self, approval_id: str) -> list[dict[str, Any]]:
        """Per-party attribution for a multi-party approval."""
        pending = self._pending.get(approval_id)
        return list(pending.resolutions) if pending is not None else []

    async def wait_for(self, approval_id: str) -> bool:
        """Block until the approval resolves. Two signals are honored: the
        in-process event (fast path), and — when a repo is wired — the persisted
        approval row, polled so a resolution from ANOTHER process (e.g. the
        FastAPI approval service container) also unblocks this waiter."""
        pending = self._pending.get(approval_id)
        if pending is None and self._repo is None:
            raise KeyError(f"unknown approval: {approval_id!r}")
        loop = asyncio.get_running_loop()
        deadline = (loop.time() + self._timeout) if self._timeout is not None else None
        while True:
            if pending is not None and pending.event.is_set():
                return pending.approved
            if self._repo is not None:
                row = await self._repo.get(approval_id)
                status = (row or {}).get("status")
                if status == "approved":
                    return True
                if status == "denied":
                    return False
            if deadline is not None and loop.time() >= deadline:
                raise TimeoutError(f"approval {approval_id!r} timed out")
            wait_budget = self._poll_interval
            if deadline is not None:
                wait_budget = min(wait_budget, max(0.01, deadline - loop.time()))
            if pending is not None:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(pending.event.wait(), timeout=wait_budget)
            else:
                await asyncio.sleep(wait_budget)

    async def resolve(
        self,
        approval_id: str,
        *,
        approved: bool,
        reason: str = "",
        tenant_id: str | None = None,
        principal: Any | None = None,
    ) -> None:
        pending = self._pending.get(approval_id)
        if pending is not None and pending.required_roles:
            # Role-bound N-of-M: only matching roles count; each distinct
            # principal counts once; one qualified deny fails the approval.
            roles = set(getattr(principal, "roles", []) or []) if principal else set()
            if principal is None or not roles & set(pending.required_roles):
                raise PermissionError(
                    f"approval {approval_id!r} requires one of roles "
                    f"{pending.required_roles}; got {sorted(roles)}"
                )
            if not approved:
                pending.approved = False
                pending.reason = reason
                pending.event.set()
            else:
                seen = {r["principal_id"] for r in pending.resolutions}
                if principal.id not in seen:
                    pending.resolutions.append(
                        {
                            "principal_id": principal.id,
                            "org": getattr(principal, "org", ""),
                            "roles": list(getattr(principal, "roles", [])),
                            "approved": True,
                        }
                    )
                if len(pending.resolutions) < pending.parties:
                    return  # await further parties
                pending.approved = True
                pending.event.set()
            if self._repo is not None:
                await self._repo.resolve(
                    approval_id, approved=pending.approved, tenant_id=tenant_id
                )
            await self._emit(
                "approval_granted" if pending.approved else "approval_denied",
                "",
                "",
                approval_id,
                reason,
            )
            return
        if self._repo is not None:
            await self._repo.resolve(approval_id, approved=approved, tenant_id=tenant_id)
        if pending is not None:
            pending.approved = approved
            pending.reason = reason
            pending.event.set()
        await self._emit(
            "approval_granted" if approved else "approval_denied", "", "", approval_id, reason
        )

    async def wait(
        self,
        sc: StageContext,
        *,
        reason: str,
        required_roles: list[str] | None = None,
        parties: int = 1,
    ) -> None:
        """Stage entry point: block until approved (N-of-M role-bound parties when
        ``required_roles`` is set); raise ApprovalRequired on denial; gate timeout
        fails closed."""
        if self._auto_approve:
            return
        approval_id = await self.request(
            sc.tenant.tenant_id, sc.run_id, reason, required_roles=required_roles, parties=parties
        )
        sc.extras["approval_id"] = approval_id
        if not await self.wait_for(approval_id):
            raise ApprovalRequired(reason or "approval denied")

    async def _emit(
        self, event_type: str, tenant_id: str, run_id: str, approval_id: str, reason: str
    ) -> None:
        if self._event_sink is None:
            return
        await self._event_sink.emit(
            AuditEvent(
                tenant_id=tenant_id,
                intent_id=None,
                event_type=event_type,
                envelope={"run_id": run_id},
                details={"approval_id": approval_id, "reason": reason},
            )
        )


def create_approval_app(
    gate: ApprovalGate,
    *,
    signing_secret: str = "",
    on_startup: Callable[[], Awaitable[None]] | None = None,
    on_shutdown: Callable[[], Awaitable[None]] | None = None,
) -> Any:
    """Build the FastAPI approval service (requires the ``api`` extra). The
    startup/shutdown hooks run inside the app's lifespan — e.g. binding the
    repo's asyncpg pool to uvicorn's event loop."""
    from contextlib import asynccontextmanager

    from fastapi import FastAPI, HTTPException

    from maof.approval.tokens import verify_approval_token

    @asynccontextmanager
    async def lifespan(_: Any) -> Any:
        if on_startup is not None:
            await on_startup()
        try:
            yield
        finally:
            if on_shutdown is not None:
                await on_shutdown()

    app = FastAPI(title="MAOF Approvals", lifespan=lifespan)

    def _check(approval_id: str, token: str) -> None:
        if signing_secret:
            from maof.errors import SignatureError

            try:
                if verify_approval_token(token, signing_secret) != approval_id:
                    raise HTTPException(status_code=403, detail="token/approval mismatch")
            except SignatureError as exc:
                raise HTTPException(status_code=403, detail=str(exc)) from exc

    async def approve(approval_id: str, token: str = "") -> dict[str, str]:
        _check(approval_id, token)
        await gate.resolve(approval_id, approved=True)
        return {"approval_id": approval_id, "status": "approved"}

    async def deny(approval_id: str, token: str = "") -> dict[str, str]:
        _check(approval_id, token)
        await gate.resolve(approval_id, approved=False)
        return {"approval_id": approval_id, "status": "denied"}

    app.add_api_route("/approvals/{approval_id}/approve", approve, methods=["POST"])
    app.add_api_route("/approvals/{approval_id}/deny", deny, methods=["POST"])
    return app


__all__ = ["ApprovalGate", "create_approval_app"]
