"""Run operations: the console/ops-facing surface.

list / show / trace / cancel / resume / wake over the runs tables. ``cancel`` is
cooperative: it sets ``cancel_requested`` (checked at stage/iteration boundaries
and by workers); a run parked ``WAITING`` transitions to ``CANCELLED`` immediately
since nothing else will observe the flag. Exposed via :func:`create_runs_app`
(FastAPI, ``api`` extra) and ``maof runs ...``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from maof.authz import (
    SCOPE_RUNS_READ,
    SCOPE_RUNS_WRITE,
    require_run_read,
    require_run_write,
    require_scopes,
)
from maof.types import RunStatus

if TYPE_CHECKING:
    from maof.identity import Principal
    from maof.persistence.postgres import Database
    from maof.types import TenantContext


class RunOps:
    def __init__(self, db: Database, *, waker: Any | None = None) -> None:
        self._db = db
        self._waker = waker

    async def list_runs(
        self,
        *,
        tenant_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
        principal: Principal | None = None,
    ) -> list[dict[str, Any]]:
        if principal is not None:
            require_scopes(principal, {SCOPE_RUNS_READ})
        rows = await self._db.fetch(
            """
            SELECT run_id, tenant_id, goal, status, current_step, cancel_requested,
                   created_at, updated_at
              FROM runs
             WHERE ($1::text IS NULL OR tenant_id = $1)
               AND ($2::text IS NULL OR status = $2)
             ORDER BY created_at DESC
             LIMIT $3
            """,
            tenant_id,
            status,
            limit,
        )
        return [dict(r) for r in rows]

    async def show(
        self,
        run_id: str,
        *,
        principal: Principal | None = None,
        tenant: TenantContext | None = None,
    ) -> dict[str, Any] | None:
        row = await self._db.fetchrow("SELECT * FROM runs WHERE run_id = $1", run_id)
        if row is None:
            return None
        if principal is not None:  # RBAC engaged: runs:read + tenant isolation
            require_run_read(row["tenant_id"], principal, tenant=tenant)
        return dict(row)

    async def trace(
        self,
        run_id: str,
        *,
        principal: Principal | None = None,
        tenant: TenantContext | None = None,
    ) -> list[dict[str, Any]]:
        if principal is not None:  # authorize against the run's owning tenant first
            run_tenant = await self._db.fetchval(
                "SELECT tenant_id FROM runs WHERE run_id = $1", run_id
            )
            if run_tenant is None:
                return []
            require_run_read(str(run_tenant), principal, tenant=tenant)
        rows = await self._db.fetch(
            "SELECT seq, kind, step, data, ts FROM run_trace WHERE run_id = $1 ORDER BY seq",
            run_id,
        )
        return [dict(r) for r in rows]

    async def cancel(
        self,
        run_id: str,
        *,
        principal: Principal | None = None,
        tenant: TenantContext | None = None,
    ) -> None:
        if principal is not None:  # runs:write + tenant isolation against the run's owner
            owner = await self._db.fetchval("SELECT tenant_id FROM runs WHERE run_id = $1", run_id)
            if owner is None:
                return  # nothing to cancel
            require_run_write(str(owner), principal, tenant=tenant)
        await self._db.execute(
            "UPDATE runs SET cancel_requested = TRUE, updated_at = now() WHERE run_id = $1",
            run_id,
        )
        row = await self._db.fetchrow("SELECT status FROM runs WHERE run_id = $1", run_id)
        # WAITING/PENDING runs have nothing active to observe the flag — finalize now.
        if row is not None and RunStatus(row["status"]) in (RunStatus.WAITING, RunStatus.PENDING):
            await self._db.execute(
                "UPDATE runs SET status = 'cancelled', updated_at = now() WHERE run_id = $1",
                run_id,
            )
            await self._db.execute(
                "UPDATE run_wakeups SET status = 'cancelled' "
                "WHERE run_id = $1 AND status = 'pending'",
                run_id,
            )

    async def wake(self, event_key: str, *, principal: Principal | None = None) -> list[str]:
        """Fire an external event; returns the run_ids whose waits matched.
        The caller (waker poller / orchestrator service) resumes them."""
        if principal is not None:
            require_scopes(principal, {SCOPE_RUNS_WRITE})
        if self._waker is None:
            return []
        woken: list[str] = await self._waker.fire_event(event_key)
        return woken


def create_runs_app(
    ops: RunOps,
    *,
    principal_resolver: Callable[[str | None], Awaitable[Principal | None]] | None = None,
) -> Any:
    """FastAPI runs API (requires the ``api`` extra).

    ``principal_resolver`` is the bring-your-own-auth seam: given the Authorization
    header, return a Principal (or raise to fail closed). When set, it gates run reads."""
    from fastapi import FastAPI, Header, HTTPException
    from fastapi.responses import JSONResponse

    from maof.errors import AuthzError

    app = FastAPI(title="MAOF Runs")

    @app.exception_handler(AuthzError)
    async def _on_authz_error(_: Any, exc: AuthzError) -> JSONResponse:
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    async def _principal(authorization: str | None) -> Principal | None:
        return await principal_resolver(authorization) if principal_resolver is not None else None

    async def list_runs(
        tenant_id: str | None = None,
        status: str | None = None,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        principal = await _principal(authorization)
        return {
            "runs": await ops.list_runs(tenant_id=tenant_id, status=status, principal=principal)
        }

    async def show(run_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
        run = await ops.show(run_id, principal=await _principal(authorization))
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        return run

    async def trace(
        run_id: str, authorization: str | None = Header(default=None)
    ) -> dict[str, Any]:
        return {"trace": await ops.trace(run_id, principal=await _principal(authorization))}

    async def cancel(
        run_id: str, authorization: str | None = Header(default=None)
    ) -> dict[str, str]:
        await ops.cancel(run_id, principal=await _principal(authorization))
        return {"run_id": run_id, "status": "cancel_requested"}

    async def wake(
        event_key: str, authorization: str | None = Header(default=None)
    ) -> dict[str, Any]:
        return {"woken": await ops.wake(event_key, principal=await _principal(authorization))}

    app.add_api_route("/runs", list_runs, methods=["GET"])
    app.add_api_route("/runs/{run_id}", show, methods=["GET"])
    app.add_api_route("/runs/{run_id}/trace", trace, methods=["GET"])
    app.add_api_route("/runs/{run_id}/cancel", cancel, methods=["POST"])
    app.add_api_route("/runs/wake/{event_key}", wake, methods=["POST"])
    return app


__all__ = ["RunOps", "create_runs_app"]
