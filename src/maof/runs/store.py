"""Durable run store + append-only trace store.

Every orchestration is a durable run: workers stay stateless for scaling; the
*run* holds the state machine and the shared, append-only trace that delegations
reference. Default backend is Postgres (event-sourced run log).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from maof.types import RunState, RunStatus, TenantContext, TraceEntry

if TYPE_CHECKING:  # keep core importable without the postgres extra
    from maof.persistence.postgres import Database


@runtime_checkable
class RunStore(Protocol):
    async def create(self, tenant: TenantContext, goal: str) -> str: ...

    async def append_trace(self, run_id: str, entry: TraceEntry) -> None: ...

    async def read_trace(self, run_id: str, *, since: str | None = None) -> list[TraceEntry]: ...

    async def get_state(self, run_id: str) -> RunState: ...


class PostgresRunStore:
    """Postgres-backed RunStore (event-sourced runs + append-only run_trace)."""

    def __init__(self, db: Database) -> None:
        self._db = db

    async def create(self, tenant: TenantContext, goal: str) -> str:
        run_id: str = await self._db.fetchval(
            "INSERT INTO runs (tenant_id, goal) VALUES ($1, $2) RETURNING run_id",
            tenant.tenant_id,
            goal,
        )
        return run_id

    async def append_trace(self, run_id: str, entry: TraceEntry) -> None:
        # Appends are serialized per run via an advisory lock so MAX(seq)+1 cannot
        # collide under concurrent writers (context-shared subagents on one trace).
        async with self._db.pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext('run_trace:' || $1))", run_id)
            await conn.execute(
                """
                INSERT INTO run_trace (run_id, seq, kind, step, data)
                VALUES ($1,
                        COALESCE((SELECT MAX(seq) FROM run_trace WHERE run_id = $1), 0) + 1,
                        $2, $3, $4)
                """,
                run_id,
                entry.kind,
                entry.step,
                entry.data,
            )

    async def read_trace(self, run_id: str, *, since: str | None = None) -> list[TraceEntry]:
        if since is not None:
            rows = await self._db.fetch(
                "SELECT * FROM run_trace WHERE run_id = $1 AND seq > $2 ORDER BY seq ASC",
                run_id,
                int(since),
            )
        else:
            rows = await self._db.fetch(
                "SELECT * FROM run_trace WHERE run_id = $1 ORDER BY seq ASC", run_id
            )
        return [
            TraceEntry(
                run_id=run_id,
                seq=int(r["seq"]),
                kind=r["kind"],
                step=r["step"],
                data=dict(r["data"]),
                ts=r["ts"].isoformat(),
            )
            for r in rows
        ]

    async def get_state(self, run_id: str) -> RunState:
        row = await self._db.fetchrow("SELECT * FROM runs WHERE run_id = $1", run_id)
        if row is None:
            raise KeyError(f"run not found: {run_id}")
        return RunState(
            run_id=row["run_id"],
            tenant_id=row["tenant_id"],
            goal=row["goal"],
            status=RunStatus(row["status"]),
            current_step=row["current_step"],
            cancel_requested=bool(row["cancel_requested"]),
            updated_at=row["updated_at"].isoformat(),
        )

    async def request_cancel(self, run_id: str) -> None:
        """Cooperative cancel: the driver/loop/workers check this flag."""
        await self._db.execute(
            "UPDATE runs SET cancel_requested = TRUE, updated_at = now() WHERE run_id = $1",
            run_id,
        )

    async def set_state(
        self,
        run_id: str,
        *,
        status: RunStatus | None = None,
        current_step: str | None = None,
    ) -> None:
        """Advance the run state machine (used by checkpoint/resume in later phases)."""
        await self._db.execute(
            """
            UPDATE runs
               SET status = COALESCE($2, status),
                   current_step = COALESCE($3, current_step),
                   updated_at = now()
             WHERE run_id = $1
            """,
            run_id,
            status.value if status is not None else None,
            current_step,
        )


class InMemoryRunStore:
    """Process-local RunStore for tests and embedded single-process runs."""

    def __init__(self) -> None:
        self._runs: dict[str, RunState] = {}
        self._traces: dict[str, list[TraceEntry]] = {}
        self._seq = 0

    async def create(self, tenant: TenantContext, goal: str) -> str:
        self._seq += 1
        run_id = f"run-{self._seq}"
        self._runs[run_id] = RunState(run_id=run_id, tenant_id=tenant.tenant_id, goal=goal)
        self._traces[run_id] = []
        return run_id

    async def append_trace(self, run_id: str, entry: TraceEntry) -> None:
        bucket = self._traces.setdefault(run_id, [])
        stamped = entry.model_copy(update={"seq": len(bucket) + 1, "run_id": run_id})
        bucket.append(stamped)

    async def read_trace(self, run_id: str, *, since: str | None = None) -> list[TraceEntry]:
        entries = self._traces.get(run_id, [])
        if since is not None:
            cutoff = int(since)
            return [e for e in entries if e.seq > cutoff]
        return list(entries)

    async def get_state(self, run_id: str) -> RunState:
        if run_id not in self._runs:
            raise KeyError(f"run not found: {run_id}")
        return self._runs[run_id]

    async def request_cancel(self, run_id: str) -> None:
        state = self._runs[run_id]
        self._runs[run_id] = state.model_copy(update={"cancel_requested": True})

    async def set_state(
        self,
        run_id: str,
        *,
        status: RunStatus | None = None,
        current_step: str | None = None,
    ) -> None:
        state = self._runs[run_id]
        self._runs[run_id] = state.model_copy(
            update={
                "status": status if status is not None else state.status,
                "current_step": current_step if current_step is not None else state.current_step,
            }
        )


__all__ = ["RunStore", "PostgresRunStore", "InMemoryRunStore"]
