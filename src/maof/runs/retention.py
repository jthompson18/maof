"""Retention pruning: the append-only tables stay bounded.

Deletes run_trace/audit_events rows past their retention windows and expired
idempotency keys (``idempotency_key_ttl_s``). Run via ``maof prune`` (cron it)
or call :func:`prune` from an ops scheduler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from maof.persistence.postgres import Database


def _deleted(status: str) -> int:
    # asyncpg execute returns e.g. "DELETE 42"
    try:
        return int(status.rsplit(" ", 1)[-1])
    except (ValueError, IndexError):
        return 0


async def prune(
    db: Database,
    *,
    trace_retention_days: int = 30,
    audit_retention_days: int = 90,
    idempotency_ttl_s: int = 86_400,
) -> dict[str, int]:
    """Apply the retention windows; returns rows deleted per table."""
    summary: dict[str, int] = {}
    summary["run_trace"] = _deleted(
        await db.execute(
            "DELETE FROM run_trace WHERE ts < now() - make_interval(days => $1)",
            trace_retention_days,
        )
    )
    summary["audit_events"] = _deleted(
        await db.execute(
            "DELETE FROM audit_events WHERE ts < now() - make_interval(days => $1)",
            audit_retention_days,
        )
    )
    summary["idempotency_keys"] = _deleted(
        await db.execute(
            "DELETE FROM idempotency_keys WHERE created_at < now() - make_interval(secs => $1)",
            idempotency_ttl_s,
        )
    )
    summary["run_wakeups"] = _deleted(
        await db.execute(
            "DELETE FROM run_wakeups WHERE status != 'pending' "
            "AND created_at < now() - make_interval(days => $1)",
            trace_retention_days,
        )
    )
    return summary


__all__ = ["prune"]
