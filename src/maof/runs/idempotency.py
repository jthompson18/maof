"""Idempotency guard for side-effecting actions.

REQUIRED around every side effect (publish, external tool writes, payments,
emails): run ``fn`` at most once per deterministic key; on replay return the
prior result instead of re-firing. This is what makes resumed runs safe.

Concurrency: executions of the SAME key are serialized — Postgres via a
transaction-scoped advisory lock (works across processes), in-memory via a
per-key asyncio.Lock — so two consumers racing a redelivered message cannot
both fire the side effect.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from maof.persistence.postgres import Database

T = TypeVar("T")


def make_idempotency_key(run_id: str, step_id: str, task_type: str, body: dict[str, Any]) -> str:
    """The deterministic key: ``sha256(run_id, step_id, task_type, canonical(body))``.

    Canonical JSON (sorted keys) so equal bodies in any key order hash equally."""
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    material = "\x00".join([run_id, step_id, task_type, canonical])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@runtime_checkable
class IdempotencyGuard(Protocol):
    async def once(self, key: str, fn: Callable[[], Awaitable[T]]) -> T: ...


class InMemoryIdempotencyGuard:
    """Process-local guard (tests, embedded single-process runs). Same-key calls
    are serialized by a per-key lock so concurrent racers cannot both run fn."""

    def __init__(self) -> None:
        self._results: dict[str, Any] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def once(self, key: str, fn: Callable[[], Awaitable[T]]) -> T:
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            if key in self._results:
                return self._results[key]  # type: ignore[no-any-return]
            result = await fn()
            self._results[key] = result
            return result


class PostgresIdempotencyGuard:
    """Durable guard backed by the ``idempotency_keys`` table. The result must be a
    JSON-serializable value (e.g. a dict) so it survives replay across processes.

    Same-key executions are serialized via ``pg_advisory_xact_lock(hashtext(key))``
    held for the check -> fn() -> record critical section (released on commit), so
    the check-then-act sequence is race-free across processes. Note: ``fn`` runs
    while one pool connection is held — keep the pool sized above worker concurrency.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    async def once(self, key: str, fn: Callable[[], Awaitable[T]]) -> T:
        async with self._db.pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", key)
            existing = await conn.fetchrow(
                "SELECT result FROM idempotency_keys WHERE key = $1", key
            )
            if existing is not None:
                return existing["result"]  # type: ignore[no-any-return]
            result = await fn()
            await conn.execute(
                "INSERT INTO idempotency_keys (key, result) VALUES ($1, $2) "
                "ON CONFLICT (key) DO NOTHING",
                key,
                result,
            )
            return result


__all__ = [
    "IdempotencyGuard",
    "InMemoryIdempotencyGuard",
    "PostgresIdempotencyGuard",
    "make_idempotency_key",
]
