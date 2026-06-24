"""In-memory Broker for tests and embedded single-shot use.

Mirrors the real adapters' DLQ + retry-with-backoff semantics via
:class:`RetryPolicy` and per-attempt headers, so coordination-mode-(a) logic can
be exercised offline. NOTE: unlike a real broker, :meth:`consume` *drains* the
queue and returns rather than blocking forever.
"""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from maof.transport.retry import ATTEMPT_HEADER, RetryPolicy
from maof.types import IncomingMessage, QueueSpec

if TYPE_CHECKING:
    from maof.transport.base import Broker


@dataclass
class _Stored:
    body: bytes
    headers: dict[str, str]
    message_id: str
    correlation_id: str | None


class InMemoryBroker:
    def __init__(self) -> None:
        self._queues: dict[str, deque[_Stored]] = defaultdict(deque)
        self._policies: dict[str, RetryPolicy] = {}
        self._dlq: dict[str, str] = {}
        #: Backoff delays scheduled during the last consume — for test assertions.
        self.scheduled_delays: list[float] = []

    async def ensure_topology(self, queues: list[QueueSpec]) -> None:
        for q in queues:
            self._queues.setdefault(q.name, deque())
            if q.retry_steps:
                self._policies[q.name] = RetryPolicy(q.retry_steps)
            if q.dlq_name:
                self._dlq[q.name] = q.dlq_name
                self._queues.setdefault(q.dlq_name, deque())

    async def publish(
        self,
        queue: str,
        body: bytes,
        *,
        headers: dict[str, str],
        message_id: str,
        correlation_id: str | None = None,
        persistent: bool = True,
    ) -> None:
        self._queues[queue].append(_Stored(body, dict(headers), message_id, correlation_id))

    async def consume(
        self,
        queue: str,
        *,
        prefetch: int,
        handler: Callable[[IncomingMessage], Awaitable[None]],
    ) -> None:
        q = self._queues[queue]
        while q:
            stored = q.popleft()
            attempt = int(stored.headers.get(ATTEMPT_HEADER, "1"))
            msg = IncomingMessage(
                body=stored.body,
                headers=dict(stored.headers),
                message_id=stored.message_id,
                queue=queue,
                correlation_id=stored.correlation_id,
                redelivered=attempt > 1,
                attempt=attempt,
            )
            try:
                await handler(msg)
            except Exception:  # noqa: BLE001 - broker must isolate handler failures
                self._on_failure(queue, stored, attempt)

    def _on_failure(self, queue: str, stored: _Stored, attempt: int) -> None:
        policy = self._policies.get(queue)
        delay = policy.delay_for_attempt(attempt) if policy is not None else None
        if delay is not None:
            self.scheduled_delays.append(delay)
            retried = dict(stored.headers)
            retried[ATTEMPT_HEADER] = str(attempt + 1)
            self._queues[queue].append(
                _Stored(stored.body, retried, stored.message_id, stored.correlation_id)
            )
            return
        dlq = self._dlq.get(queue)
        if dlq is not None:
            self._queues[dlq].append(stored)

    def depth(self, queue: str) -> int:
        return len(self._queues[queue])

    def peek(self, queue: str) -> list[tuple[bytes, dict[str, str], str, str | None]]:
        """Inspect queued messages without consuming (test/debug + redelivery sims)."""
        return [
            (s.body, dict(s.headers), s.message_id, s.correlation_id) for s in self._queues[queue]
        ]


if TYPE_CHECKING:
    _assert_broker: Broker = InMemoryBroker()  # structural conformance check


__all__ = ["InMemoryBroker"]
