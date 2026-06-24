"""Broker interface — coordination mode (a).

Governed async dispatch of *independent* tasks. NOT the path for interdependent
reasoning (that is the in-process context-shared subagent path via the
Coordinator). DLQ + retry-with-backoff + signing semantics are defined
here so every adapter (RabbitMQ default, Kafka, Redis, SQS) behaves the same.
Side-effecting consumers must honor idempotency keys so replay is safe.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from maof.types import IncomingMessage, QueueSpec


@runtime_checkable
class Broker(Protocol):
    async def publish(
        self,
        queue: str,
        body: bytes,
        *,
        headers: dict[str, str],
        message_id: str,
        correlation_id: str,
        persistent: bool = True,
    ) -> None: ...

    async def consume(
        self,
        queue: str,
        *,
        prefetch: int,
        handler: Callable[[IncomingMessage], Awaitable[None]],
    ) -> None: ...

    async def ensure_topology(self, queues: list[QueueSpec]) -> None: ...


__all__ = ["Broker"]
