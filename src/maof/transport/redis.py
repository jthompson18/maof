"""Redis broker adapter. LIST-based queues + DLQ + header retry.

Requires the ``redis`` extra. Integration-tested against a live Redis; not part of
the offline unit suite. DI client so the module imports without redis installed.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from maof.transport.retry import ATTEMPT_HEADER, RetryPolicy
from maof.transport.wire import pack, unpack
from maof.types import IncomingMessage, QueueSpec

if TYPE_CHECKING:
    from maof.transport.base import Broker


class RedisStreamsBroker:
    def __init__(self, url: str, *, client: Any | None = None) -> None:
        self._url = url
        self._client = client
        self._policies: dict[str, RetryPolicy] = {}
        self._dlq: dict[str, str] = {}

    async def connect(self) -> None:
        if self._client is None:
            import redis.asyncio as redis_async

            self._client = redis_async.from_url(self._url)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def _ensure(self) -> Any:
        if self._client is None:
            await self.connect()
        return self._client

    async def ensure_topology(self, queues: list[QueueSpec]) -> None:
        for q in queues:
            if q.retry_steps:
                self._policies[q.name] = RetryPolicy(q.retry_steps)
            if q.dlq_name:
                self._dlq[q.name] = q.dlq_name

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
        client = await self._ensure()
        await client.lpush(queue, pack(body, headers, message_id, correlation_id))

    async def consume(
        self,
        queue: str,
        *,
        prefetch: int,
        handler: Callable[[IncomingMessage], Awaitable[None]],
    ) -> None:
        # BLMOVE into a processing list (not destructive BRPOP): a crash mid-handler
        # leaves the message parked in <queue>.processing for reclaim instead of lost.
        client = await self._ensure()
        processing = f"{queue}.processing"
        while True:
            raw = await client.blmove(queue, processing, 1, "RIGHT", "LEFT")
            if raw is not None:
                await self._dispatch(queue, raw, handler)
                await client.lrem(processing, 1, raw)

    async def consume_once(
        self,
        queue: str,
        *,
        prefetch: int,
        handler: Callable[[IncomingMessage], Awaitable[None]],
        timeout: float = 5.0,  # noqa: ASYNC109 - forwarded to redis BLMOVE
    ) -> None:
        client = await self._ensure()
        processing = f"{queue}.processing"
        raw = await client.blmove(queue, processing, timeout, "RIGHT", "LEFT")
        if raw is not None:
            await self._dispatch(queue, raw, handler)
            await client.lrem(processing, 1, raw)

    async def _dispatch(
        self, queue: str, raw: bytes, handler: Callable[[IncomingMessage], Awaitable[None]]
    ) -> None:
        body, headers, message_id, correlation_id = unpack(raw)
        attempt = int(headers.get(ATTEMPT_HEADER, "1"))
        msg = IncomingMessage(
            body=body,
            headers=headers,
            message_id=message_id,
            queue=queue,
            correlation_id=correlation_id,
            redelivered=attempt > 1,
            attempt=attempt,
        )
        try:
            await handler(msg)
        except Exception:  # noqa: BLE001 - retry or dead-letter below
            client = await self._ensure()
            policy = self._policies.get(queue)
            delay = policy.delay_for_attempt(attempt) if policy is not None else None
            if delay is not None:
                retried = dict(headers)
                retried[ATTEMPT_HEADER] = str(attempt + 1)
                await client.lpush(queue, pack(body, retried, message_id, correlation_id))
            elif queue in self._dlq:
                await client.lpush(self._dlq[queue], raw)


if TYPE_CHECKING:
    _assert_broker: Broker = RedisStreamsBroker("")


__all__ = ["RedisStreamsBroker"]
