"""Kafka broker adapter. Requires the ``kafka`` extra (aiokafka).

DLQ emulated as a ``<topic>.dlq`` topic; retry-with-backoff via header + re-produce.
Integration-tested against a live broker; DI producer/consumer for offline import.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from maof.transport.retry import ATTEMPT_HEADER, RetryPolicy
from maof.transport.wire import pack, unpack
from maof.types import IncomingMessage, QueueSpec

if TYPE_CHECKING:
    from maof.transport.base import Broker


class KafkaBroker:
    def __init__(self, bootstrap_servers: str, *, producer: Any | None = None) -> None:
        self._bootstrap = bootstrap_servers
        self._producer = producer
        self._policies: dict[str, RetryPolicy] = {}
        self._dlq: dict[str, str] = {}

    async def connect(self) -> None:
        if self._producer is None:
            from aiokafka import AIOKafkaProducer

            self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap)
            await self._producer.start()

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

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
        if self._producer is None:
            await self.connect()
        assert self._producer is not None
        await self._producer.send_and_wait(queue, pack(body, headers, message_id, correlation_id))

    async def consume(
        self,
        queue: str,
        *,
        prefetch: int,
        handler: Callable[[IncomingMessage], Awaitable[None]],
    ) -> None:
        from aiokafka import AIOKafkaConsumer

        # Manual commits AFTER handling: auto-commit could advance the offset past
        # an in-flight handler, losing the message if the process died mid-handler.
        # auto_offset_reset="earliest" gives queue semantics: a worker whose group has
        # no committed offset yet must process tasks published before it subscribed
        # (the aiokafka default, "latest", silently skips them).
        consumer = AIOKafkaConsumer(
            queue,
            bootstrap_servers=self._bootstrap,
            enable_auto_commit=False,
            group_id=f"maof-{queue}",
            auto_offset_reset="earliest",
        )
        await consumer.start()
        try:
            async for record in consumer:
                await self._dispatch(queue, record.value, handler)
                await consumer.commit()
        finally:
            await consumer.stop()

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
            policy = self._policies.get(queue)
            delay = policy.delay_for_attempt(attempt) if policy is not None else None
            if delay is not None:
                await asyncio.sleep(delay)
                retried = dict(headers)
                retried[ATTEMPT_HEADER] = str(attempt + 1)
                await self.publish(
                    queue,
                    body,
                    headers=retried,
                    message_id=message_id,
                    correlation_id=correlation_id,
                )
            elif queue in self._dlq:
                await self.publish(
                    self._dlq[queue],
                    body,
                    headers=headers,
                    message_id=message_id,
                    correlation_id=correlation_id,
                )


if TYPE_CHECKING:
    _assert_broker: Broker = KafkaBroker("")


__all__ = ["KafkaBroker"]
