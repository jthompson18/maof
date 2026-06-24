"""RabbitMQ broker adapter — the default transport.

Uses aio-pika. DLQ via a dead-letter exchange; retry-with-backoff via per-attempt
headers + delayed republish, matching the uniform semantics in ``retry.py``.

Requires the ``rabbitmq`` extra. Exercised by the skippable integration test and
the compose stack — not the offline unit suite.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import aio_pika

from maof.transport.retry import ATTEMPT_HEADER, RetryPolicy, parse_duration
from maof.types import IncomingMessage, QueueSpec

if TYPE_CHECKING:
    from maof.transport.base import Broker


class RabbitMQBroker:
    def __init__(self, url: str, *, app_id: str = "maof") -> None:
        self._url = url
        self._app_id = app_id
        self._connection: Any | None = None
        self._channel: Any | None = None
        self._policies: dict[str, RetryPolicy] = {}
        self._dlq: dict[str, str] = {}
        self._retry_queues: dict[str, list[str]] = {}

    async def connect(self) -> None:
        if self._connection is None:
            self._connection = await aio_pika.connect_robust(self._url)
            self._channel = await self._connection.channel()

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None

    async def _ensure_channel(self) -> Any:
        if self._channel is None:
            await self.connect()
        return self._channel

    async def _queue_for_consume(self, queue: str, prefetch: int) -> Any:
        """Resolve the queue to consume WITHOUT redefining it: a plain redeclare of
        a queue that ``ensure_topology`` created with DLX/TTL arguments is an AMQP
        ``PRECONDITION_FAILED`` channel kill. Passive declare asserts existence;
        only a genuinely missing queue is created (plain, durable)."""
        channel = await self._ensure_channel()
        await channel.set_qos(prefetch_count=prefetch)
        try:
            return await channel.declare_queue(queue, durable=True, passive=True)
        except aio_pika.exceptions.ChannelNotFoundEntity:
            # the failed passive declare closed the channel — reopen, then create
            self._channel = await self._connection.channel()  # type: ignore[union-attr]
            channel = self._channel
            await channel.set_qos(prefetch_count=prefetch)
            return await channel.declare_queue(queue, durable=True)

    async def ensure_topology(self, queues: list[QueueSpec]) -> None:
        channel = await self._ensure_channel()
        for q in queues:
            if q.retry_steps:
                self._policies[q.name] = RetryPolicy(q.retry_steps)
                # The canonical TTL retry-queue pattern: each backoff step is a queue
                # whose message TTL dead-letters back to the main queue. Crash-safe
                # and non-blocking — no consumer slot is held for the backoff.
                names: list[str] = []
                for i, step in enumerate(q.retry_steps, start=1):
                    retry_name = f"{q.name}.retry.{i}"
                    await channel.declare_queue(
                        retry_name,
                        durable=True,
                        arguments={
                            "x-message-ttl": int(parse_duration(step) * 1000),
                            "x-dead-letter-exchange": "",
                            "x-dead-letter-routing-key": q.name,
                        },
                    )
                    names.append(retry_name)
                self._retry_queues[q.name] = names
            args: dict[str, Any] = {}
            if q.dlq_name:
                self._dlq[q.name] = q.dlq_name
                args["x-dead-letter-exchange"] = ""
                args["x-dead-letter-routing-key"] = q.dlq_name
                dlq_args: dict[str, Any] = {}
                if q.dlq_ttl:
                    dlq_args["x-message-ttl"] = int(parse_duration(q.dlq_ttl) * 1000)
                if q.dlq_max_len is not None:
                    dlq_args["x-max-length"] = q.dlq_max_len
                await channel.declare_queue(q.dlq_name, durable=True, arguments=dlq_args or None)
            await channel.declare_queue(q.name, durable=True, arguments=args or None)

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
        channel = await self._ensure_channel()
        delivery_mode = (
            aio_pika.DeliveryMode.PERSISTENT if persistent else aio_pika.DeliveryMode.NOT_PERSISTENT
        )
        message = aio_pika.Message(
            body=body,
            headers=dict(headers),
            message_id=message_id,
            correlation_id=correlation_id,
            app_id=self._app_id,
            delivery_mode=delivery_mode,
        )
        await channel.default_exchange.publish(message, routing_key=queue)

    async def consume(
        self,
        queue: str,
        *,
        prefetch: int,
        handler: Callable[[IncomingMessage], Awaitable[None]],
    ) -> None:
        declared = await self._queue_for_consume(queue, prefetch)

        async def on_message(message: Any) -> None:
            await self._handle_delivery(queue, message, handler)

        await declared.consume(on_message)
        await asyncio.Future()  # block forever (real-broker semantics)

    async def consume_once(
        self,
        queue: str,
        *,
        prefetch: int,
        handler: Callable[[IncomingMessage], Awaitable[None]],
        timeout: float = 5.0,  # noqa: ASYNC109 - forwarded to aio_pika queue.get()
    ) -> None:
        """Consume a single message then return — used by the integration test."""
        declared = await self._queue_for_consume(queue, prefetch)
        incoming = await declared.get(timeout=timeout, fail=False)
        if incoming is not None:
            await self._handle_delivery(queue, incoming, handler)

    async def _handle_delivery(
        self,
        queue: str,
        message: Any,
        handler: Callable[[IncomingMessage], Awaitable[None]],
    ) -> None:
        headers = {str(k): str(v) for k, v in (message.headers or {}).items()}
        attempt = int(headers.get(ATTEMPT_HEADER, "1"))
        msg = IncomingMessage(
            body=bytes(message.body),
            headers=headers,
            message_id=message.message_id or "",
            queue=queue,
            correlation_id=message.correlation_id,
            redelivered=bool(message.redelivered),
            attempt=attempt,
        )
        try:
            await handler(msg)
            await message.ack()
        except Exception:  # noqa: BLE001 - decide retry vs dead-letter below
            policy = self._policies.get(queue)
            delay = policy.delay_for_attempt(attempt) if policy is not None else None
            retry_queues = self._retry_queues.get(queue, [])
            if delay is not None and attempt <= len(retry_queues):
                # Park the message on the TTL retry queue (no blocking sleep); it
                # dead-letters back to the main queue after the backoff. Duplicates
                # from the republish->ack window are deduped by the consumer guard.
                await self._republish(retry_queues[attempt - 1], message, attempt + 1)
                await message.ack()
            else:
                await message.reject(requeue=False)  # dead-letter via DLX

    async def _republish(self, routing_key: str, message: Any, next_attempt: int) -> None:
        channel = await self._ensure_channel()
        # dict[str, Any]: aio-pika types header values as its FieldValue union.
        headers: dict[str, Any] = {str(k): str(v) for k, v in (message.headers or {}).items()}
        headers[ATTEMPT_HEADER] = str(next_attempt)
        republished = aio_pika.Message(
            body=message.body,
            headers=headers,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            app_id=self._app_id,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await channel.default_exchange.publish(republished, routing_key=routing_key)


if TYPE_CHECKING:
    _assert_broker: Broker = RabbitMQBroker("")  # structural conformance check


__all__ = ["RabbitMQBroker"]
