"""RabbitMQ adapter logic offline — fake aio-pika objects.

The live round trip is tests/test_rabbitmq.py (live tier); these cover the
adapter's topology declarations, publish shape, and ack/retry/dead-letter
decisions without a broker.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

pytest.importorskip("aio_pika")

import aio_pika  # noqa: E402

from maof.transport.rabbitmq import RabbitMQBroker  # noqa: E402
from maof.transport.retry import ATTEMPT_HEADER  # noqa: E402
from maof.types import IncomingMessage, QueueSpec  # noqa: E402


class _FakeQueue:
    def __init__(self, name: str) -> None:
        self.name = name
        self.callback: Any = None
        self.gets: list[Any] = []

    async def consume(self, callback: Any) -> None:
        self.callback = callback

    async def get(self, *, timeout: float, fail: bool) -> Any:  # noqa: ASYNC109 - mirrors aio-pika
        return self.gets.pop(0) if self.gets else None


class _FakeExchange:
    def __init__(self) -> None:
        self.published: list[tuple[Any, str]] = []

    async def publish(self, message: Any, routing_key: str) -> None:
        self.published.append((message, routing_key))


class _FakeChannel:
    def __init__(self, *, missing: set[str] | None = None) -> None:
        self.declared: list[tuple[str, dict[str, Any] | None, bool]] = []
        self.default_exchange = _FakeExchange()
        self.queues: dict[str, _FakeQueue] = {}
        self.prefetch: int | None = None
        self._missing = missing or set()

    async def set_qos(self, prefetch_count: int) -> None:
        self.prefetch = prefetch_count

    async def declare_queue(
        self,
        name: str,
        *,
        durable: bool = True,
        passive: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> _FakeQueue:
        if passive and name in self._missing:
            raise aio_pika.exceptions.ChannelNotFoundEntity(404, "no queue")
        self.declared.append((name, arguments, passive))
        return self.queues.setdefault(name, _FakeQueue(name))


class _FakeConnection:
    def __init__(self, channels: list[_FakeChannel]) -> None:
        self._channels = channels
        self.closed = False

    async def channel(self) -> _FakeChannel:
        return self._channels.pop(0)

    async def close(self) -> None:
        self.closed = True


class _FakeIncoming:
    def __init__(self, *, headers: dict[str, Any] | None = None, body: bytes = b"x") -> None:
        self.headers = headers or {}
        self.body = body
        self.message_id = "m-1"
        self.correlation_id = "c-1"
        self.redelivered = False
        self.acked = False
        self.rejected: bool | None = None

    async def ack(self) -> None:
        self.acked = True

    async def reject(self, requeue: bool) -> None:
        self.rejected = requeue


def _broker(channel: _FakeChannel) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://unused")
    broker._connection = _FakeConnection([])  # noqa: SLF001 - injected test double
    broker._channel = channel  # noqa: SLF001
    return broker


async def test_ensure_topology_declares_ttl_retry_queues_dlq_and_dlx() -> None:
    channel = _FakeChannel()
    broker = _broker(channel)
    await broker.ensure_topology(
        [
            QueueSpec(
                name="tasks.x",
                dlq_name="tasks.x.dlq",
                dlq_ttl="10m",
                dlq_max_len=100,
                retry_steps=["1s", "2s"],
            )
        ]
    )
    declared = {name: args for name, args, _ in channel.declared}
    assert declared["tasks.x.retry.1"] == {
        "x-message-ttl": 1000,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "tasks.x",
    }
    assert declared["tasks.x.retry.2"]["x-message-ttl"] == 2000
    assert declared["tasks.x.dlq"] == {"x-message-ttl": 600000, "x-max-length": 100}
    assert declared["tasks.x"]["x-dead-letter-routing-key"] == "tasks.x.dlq"


async def test_publish_carries_headers_ids_and_delivery_mode() -> None:
    channel = _FakeChannel()
    broker = _broker(channel)
    await broker.publish(
        "tasks.x",
        b"body",
        headers={"kid": "k", "sig": "s"},
        message_id="ik-1",
        correlation_id="run-1",
    )
    await broker.publish(
        "tasks.x",
        b"body2",
        headers={},
        message_id="ik-2",
        correlation_id=None,
        persistent=False,
    )
    first, key = channel.default_exchange.published[0]
    assert key == "tasks.x"
    assert first.body == b"body" and first.message_id == "ik-1"
    assert first.headers == {"kid": "k", "sig": "s"}
    assert first.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    second, _ = channel.default_exchange.published[1]
    assert second.delivery_mode == aio_pika.DeliveryMode.NOT_PERSISTENT


async def test_handler_success_acks() -> None:
    channel = _FakeChannel()
    broker = _broker(channel)
    incoming = _FakeIncoming(headers={"kid": "k"})
    seen: list[IncomingMessage] = []

    async def handler(msg: IncomingMessage) -> None:
        seen.append(msg)

    await broker._handle_delivery("tasks.x", incoming, handler)  # noqa: SLF001
    assert incoming.acked and seen[0].message_id == "m-1" and seen[0].attempt == 1


async def test_failing_handler_parks_on_ttl_retry_queue_with_next_attempt() -> None:
    channel = _FakeChannel()
    broker = _broker(channel)
    await broker.ensure_topology([QueueSpec(name="tasks.x", retry_steps=["1s"])])
    incoming = _FakeIncoming()

    async def failing(msg: IncomingMessage) -> None:
        raise RuntimeError("boom")

    await broker._handle_delivery("tasks.x", incoming, failing)  # noqa: SLF001
    republished, routing_key = channel.default_exchange.published[-1]
    assert routing_key == "tasks.x.retry.1"
    assert republished.headers[ATTEMPT_HEADER] == "2"
    assert incoming.acked and incoming.rejected is None


async def test_exhausted_retries_reject_to_dlx() -> None:
    channel = _FakeChannel()
    broker = _broker(channel)
    await broker.ensure_topology(
        [QueueSpec(name="tasks.x", dlq_name="tasks.x.dlq", retry_steps=["1s"])]
    )
    incoming = _FakeIncoming(headers={ATTEMPT_HEADER: "2"})  # past the single step

    async def failing(msg: IncomingMessage) -> None:
        raise RuntimeError("boom")

    await broker._handle_delivery("tasks.x", incoming, failing)  # noqa: SLF001
    assert incoming.rejected is False  # requeue=False -> dead-letter via DLX
    assert not incoming.acked


async def test_consume_once_handles_one_message_and_tolerates_empty() -> None:
    channel = _FakeChannel()
    broker = _broker(channel)
    queue = await channel.declare_queue("tasks.x")
    queue.gets.append(_FakeIncoming(body=b"once"))
    seen: list[bytes] = []

    async def handler(msg: IncomingMessage) -> None:
        seen.append(msg.body)

    await broker.consume_once("tasks.x", prefetch=1, handler=handler)
    await broker.consume_once("tasks.x", prefetch=1, handler=handler)  # empty: no-op
    assert seen == [b"once"]
    assert channel.prefetch == 1


async def test_consume_reopens_channel_when_passive_declare_fails() -> None:
    replacement = _FakeChannel()
    first = _FakeChannel(missing={"tasks.new"})
    broker = RabbitMQBroker("amqp://unused")
    broker._connection = _FakeConnection([replacement])  # noqa: SLF001
    broker._channel = first  # noqa: SLF001

    handled = asyncio.Event()

    async def handler(msg: IncomingMessage) -> None:
        handled.set()

    task = asyncio.create_task(broker.consume("tasks.new", prefetch=2, handler=handler))
    await asyncio.sleep(0.05)
    queue = replacement.queues["tasks.new"]  # created on the reopened channel
    assert queue.callback is not None
    await queue.callback(_FakeIncoming(body=b"via-consume"))
    await asyncio.wait_for(handled.wait(), timeout=2)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert replacement.prefetch == 2


async def test_close_resets_connection() -> None:
    channel = _FakeChannel()
    broker = _broker(channel)
    connection = broker._connection  # noqa: SLF001
    await broker.close()
    assert connection.closed  # type: ignore[union-attr]
    assert broker._connection is None  # noqa: SLF001
