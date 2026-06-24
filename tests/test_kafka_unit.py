"""Kafka adapter logic offline — injected fake producer + stubbed aiokafka consumer.

The live round trip lives in tests/local (live tier); these cover wire shape,
queue-semantics consumer settings, retry republish, and DLQ routing without a
broker or the aiokafka package.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest

from maof.transport.kafka import KafkaBroker
from maof.transport.retry import ATTEMPT_HEADER
from maof.transport.wire import pack, unpack
from maof.types import IncomingMessage, QueueSpec


class _FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes]] = []
        self.stopped = False

    async def send_and_wait(self, topic: str, value: bytes) -> None:
        self.sent.append((topic, value))

    async def stop(self) -> None:
        self.stopped = True


async def test_publish_round_trips_the_wire_envelope() -> None:
    producer = _FakeProducer()
    broker = KafkaBroker("ignored:9092", producer=producer)
    await broker.publish(
        "tasks.x",
        b'{"t":1}',
        headers={"idempotency_key": "ik"},
        message_id="ik",
        correlation_id="run-1",
    )
    topic, raw = producer.sent[0]
    body, headers, message_id, correlation_id = unpack(raw)
    assert topic == "tasks.x" and body == b'{"t":1}'
    assert headers["idempotency_key"] == "ik"
    assert message_id == "ik" and correlation_id == "run-1"


async def test_dispatch_retry_republishes_with_attempt_header() -> None:
    producer = _FakeProducer()
    broker = KafkaBroker("ignored:9092", producer=producer)
    await broker.ensure_topology([QueueSpec(name="tasks.x", retry_steps=["0s"])])

    async def failing(msg: IncomingMessage) -> None:
        raise RuntimeError("boom")

    raw = pack(b"poison", {}, "m1", None)
    await broker._dispatch("tasks.x", raw, failing)  # noqa: SLF001 - unit seam
    topic, retried_raw = producer.sent[0]
    _, headers, _, _ = unpack(retried_raw)
    assert topic == "tasks.x" and headers[ATTEMPT_HEADER] == "2"


async def test_dispatch_exhausted_retries_routes_to_dlq_topic() -> None:
    producer = _FakeProducer()
    broker = KafkaBroker("ignored:9092", producer=producer)
    await broker.ensure_topology(
        [QueueSpec(name="tasks.x", dlq_name="tasks.x.dlq", retry_steps=["0s"])]
    )

    async def failing(msg: IncomingMessage) -> None:
        raise RuntimeError("boom")

    raw = pack(b"poison", {ATTEMPT_HEADER: "2"}, "m1", None)
    await broker._dispatch("tasks.x", raw, failing)  # noqa: SLF001
    topic, dead_raw = producer.sent[0]
    body, _, _, _ = unpack(dead_raw)
    assert topic == "tasks.x.dlq" and body == b"poison"


async def test_consume_uses_queue_semantics_and_commits_after_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stub the aiokafka import inside consume(): assert auto_offset_reset='earliest'
    (a worker joining after publish must still see queued tasks) and that the offset
    commit happens only after the handler returns."""
    events: list[str] = []

    class _FakeConsumer:
        def __init__(self, topic: str, **kwargs: Any) -> None:
            assert kwargs["enable_auto_commit"] is False
            assert kwargs["auto_offset_reset"] == "earliest"
            assert kwargs["group_id"] == f"maof-{topic}"
            self._records = [SimpleNamespace(value=pack(b"job", {}, "m1", "c1"))]

        async def start(self) -> None:
            events.append("start")

        async def stop(self) -> None:
            events.append("stop")

        def __aiter__(self) -> Any:
            return self

        async def __anext__(self) -> Any:
            if self._records:
                return self._records.pop(0)
            raise StopAsyncIteration

        async def commit(self) -> None:
            events.append("commit")

    monkeypatch.setitem(sys.modules, "aiokafka", SimpleNamespace(AIOKafkaConsumer=_FakeConsumer))

    broker = KafkaBroker("ignored:9092", producer=_FakeProducer())

    async def handler(msg: IncomingMessage) -> None:
        events.append(f"handled:{msg.body.decode()}")
        assert msg.correlation_id == "c1"

    await broker.consume("tasks.x", prefetch=1, handler=handler)
    assert events == ["start", "handled:job", "commit", "stop"]


async def test_close_stops_producer() -> None:
    producer = _FakeProducer()
    broker = KafkaBroker("ignored:9092", producer=producer)
    await broker.close()
    assert producer.stopped
