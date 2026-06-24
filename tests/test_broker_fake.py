"""In-memory broker: publish/consume round-trip, uniform DLQ + retry-with-backoff."""

from __future__ import annotations

from maof.transport.fake import InMemoryBroker
from maof.types import IncomingMessage, QueueSpec


async def test_publish_consume_round_trip() -> None:
    broker = InMemoryBroker()
    await broker.ensure_topology([QueueSpec(name="q")])
    received: list[IncomingMessage] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg)

    await broker.publish(
        "q", b"hello", headers={"schema_id": "x"}, message_id="m1", correlation_id="c1"
    )
    await broker.consume("q", prefetch=10, handler=handler)

    assert len(received) == 1
    assert received[0].body == b"hello"
    assert received[0].message_id == "m1"
    assert received[0].correlation_id == "c1"
    assert received[0].attempt == 1
    assert received[0].headers["schema_id"] == "x"
    assert broker.depth("q") == 0


async def test_retry_then_dlq() -> None:
    broker = InMemoryBroker()
    await broker.ensure_topology(
        [QueueSpec(name="q", dlq_name="q.dlq", retry_steps=["5s", "30s", "2m"])]
    )
    attempts: list[int] = []

    async def always_fail(msg: IncomingMessage) -> None:
        attempts.append(msg.attempt)
        raise RuntimeError("boom")

    await broker.publish("q", b"x", headers={}, message_id="m", correlation_id=None)
    await broker.consume("q", prefetch=10, handler=always_fail)

    # initial delivery + 3 retries, then dead-lettered
    assert attempts == [1, 2, 3, 4]
    assert broker.scheduled_delays == [5.0, 30.0, 120.0]
    assert broker.depth("q") == 0
    assert broker.depth("q.dlq") == 1


async def test_no_retry_goes_straight_to_dlq() -> None:
    broker = InMemoryBroker()
    await broker.ensure_topology([QueueSpec(name="q", dlq_name="q.dlq")])

    async def fail(msg: IncomingMessage) -> None:
        raise RuntimeError("boom")

    await broker.publish("q", b"x", headers={}, message_id="m", correlation_id=None)
    await broker.consume("q", prefetch=10, handler=fail)

    assert broker.depth("q.dlq") == 1
    assert broker.scheduled_delays == []


async def test_success_acks_and_empties() -> None:
    broker = InMemoryBroker()
    await broker.ensure_topology([QueueSpec(name="q", dlq_name="q.dlq", retry_steps=["1s"])])
    seen = 0

    async def ok(msg: IncomingMessage) -> None:
        nonlocal seen
        seen += 1

    await broker.publish("q", b"x", headers={}, message_id="m", correlation_id=None)
    await broker.consume("q", prefetch=10, handler=ok)

    assert seen == 1
    assert broker.depth("q") == 0
    assert broker.depth("q.dlq") == 0
