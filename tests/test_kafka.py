"""Kafka adapter vs a live broker (Redpanda/Kafka). Gated: MAOF_TEST_KAFKA_URL."""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

pytest.importorskip("aiokafka")

KAFKA_URL = os.getenv("MAOF_TEST_KAFKA_URL")

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(not KAFKA_URL, reason="set MAOF_TEST_KAFKA_URL to run Kafka integration"),
]


def _topic() -> str:
    return f"maof.eval.{uuid.uuid4().hex[:8]}"


async def _consume_until(broker, topic, *, count, wait=20.0):  # type: ignore[no-untyped-def]
    from maof.types import IncomingMessage

    received: list[IncomingMessage] = []
    done = asyncio.Event()

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg)
        if len(received) >= count:
            done.set()

    task = asyncio.create_task(broker.consume(topic, prefetch=1, handler=handler))
    try:
        await asyncio.wait_for(done.wait(), timeout=wait)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    return received


async def test_publish_consume_round_trip_with_headers() -> None:
    from maof.transport.kafka import KafkaBroker
    from maof.types import QueueSpec

    assert KAFKA_URL is not None
    broker = KafkaBroker(KAFKA_URL)
    await broker.connect()
    try:
        topic = _topic()
        await broker.ensure_topology([QueueSpec(name=topic)])
        await broker.publish(
            topic,
            b'{"task":"x"}',
            headers={"kid": "default", "sig": "abc", "idempotency_key": "ik-1"},
            message_id="ik-1",
            correlation_id="run-1",
        )
        received = await _consume_until(broker, topic, count=1)
        msg = received[0]
        assert msg.body == b'{"task":"x"}'
        assert msg.headers["idempotency_key"] == "ik-1"
        assert msg.message_id == "ik-1"
        assert msg.correlation_id == "run-1"
        assert msg.attempt == 1 and not msg.redelivered
    finally:
        await broker.close()


async def test_failing_handler_lands_on_dlq_after_retry() -> None:
    from maof.transport.kafka import KafkaBroker
    from maof.types import IncomingMessage, QueueSpec

    assert KAFKA_URL is not None
    broker = KafkaBroker(KAFKA_URL)
    await broker.connect()
    try:
        topic = _topic()
        dlq = f"{topic}.dlq"
        await broker.ensure_topology([QueueSpec(name=topic, dlq_name=dlq, retry_steps=["1s"])])
        await broker.publish(
            topic, b"poison", headers={}, message_id="poison-1", correlation_id=None
        )

        attempts: list[int] = []
        dead = asyncio.Event()

        async def failing(msg: IncomingMessage) -> None:
            attempts.append(msg.attempt)
            raise RuntimeError("always fails")

        consume_task = asyncio.create_task(broker.consume(topic, prefetch=1, handler=failing))

        async def dlq_handler(msg: IncomingMessage) -> None:
            assert msg.body == b"poison"
            dead.set()

        dlq_task = asyncio.create_task(broker.consume(dlq, prefetch=1, handler=dlq_handler))
        try:
            await asyncio.wait_for(dead.wait(), timeout=30.0)
        finally:
            for task in (consume_task, dlq_task):
                task.cancel()
            await asyncio.gather(consume_task, dlq_task, return_exceptions=True)
        # one retry step => delivered at attempt 1, retried as attempt 2, then DLQ
        assert 1 in attempts and 2 in attempts
    finally:
        await broker.close()
