"""RabbitMQ adapter integration test.

Skipped unless aio-pika is installed AND ``MAOF_TEST_RABBITMQ_URL`` points at a
live broker — lets the default adapter be exercised against a real broker on demand.
"""

from __future__ import annotations

import os

import pytest

pytest.importorskip("aio_pika")

pytestmark = pytest.mark.live

RABBIT_URL = os.getenv("MAOF_TEST_RABBITMQ_URL")


@pytest.mark.skipif(not RABBIT_URL, reason="set MAOF_TEST_RABBITMQ_URL to run RabbitMQ integration")
async def test_rabbitmq_publish_consume_round_trip() -> None:
    from maof.transport.rabbitmq import RabbitMQBroker
    from maof.types import IncomingMessage, QueueSpec

    assert RABBIT_URL is not None
    broker = RabbitMQBroker(RABBIT_URL)
    await broker.connect()
    try:
        queue = "maof.test.roundtrip"
        await broker.ensure_topology([QueueSpec(name=queue)])
        received: list[IncomingMessage] = []

        async def handler(msg: IncomingMessage) -> None:
            received.append(msg)

        await broker.publish(
            queue, b"ping", headers={"schema_id": "x"}, message_id="m1", correlation_id="c1"
        )
        # consume one message with a short bound then stop
        await broker.consume_once(queue, prefetch=1, handler=handler, timeout=5.0)
        assert received and received[0].body == b"ping"
    finally:
        await broker.close()
