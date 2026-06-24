"""Redis adapter vs a live Redis. Gated: MAOF_TEST_REDIS_URL."""

from __future__ import annotations

import os
import uuid

import pytest

pytest.importorskip("redis")

REDIS_URL = os.getenv("MAOF_TEST_REDIS_URL")

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(not REDIS_URL, reason="set MAOF_TEST_REDIS_URL to run Redis integration"),
]


def _queue() -> str:
    return f"maof.eval.{uuid.uuid4().hex[:8]}"


async def test_publish_consume_round_trip_with_headers() -> None:
    from maof.transport.redis import RedisStreamsBroker
    from maof.types import IncomingMessage, QueueSpec

    assert REDIS_URL is not None
    broker = RedisStreamsBroker(REDIS_URL)
    await broker.connect()
    try:
        queue = _queue()
        await broker.ensure_topology([QueueSpec(name=queue)])
        await broker.publish(
            queue,
            b'{"task":"x"}',
            headers={"kid": "default", "sig": "abc", "idempotency_key": "ik-1"},
            message_id="ik-1",
            correlation_id="run-1",
        )
        received: list[IncomingMessage] = []

        async def handler(msg: IncomingMessage) -> None:
            received.append(msg)

        await broker.consume_once(queue, prefetch=1, handler=handler, timeout=5.0)
        assert received and received[0].headers["idempotency_key"] == "ik-1"
        assert received[0].correlation_id == "run-1"
    finally:
        await broker.close()


async def test_failing_handler_retries_then_dead_letters() -> None:
    from maof.transport.redis import RedisStreamsBroker
    from maof.types import IncomingMessage, QueueSpec

    assert REDIS_URL is not None
    broker = RedisStreamsBroker(REDIS_URL)
    await broker.connect()
    try:
        queue = _queue()
        dlq = f"{queue}.dlq"
        await broker.ensure_topology([QueueSpec(name=queue, dlq_name=dlq, retry_steps=["0s"])])
        await broker.publish(queue, b"poison", headers={}, message_id="p1", correlation_id=None)

        attempts: list[int] = []

        async def failing(msg: IncomingMessage) -> None:
            attempts.append(msg.attempt)
            raise RuntimeError("always fails")

        # attempt 1 -> re-queued with attempt 2; attempt 2 -> dead-lettered
        await broker.consume_once(queue, prefetch=1, handler=failing, timeout=5.0)
        await broker.consume_once(queue, prefetch=1, handler=failing, timeout=5.0)
        assert attempts == [1, 2]

        dead: list[IncomingMessage] = []

        async def dlq_handler(msg: IncomingMessage) -> None:
            dead.append(msg)

        await broker.consume_once(dlq, prefetch=1, handler=dlq_handler, timeout=5.0)
        assert dead and dead[0].body == b"poison"
    finally:
        await broker.close()


async def test_crash_safety_parks_message_in_processing_list() -> None:
    """BLMOVE semantics: a message being handled lives in <queue>.processing."""
    import redis.asyncio as redis_async

    from maof.transport.redis import RedisStreamsBroker
    from maof.types import IncomingMessage

    assert REDIS_URL is not None
    broker = RedisStreamsBroker(REDIS_URL)
    await broker.connect()
    inspector = redis_async.from_url(REDIS_URL)
    try:
        queue = _queue()
        await broker.publish(queue, b"inflight", headers={}, message_id="m", correlation_id=None)

        seen_during_handling: list[int] = []

        async def handler(msg: IncomingMessage) -> None:
            seen_during_handling.append(await inspector.llen(f"{queue}.processing"))

        await broker.consume_once(queue, prefetch=1, handler=handler, timeout=5.0)
        assert seen_during_handling == [1]  # parked while handling
        assert await inspector.llen(f"{queue}.processing") == 0  # removed after success
    finally:
        await inspector.aclose()
        await broker.close()
