"""SQS adapter vs LocalStack (exercises the new endpoint_url). Gated: MAOF_TEST_SQS_URL."""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

pytest.importorskip("aioboto3")

SQS_URL = os.getenv("MAOF_TEST_SQS_URL")  # e.g. http://127.0.0.1:14566

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        not SQS_URL, reason="set MAOF_TEST_SQS_URL (LocalStack endpoint) to run SQS integration"
    ),
]

REGION = "us-east-1"


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.getenv("AWS_ACCESS_KEY_ID", "test"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)


async def _create_queue(name: str) -> str:
    import aioboto3

    session = aioboto3.Session()
    async with session.client("sqs", region_name=REGION, endpoint_url=SQS_URL) as sqs:
        response = await sqs.create_queue(QueueName=name)
        return str(response["QueueUrl"])


async def _consume_until(broker, queue_url, *, count, wait=25.0):  # type: ignore[no-untyped-def]
    from maof.types import IncomingMessage

    received: list[IncomingMessage] = []
    done = asyncio.Event()

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg)
        if len(received) >= count:
            done.set()

    task = asyncio.create_task(broker.consume(queue_url, prefetch=1, handler=handler))
    try:
        await asyncio.wait_for(done.wait(), timeout=wait)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    return received


async def test_publish_consume_round_trip_via_endpoint_url() -> None:
    from maof.transport.sqs import SQSBroker
    from maof.types import QueueSpec

    name = f"maof-eval-{uuid.uuid4().hex[:8]}"
    queue_url = await _create_queue(name)
    broker = SQSBroker(region=REGION, endpoint_url=SQS_URL)
    await broker.ensure_topology([QueueSpec(name=queue_url)])
    await broker.publish(
        queue_url,
        b'{"task":"x"}',
        headers={"kid": "default", "sig": "abc", "idempotency_key": "ik-1"},
        message_id="ik-1",
        correlation_id="run-1",
    )
    received = await _consume_until(broker, queue_url, count=1)
    msg = received[0]
    assert msg.body == b'{"task":"x"}'
    assert msg.headers["idempotency_key"] == "ik-1"
    assert msg.correlation_id == "run-1"


async def test_failing_handler_uses_delay_seconds_then_dlq() -> None:
    from maof.transport.sqs import SQSBroker
    from maof.types import IncomingMessage, QueueSpec

    name = f"maof-eval-{uuid.uuid4().hex[:8]}"
    queue_url = await _create_queue(name)
    dlq_url = await _create_queue(f"{name}-dlq")
    broker = SQSBroker(region=REGION, endpoint_url=SQS_URL)
    await broker.ensure_topology([QueueSpec(name=queue_url, dlq_name=dlq_url, retry_steps=["1s"])])
    await broker.publish(queue_url, b"poison", headers={}, message_id="p1", correlation_id=None)

    attempts: list[int] = []

    async def failing(msg: IncomingMessage) -> None:
        attempts.append(msg.attempt)
        raise RuntimeError("always fails")

    # Keep the failing consumer running until the dead letter actually lands —
    # cancelling it on attempt 2 would race the DLQ publish inside _dispatch.
    task = asyncio.create_task(broker.consume(queue_url, prefetch=1, handler=failing))
    try:
        dead = await _consume_until(broker, dlq_url, count=1, wait=40.0)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert attempts[:2] == [1, 2]
    assert dead[0].body == b"poison"
