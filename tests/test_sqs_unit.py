"""SQS adapter logic offline — fake aioboto3-style session/client.

The live round trip (ElasticMQ/LocalStack) lives in tests/local (live tier);
these cover publish, receive/delete, native DelaySeconds retry, and DLQ routing.
"""

from __future__ import annotations

import asyncio
from typing import Any

from maof.transport.retry import ATTEMPT_HEADER
from maof.transport.sqs import SQSBroker
from maof.transport.wire import unpack
from maof.types import IncomingMessage, QueueSpec


class _FakeSQSClient:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.receive_scripts: list[dict[str, Any]] = []

    async def __aenter__(self) -> _FakeSQSClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def send_message(self, **kwargs: Any) -> None:
        self.sent.append(kwargs)

    async def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        if self.receive_scripts:
            return self.receive_scripts.pop(0)
        await asyncio.sleep(0.01)  # idle long-poll
        return {}

    async def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> None:
        self.deleted.append(ReceiptHandle)


class _FakeSession:
    def __init__(self, client: _FakeSQSClient) -> None:
        self._client = client
        self.kwargs: dict[str, Any] = {}

    def client(self, service: str, **kwargs: Any) -> _FakeSQSClient:
        self.kwargs = {"service": service, **kwargs}
        return self._client


def _broker(client: _FakeSQSClient) -> SQSBroker:
    return SQSBroker(
        region="us-east-1", session=_FakeSession(client), endpoint_url="http://sqs.local"
    )


async def _consume_one(broker: SQSBroker, queue: str, handler: Any) -> None:
    task = asyncio.create_task(broker.consume(queue, prefetch=1, handler=handler))
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_publish_then_consume_delivers_and_deletes() -> None:
    client = _FakeSQSClient()
    broker = _broker(client)
    await broker.publish(
        "http://q/main",
        b"job",
        headers={"idempotency_key": "ik"},
        message_id="ik",
        correlation_id="run-1",
    )
    body_text = client.sent[0]["MessageBody"]
    client.receive_scripts.append({"Messages": [{"Body": body_text, "ReceiptHandle": "rh-1"}]})
    seen: list[IncomingMessage] = []

    async def handler(msg: IncomingMessage) -> None:
        seen.append(msg)

    await _consume_one(broker, "http://q/main", handler)
    assert seen[0].body == b"job" and seen[0].correlation_id == "run-1"
    assert client.deleted == ["rh-1"]


async def test_failing_handler_uses_delay_seconds_and_deletes_original() -> None:
    client = _FakeSQSClient()
    broker = _broker(client)
    await broker.ensure_topology([QueueSpec(name="http://q/main", retry_steps=["30s"])])
    await broker.publish(
        "http://q/main", b"poison", headers={}, message_id="p1", correlation_id=None
    )
    body_text = client.sent.pop()["MessageBody"]
    client.receive_scripts.append({"Messages": [{"Body": body_text, "ReceiptHandle": "rh-2"}]})

    async def failing(msg: IncomingMessage) -> None:
        raise RuntimeError("boom")

    await _consume_one(broker, "http://q/main", failing)
    retried = client.sent[0]
    assert retried["DelaySeconds"] == 30
    _, headers, _, _ = unpack(retried["MessageBody"].encode("utf-8"))
    assert headers[ATTEMPT_HEADER] == "2"
    assert client.deleted == ["rh-2"]


async def test_exhausted_retries_route_to_dlq_queue_url() -> None:
    client = _FakeSQSClient()
    broker = _broker(client)
    await broker.ensure_topology(
        [QueueSpec(name="http://q/main", dlq_name="http://q/dlq", retry_steps=["30s"])]
    )
    await broker.publish(
        "http://q/main",
        b"poison",
        headers={ATTEMPT_HEADER: "2"},
        message_id="p1",
        correlation_id=None,
    )
    body_text = client.sent.pop()["MessageBody"]
    client.receive_scripts.append({"Messages": [{"Body": body_text, "ReceiptHandle": "rh-3"}]})

    async def failing(msg: IncomingMessage) -> None:
        raise RuntimeError("boom")

    await _consume_one(broker, "http://q/main", failing)
    assert client.sent[0]["QueueUrl"] == "http://q/dlq"
    body, _, _, _ = unpack(client.sent[0]["MessageBody"].encode("utf-8"))
    assert body == b"poison"
    assert client.deleted == ["rh-3"]


async def test_client_kwargs_carry_region_and_endpoint() -> None:
    client = _FakeSQSClient()
    session = _FakeSession(client)
    broker = SQSBroker(region="eu-west-1", session=session, endpoint_url="http://sqs.local")
    broker._client()  # noqa: SLF001 - asserts construction kwargs
    assert session.kwargs == {
        "service": "sqs",
        "region_name": "eu-west-1",
        "endpoint_url": "http://sqs.local",
    }
