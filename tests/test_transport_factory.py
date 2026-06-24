"""Broker factory routing + the shared wire envelope (Kafka/Redis/SQS)."""

from __future__ import annotations

from maof.config import Settings
from maof.transport.factory import build_broker
from maof.transport.wire import pack, unpack


def test_wire_round_trip() -> None:
    body = b'{"task":"funds_commit"}'
    headers = {"kid": "default", "sig": "abc", "idempotency_key": "k1"}
    raw = pack(body, headers, "m1", "c1")
    out_body, out_headers, message_id, correlation_id = unpack(raw)
    assert out_body == body
    assert out_headers == headers
    assert message_id == "m1" and correlation_id == "c1"


def test_wire_handles_binary_and_no_correlation() -> None:
    body = bytes(range(256))  # non-UTF8 bytes survive base64
    out_body, _, _, correlation_id = unpack(pack(body, {}, "m", None))
    assert out_body == body
    assert correlation_id is None


def test_build_broker_memory() -> None:
    from maof.transport.fake import InMemoryBroker

    assert isinstance(build_broker(Settings(broker_kind="memory")), InMemoryBroker)


def test_build_broker_redis_kafka_sqs_construct_offline() -> None:
    from maof.transport.kafka import KafkaBroker
    from maof.transport.redis import RedisStreamsBroker
    from maof.transport.sqs import SQSBroker

    assert isinstance(build_broker(Settings(broker_kind="redis")), RedisStreamsBroker)
    assert isinstance(build_broker(Settings(broker_kind="kafka")), KafkaBroker)
    assert isinstance(build_broker(Settings(broker_kind="sqs")), SQSBroker)


def test_build_broker_sqs_threads_http_endpoint() -> None:
    from maof.transport.sqs import SQSBroker

    broker = build_broker(Settings(broker_kind="sqs", broker_url="http://localhost:14566"))
    assert isinstance(broker, SQSBroker)
    assert broker._endpoint_url == "http://localhost:14566"  # noqa: SLF001


def test_sqs_client_receives_endpoint_url() -> None:
    from maof.transport.sqs import SQSBroker

    class _FakeSession:
        def __init__(self) -> None:
            self.kwargs: dict[str, object] = {}

        def client(self, service: str, **kwargs: object) -> object:
            self.kwargs = {"service": service, **kwargs}
            return object()

    session = _FakeSession()
    SQSBroker(region="us-east-1", session=session, endpoint_url="http://localhost:14566")._client()
    assert session.kwargs == {
        "service": "sqs",
        "region_name": "us-east-1",
        "endpoint_url": "http://localhost:14566",
    }
