"""Broker factory — select the transport adapter from config."""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.errors import ConfigError

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.transport.base import Broker


def build_broker(settings: Settings) -> Broker:
    if settings.embedded_l2:
        # Embedded mode: orchestrator + workers in one process — the in-memory
        # broker replaces whatever broker_kind is configured.
        from maof.transport.fake import InMemoryBroker

        return InMemoryBroker()
    kind = settings.broker_kind
    if kind == "rabbitmq":
        try:
            from maof.transport.rabbitmq import RabbitMQBroker
        except ImportError as exc:  # pragma: no cover - depends on installed extras
            raise ConfigError("broker_kind 'rabbitmq' requires the 'rabbitmq' extra") from exc
        return RabbitMQBroker(settings.broker_url)
    if kind == "memory":
        from maof.transport.fake import InMemoryBroker

        return InMemoryBroker()
    if kind == "kafka":
        from maof.transport.kafka import KafkaBroker

        return KafkaBroker(settings.broker_url)
    if kind == "redis":
        from maof.transport.redis import RedisStreamsBroker

        return RedisStreamsBroker(settings.broker_url)
    if kind == "sqs":
        from maof.transport.sqs import SQSBroker

        # broker_url doubles as the SQS endpoint for LocalStack/private endpoints;
        # non-http values (e.g. an amqp:// default) mean "use the AWS endpoint".
        endpoint = settings.broker_url if settings.broker_url.startswith("http") else None
        return SQSBroker(region=settings.region, endpoint_url=endpoint)
    raise ConfigError(f"unknown broker_kind: {kind!r}")


__all__ = ["build_broker"]
