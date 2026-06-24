"""AWS SQS broker adapter. Requires the ``sqs`` extra (aioboto3).

DLQ emulated as a ``<queue>-dlq`` queue (or a native redrive policy in production);
retry-with-backoff via header + re-send. DI client for offline import.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from maof.transport.retry import ATTEMPT_HEADER, RetryPolicy
from maof.transport.wire import pack, unpack
from maof.types import IncomingMessage, QueueSpec

if TYPE_CHECKING:
    from maof.transport.base import Broker


class SQSBroker:
    """Queue names are SQS queue URLs. ``session`` is an injected aioboto3 Session.

    ``endpoint_url`` targets SQS-compatible endpoints (LocalStack, VPC/private
    endpoints); ``None`` uses the AWS default for the region.
    """

    def __init__(
        self,
        *,
        region: str | None = None,
        session: Any | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        self._region = region
        self._session = session
        self._endpoint_url = endpoint_url
        self._policies: dict[str, RetryPolicy] = {}
        self._dlq: dict[str, str] = {}

    def _client(self) -> Any:
        if self._session is None:
            import aioboto3

            self._session = aioboto3.Session()
        return self._session.client(
            "sqs", region_name=self._region, endpoint_url=self._endpoint_url
        )

    async def ensure_topology(self, queues: list[QueueSpec]) -> None:
        for q in queues:
            if q.retry_steps:
                self._policies[q.name] = RetryPolicy(q.retry_steps)
            if q.dlq_name:
                self._dlq[q.name] = q.dlq_name

    async def publish(
        self,
        queue: str,
        body: bytes,
        *,
        headers: dict[str, str],
        message_id: str,
        correlation_id: str | None = None,
        persistent: bool = True,
    ) -> None:
        async with self._client() as sqs:
            await sqs.send_message(
                QueueUrl=queue,
                MessageBody=pack(body, headers, message_id, correlation_id).decode("utf-8"),
            )

    async def consume(
        self,
        queue: str,
        *,
        prefetch: int,
        handler: Callable[[IncomingMessage], Awaitable[None]],
    ) -> None:
        async with self._client() as sqs:
            while True:
                resp = await sqs.receive_message(
                    QueueUrl=queue, MaxNumberOfMessages=min(prefetch, 10), WaitTimeSeconds=5
                )
                for record in resp.get("Messages", []):
                    await self._dispatch(queue, sqs, record, handler)

    async def _dispatch(
        self,
        queue: str,
        sqs: Any,
        record: dict[str, Any],
        handler: Callable[[IncomingMessage], Awaitable[None]],
    ) -> None:
        body, headers, message_id, correlation_id = unpack(record["Body"].encode("utf-8"))
        attempt = int(headers.get(ATTEMPT_HEADER, "1"))
        msg = IncomingMessage(
            body=body,
            headers=headers,
            message_id=message_id,
            queue=queue,
            correlation_id=correlation_id,
            redelivered=attempt > 1,
            attempt=attempt,
        )
        try:
            await handler(msg)
            await sqs.delete_message(QueueUrl=queue, ReceiptHandle=record["ReceiptHandle"])
        except Exception:  # noqa: BLE001 - retry or dead-letter below
            policy = self._policies.get(queue)
            delay = policy.delay_for_attempt(attempt) if policy is not None else None
            if delay is not None:
                # Native delayed delivery (<=900s) — no blocking sleep holding a slot.
                retried = dict(headers)
                retried[ATTEMPT_HEADER] = str(attempt + 1)
                await sqs.send_message(
                    QueueUrl=queue,
                    MessageBody=pack(body, retried, message_id, correlation_id).decode("utf-8"),
                    DelaySeconds=min(int(delay), 900),
                )
                await sqs.delete_message(QueueUrl=queue, ReceiptHandle=record["ReceiptHandle"])
            elif queue in self._dlq:
                await self.publish(
                    self._dlq[queue],
                    body,
                    headers=headers,
                    message_id=message_id,
                    correlation_id=correlation_id,
                )
                await sqs.delete_message(QueueUrl=queue, ReceiptHandle=record["ReceiptHandle"])


if TYPE_CHECKING:
    _assert_broker: Broker = SQSBroker()


__all__ = ["SQSBroker"]
