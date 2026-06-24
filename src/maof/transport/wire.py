"""Shared on-wire envelope for transports without native headers (Kafka/Redis/SQS).

RabbitMQ carries headers natively; the others pack body + headers + ids into one
JSON value so the signing/idempotency headers (kid, sig, idempotency_key, attempt)
survive the round trip uniformly.
"""

from __future__ import annotations

import base64
import json
from typing import Any


def pack(
    body: bytes, headers: dict[str, str], message_id: str, correlation_id: str | None
) -> bytes:
    payload = {
        "body": base64.b64encode(body).decode("ascii"),
        "headers": headers,
        "message_id": message_id,
        "correlation_id": correlation_id,
    }
    return json.dumps(payload).encode("utf-8")


def unpack(raw: bytes) -> tuple[bytes, dict[str, str], str, str | None]:
    payload: dict[str, Any] = json.loads(raw)
    body = base64.b64decode(payload["body"])
    headers = {str(k): str(v) for k, v in payload.get("headers", {}).items()}
    return body, headers, str(payload.get("message_id", "")), payload.get("correlation_id")


__all__ = ["pack", "unpack"]
