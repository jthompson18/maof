"""Structured event sink + audit event shape.

OTel covers traces/metrics; the EventSink routes *domain* events (prompt audits,
policy decisions, approvals, task/run lifecycle) anywhere the adopter wants
(SIEM, Kafka, webhook, stdout). Ship Postgres + stdout sinks by default.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from maof.types import utcnow


@dataclass
class AuditEvent:
    """The canonical audit event shape (AuditEventV1)."""

    tenant_id: str
    intent_id: str | None
    event_type: str
    envelope: dict[str, Any] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)
    severity: str = "info"
    kind: str = ""
    # actor attribution: Principal.as_actor() of whoever caused the event
    actor: dict[str, Any] | None = None
    timestamp: str = field(default_factory=utcnow)


@runtime_checkable
class EventSink(Protocol):
    async def emit(self, event: AuditEvent) -> None: ...


class FanoutEventSink:
    """Deliver every event to multiple sinks in order (e.g. stdout + Postgres +
    a webhook notifier). A failing sink propagates — wrap unreliable delivery
    targets in their own swallow-and-log sink (the webhook sink does this)."""

    def __init__(self, sinks: Sequence[EventSink]) -> None:
        self._sinks = list(sinks)

    async def emit(self, event: AuditEvent) -> None:
        for sink in self._sinks:
            await sink.emit(event)


__all__ = ["AuditEvent", "EventSink", "FanoutEventSink"]
