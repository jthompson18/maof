"""Postgres event sink — persists AuditEvents to ``audit_events``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.observability.events import AuditEvent
from maof.persistence.postgres import Database

if TYPE_CHECKING:
    from maof.observability.events import EventSink


class PostgresEventSink:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def emit(self, event: AuditEvent) -> None:
        await self._db.execute(
            """
            INSERT INTO audit_events
              (tenant_id, intent_id, event_type, severity, kind, envelope, details)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            event.tenant_id,
            event.intent_id,
            event.event_type,
            event.severity,
            event.kind,
            event.envelope,
            event.details,
        )


if TYPE_CHECKING:
    _assert_sink: EventSink = PostgresEventSink(Database(""))


__all__ = ["PostgresEventSink"]
