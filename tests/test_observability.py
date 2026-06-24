"""Observability: event sinks, no-op OTel, trajectory capture."""

from __future__ import annotations

import io
import json
from uuid import uuid4

from maof.observability.events import AuditEvent
from maof.observability.otel import get_meter, get_tracer
from maof.observability.sinks.stdout_sink import StdoutEventSink
from maof.observability.trajectory import TrajectoryRecorder
from maof.persistence.postgres import Database


async def test_stdout_sink_emits_json_line() -> None:
    buf = io.StringIO()
    sink = StdoutEventSink(buf)
    await sink.emit(
        AuditEvent(
            tenant_id="t", intent_id=None, event_type="run_started", details={"run_id": "r1"}
        )
    )
    payload = json.loads(buf.getvalue().strip())
    assert payload["event_type"] == "run_started"
    assert payload["tenant_id"] == "t"
    assert payload["details"] == {"run_id": "r1"}


async def test_postgres_event_sink_persists(db: Database) -> None:
    from maof.observability.sinks.postgres_sink import PostgresEventSink

    sink = PostgresEventSink(db)
    tenant_id = f"t-{uuid4()}"
    await sink.emit(
        AuditEvent(
            tenant_id=tenant_id,
            intent_id="i1",
            event_type="policy_decision",
            details={"denied": True},
        )
    )
    row = await db.fetchrow("SELECT * FROM audit_events WHERE tenant_id = $1", tenant_id)
    assert row is not None
    assert row["event_type"] == "policy_decision"
    assert row["details"] == {"denied": True}


def test_otel_noop_tracer_and_meter() -> None:
    tracer = get_tracer(None)
    with tracer.span("stage.chat", run_id="r1") as span:
        span.set_attribute("k", "v")  # no-op, must not raise

    meter = get_meter(None)
    meter.counter("tasks_published").add(1)
    meter.histogram("task_duration_s").record(0.5)


def test_trajectory_recorder_captures_structure() -> None:
    rec = TrajectoryRecorder()
    rec.record("stage", "chat")
    rec.record("delegation", "sub1", parent="chat", mode="queue")
    rec.record("tool_call", "commitments", parent="sub1", tool="buy")

    struct = rec.structure()
    assert struct["total"] == 3
    assert struct["counts"]["delegation"] == 1
    assert {"from": "chat", "to": "sub1", "kind": "delegation"} in struct["edges"]
    assert len(rec.events) == 3
