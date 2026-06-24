"""OTel wrapper against the real SDK, offline.

Spans/attributes through ``_OTelTracer`` with an in-memory exporter, the OTLP
wiring path of ``get_tracer``, and the no-op fallback. The live-collector check
(spans visible in Jaeger) stays in tests/local (live tier).
"""

from __future__ import annotations

import pytest

from maof.observability.otel import NoOpMeter, NoOpTracer, get_tracer


def test_get_tracer_without_endpoint_is_noop_and_safe() -> None:
    tracer = get_tracer(None)
    assert isinstance(tracer, NoOpTracer)
    with tracer.span("anything", run_id="r") as span:
        span.set_attribute("k", "v")  # no-op, must not raise
    meter = NoOpMeter()
    meter.counter("c").add(1)
    meter.histogram("h").record(0.5)


def test_real_sdk_spans_and_attributes_flow_through_wrapper() -> None:
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from opentelemetry.trace import get_tracer as sdk_get_tracer

    from maof.observability.otel import _OTelTracer

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = _OTelTracer(sdk_get_tracer("maof-test", tracer_provider=provider))

    with tracer.span("stage.action_plan", run_id="run-7", tenant_id="t1") as span:
        span.set_attribute("policy.matched", 2)

    spans = exporter.get_finished_spans()
    assert [s.name for s in spans] == ["stage.action_plan"]
    attributes = dict(spans[0].attributes or {})
    assert attributes["run_id"] == "run-7"
    assert attributes["policy.matched"] == 2


def test_get_tracer_wires_the_otlp_exporter_when_configured() -> None:
    pytest.importorskip("opentelemetry.sdk")
    pytest.importorskip("opentelemetry.exporter.otlp")
    from maof.observability.otel import _OTelTracer

    # Construction only — the batch exporter never connects unless spans flush.
    tracer = get_tracer("http://127.0.0.1:1", service_name="maof-otel-unit")
    assert isinstance(tracer, _OTelTracer)
