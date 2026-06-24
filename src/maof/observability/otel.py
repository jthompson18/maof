"""OpenTelemetry setup — no-op when no collector is configured.

By default (no endpoint, or the ``otel`` extra not installed) tracing/metrics are
no-ops. When an OTLP endpoint is configured and opentelemetry is available, a real
tracer is wired. Stage/loop/publish instrumentation hangs off these in later phases.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from contextlib import AbstractContextManager
from typing import Any, Protocol


class Span(Protocol):
    def set_attribute(self, key: str, value: Any) -> None: ...


class Tracer(Protocol):
    def span(self, name: str, **attributes: Any) -> AbstractContextManager[Span]: ...


class _NoOpSpan:
    def set_attribute(self, key: str, value: Any) -> None:
        return None


class NoOpTracer:
    @contextlib.contextmanager
    def span(self, name: str, **attributes: Any) -> Iterator[_NoOpSpan]:
        yield _NoOpSpan()


class _OTelTracer:
    def __init__(self, tracer: Any) -> None:
        self._tracer = tracer

    @contextlib.contextmanager
    def span(self, name: str, **attributes: Any) -> Iterator[Any]:
        with self._tracer.start_as_current_span(name) as span:
            for key, value in attributes.items():
                span.set_attribute(key, value)
            yield span


def _try_otel_tracer(endpoint: str, service_name: str) -> Tracer | None:
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        return None
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    return _OTelTracer(trace.get_tracer(service_name))


def get_tracer(endpoint: str | None = None, *, service_name: str = "maof") -> Tracer:
    """Return an OTLP-backed tracer if configured + available, else a no-op tracer."""
    if endpoint:
        otel = _try_otel_tracer(endpoint, service_name)
        if otel is not None:
            return otel
    return NoOpTracer()


class _NoOpInstrument:
    def add(self, amount: float = 1, attributes: dict[str, Any] | None = None) -> None:
        return None

    def record(self, amount: float, attributes: dict[str, Any] | None = None) -> None:
        return None


class NoOpMeter:
    def counter(self, name: str, **kwargs: Any) -> _NoOpInstrument:
        return _NoOpInstrument()

    def histogram(self, name: str, **kwargs: Any) -> _NoOpInstrument:
        return _NoOpInstrument()


def get_meter(endpoint: str | None = None, *, service_name: str = "maof") -> NoOpMeter:
    """A no-op meter by default; real OTLP metrics wire up with instrumentation."""
    return NoOpMeter()


__all__ = ["Span", "Tracer", "NoOpTracer", "get_tracer", "NoOpMeter", "get_meter"]
