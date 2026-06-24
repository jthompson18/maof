"""Stdout event sink — emits one JSON line per AuditEvent."""

from __future__ import annotations

import dataclasses
import json
import sys
from typing import TYPE_CHECKING, Any

from maof.observability.events import AuditEvent

if TYPE_CHECKING:
    from maof.observability.events import EventSink


class StdoutEventSink:
    def __init__(self, stream: Any = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    async def emit(self, event: AuditEvent) -> None:
        self._stream.write(json.dumps(dataclasses.asdict(event)) + "\n")
        self._stream.flush()


if TYPE_CHECKING:
    _assert_sink: EventSink = StdoutEventSink()


__all__ = ["StdoutEventSink"]
