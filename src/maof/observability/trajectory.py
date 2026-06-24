"""Trajectory capture.

Records agent *decision patterns and interaction structure* — how delegations
branched, which tools fired, where retries/compaction happened — as **structure,
not conversation content**, to preserve privacy. This is what makes
non-deterministic multi-agent runs debuggable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from maof.types import utcnow


@dataclass
class TrajectoryEvent:
    """A single structural event. ``attributes`` are coerced to strings and must
    carry structure (counts, ids, kinds) — never conversation content."""

    kind: str  # "stage" | "delegation" | "tool_call" | "retry" | "compaction" | ...
    node: str  # logical node/step id
    parent: str | None = None
    attributes: dict[str, str] = field(default_factory=dict)
    ts: str = field(default_factory=utcnow)


class TrajectoryRecorder:
    def __init__(self) -> None:
        self._events: list[TrajectoryEvent] = []

    def record(self, kind: str, node: str, *, parent: str | None = None, **attributes: Any) -> None:
        self._events.append(
            TrajectoryEvent(
                kind=kind,
                node=node,
                parent=parent,
                attributes={k: str(v) for k, v in attributes.items()},
            )
        )

    @property
    def events(self) -> list[TrajectoryEvent]:
        return list(self._events)

    def structure(self) -> dict[str, Any]:
        """Privacy-preserving summary: counts per kind + parent->child edges."""
        counts: dict[str, int] = {}
        edges: list[dict[str, str]] = []
        for event in self._events:
            counts[event.kind] = counts.get(event.kind, 0) + 1
            if event.parent is not None:
                edges.append({"from": event.parent, "to": event.node, "kind": event.kind})
        return {"counts": counts, "edges": edges, "total": len(self._events)}


__all__ = ["TrajectoryEvent", "TrajectoryRecorder"]
