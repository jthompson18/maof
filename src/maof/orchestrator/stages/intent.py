"""Default ``intent_synthesis`` stage — assigns the intent_id."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from maof.types import Intent

if TYPE_CHECKING:
    from maof.types import StageContext


class IntentStage:
    name = "intent_synthesis"

    def __init__(self, *, task_types: list[str] | None = None, summary: str = "") -> None:
        self._task_types = list(task_types) if task_types else []
        self._summary = summary

    async def execute(self, sc: StageContext) -> StageContext:
        sc.intent = Intent(
            intent_id=str(uuid4()),
            goal=sc.goal,
            summary=self._summary,
            task_types=list(self._task_types),
        )
        return sc


__all__ = ["IntentStage"]
