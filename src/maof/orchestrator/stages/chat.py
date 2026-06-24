"""Default ``chat`` stage — seeds the dialogue, optionally via the LLM."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from maof.models.base import LLMProvider
    from maof.types import StageContext


class ChatStage:
    name = "chat"

    def __init__(self, llm: LLMProvider | None = None, *, system: str | None = None) -> None:
        self._llm = llm
        self._system = system

    async def execute(self, sc: StageContext) -> StageContext:
        sc.dialogue.append(f"goal: {sc.goal}")
        if self._llm is not None:
            reply = await self._llm.generate(sc.goal, system=self._system, run_id=sc.run_id)
            sc.dialogue.append(reply)
        return sc


__all__ = ["ChatStage"]
