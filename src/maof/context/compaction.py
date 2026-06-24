"""Compactor: summarize-and-reinitialize near the window limit.

Preserve decisions/plans/open issues; discard redundant tool output (tune for
recall first, then precision). This is the compaction *engine*; the
MemoryService.compact facade delegates here — no double implementation.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from maof.types import CompactedContext, StageContext

if TYPE_CHECKING:
    from maof.models.base import LLMProvider

#: Markers that flag a dialogue line as load-bearing (kept verbatim on compaction).
_SALIENT_MARKERS = (
    "decision",
    "plan",
    "open issue",
    "todo",
    "must",
    "commit",
    "blocker",
    "constraint",
)

_COMPACTION_SYSTEM = (
    "You compact an agent's working context. Preserve decisions, plans, and open "
    "issues faithfully; discard redundant or repeated tool output. Be concise."
)


def estimate_tokens(text: str) -> int:
    """Cheap heuristic token estimate (~4 chars/token). Compaction only needs an
    approximate budget; the real tiktoken-backed budgeter lives in budget.py."""
    return len(text) // 4


def _is_salient(line: str) -> bool:
    lowered = line.lower()
    return any(marker in lowered for marker in _SALIENT_MARKERS)


@runtime_checkable
class Compactor(Protocol):
    async def compact(self, sc: StageContext, *, target_tokens: int) -> StageContext: ...


class LLMCompactor:
    """Default compactor: an LLM summarizes history while salient lines (decisions,
    plans, open issues) are preserved verbatim and prioritized to fit the budget."""

    def __init__(self, llm: LLMProvider) -> None:
        self._llm = llm

    async def make_digest(self, sc: StageContext, *, target_tokens: int) -> CompactedContext:
        original = "\n".join(sc.dialogue)
        original_tokens = estimate_tokens(original)
        preserved = [line for line in sc.dialogue if _is_salient(line)]
        summary = await self._llm.generate(self._build_prompt(sc), system=_COMPACTION_SYSTEM)
        digest_text = self._fit(preserved, summary, target_tokens)
        token_count = estimate_tokens(digest_text)
        return CompactedContext(
            digest=digest_text,
            preserved=preserved,
            token_count=token_count,
            dropped_tokens=max(0, original_tokens - token_count),
        )

    async def compact(self, sc: StageContext, *, target_tokens: int) -> StageContext:
        digest = await self.make_digest(sc, target_tokens=target_tokens)
        new_dialogue = [digest.digest, *sc.dialogue[-2:]]
        return dataclasses.replace(sc, dialogue=new_dialogue)

    @staticmethod
    def _build_prompt(sc: StageContext) -> str:
        body = "\n".join(sc.dialogue)
        return f"Goal: {sc.goal}\n\nWorking context to compact:\n{body}"

    @staticmethod
    def _fit(preserved: list[str], summary: str, target_tokens: int) -> str:
        full = "\n".join([*preserved, summary])
        if estimate_tokens(full) <= target_tokens:
            return full
        kept = "\n".join(preserved)  # decisions/plans win over the prose summary
        if estimate_tokens(kept) <= target_tokens:
            return kept
        return kept[: target_tokens * 4]  # last resort: hard-trim


if TYPE_CHECKING:
    _assert_compactor: Compactor = LLMCompactor.__new__(LLMCompactor)


__all__ = ["Compactor", "LLMCompactor", "estimate_tokens"]
