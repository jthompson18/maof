"""Token budgeter — real, not a no-op.

Context is a finite resource subject to context rot. The budgeter counts tokens
and trims/prioritizes sources to fit a cap; when the working context is over and a
Compactor is wired, it triggers compaction.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from maof.context.compaction import estimate_tokens

if TYPE_CHECKING:
    from maof.context.compaction import Compactor
    from maof.types import ContextEnvelope, StageContext


def tiktoken_counter(encoding: str = "cl100k_base") -> Callable[[str], int]:
    """A real tokenizer-backed counter (requires the ``tokenizers`` extra)."""
    import tiktoken

    enc = tiktoken.get_encoding(encoding)
    return lambda text: len(enc.encode(text))


@runtime_checkable
class Budgeter(Protocol):
    def count(self, env: ContextEnvelope) -> int: ...

    async def enforce(self, sc: StageContext, *, max_tokens: int) -> StageContext: ...


class TokenBudgeter:
    """Default budgeter. ``enforce`` compacts (when a Compactor is set) or trims;
    ``fit_envelope`` shrinks an assembled envelope by dropping the lowest-priority
    content (memories, then oldest dialogue) until it fits. ``counter`` is
    injectable (e.g. :func:`tiktoken_counter`); the default is a fast heuristic."""

    def __init__(
        self,
        compactor: Compactor | None = None,
        *,
        counter: Callable[[str], int] | None = None,
    ) -> None:
        self._compactor = compactor
        self._count_text = counter if counter is not None else estimate_tokens

    @staticmethod
    def _envelope_text(env: ContextEnvelope) -> str:
        parts = [env.goal, *env.dialogue]
        parts.extend(m.content for m in env.memories)
        parts.extend(dp.note for dp in env.data_pointers)
        return "\n".join(p for p in parts if p)

    def count(self, env: ContextEnvelope) -> int:
        return self._count_text(self._envelope_text(env))

    async def enforce(self, sc: StageContext, *, max_tokens: int) -> StageContext:
        if sc.envelope is not None:
            tokens = self.count(sc.envelope)
        else:
            tokens = self._count_text("\n".join(sc.dialogue))
        if tokens <= max_tokens:
            return sc
        if self._compactor is not None:
            return await self._compactor.compact(sc, target_tokens=max_tokens)
        return dataclasses.replace(sc, dialogue=sc.dialogue[-3:])

    def fit_envelope(self, env: ContextEnvelope, *, max_tokens: int) -> ContextEnvelope:
        while self.count(env) > max_tokens and env.memories:
            env.memories.pop()  # memories are the lowest-priority source
        while self.count(env) > max_tokens and len(env.dialogue) > 1:
            env.dialogue.pop(0)
        if self.count(env) > max_tokens:
            # last resort: hard-trim to fit
            allowed = max_tokens * 4
            env.goal = env.goal[:allowed]
            remaining = max(0, allowed - len(env.goal) - 1)
            env.dialogue = [env.dialogue[-1][:remaining]] if env.dialogue and remaining else []
        return env


__all__ = ["Budgeter", "TokenBudgeter", "tiktoken_counter"]
