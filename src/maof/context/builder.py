"""Context-building pipeline + source interface.

A ContextBuilder runs sources in order, then applies a (functional) redactor and a
(real) token budgeter that trims the envelope to fit. Vector recall is just one
source here; compaction/note-taking/JIT live elsewhere in ``context/``.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from maof.types import ContextEnvelope

if TYPE_CHECKING:
    from maof.context.budget import TokenBudgeter
    from maof.context.compaction import Compactor
    from maof.context.redactor import Redactor
    from maof.types import StageContext


@runtime_checkable
class ContextSource(Protocol):
    """A pluggable contributor to the context envelope (policy flags, semantic
    model, tools, data pointers, memories)."""

    name: str

    async def contribute(self, sc: StageContext, env: ContextEnvelope) -> ContextEnvelope: ...


class ContextBuilder:
    """Runs sources -> redactor -> budgeter(-> compactor when over) -> fit.
    The redactor and budgeter are functional, not no-ops: when the
    assembled envelope exceeds the budget, the Compactor summarizes the dialogue
    (preserving decisions) BEFORE the budgeter's blunt trim guarantees the cap."""

    def __init__(
        self,
        sources: list[ContextSource],
        *,
        redactor: Redactor | None = None,
        budgeter: TokenBudgeter | None = None,
        max_tokens: int | None = None,
        compactor: Compactor | None = None,
    ) -> None:
        self._sources = list(sources)
        self._redactor = redactor
        self._budgeter = budgeter
        self._max_tokens = max_tokens
        self._compactor = compactor

    def add_source(self, source: ContextSource) -> None:
        """Attach a source after construction (registry auto-attachment)."""
        self._sources.append(source)

    async def build(self, sc: StageContext) -> ContextEnvelope:
        env = ContextEnvelope(
            run_id=sc.run_id,
            tenant_id=sc.tenant.tenant_id,
            intent_id=sc.intent.intent_id if sc.intent is not None else None,
            stage=sc.stage,
            goal=sc.goal,
            dialogue=list(sc.dialogue),
        )
        for source in self._sources:
            env = await source.contribute(sc, env)
        if self._redactor is not None:
            env = self._redactor.redact(env)
        if self._budgeter is not None and self._max_tokens is not None:
            if self._compactor is not None and self._budgeter.count(env) > self._max_tokens:
                compacted = await self._compactor.compact(
                    dataclasses.replace(sc, dialogue=list(env.dialogue)),
                    target_tokens=self._max_tokens,
                )
                env.dialogue = list(compacted.dialogue)
            env = self._budgeter.fit_envelope(env, max_tokens=self._max_tokens)
        return env


__all__ = ["ContextSource", "ContextBuilder"]
