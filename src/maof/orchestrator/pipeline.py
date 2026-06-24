"""Workflow mode: a pluggable, reorderable stage graph.

Governed, predictable, the default. The default stages are
chat -> intent_synthesis -> action_plan -> approval -> publish, each
replaceable/insertable. Each stage is checkpointed by the L1 driver.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from maof.errors import MAOFError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from maof.types import StageContext


@runtime_checkable
class Stage(Protocol):
    """One pipeline step. ``name`` is typically a stage identifier."""

    name: str

    async def execute(self, sc: StageContext) -> StageContext: ...


class Pipeline:
    """An ordered, injectable stage pipeline."""

    def __init__(self, stages: list[Stage]) -> None:
        self._stages: list[Stage] = list(stages)

    @property
    def stages(self) -> list[Stage]:
        return list(self._stages)

    @property
    def stage_names(self) -> list[str]:
        return [s.name for s in self._stages]

    def _index_of(self, name: str) -> int:
        for i, stage in enumerate(self._stages):
            if stage.name == name:
                return i
        raise MAOFError(f"no stage named {name!r} in pipeline")

    def insert_before(self, name: str, stage: Stage) -> None:
        self._stages.insert(self._index_of(name), stage)

    def insert_after(self, name: str, stage: Stage) -> None:
        self._stages.insert(self._index_of(name) + 1, stage)

    def replace(self, name: str, stage: Stage) -> None:
        self._stages[self._index_of(name)] = stage

    async def run(
        self,
        sc: StageContext,
        *,
        on_stage_complete: Callable[[str, StageContext], Awaitable[None]] | None = None,
    ) -> StageContext:
        for stage in self._stages:
            sc = await stage.execute(sc)
            if on_stage_complete is not None:
                await on_stage_complete(stage.name, sc)
        return sc


__all__ = ["Stage", "Pipeline"]
