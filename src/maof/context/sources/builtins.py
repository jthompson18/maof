"""Default context sources.

Each implements the ``ContextSource`` contract (name + ``contribute``). They are
adopter-configured: the framework supplies the machinery, the adopter supplies the
concrete flags, semantic model, tools, pointers, and memory backend.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from maof.memory.base import MemoryService
    from maof.types import ContextEnvelope, DataPointer, StageContext, ToolRef


class PolicyFlagsSource:
    name = "policy_flags"

    def __init__(self, flags: dict[str, str]) -> None:
        self._flags = dict(flags)

    async def contribute(self, sc: StageContext, env: ContextEnvelope) -> ContextEnvelope:
        env.policy_flags.update(self._flags)
        return env


class SemanticModelSource:
    name = "semantic_model"

    def __init__(self, model: dict[str, object]) -> None:
        self._model = dict(model)

    async def contribute(self, sc: StageContext, env: ContextEnvelope) -> ContextEnvelope:
        env.semantic_model.update(self._model)
        return env


class ToolRegistrySource:
    name = "toolset"

    def __init__(
        self, tools: list[ToolRef], *, stage_scopes: dict[str, set[str]] | None = None
    ) -> None:
        self._tools = list(tools)
        self._stage_scopes = stage_scopes or {}

    async def contribute(self, sc: StageContext, env: ContextEnvelope) -> ContextEnvelope:
        allowed = self._stage_scopes.get(str(sc.stage))
        for tool in self._tools:
            if allowed is None or tool.name in allowed:
                env.toolset.append(tool)
        return env


class DataPointerSource:
    name = "data_pointers"

    def __init__(self, pointers: list[DataPointer]) -> None:
        self._pointers = list(pointers)

    async def contribute(self, sc: StageContext, env: ContextEnvelope) -> ContextEnvelope:
        env.data_pointers.extend(self._pointers)
        return env


class MemoriesSource:
    name = "memories"

    def __init__(self, memory: MemoryService, *, query: str | None = None, top_k: int = 5) -> None:
        self._memory = memory
        self._query = query
        self._top_k = top_k

    async def contribute(self, sc: StageContext, env: ContextEnvelope) -> ContextEnvelope:
        query = self._query or sc.goal
        env.memories.extend(await self._memory.recall(sc.tenant, query, self._top_k))
        return env


__all__ = [
    "PolicyFlagsSource",
    "SemanticModelSource",
    "ToolRegistrySource",
    "DataPointerSource",
    "MemoriesSource",
]
