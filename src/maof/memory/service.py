"""MemoryService facade over the three strategies.

A thin integration point: compaction delegates to the ``context`` Compactor, note
taking to the NoteStore, and vector recall to a VectorStore + EmbeddingProvider.
The logic lives in those engines — this class only wires them (no double impl).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.types import CompactedContext, MemorySnippet, Note, StageContext, TenantContext

if TYPE_CHECKING:
    from maof.context.compaction import LLMCompactor
    from maof.context.notes import NoteStore
    from maof.memory.base import MemoryService, VectorStore
    from maof.models.base import EmbeddingProvider


class DefaultMemoryService:
    def __init__(
        self,
        *,
        compactor: LLMCompactor,
        notes: NoteStore,
        vector: VectorStore,
        embedder: EmbeddingProvider,
    ) -> None:
        self._compactor = compactor
        self._notes = notes
        self._vector = vector
        self._embedder = embedder

    async def compact(self, sc: StageContext, *, target_tokens: int) -> CompactedContext:
        return await self._compactor.make_digest(sc, target_tokens=target_tokens)

    async def write_note(self, run_id: str, note: Note) -> str:
        return await self._notes.write_note(run_id, note)

    async def read_notes(self, run_id: str, *, tags: list[str] | None = None) -> list[Note]:
        return await self._notes.read_notes(run_id, tags=tags)

    async def recall(self, tenant: TenantContext, query: str, top_k: int) -> list[MemorySnippet]:
        embedding = (await self._embedder.embed([query]))[0]
        return await self._vector.query(tenant, embedding, top_k)


if TYPE_CHECKING:
    _assert_memory_service: MemoryService = DefaultMemoryService.__new__(DefaultMemoryService)


__all__ = ["DefaultMemoryService"]
