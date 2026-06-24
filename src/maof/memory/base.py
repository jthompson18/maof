"""Vector store + MemoryService.

MemoryService exposes THREE strategies, not just RAG: compaction, structured
note-taking / agentic memory, and vector recall. The ``compact`` and note
methods are thin facades delegating to the ``context/`` engines; vector
recall is the reference behavior, demoted to one source among three.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from maof.types import (
        CompactedContext,
        MemorySnippet,
        Note,
        StageContext,
        TenantContext,
    )


@runtime_checkable
class VectorStore(Protocol):
    async def upsert(self, tenant: TenantContext, items: list[MemorySnippet]) -> None: ...

    async def query(
        self, tenant: TenantContext, embedding: list[float], top_k: int
    ) -> list[MemorySnippet]: ...


@runtime_checkable
class MemoryService(Protocol):
    # 1) compaction
    async def compact(self, sc: StageContext, *, target_tokens: int) -> CompactedContext: ...

    # 2) structured note-taking / agentic memory
    async def write_note(self, run_id: str, note: Note) -> str: ...

    async def read_notes(self, run_id: str, *, tags: list[str] | None = None) -> list[Note]: ...

    # 3) vector recall
    async def recall(
        self, tenant: TenantContext, query: str, top_k: int
    ) -> list[MemorySnippet]: ...


__all__ = ["VectorStore", "MemoryService"]
