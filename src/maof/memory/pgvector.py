"""pgvector-backed VectorStore — the default adapter.

Embeddings cross the boundary as pgvector text literals (``[v1,v2,...]``) cast to
``::vector``, so no extra client dependency or numpy is required. The column
dimension is set from config at migration time. Vector recall is one
memory source among three, not "the memory system".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.persistence.postgres import Database
from maof.types import MemorySnippet, TenantContext

if TYPE_CHECKING:
    from maof.memory.base import VectorStore


def _vector_literal(vec: list[float]) -> str:
    return "[" + ",".join(str(float(x)) for x in vec) + "]"


class PgVectorStore:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def upsert(self, tenant: TenantContext, items: list[MemorySnippet]) -> None:
        for item in items:
            if item.embedding is None:
                raise ValueError("MemorySnippet.embedding is required for vector upsert")
            await self._db.execute(
                """
                INSERT INTO memories (tenant_id, kind, content, prov, embedding)
                VALUES ($1, $2, $3, $4, $5::vector)
                """,
                tenant.tenant_id,
                item.kind,
                item.content,
                item.prov,
                _vector_literal(item.embedding),
            )

    async def query(
        self, tenant: TenantContext, embedding: list[float], top_k: int
    ) -> list[MemorySnippet]:
        rows = await self._db.fetch(
            """
            SELECT kind, content, prov, 1 - (embedding <=> $2::vector) AS score
            FROM memories
            WHERE tenant_id = $1
            ORDER BY embedding <=> $2::vector
            LIMIT $3
            """,
            tenant.tenant_id,
            _vector_literal(embedding),
            top_k,
        )
        return [
            MemorySnippet(
                kind=r["kind"], content=r["content"], prov=r["prov"], score=float(r["score"])
            )
            for r in rows
        ]


if TYPE_CHECKING:
    _assert_vector_store: VectorStore = PgVectorStore(Database(""))


__all__ = ["PgVectorStore"]
