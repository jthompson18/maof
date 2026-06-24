"""pgvector store: upsert + top-k similarity query (TEST_EMBED_DIM = 8)."""

from __future__ import annotations

from uuid import uuid4

import pytest

from maof.memory.pgvector import PgVectorStore
from maof.persistence.postgres import Database
from maof.types import MemorySnippet, TenantContext


async def test_vector_upsert_and_query(db: Database) -> None:
    store = PgVectorStore(db)
    tenant = TenantContext(tenant_id=f"t-{uuid4()}")
    await store.upsert(
        tenant,
        [
            MemorySnippet(kind="doc", content="alpha", embedding=[1, 0, 0, 0, 0, 0, 0, 0]),
            MemorySnippet(kind="doc", content="beta", embedding=[0, 1, 0, 0, 0, 0, 0, 0]),
            MemorySnippet(kind="doc", content="gamma", embedding=[0.9, 0.1, 0, 0, 0, 0, 0, 0]),
        ],
    )

    results = await store.query(tenant, [1, 0, 0, 0, 0, 0, 0, 0], top_k=2)
    assert len(results) == 2
    assert results[0].content == "alpha"  # nearest neighbour to the query vector
    assert results[0].score >= results[1].score  # ordered by descending similarity


async def test_vector_query_is_tenant_scoped(db: Database) -> None:
    store = PgVectorStore(db)
    t1 = TenantContext(tenant_id=f"t-{uuid4()}")
    t2 = TenantContext(tenant_id=f"t-{uuid4()}")
    await store.upsert(t1, [MemorySnippet(kind="d", content="only-t1", embedding=[1] + [0] * 7)])
    results = await store.query(t2, [1] + [0] * 7, top_k=5)
    assert results == []


async def test_vector_upsert_requires_embedding(db: Database) -> None:
    store = PgVectorStore(db)
    with pytest.raises(ValueError):
        await store.upsert(TenantContext(tenant_id="t"), [MemorySnippet(kind="x", content="y")])
