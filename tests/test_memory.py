"""Memory: compaction (preserve decisions + hit target), notes, and vector recall."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from maof.context.compaction import LLMCompactor
from maof.context.notes import InMemoryNoteStore, PostgresNoteStore
from maof.memory.pgvector import PgVectorStore
from maof.memory.service import DefaultMemoryService
from maof.models.base import HashingEmbeddingProvider
from maof.persistence.postgres import Database
from maof.types import MemorySnippet, Note, StageContext, TenantContext


class MockLLM:
    def __init__(self, output: str = "compact summary") -> None:
        self._output = output

    async def generate(
        self, prompt: str, *, system: Any = None, json_schema: Any = None, **opts: Any
    ) -> str:
        return self._output


class FakeVectorStore:
    async def upsert(self, tenant: TenantContext, items: list[MemorySnippet]) -> None:
        return None

    async def query(
        self, tenant: TenantContext, embedding: list[float], top_k: int
    ) -> list[MemorySnippet]:
        return []


def _tenant() -> TenantContext:
    return TenantContext(tenant_id=f"t-{uuid4()}")


# compaction
async def test_compactor_preserves_decisions_and_hits_target() -> None:
    dialogue = (
        ["DECISION: commit next-quarter east-region buy capped at cleared funds"]
        + [f"tool output line {i} with redundant detail" for i in range(50)]
        + ["open issue: await funds confirmation before commit"]
    )
    sc = StageContext(run_id="r1", tenant=_tenant(), goal="launch next-quarter", dialogue=dialogue)
    compactor = LLMCompactor(MockLLM("short prose summary of the run"))

    digest = await compactor.make_digest(sc, target_tokens=80)
    assert digest.token_count <= 80
    assert any("commit next-quarter east-region buy" in line for line in digest.preserved)
    assert any("open issue" in line.lower() for line in digest.preserved)
    assert digest.dropped_tokens > 0  # the redundant tool output was dropped


async def test_compactor_reinitializes_stage_context() -> None:
    sc = StageContext(
        run_id="r1",
        tenant=_tenant(),
        goal="g",
        dialogue=["a", "b", "DECISION: x", "c", "d"],
    )
    compactor = LLMCompactor(MockLLM("sum"))
    new_sc = await compactor.compact(sc, target_tokens=100)
    assert new_sc.run_id == "r1"
    assert len(new_sc.dialogue) < len(sc.dialogue)
    assert "DECISION: x" in new_sc.dialogue[0]  # digest carries the decision


# notes
async def test_in_memory_note_store() -> None:
    store = InMemoryNoteStore()
    run_id = "r1"
    note_id = await store.write_note(run_id, Note(run_id=run_id, content="c1", tags=["a"]))
    assert note_id
    await store.write_note(run_id, Note(run_id=run_id, content="c2", tags=["b"]))
    assert len(await store.read_notes(run_id)) == 2
    assert len(await store.read_notes(run_id, tags=["a"])) == 1
    assert await store.read_notes(run_id, tags=["missing"]) == []


async def test_memory_service_notes_delegate_postgres(db: Database) -> None:
    svc = DefaultMemoryService(
        compactor=LLMCompactor(MockLLM()),
        notes=PostgresNoteStore(db),
        vector=PgVectorStore(db),
        embedder=HashingEmbeddingProvider(dimension=8),
    )
    run_id = f"run-{uuid4()}"
    note_id = await svc.write_note(
        run_id, Note(run_id=run_id, content="decided X", tags=["decision"])
    )
    assert note_id
    notes = await svc.read_notes(run_id)
    assert len(notes) == 1
    assert notes[0].content == "decided X"
    assert len(await svc.read_notes(run_id, tags=["decision"])) == 1
    assert await svc.read_notes(run_id, tags=["nope"]) == []


# vector recall (end-to-end through the facade)
async def test_memory_service_recall_end_to_end(db: Database) -> None:
    embedder = HashingEmbeddingProvider(dimension=8)  # matches TEST_EMBED_DIM
    vector = PgVectorStore(db)
    tenant = _tenant()
    docs = [
        "quarterly budget planning for east-region",
        "shipment preparation specs and formats",
        "billing reconciliation notes",
    ]
    embeddings = await embedder.embed(docs)
    await vector.upsert(
        tenant,
        [
            MemorySnippet(kind="doc", content=d, embedding=e)
            for d, e in zip(docs, embeddings, strict=True)
        ],
    )

    svc = DefaultMemoryService(
        compactor=LLMCompactor(MockLLM()),
        notes=PostgresNoteStore(db),
        vector=vector,
        embedder=embedder,
    )
    results = await svc.recall(tenant, "quarterly budget planning", top_k=3)
    assert results
    assert results[0].content == docs[0]  # nearest neighbour to the budget query


async def test_memory_service_compact_delegates() -> None:
    svc = DefaultMemoryService(
        compactor=LLMCompactor(MockLLM("S")),
        notes=InMemoryNoteStore(),
        vector=FakeVectorStore(),
        embedder=HashingEmbeddingProvider(dimension=8),
    )
    sc = StageContext(run_id="r1", tenant=_tenant(), goal="g", dialogue=["DECISION: keep"])
    digest = await svc.compact(sc, target_tokens=50)
    assert "DECISION: keep" in digest.preserved
