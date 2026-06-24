"""Context-engineering layer: redactor, budgeter, JIT resolver, builder + sources."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from maof.context.budget import TokenBudgeter
from maof.context.builder import ContextBuilder
from maof.context.jit import DefaultReferenceResolver
from maof.context.redactor import REDACTION, RegexRedactor
from maof.context.sources.builtins import (
    DataPointerSource,
    MemoriesSource,
    PolicyFlagsSource,
    SemanticModelSource,
    ToolRegistrySource,
)
from maof.types import (
    ContextEnvelope,
    DataPointer,
    MemorySnippet,
    Stage,
    StageContext,
    TenantContext,
    ToolRef,
)


def _sc(**kw: Any) -> StageContext:
    return StageContext(
        run_id="r1",
        tenant=TenantContext(tenant_id="t1"),
        goal=kw.get("goal", "launch"),
        stage=kw.get("stage", Stage.CHAT),
        dialogue=kw.get("dialogue", []),
        envelope=kw.get("envelope"),
    )


# redactor
def test_redactor_scrubs_pii_when_enabled() -> None:
    env = ContextEnvelope(
        run_id="r",
        tenant_id="t",
        stage=Stage.CHAT,
        goal="email alice@example.com",
        policy_flags={"pii": "redact"},
        dialogue=["call 415-555-1234 about budget 250000"],
        memories=[MemorySnippet(kind="m", content="ssn 123-45-6789")],
    )
    out = RegexRedactor().redact(env)
    assert "alice@example.com" not in out.goal and REDACTION in out.goal
    assert "415-555-1234" not in out.dialogue[0]
    assert "250000" in out.dialogue[0]  # plain numbers (budgets) are preserved
    assert "123-45-6789" not in out.memories[0].content


def test_redactor_noop_when_flag_absent() -> None:
    env = ContextEnvelope(
        run_id="r", tenant_id="t", stage=Stage.CHAT, goal="email alice@example.com"
    )
    assert RegexRedactor().redact(env).goal == "email alice@example.com"


# budgeter
def test_budgeter_count() -> None:
    env = ContextEnvelope(run_id="r", tenant_id="t", stage=Stage.CHAT, goal="x" * 40)
    assert TokenBudgeter().count(env) == 10  # 40 chars // 4


def test_fit_envelope_trims_to_budget() -> None:
    env = ContextEnvelope(
        run_id="r",
        tenant_id="t",
        stage=Stage.CHAT,
        goal="g",
        dialogue=["line one " * 5, "line two " * 5, "line three " * 5],
        memories=[MemorySnippet(kind="m", content="mem " * 20) for _ in range(3)],
    )
    budgeter = TokenBudgeter()
    out = budgeter.fit_envelope(env, max_tokens=20)
    assert budgeter.count(out) <= 20
    assert len(out.memories) < 3  # memories dropped first


async def test_budgeter_enforce_triggers_compaction() -> None:
    class StubCompactor:
        async def compact(self, sc: StageContext, *, target_tokens: int) -> StageContext:
            return dataclasses.replace(sc, dialogue=["COMPACTED"])

    budgeter = TokenBudgeter(StubCompactor())
    sc = _sc(dialogue=["x" * 1000])
    out = await budgeter.enforce(sc, max_tokens=10)
    assert out.dialogue == ["COMPACTED"]


async def test_budgeter_enforce_noop_under_budget() -> None:
    sc = _sc(dialogue=["short"])
    out = await TokenBudgeter().enforce(sc, max_tokens=1000)
    assert out is sc


# JIT resolver
async def test_jit_custom_loader() -> None:
    async def loader(rest: str) -> str:
        return f"loaded:{rest}"

    resolver = DefaultReferenceResolver(loaders={"test": loader})
    assert await resolver.resolve("test://hi", _sc()) == "loaded:hi"


async def test_jit_artifact_scheme() -> None:
    class FakeArtifacts:
        async def put(self, *a: Any, **k: Any) -> str:
            return "ref"

        async def get(self, ref: str) -> bytes:
            return b"artifact-body"

    resolver = DefaultReferenceResolver(artifacts=FakeArtifacts())
    assert await resolver.resolve("artifact://abc", _sc()) == "artifact-body"


async def test_jit_data_pointer_alias() -> None:
    env = ContextEnvelope(
        run_id="r",
        tenant_id="t",
        stage=Stage.CHAT,
        data_pointers=[DataPointer(alias="purchase_plan", uri="s3://plan")],
    )
    resolver = DefaultReferenceResolver()
    assert await resolver.resolve("purchase_plan", _sc(envelope=env)) == "s3://plan"


async def test_jit_unknown_raises() -> None:
    with pytest.raises(KeyError):
        await DefaultReferenceResolver().resolve("nope://x", _sc())


# builder + sources
async def test_context_builder_runs_sources() -> None:
    builder = ContextBuilder(
        [
            PolicyFlagsSource({"funds_received": "true"}),
            SemanticModelSource({"platform_core": "v1"}),
            ToolRegistrySource(
                [ToolRef(name="commitments"), ToolRef(name="fulfillment")],
                stage_scopes={"action_plan": {"commitments"}},
            ),
            DataPointerSource([DataPointer(alias="plan", uri="s3://x")]),
        ]
    )
    env = await builder.build(_sc(stage=Stage.ACTION_PLAN))
    assert env.policy_flags["funds_received"] == "true"
    assert env.semantic_model["platform_core"] == "v1"
    assert [t.name for t in env.toolset] == ["commitments"]  # fulfillment filtered by stage scope
    assert env.data_pointers[0].alias == "plan"


async def test_context_builder_redacts_and_budgets() -> None:
    builder = ContextBuilder(
        [PolicyFlagsSource({"pii": "redact"})],
        redactor=RegexRedactor(),
        budgeter=TokenBudgeter(),
        max_tokens=20,
    )
    sc = _sc(goal="reach alice@example.com", dialogue=["a " * 50, "b " * 50, "c " * 50])
    env = await builder.build(sc)
    assert "alice@example.com" not in env.goal
    assert TokenBudgeter().count(env) <= 20


async def test_memories_source_recall() -> None:
    class FakeMemory:
        async def recall(self, tenant: Any, query: str, top_k: int) -> list[MemorySnippet]:
            return [MemorySnippet(kind="recall", content=f"hit:{query}")]

    builder = ContextBuilder([MemoriesSource(FakeMemory(), top_k=3)])
    env = await builder.build(_sc(goal="budget"))
    assert env.memories[0].content == "hit:budget"
