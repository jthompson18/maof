"""Pipeline graph surgery + the per-stage completion hook."""

from __future__ import annotations

import pytest

from maof.errors import MAOFError
from maof.orchestrator.pipeline import Pipeline
from maof.types import StageContext, TenantContext


class _Stage:
    def __init__(self, name: str) -> None:
        self.name = name

    async def execute(self, sc: StageContext) -> StageContext:
        sc.dialogue.append(self.name)
        return sc


def _sc() -> StageContext:
    return StageContext(run_id="p1", tenant=TenantContext(tenant_id="t"), goal="g")


async def test_insert_before_after_and_replace_order() -> None:
    pipeline = Pipeline([_Stage("a"), _Stage("c")])
    pipeline.insert_before("c", _Stage("b"))
    pipeline.insert_after("c", _Stage("d"))
    pipeline.replace("a", _Stage("a2"))
    assert pipeline.stage_names == ["a2", "b", "c", "d"]
    sc = await pipeline.run(_sc())
    assert sc.dialogue == ["a2", "b", "c", "d"]


async def test_unknown_stage_name_raises() -> None:
    pipeline = Pipeline([_Stage("a")])
    with pytest.raises(MAOFError):
        pipeline.insert_before("nope", _Stage("x"))


async def test_on_stage_complete_hook_fires_per_stage() -> None:
    completed: list[str] = []

    async def hook(name: str, sc: StageContext) -> None:
        completed.append(name)

    await Pipeline([_Stage("a"), _Stage("b")]).run(_sc(), on_stage_complete=hook)
    assert completed == ["a", "b"]
