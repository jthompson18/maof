"""Checkpointer: snapshot after each step, resume-from-failure.

On restart, resume from the last good StageContext — never re-run completed work.
Serialization captures the resumable working state (run/intent/envelope/plan/
dialogue/flags); live handles (run store, cost ledger) are re-wired by the driver.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from maof.types import (
    ContextEnvelope,
    DecisionTrace,
    Intent,
    Plan,
    Stage,
    StageContext,
    TenantContext,
)

if TYPE_CHECKING:
    from maof.persistence.base import CheckpointRepo


def checkpoint_serialize(sc: StageContext) -> bytes:
    payload = {
        "run_id": sc.run_id,
        "tenant": sc.tenant.model_dump(),
        "goal": sc.goal,
        "stage": sc.stage.value,
        "dialogue": sc.dialogue,
        "intent": sc.intent.model_dump() if sc.intent is not None else None,
        "envelope": sc.envelope.model_dump() if sc.envelope is not None else None,
        "plan": sc.plan.model_dump() if sc.plan is not None else None,
        "policy_decisions": [d.model_dump() for d in sc.policy_decisions],
        "mode": sc.mode,
        "region": sc.region,
        "trace_ref": sc.trace_ref,
        "extras": sc.extras,
    }
    return json.dumps(payload).encode("utf-8")


def checkpoint_deserialize(blob: bytes) -> StageContext:
    data = json.loads(blob)
    return StageContext(
        run_id=data["run_id"],
        tenant=TenantContext(**data["tenant"]),
        goal=data["goal"],
        stage=Stage(data["stage"]),
        dialogue=list(data["dialogue"]),
        intent=Intent(**data["intent"]) if data["intent"] is not None else None,
        envelope=ContextEnvelope(**data["envelope"]) if data["envelope"] is not None else None,
        plan=Plan(**data["plan"]) if data["plan"] is not None else None,
        policy_decisions=[DecisionTrace(**d) for d in data["policy_decisions"]],
        mode=data["mode"],
        region=data["region"],
        trace_ref=data.get("trace_ref"),
        extras=data.get("extras", {}),
    )


@runtime_checkable
class Checkpointer(Protocol):
    async def save(self, run_id: str, step: str, sc: StageContext) -> None: ...

    async def resume(self, run_id: str) -> StageContext | None: ...


class InMemoryCheckpointer:
    def __init__(self) -> None:
        self._latest: dict[str, bytes] = {}

    async def save(self, run_id: str, step: str, sc: StageContext) -> None:
        self._latest[run_id] = checkpoint_serialize(sc)

    async def resume(self, run_id: str) -> StageContext | None:
        blob = self._latest.get(run_id)
        return checkpoint_deserialize(blob) if blob is not None else None


class PostgresCheckpointer:
    def __init__(self, repo: CheckpointRepo) -> None:
        self._repo = repo

    async def save(self, run_id: str, step: str, sc: StageContext) -> None:
        await self._repo.save(run_id, step, checkpoint_serialize(sc))

    async def resume(self, run_id: str) -> StageContext | None:
        blob = await self._repo.latest(run_id)
        return checkpoint_deserialize(blob) if blob is not None else None


__all__ = [
    "Checkpointer",
    "InMemoryCheckpointer",
    "PostgresCheckpointer",
    "checkpoint_serialize",
    "checkpoint_deserialize",
]
