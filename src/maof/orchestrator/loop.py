"""Autonomous mode: an LLM-in-a-loop orchestrator.

The lead agent plans, spawns subagents under DelegationContracts, collects
distilled results, and decides whether to continue — bounded by an effort budget
and the cost gate. Every iteration is checkpointed. Reserve this
for genuinely open-ended tasks; the workflow pipeline is the default.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from maof.types import CostProjection

if TYPE_CHECKING:
    from maof.cost.accounting import WorthItGate
    from maof.models.base import LLMProvider
    from maof.orchestrator.coordinator import Coordinator
    from maof.orchestrator.delegation import DelegationContract
    from maof.policy.engine import PolicyEngine
    from maof.runs.checkpoint import Checkpointer
    from maof.types import EffortBudget, StageContext, SubResult

#: Returns the delegations to spawn next given the run state + results so far.
#: Returning an empty list ends the loop. Adopters inject this (it is the lead
#: agent's reasoning); it may itself call the LLM.
LoopPlanner = Callable[["StageContext", "list[SubResult]"], "Awaitable[list[DelegationContract]]"]


class OrchestratorLoop:
    def __init__(
        self,
        llm: LLMProvider,
        coordinator: Coordinator,
        budget: EffortBudget,
        policy: PolicyEngine,
        *,
        planner: LoopPlanner | None = None,
        checkpointer: Checkpointer | None = None,
        worth_it_gate: WorthItGate | None = None,
        max_iterations: int = 10,
    ) -> None:
        self._llm = llm
        self._coordinator = coordinator
        self._budget = budget
        self._policy = policy
        self._planner = planner
        self._checkpointer = checkpointer
        self._worth_it_gate = worth_it_gate
        self._max_iterations = max_iterations

    async def run(self, sc: StageContext) -> StageContext:
        subresults: list[SubResult] = []
        iteration = 0
        while iteration < self._max_iterations:
            if await self._cancelled(sc):
                sc.dialogue.append("run cancelled by operator; loop halted")
                break
            delegations = await self._plan(sc, subresults)
            if not delegations:
                break
            budget_left = self._budget.max_subagents - len(subresults)
            if budget_left <= 0:
                sc.dialogue.append("effort budget exhausted: max_subagents reached")
                break
            for index, delegation in enumerate(delegations[:budget_left]):
                if not await self._worth_it(sc, subresults, fan_out=len(delegations)):
                    break
                if delegation.step_ref is None:
                    # Stable step identity for idempotency keys: same iteration+index
                    # on a resumed replay -> same key -> dedupe.
                    delegation = delegation.model_copy(update={"step_ref": f"{iteration}:{index}"})
                result = await self._coordinator.dispatch(delegation, sc)
                subresults.append(result)
                sc.dialogue.append(
                    f"subresult[{delegation.objective[:40]}]: {result.summary[:200]}"
                )
            iteration += 1
            if self._checkpointer is not None:
                await self._checkpointer.save(sc.run_id, f"loop-{iteration}", sc)
        sc.extras["subresults"] = [r.model_dump() for r in subresults]
        sc.extras["iterations"] = iteration
        return sc

    async def _plan(
        self, sc: StageContext, subresults: list[SubResult]
    ) -> list[DelegationContract]:
        if self._planner is not None:
            return await self._planner(sc, subresults)
        return []

    @staticmethod
    async def _cancelled(sc: StageContext) -> bool:
        if sc.run_store is None:
            return False
        try:
            state = await sc.run_store.get_state(sc.run_id)
        except KeyError:
            return False
        from maof.types import RunStatus

        return state.cancel_requested or state.status is RunStatus.CANCELLED

    async def _worth_it(
        self, sc: StageContext, subresults: list[SubResult], *, fan_out: int
    ) -> bool:
        if self._worth_it_gate is None:
            return True
        projection = CostProjection(
            projected_subagents=len(subresults) + 1,
            projected_tokens=self._budget.max_tokens,  # prospective spend per subagent
            fan_out=fan_out,
        )
        decision = await self._worth_it_gate.check(sc, projection)
        if decision.denied or decision.require_approval:
            reason = decision.denial_reason or decision.approval_reason
            sc.dialogue.append(f"worth-it gate halted further fan-out: {reason}")
            return False
        return True


__all__ = ["OrchestratorLoop", "LoopPlanner"]
