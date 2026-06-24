"""Default ``action_plan`` stage.

Builds the context envelope, runs the policy pre-prompt hook, invokes the adopter's
planner to produce a Plan, then runs the policy post-plan hook (which may clamp/
strip/deny/require-approval). The planner is injected — MAOF ships no domain logic.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from maof.errors import PolicyDenied
from maof.types import ContextEnvelope, Plan, Stage

if TYPE_CHECKING:
    from maof.context.builder import ContextBuilder
    from maof.policy.engine import PolicyEngine
    from maof.types import StageContext

Planner = Callable[["StageContext"], Awaitable[Plan]]


class ActionPlanStage:
    name = "action_plan"

    def __init__(
        self,
        planner: Planner,
        *,
        policy: PolicyEngine | None = None,
        context_builder: ContextBuilder | None = None,
        ruleset_ref: str | None = None,
    ) -> None:
        self._planner = planner
        self._policy = policy
        self._context_builder = context_builder
        self._ruleset_ref = ruleset_ref

    async def execute(self, sc: StageContext) -> StageContext:
        if self._context_builder is not None:
            sc.envelope = await self._context_builder.build(sc)
        if sc.envelope is None:
            sc.envelope = ContextEnvelope(
                run_id=sc.run_id,
                tenant_id=sc.tenant.tenant_id,
                stage=Stage.ACTION_PLAN,
                goal=sc.goal,
            )
        sc.envelope.stage = Stage.ACTION_PLAN
        if sc.intent is not None:
            sc.envelope.intent_id = sc.intent.intent_id

        if self._policy is not None and sc.intent is not None:
            pre = await self._policy.pre_prompt(sc.envelope, sc.intent)
            sc.policy_decisions.extend(pre.trace)
            if pre.denied:
                raise PolicyDenied(pre.denial_reason)

        plan = await self._planner(sc)
        sc.plan = plan

        if self._policy is not None and sc.intent is not None:
            decision, plan = await self._policy.post_plan(sc.envelope, sc.intent, plan)
            sc.plan = plan
            sc.policy_decisions.extend(decision.trace)
            sc.extras["policy"] = {
                "denied": decision.denied,
                "denial_reason": decision.denial_reason,
                "require_approval": decision.require_approval,
                "approval_reason": decision.approval_reason,
                "approval_roles": list(decision.approval_roles),
                "approval_parties": decision.approval_parties,
                "nudges": decision.added_nudges,
            }
            if decision.denied:
                raise PolicyDenied(decision.denial_reason)
        return sc


__all__ = ["ActionPlanStage", "Planner"]
