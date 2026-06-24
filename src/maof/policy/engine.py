"""Policy engine: native pre/post hooks, pluggable rules.

Pre-prompt / post-plan hooks with the full action set (nudge, set-flag,
strip-tool, strip-task-type, clamp/mutate, require-approval, deny). Rulesets are
versioned and canary-able. Every decision is audited. The JSON condition DSL and
a Python-callable rule type are both first-class.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from maof.observability.events import AuditEvent
from maof.policy.actions import ActionContext, apply_actions
from maof.policy.dsl import EvalContext
from maof.policy.dsl import evaluate as dsl_evaluate
from maof.types import DecisionTrace, LoadedRuleset, TenantContext

if TYPE_CHECKING:
    from maof.observability.events import EventSink
    from maof.persistence.base import PolicyRepo
    from maof.types import ContextEnvelope, Envelope, Intent, Plan, Task, TaskResult


class RuleDecision(BaseModel):
    """The outcome of a policy hook."""

    require_approval: bool = False
    approval_reason: str = ""
    approval_roles: list[str] = Field(default_factory=list)  # role-bound parties
    approval_parties: int = 1
    denied: bool = False
    denial_reason: str = ""
    added_nudges: list[str] = Field(default_factory=list)
    trace: list[DecisionTrace] = Field(default_factory=list)


@runtime_checkable
class PolicyEngine(Protocol):
    async def load(self, tenant: TenantContext, ruleset_ref: str) -> LoadedRuleset: ...

    async def pre_prompt(self, env: ContextEnvelope, intent: Intent) -> RuleDecision: ...

    async def post_plan(
        self, env: ContextEnvelope, intent: Intent, plan: Plan
    ) -> tuple[RuleDecision, Plan]: ...

    async def post_result(self, env: Envelope, task: Task, result: TaskResult) -> RuleDecision: ...


@dataclass
class CallableRule:
    """A Python-callable rule — a first-class alternative to the JSON DSL."""

    rule_id: str
    when: Callable[[EvalContext], bool]
    actions: list[dict[str, Any]]
    stage: str = "*"
    priority: int = 100
    description: str = ""


class NativePolicyEngine:
    """Pure-Python policy engine (no external policy-engine dependency).

    Mutates the working ``env`` (policy_flags, toolset) and returns a possibly
    mutated plan so callers observe set_flag/strip/clamp effects.
    """

    def __init__(
        self,
        *,
        ruleset_ref: str = "default",
        repo: PolicyRepo | None = None,
        event_sink: EventSink | None = None,
        callable_rules: list[CallableRule] | None = None,
    ) -> None:
        self._ruleset_ref = ruleset_ref
        self._repo = repo
        self._event_sink = event_sink
        self._callable_rules = list(callable_rules) if callable_rules else []

    async def load(self, tenant: TenantContext, ruleset_ref: str) -> LoadedRuleset:
        if self._repo is not None:
            loaded = await self._repo.load_ruleset(tenant, ruleset_ref)
            if loaded is not None:
                return loaded
        return LoadedRuleset(ruleset_ref=ruleset_ref, version=0, rules=[])

    async def pre_prompt(self, env: ContextEnvelope, intent: Intent) -> RuleDecision:
        decision, _ = await self._evaluate(env, intent, plan=None)
        return decision

    async def post_plan(
        self, env: ContextEnvelope, intent: Intent, plan: Plan
    ) -> tuple[RuleDecision, Plan]:
        decision, mutated = await self._evaluate(env, intent, plan=plan)
        return decision, mutated if mutated is not None else plan

    async def post_result(self, env: Envelope, task: Task, result: TaskResult) -> RuleDecision:
        """Result-conformance governance: rules with stage
        ``"post_result"`` evaluate against ``semantic.result.*`` / ``semantic.task.*``
        and can deny/flag an output before dependent steps consume it."""
        tenant = TenantContext(tenant_id=env.tenant_id)
        ruleset = await self.load(tenant, self._ruleset_ref)
        decision = RuleDecision()
        flags: dict[str, str] = {}
        ectx = EvalContext(
            flags=flags,
            semantic={
                "result": dict(result.output),
                "task": task.model_dump(),
                "status": result.status,
            },
        )
        actx = ActionContext(decision=decision, flags=flags)
        for rule in sorted(
            (r for r in ruleset.rules if r.enabled and self._stage_match(r.stage, "post_result")),
            key=lambda r: r.priority,
        ):
            if dsl_evaluate(rule.when, ectx):
                apply_actions(rule.actions, actx)
                decision.trace.append(
                    DecisionTrace(
                        rule_id=rule.rule_id,
                        ruleset_ref=ruleset.ruleset_ref,
                        version=ruleset.version,
                        stage="post_result",
                        actions=[a.get("type", "") for a in rule.actions],
                    )
                )
        if self._event_sink is not None and decision.trace:
            await self._event_sink.emit(
                AuditEvent(
                    tenant_id=env.tenant_id,
                    intent_id=env.intent_id,
                    event_type="policy_decision",
                    envelope={"stage": "post_result", "task_type": task.task_type},
                    details={
                        "matched_rules": [t.rule_id for t in decision.trace],
                        "denied": decision.denied,
                    },
                )
            )
        return decision

    @staticmethod
    def _stage_match(rule_stage: str, env_stage: str) -> bool:
        return rule_stage == "*" or rule_stage == env_stage

    async def _evaluate(
        self, env: ContextEnvelope, intent: Intent | None, *, plan: Plan | None
    ) -> tuple[RuleDecision, Plan | None]:
        tenant = TenantContext(tenant_id=env.tenant_id)
        ruleset = await self.load(tenant, self._ruleset_ref)

        decision = RuleDecision()
        flags = dict(env.policy_flags)
        toolset = list(env.toolset)
        working_plan = plan.model_copy(deep=True) if plan is not None else None
        actx = ActionContext(decision=decision, flags=flags, plan=working_plan, toolset=toolset)
        ectx = EvalContext(
            flags=flags,
            intent=intent,
            plan=working_plan,
            semantic=env.semantic_model,
            toolset=toolset,
        )

        dsl_rules = sorted(
            (r for r in ruleset.rules if r.enabled and self._stage_match(r.stage, env.stage)),
            key=lambda r: r.priority,
        )
        for rule in dsl_rules:
            if dsl_evaluate(rule.when, ectx):
                apply_actions(rule.actions, actx)
                decision.trace.append(
                    DecisionTrace(
                        rule_id=rule.rule_id,
                        ruleset_ref=ruleset.ruleset_ref,
                        version=ruleset.version,
                        stage=str(env.stage),
                        actions=[a.get("type", "") for a in rule.actions],
                    )
                )

        for crule in sorted(self._callable_rules, key=lambda r: r.priority):
            if self._stage_match(crule.stage, env.stage) and crule.when(ectx):
                apply_actions(crule.actions, actx)
                decision.trace.append(
                    DecisionTrace(
                        rule_id=crule.rule_id,
                        ruleset_ref=ruleset.ruleset_ref,
                        version=ruleset.version,
                        stage=str(env.stage),
                        actions=[a.get("type", "") for a in crule.actions],
                    )
                )

        # reflect mutations back onto the working envelope
        env.policy_flags.clear()
        env.policy_flags.update(flags)
        env.toolset[:] = toolset

        if self._event_sink is not None and decision.trace:
            await self._event_sink.emit(
                AuditEvent(
                    tenant_id=env.tenant_id,
                    intent_id=env.intent_id,
                    event_type="policy_decision",
                    envelope={"stage": str(env.stage), "ruleset": ruleset.ruleset_ref},
                    details={
                        "matched_rules": [t.rule_id for t in decision.trace],
                        "denied": decision.denied,
                        "require_approval": decision.require_approval,
                        "nudges": decision.added_nudges,
                    },
                )
            )

        return decision, working_plan


__all__ = ["RuleDecision", "PolicyEngine", "NativePolicyEngine", "CallableRule"]
