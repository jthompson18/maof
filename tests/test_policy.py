"""Policy engine: condition DSL, the full action set, canary, and audited decisions."""

from __future__ import annotations

from typing import Any

import pytest

from maof.policy.actions import ActionContext, apply_action
from maof.policy.dsl import EvalContext, evaluate
from maof.policy.engine import CallableRule, NativePolicyEngine, RuleDecision
from maof.policy.rulesets import choose_ruleset, in_canary
from maof.types import ContextEnvelope, Intent, LoadedRuleset, Plan, Rule, Stage, Task, ToolRef


def _ctx(**kw: Any) -> EvalContext:
    return EvalContext(
        flags=kw.get("flags", {}),
        intent=kw.get("intent"),
        plan=kw.get("plan"),
        semantic=kw.get("semantic", {}),
        toolset=kw.get("toolset", []),
    )


# DSL
def test_dsl_always() -> None:
    assert evaluate({"op": "always"}, _ctx()) is True


def test_dsl_flag_eq() -> None:
    c = _ctx(flags={"funds_received": "true"})
    assert evaluate({"op": "flag_eq", "key": "funds_received", "value": "true"}, c)
    assert not evaluate({"op": "flag_eq", "key": "funds_received", "value": "false"}, c)


def test_dsl_gt_lt_with_paths() -> None:
    c = _ctx(flags={"spend_cap_usd": "250000"})
    assert evaluate({"op": "gt", "lhs": "flags.spend_cap_usd", "rhs": 100000}, c)
    assert not evaluate({"op": "gt", "lhs": "flags.spend_cap_usd", "rhs": 300000}, c)
    assert evaluate({"op": "lt", "lhs": "flags.spend_cap_usd", "rhs": 300000}, c)


def test_dsl_eq_and_exists() -> None:
    c = _ctx(
        flags={"mode": "sandbox"},
        intent=Intent(intent_id="i", goal="g", details={"budget": 1000}),
    )
    assert evaluate({"op": "eq", "lhs": "flags.mode", "rhs": "sandbox"}, c)
    assert evaluate({"op": "exists", "lhs": "intent.details.budget"}, c)
    assert not evaluate({"op": "exists", "lhs": "intent.details.missing"}, c)


def test_dsl_toolset_and_plan_paths() -> None:
    c = _ctx(toolset=[ToolRef(name="commitments")], plan=Plan(task_types=["funds_commit"]))
    assert evaluate({"op": "exists", "lhs": "toolset.commitments"}, c)
    assert not evaluate({"op": "exists", "lhs": "toolset.fulfillment"}, c)
    assert evaluate({"op": "eq", "lhs": "plan.task_types", "rhs": ["funds_commit"]}, c)


def test_dsl_boolean_combinators() -> None:
    c = _ctx(flags={"a": "1", "b": "2"})
    assert evaluate(
        {
            "op": "and",
            "clauses": [
                {"op": "flag_eq", "key": "a", "value": "1"},
                {"op": "flag_eq", "key": "b", "value": "2"},
            ],
        },
        c,
    )
    assert evaluate(
        {"op": "or", "clauses": [{"op": "flag_eq", "key": "a", "value": "9"}, {"op": "always"}]}, c
    )
    assert evaluate({"op": "not", "clause": {"op": "flag_eq", "key": "a", "value": "9"}}, c)


def test_dsl_unknown_op_raises() -> None:
    with pytest.raises(ValueError):
        evaluate({"op": "bogus"}, _ctx())


# actions
def _actx(**kw: Any) -> ActionContext:
    return ActionContext(
        decision=RuleDecision(),
        flags=kw.get("flags", {}),
        plan=kw.get("plan"),
        toolset=kw.get("toolset", []),
    )


def test_action_set_flag() -> None:
    a = _actx()
    apply_action({"type": "set_flag", "key": "x", "value": 5}, a)
    assert a.flags["x"] == "5"


def test_action_add_nudge_dedups() -> None:
    a = _actx()
    apply_action({"type": "add_nudge", "text": "hi"}, a)
    apply_action({"type": "add_nudge", "text": "hi"}, a)
    assert a.decision.added_nudges == ["hi"]


def test_action_require_approval_and_deny() -> None:
    a = _actx()
    apply_action({"type": "require_approval", "reason": "over cap"}, a)
    apply_action({"type": "deny_plan", "reason": "no funds"}, a)
    assert a.decision.require_approval and a.decision.approval_reason == "over cap"
    assert a.decision.denied and a.decision.denial_reason == "no funds"


def test_action_strip_tool() -> None:
    a = _actx(toolset=[ToolRef(name="commitments"), ToolRef(name="fulfillment")])
    apply_action({"type": "strip_tool", "name": "commitments"}, a)
    assert [t.name for t in a.toolset] == ["fulfillment"]


def test_action_strip_task_type() -> None:
    plan = Plan(
        tasks=[
            Task(task_id="1", task_type="funds_commit", description="d", idempotency_key="k1"),
            Task(task_id="2", task_type="order_placement", description="d", idempotency_key="k2"),
        ],
        task_types=["funds_commit", "order_placement"],
    )
    a = _actx(plan=plan)
    apply_action({"type": "strip_task_type", "task_type": "funds_commit"}, a)
    assert [t.task_type for t in a.plan.tasks] == ["order_placement"]  # type: ignore[union-attr]
    assert a.plan.task_types == ["order_placement"]  # type: ignore[union-attr]


def test_action_clamp_and_cap_discount() -> None:
    a = _actx(flags={"committed_spend_usd": "500000", "discount_percent": "40"})
    apply_action({"type": "clamp", "flag": "committed_spend_usd", "max": 250000}, a)
    apply_action({"type": "cap_discount", "max_percent": 25}, a)
    assert a.flags["committed_spend_usd"] == "250000"
    assert a.flags["discount_percent"] == "25"


def test_action_unknown_raises() -> None:
    with pytest.raises(ValueError):
        apply_action({"type": "bogus"}, _actx())


# canary
def test_in_canary_bounds_and_determinism() -> None:
    assert in_canary("k", 0) is False
    assert in_canary("k", 100) is True
    assert in_canary("run-1", 50) == in_canary("run-1", 50)


def test_choose_ruleset() -> None:
    stable = LoadedRuleset(ruleset_ref="r", version=1)
    canary = LoadedRuleset(ruleset_ref="r", version=2, canary_pct=100)
    assert choose_ruleset(stable, canary, key="x").version == 2
    assert choose_ruleset(stable, None, key="x").version == 1
    cold = LoadedRuleset(ruleset_ref="r", version=2, canary_pct=0)
    assert choose_ruleset(stable, cold, key="x").version == 1


# engine
class FakePolicyRepo:
    def __init__(self, ruleset: LoadedRuleset) -> None:
        self._ruleset = ruleset

    async def load_ruleset(self, tenant: Any, ruleset_ref: str) -> LoadedRuleset:
        return self._ruleset


class FakeSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def emit(self, event: Any) -> None:
        self.events.append(event)


def _env(stage: Stage = Stage.ACTION_PLAN, **kw: Any) -> ContextEnvelope:
    return ContextEnvelope(
        run_id="r1",
        tenant_id="t1",
        stage=stage,
        policy_flags=kw.get("flags", {}),
        toolset=kw.get("toolset", []),
    )


async def test_engine_denies_over_cap_and_audits() -> None:
    ruleset = LoadedRuleset(
        ruleset_ref="seq-spend-policy",
        version=3,
        rules=[
            Rule(
                rule_id="deny-over-cap",
                ruleset_ref="seq-spend-policy",
                version=3,
                stage="action_plan",
                when={
                    "op": "gt",
                    "lhs": "flags.committed_spend_usd",
                    "rhs": "flags.spend_cap_usd",
                },
                actions=[{"type": "deny_plan", "reason": "commit exceeds cleared funds"}],
            )
        ],
    )
    sink = FakeSink()
    engine = NativePolicyEngine(
        ruleset_ref="seq-spend-policy", repo=FakePolicyRepo(ruleset), event_sink=sink
    )
    env = _env(flags={"committed_spend_usd": "500000", "spend_cap_usd": "250000"})
    decision, _ = await engine.post_plan(env, Intent(intent_id="i", goal="g"), Plan())
    assert decision.denied
    assert "exceeds" in decision.denial_reason
    assert decision.trace[0].rule_id == "deny-over-cap"
    assert decision.trace[0].version == 3
    assert sink.events[0].event_type == "policy_decision"


async def test_engine_clamps_and_writes_flags_back() -> None:
    ruleset = LoadedRuleset(
        ruleset_ref="r",
        version=1,
        rules=[
            Rule(
                rule_id="clamp",
                ruleset_ref="r",
                version=1,
                stage="action_plan",
                when={"op": "always"},
                actions=[
                    {"type": "clamp", "flag": "committed_spend_usd", "max": 250000},
                    {"type": "add_nudge", "text": "clamped to cleared funds"},
                ],
            )
        ],
    )
    engine = NativePolicyEngine(ruleset_ref="r", repo=FakePolicyRepo(ruleset))
    env = _env(flags={"committed_spend_usd": "500000"})
    decision, _ = await engine.post_plan(env, Intent(intent_id="i", goal="g"), Plan())
    assert env.policy_flags["committed_spend_usd"] == "250000"  # written back onto the envelope
    assert "clamped to cleared funds" in decision.added_nudges


async def test_engine_strip_task_type_does_not_mutate_input_plan() -> None:
    ruleset = LoadedRuleset(
        ruleset_ref="r",
        version=1,
        rules=[
            Rule(
                rule_id="strip",
                ruleset_ref="r",
                version=1,
                stage="action_plan",
                when={"op": "always"},
                actions=[{"type": "strip_task_type", "task_type": "funds_commit"}],
            )
        ],
    )
    engine = NativePolicyEngine(ruleset_ref="r", repo=FakePolicyRepo(ruleset))
    plan = Plan(
        tasks=[Task(task_id="1", task_type="funds_commit", description="d", idempotency_key="k")],
        task_types=["funds_commit"],
    )
    _, mutated = await engine.post_plan(_env(), Intent(intent_id="i", goal="g"), plan)
    assert mutated.tasks == []
    assert len(plan.tasks) == 1  # original untouched (deep copy)


async def test_engine_callable_rule_first_class() -> None:
    engine = NativePolicyEngine(
        ruleset_ref="r",
        callable_rules=[
            CallableRule(
                rule_id="vip",
                when=lambda ectx: ectx.flags.get("vip") == "true",
                actions=[{"type": "add_nudge", "text": "vip path"}],
            )
        ],
    )
    decision = await engine.pre_prompt(_env(flags={"vip": "true"}), Intent(intent_id="i", goal="g"))
    assert "vip path" in decision.added_nudges
    assert decision.trace[0].rule_id == "vip"


async def test_engine_stage_filtering() -> None:
    ruleset = LoadedRuleset(
        ruleset_ref="r",
        version=1,
        rules=[
            Rule(
                rule_id="publish-only",
                ruleset_ref="r",
                version=1,
                stage="publish",
                when={"op": "always"},
                actions=[{"type": "add_nudge", "text": "pub"}],
            )
        ],
    )
    engine = NativePolicyEngine(ruleset_ref="r", repo=FakePolicyRepo(ruleset))
    decision = await engine.pre_prompt(
        _env(stage=Stage.ACTION_PLAN), Intent(intent_id="i", goal="g")
    )
    assert decision.added_nudges == []  # rule targets the publish stage only
