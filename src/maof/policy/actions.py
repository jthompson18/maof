"""Policy action handlers.

Full set: ``set_flag``, ``add_nudge``, ``require_approval``, ``deny_plan``,
``strip_tool``, ``strip_task_type``, and the generalized ``clamp``/``mutate``
(``cap_discount`` is a special case). Actions mutate a working ActionContext.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from maof.policy.engine import RuleDecision
    from maof.types import Plan, ToolRef


@dataclass
class ActionContext:
    decision: RuleDecision
    flags: dict[str, str]
    plan: Plan | None = None
    toolset: list[ToolRef] = field(default_factory=list)


def _num(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)


def _fmt(value: float) -> str:
    return str(int(value)) if value.is_integer() else str(value)


def apply_action(action: dict[str, Any], ctx: ActionContext) -> None:
    action_type = action.get("type", "")
    if action_type == "set_flag":
        ctx.flags[action["key"]] = str(action["value"])
    elif action_type == "add_nudge":
        if action["text"] not in ctx.decision.added_nudges:
            ctx.decision.added_nudges.append(action["text"])
    elif action_type == "require_approval":
        ctx.decision.require_approval = True
        ctx.decision.approval_reason = action.get("reason", "")
        if action.get("roles"):
            ctx.decision.approval_roles = list(action["roles"])
        if action.get("parties"):
            ctx.decision.approval_parties = int(action["parties"])
    elif action_type == "deny_plan":
        ctx.decision.denied = True
        ctx.decision.denial_reason = action.get("reason", "")
    elif action_type == "strip_tool":
        name = action["name"]
        ctx.toolset[:] = [tool for tool in ctx.toolset if tool.name != name]
    elif action_type == "strip_task_type":
        if ctx.plan is not None:
            task_type = action["task_type"]
            ctx.plan.tasks = [t for t in ctx.plan.tasks if t.task_type != task_type]
            ctx.plan.task_types = [tt for tt in ctx.plan.task_types if tt != task_type]
    elif action_type in ("clamp", "cap_discount"):
        key = action.get("flag") or ("discount_percent" if action_type == "cap_discount" else None)
        if key is None:
            raise ValueError("clamp action requires a 'flag'")
        upper = action.get("max", action.get("max_percent"))
        if upper is None and "max_flag" in action:
            upper = _num(ctx.flags.get(action["max_flag"], 0))
        lower = action.get("min")
        if lower is None and "min_flag" in action:
            lower = _num(ctx.flags.get(action["min_flag"], 0))
        value = _num(ctx.flags.get(key, 0))
        if upper is not None:
            value = min(value, float(upper))
        if lower is not None:
            value = max(value, float(lower))
        ctx.flags[key] = _fmt(value)
    else:
        raise ValueError(f"unknown policy action: {action_type!r}")


def apply_actions(actions: list[dict[str, Any]], ctx: ActionContext) -> None:
    for action in actions:
        apply_action(action, ctx)


__all__ = ["ActionContext", "apply_action", "apply_actions"]
