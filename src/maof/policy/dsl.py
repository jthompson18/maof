"""Condition DSL evaluator.

Operators: ``always``, ``flag_eq{key,value}``, ``gt[lhs,rhs]``, ``lt``, ``eq``,
``exists``, ``and``, ``or``, ``not``. Path operands resolve against ``flags.<key>``,
``intent.<path>``, ``toolset.<name>``, ``plan.task_types``, ``semantic.<path>``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from maof.types import Intent, Plan, ToolRef

_PATH_ROOTS = {"flags", "intent", "semantic", "plan", "toolset"}


@dataclass
class EvalContext:
    flags: dict[str, str]
    intent: Intent | None = None
    plan: Plan | None = None
    semantic: dict[str, Any] = field(default_factory=dict)
    toolset: list[ToolRef] = field(default_factory=list)


def _dig(data: Any, dotted: str) -> Any:
    current = data
    for part in dotted.split("."):
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def resolve_path(path: str, ctx: EvalContext) -> Any:
    head, _, rest = path.partition(".")
    if head == "flags":
        return ctx.flags.get(rest)
    if head == "semantic":
        return _dig(ctx.semantic, rest) if rest else ctx.semantic
    if head == "intent":
        if ctx.intent is None:
            return None
        return _dig(ctx.intent.model_dump(), rest) if rest else ctx.intent
    if head == "plan":
        if ctx.plan is None:
            return None
        if rest == "task_types":
            return ctx.plan.task_types
        return _dig(ctx.plan.model_dump(), rest) if rest else ctx.plan
    if head == "toolset":
        return rest in {tool.name for tool in ctx.toolset}
    return None


def _operand(value: Any, ctx: EvalContext) -> Any:
    if isinstance(value, str) and value.partition(".")[0] in _PATH_ROOTS:
        return resolve_path(value, ctx)
    return value


def _num(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    return float(value)


def evaluate(condition: dict[str, Any], ctx: EvalContext) -> bool:
    op = condition.get("op", "always")
    if op == "always":
        return True
    if op == "flag_eq":
        return ctx.flags.get(condition["key"]) == str(condition["value"])
    if op == "eq":
        return bool(_operand(condition["lhs"], ctx) == _operand(condition["rhs"], ctx))
    if op == "gt":
        return _num(_operand(condition["lhs"], ctx)) > _num(_operand(condition["rhs"], ctx))
    if op == "lt":
        return _num(_operand(condition["lhs"], ctx)) < _num(_operand(condition["rhs"], ctx))
    if op == "exists":
        value = resolve_path(condition["lhs"], ctx)
        return value not in (None, False, [], "", {})
    if op == "and":
        return all(evaluate(clause, ctx) for clause in condition.get("clauses", []))
    if op == "or":
        return any(evaluate(clause, ctx) for clause in condition.get("clauses", []))
    if op == "not":
        return not evaluate(condition["clause"], ctx)
    raise ValueError(f"unknown condition op: {op!r}")


__all__ = ["EvalContext", "evaluate", "resolve_path"]
