"""Workflow definitions — the workflow-as-data surface.

A YAML/JSON DAG of steps using registry agents: depends_on edges, input templates
over prior step outputs and run context, per-step approval/coordination/version
pins. Validated structurally (unique ids, resolvable deps, acyclic) at parse time.
"""

from __future__ import annotations

import re
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, model_validator


class ApprovalSpec(BaseModel):
    required: bool = False
    roles: list[str] = Field(default_factory=list)  # role-bound parties
    parties: int = 1


class WorkflowStep(BaseModel):
    id: str
    kind: Literal["task", "gate"] = "task"
    task_type: str | None = None  # routed via the registry
    agent: str | None = None  # optional explicit registry agent id
    depends_on: list[str] = Field(default_factory=list)
    input: dict[str, Any] = Field(default_factory=dict)  # templates bind at dispatch
    coordination_mode: str | None = None  # queue | in_process (per step)
    approval: ApprovalSpec | None = None
    pins: dict[str, str] = Field(
        default_factory=dict
    )  # agent_version / model — enforced at routing


class WorkflowDefinition(BaseModel):
    name: str
    version: int
    description: str = ""
    steps: list[WorkflowStep] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_dag(self) -> WorkflowDefinition:
        ids = [s.id for s in self.steps]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate step ids in workflow")
        known = set(ids)
        for step in self.steps:
            unknown = [d for d in step.depends_on if d not in known]
            if unknown:
                raise ValueError(f"step {step.id!r} depends on unknown steps: {unknown}")
        # Kahn's algorithm: every step must be orderable or there is a cycle.
        remaining = {s.id: set(s.depends_on) for s in self.steps}
        while remaining:
            ready = [sid for sid, deps in remaining.items() if not deps]
            if not ready:
                raise ValueError(f"cycle detected among steps: {sorted(remaining)}")
            for sid in ready:
                del remaining[sid]
            for deps in remaining.values():
                deps.difference_update(ready)
        return self


def load_workflow_yaml(text: str) -> WorkflowDefinition:
    return WorkflowDefinition.model_validate(yaml.safe_load(text))


_TEMPLATE_RE = re.compile(r"\{\{\s*([\w.\-]+)\s*\}\}")


def _resolve_path(path: str, *, context: dict[str, Any], outputs: dict[str, Any]) -> Any:
    parts = path.split(".")
    if parts[0] == "context":
        current: Any = context
        parts = parts[1:]
    elif parts[0] == "steps":
        # steps.<id>.output.<path...>
        if len(parts) < 3 or parts[2] != "output":
            return None
        current = outputs.get(parts[1], {})
        parts = parts[3:]
    else:
        return None
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def bind_templates(value: Any, *, context: dict[str, Any], outputs: dict[str, Any]) -> Any:
    """Recursively bind ``{{ context.* }}`` / ``{{ steps.<id>.output.* }}`` templates."""
    if isinstance(value, str):
        full = _TEMPLATE_RE.fullmatch(value.strip())
        if full:  # whole-value template: preserve the resolved type
            return _resolve_path(full.group(1), context=context, outputs=outputs)
        return _TEMPLATE_RE.sub(
            lambda m: str(_resolve_path(m.group(1), context=context, outputs=outputs) or ""),
            value,
        )
    if isinstance(value, dict):
        return {k: bind_templates(v, context=context, outputs=outputs) for k, v in value.items()}
    if isinstance(value, list):
        return [bind_templates(v, context=context, outputs=outputs) for v in value]
    return value


__all__ = [
    "ApprovalSpec",
    "WorkflowStep",
    "WorkflowDefinition",
    "load_workflow_yaml",
    "bind_templates",
]
