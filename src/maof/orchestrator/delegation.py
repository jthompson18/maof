"""The delegation contract carried on every L1->subagent/L2 handoff.

This is the concrete defense against subagents acting on conflicting implicit
assumptions: each handoff states its objective, the exact output shape,
which tools/sources to use, explicit boundaries, an effort budget scaled to
complexity, and a *reference* into the run/trace store (never an inline dump).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from maof.types import EffortBudget


class DelegationContract(BaseModel):
    """An explicit handoff contract. Subagents return distilled results
    (~1–2k tokens) + artifact references, never raw transcripts."""

    objective: str  # what to accomplish — specific, not "research X"
    output_format: str  # exact shape expected back (schema id or description)
    tool_guidance: list[str] = Field(default_factory=list)  # which tools/sources and how
    boundaries: list[str] = Field(default_factory=list)  # explicit scope limits; what NOT to do
    effort_budget: EffortBudget = Field(default_factory=EffortBudget)
    parent_trace_ref: str = ""  # reference into the run/trace store, NOT an inline dump
    accepted_schema: str | None = None  # task schema id for queue-mode routing
    # Adopter's per-task choice of coordination mode. None -> Coordinator heuristic.
    coordination_mode: str | None = None  # "queue" (independent) | "in_process" (interdependent)
    task_type: str | None = None  # routing hint for queue-mode dispatch
    # Stable logical step identity for idempotency-key derivation. Replays of
    # the same logical delegation MUST carry the same step_ref; the loop stamps
    # "iteration:index"; the workflow executor uses the step id. Falls back to a
    # hash of the objective when unset.
    step_ref: str | None = None
    # Structured input for the task (workflow template bindings ride here,).
    payload: dict[str, Any] = Field(default_factory=dict)
    # Version pins enforced at routing + optional explicit agent id.
    pins: dict[str, str] = Field(default_factory=dict)
    agent: str | None = None


__all__ = ["DelegationContract"]
