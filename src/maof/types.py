"""Canonical MAOF data contracts.

These are the locked shapes everything else depends on. Wire/serializable
contracts are Pydantic v2 models; the two carriers that hold live runtime
handles (:class:`StageContext`, :class:`L2Context`) are dataclasses so they can
reference un-serializable objects (stores, guards) without Pydantic validation.

Cross-module handle types (RunStore, CostLedger, ArtifactStore, ...) are imported
only under ``TYPE_CHECKING`` to keep ``types`` import-cycle free at runtime: those
modules import *from* here, never the other way around at import time.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from maof.context.jit import ReferenceResolver
    from maof.cost.accounting import CostLedger
    from maof.observability.events import AuditEvent
    from maof.runs.artifacts import ArtifactStore
    from maof.runs.idempotency import IdempotencyGuard
    from maof.runs.store import RunStore


def utcnow() -> str:
    """RFC3339 UTC timestamp (the audit/event time format)."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


# Enumerations
class Stage(StrEnum):
    """Canonical stage identifiers. These exact strings ride the wire."""

    CHAT = "chat"
    INTENT_SYNTHESIS = "intent_synthesis"
    ACTION_PLAN = "action_plan"
    APPROVAL = "approval"
    PUBLISH = "publish"


class RunStatus(StrEnum):
    """Durable-run state machine states."""

    PENDING = "pending"
    RUNNING = "running"
    AWAITING_APPROVAL = "awaiting_approval"
    CHECKPOINTED = "checkpointed"
    WAITING = "waiting"  # parked on a wake condition
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"  # terminal, cooperative
    RESUMING = "resuming"


class CoordinationMode(StrEnum):
    """The two coordination modes."""

    QUEUE = "queue"  # mode (a): governed async dispatch of INDEPENDENT tasks
    IN_PROCESS = "in_process"  # mode (b): context-shared subagents for INTERDEPENDENT decisions


class OrchestrationMode(StrEnum):
    """L1 execution modes."""

    WORKFLOW = "workflow"
    AUTONOMOUS = "autonomous"


# Leaf value objects
class TenantContext(BaseModel):
    """Tenant identity threaded through every interface. Single-tenant
    deployments still carry it, defaulted."""

    tenant_id: str
    multi_tenant: bool = True
    attributes: dict[str, str] = Field(default_factory=dict)


class ToolRef(BaseModel):
    """A tool the agent may use, with its RBAC scope (toolset item)."""

    name: str
    version: str = "v1"
    scope: str = "tenant"
    rbac: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class DataPointer(BaseModel):
    """A lightweight reference to large data — load just-in-time, never inline."""

    alias: str
    uri: str
    note: str = ""


class MemorySnippet(BaseModel):
    """A memory item. ``score`` is the similarity on recall; ``embedding``
    carries the vector on upsert (None on recall results)."""

    kind: str
    content: str
    prov: str = ""
    score: float = 0.0
    embedding: list[float] | None = None


class EffortBudget(BaseModel):
    """Bounds a delegation/loop, scaled to complexity."""

    max_tool_calls: int = 10
    max_subagents: int = 3
    max_tokens: int = 100_000
    deadline_s: float | None = None


# Envelopes & tasks
class Envelope(BaseModel):
    """The task-message envelope published to L2 workers."""

    run_id: str
    tenant_id: str
    intent_id: str | None = None
    stage: Stage
    ruleset_ref: str | None = None
    ruleset_version: int | None = None
    schema_id: str | None = None
    semantic_model_versions: dict[str, str] = Field(default_factory=dict)
    tool_versions: dict[str, str] = Field(default_factory=dict)
    mode: str = "sandbox"
    region: str = "us-east-1"
    # The Principal on whose behalf the run executes — wire shape of
    # Principal.as_actor(); worker-side RBAC and audit attribute per actor.
    actor: dict[str, Any] | None = None
    timestamp: str = Field(default_factory=utcnow)


class ContextEnvelope(BaseModel):
    """The context envelope built by L1 and delivered to prompts."""

    version: str = "1"
    run_id: str
    tenant_id: str
    intent_id: str | None = None
    stage: Stage
    timestamp: str = Field(default_factory=utcnow)
    goal: str = ""
    policy_flags: dict[str, str] = Field(default_factory=dict)
    semantic_model: dict[str, Any] = Field(default_factory=dict)
    toolset: list[ToolRef] = Field(default_factory=list)
    data_pointers: list[DataPointer] = Field(default_factory=list)
    memories: list[MemorySnippet] = Field(default_factory=list)
    constraints_ref: str | None = None
    dialogue: list[str] = Field(default_factory=list)
    extras: dict[str, Any] = Field(default_factory=dict)


class Task(BaseModel):
    """A unit of work routed to an L2 agent (task block).

    ``idempotency_key`` is the one field MAOF mandates beyond the reference —
    ``sha256(run_id, step_id, task_type, canonical(body))``.
    """

    task_id: str
    task_type: str
    priority: int = 5
    description: str
    intent_id: str | None = None
    policy_notes: list[str] = Field(default_factory=list)
    idempotency_key: str
    # Stable logical step identity within the run; seeds the idempotency key
    # and correlates the result back to the waiting step.
    step_ref: str | None = None
    reply_to: str | None = None  # results queue override (default: the worker's)
    payload: dict[str, Any] = Field(default_factory=dict)  # structured input (workflow bindings)


class Intent(BaseModel):
    """The synthesized intent (assigned at the intent_synthesis stage)."""

    intent_id: str
    goal: str
    summary: str = ""
    task_types: list[str] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)


class Plan(BaseModel):
    """The action plan produced at the action_plan stage."""

    tasks: list[Task] = Field(default_factory=list)
    task_types: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class TaskResult(BaseModel):
    """What an L2 agent returns from ``handle``. Large outputs are
    artifact references, not inline blobs."""

    status: str
    task_id: str
    output: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[str] = Field(default_factory=list)
    error: str | None = None
    idempotency_key: str | None = None
    run_id: str | None = None
    step_ref: str | None = None


# Policy
class DecisionTrace(BaseModel):
    """An audited policy decision: which rule matched and what it did."""

    rule_id: str
    ruleset_ref: str
    version: int
    stage: str
    actions: list[str] = Field(default_factory=list)
    matched: bool = True
    detail: str = ""


class Rule(BaseModel):
    """A policy rule. ``when`` is the condition DSL; ``actions`` the
    action specs. Lower ``priority`` evaluates first."""

    rule_id: str
    ruleset_ref: str
    version: int
    priority: int = 100
    stage: str = "*"
    enabled: bool = True
    when: dict[str, Any] = Field(default_factory=dict)
    actions: list[dict[str, Any]] = Field(default_factory=list)
    description: str = ""


class LoadedRuleset(BaseModel):
    """A resolved ruleset for a tenant, with version + canary selection."""

    ruleset_ref: str
    version: int
    rules: list[Rule] = Field(default_factory=list)
    canary_pct: float = 0.0


# Transport
class QueueSpec(BaseModel):
    """A queue + its DLQ + retry schedule (the consumers.yaml model)."""

    name: str
    dlq_name: str | None = None
    dlq_ttl: str | None = None
    dlq_max_len: int | None = None
    retry_steps: list[str] = Field(default_factory=list)


@dataclass
class IncomingMessage:
    """A message delivered to a consumer. Carries raw bytes + transport
    headers; ack/nack are mediated by the broker/worker, not this object."""

    body: bytes
    headers: dict[str, str]
    message_id: str
    queue: str
    correlation_id: str | None = None
    redelivered: bool = False
    attempt: int = 1


# Durable execution & memory
class RunState(BaseModel):
    """The persisted run state machine."""

    run_id: str
    tenant_id: str
    goal: str
    status: RunStatus = RunStatus.PENDING
    current_step: str | None = None
    cancel_requested: bool = False
    updated_at: str = Field(default_factory=utcnow)


class TraceEntry(BaseModel):
    """An append-only, shareable trace entry. The parent slice handed to
    a delegation is a window over these."""

    run_id: str
    seq: int
    kind: str
    step: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    ts: str = Field(default_factory=utcnow)


class Note(BaseModel):
    """A durable structured note the agent re-reads (agentic memory)."""

    run_id: str
    content: str
    id: str | None = None
    tags: list[str] = Field(default_factory=list)
    created_at: str = Field(default_factory=utcnow)


class CompactedContext(BaseModel):
    """The result of compaction: a high-fidelity digest that preserves
    decisions/plans/open issues and drops redundant tool output."""

    digest: str
    preserved: list[str] = Field(default_factory=list)
    token_count: int = 0
    dropped_tokens: int = 0


# Cost & evaluation
class CostSummary(BaseModel):
    """Per-run token + cost totals."""

    run_id: str
    in_tokens: int = 0
    out_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0
    by_model: dict[str, int] = Field(default_factory=dict)


class CostProjection(BaseModel):
    """Projected fan-out/cost fed to the worth-it gate before spawning."""

    projected_subagents: int = 0
    projected_tokens: int = 0
    projected_usd: float = 0.0
    fan_out: int = 0


class Rubric(BaseModel):
    """An LLM-as-judge rubric. Defaults to the four standard criteria."""

    name: str
    criteria: list[str] = Field(
        default_factory=lambda: [
            "factual_accuracy",
            "completeness",
            "citation_quality",
            "tool_efficiency",
        ]
    )
    weights: dict[str, float] = Field(default_factory=dict)
    pass_threshold: float = 0.7
    end_state: bool = True


class JudgeResult(BaseModel):
    """A scored judgement: 0.0–1.0 per criterion + overall + pass/fail."""

    scores: dict[str, float] = Field(default_factory=dict)
    overall: float = 0.0
    passed: bool = False
    rationale: str = ""


class EvalCase(BaseModel):
    """One evaluation case."""

    id: str
    input: str
    reference: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class EvalDataset(BaseModel):
    """A small dataset of eval cases (~20 is enough to start)."""

    name: str
    cases: list[EvalCase] = Field(default_factory=list)


class EvalReport(BaseModel):
    """The outcome of running a dataset, used by the CI gate."""

    dataset: str
    results: list[JudgeResult] = Field(default_factory=list)
    passed: int = 0
    total: int = 0
    pass_rate: float = 0.0


# Orchestration results
class OrchestrationResult(BaseModel):
    """The final result of an L1 run."""

    run_id: str
    status: str = "completed"
    intent_id: str | None = None
    plan: Plan | None = None
    summary: str = ""
    artifacts: list[str] = Field(default_factory=list)


class SubResult(BaseModel):
    """A subagent/L2 result — a distilled ~1–2k-token summary plus references to
    any large artifacts, never a raw transcript."""

    delegation_objective: str
    summary: str
    artifacts: list[str] = Field(default_factory=list)
    status: str = "ok"
    tokens: int = 0


# Runtime-handle carriers (dataclasses — they hold live, un-serializable objects)
@dataclass
class StageContext:
    """The central object passed through the pipeline / orchestrator loop.

    Both orchestration modes produce/consume this and share ``run_id``, so they
    share durability, policy, and observability. Holds live handles to the run/
    trace store and cost ledger (un-serializable — hence a dataclass)."""

    run_id: str
    tenant: TenantContext
    goal: str
    stage: Stage = Stage.CHAT
    dialogue: list[str] = field(default_factory=list)
    intent: Intent | None = None
    envelope: ContextEnvelope | None = None
    plan: Plan | None = None
    policy_decisions: list[DecisionTrace] = field(default_factory=list)
    mode: str = "sandbox"
    region: str = "us-east-1"
    audit: list[AuditEvent] = field(default_factory=list)
    extras: dict[str, Any] = field(default_factory=dict)
    # durable-execution + cost handles, wired by the L1 driver:
    run_store: RunStore | None = None
    trace_ref: str | None = None
    cost_ledger: CostLedger | None = None
    # registry-resolved agent consultation; AgentClientFactory when wired
    agents: Any | None = None
    # the actor on whose behalf this run executes; maof.identity.Principal
    principal: Any | None = None


@dataclass
class L2Context:
    """What an L2 worker reconstructs from a task message. Gives the agent
    its envelope plus the handles needed for replay-safe side effects and JIT
    retrieval."""

    envelope: Envelope
    tenant: TenantContext
    data_pointers: dict[str, str] = field(default_factory=dict)
    policy_flags: dict[str, str] = field(default_factory=dict)
    toolset: list[ToolRef] = field(default_factory=list)
    semantic_model: dict[str, Any] = field(default_factory=dict)
    idempotency_guard: IdempotencyGuard | None = None
    artifacts: ArtifactStore | None = None
    resolver: ReferenceResolver | None = None
    # registry-resolved agent consultation; AgentClientFactory when wired
    agents: Any | None = None
    extras: dict[str, Any] = field(default_factory=dict)


# Eval harness callable
@runtime_checkable
class RunHarness(Protocol):
    """Runs one eval case through the system under test to produce an output the
    Judge then scores."""

    async def run(self, case: EvalCase) -> str: ...


__all__ = [
    "Stage",
    "RunStatus",
    "CoordinationMode",
    "OrchestrationMode",
    "TenantContext",
    "ToolRef",
    "DataPointer",
    "MemorySnippet",
    "EffortBudget",
    "Envelope",
    "ContextEnvelope",
    "Task",
    "Intent",
    "Plan",
    "TaskResult",
    "DecisionTrace",
    "Rule",
    "LoadedRuleset",
    "QueueSpec",
    "IncomingMessage",
    "RunState",
    "TraceEntry",
    "Note",
    "CompactedContext",
    "CostSummary",
    "CostProjection",
    "Rubric",
    "JudgeResult",
    "EvalCase",
    "EvalDataset",
    "EvalReport",
    "OrchestrationResult",
    "SubResult",
    "StageContext",
    "L2Context",
    "RunHarness",
]
