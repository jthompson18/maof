"""Coordinator: picks between the two coordination modes.

The deciding axis: do the subtasks' actions carry decisions that depend on each
other? Yes -> mode (b) in-process context-shared subagent (shared trace). No ->
mode (a) governed async queue dispatch to L2 workers. The adopter expresses the
choice via ``DelegationContract.coordination_mode``; the Coordinator routes.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from maof.orchestrator.messages import build_task_message, publish_task
from maof.runs.idempotency import make_idempotency_key
from maof.types import SubResult, Task, TraceEntry

if TYPE_CHECKING:
    from maof.models.base import LLMProvider
    from maof.orchestrator.delegation import DelegationContract
    from maof.registry.models import AgentManifest
    from maof.runs.artifacts import ArtifactStore
    from maof.runs.idempotency import IdempotencyGuard
    from maof.transport.base import Broker
    from maof.transport.signing import Signer
    from maof.types import StageContext

_SUBAGENT_SYSTEM = (
    "You are a subagent. Honor the delegation contract: pursue only the stated "
    "objective, respect the boundaries, and return the requested output format."
)


@runtime_checkable
class Coordinator(Protocol):
    async def dispatch(self, delegation: DelegationContract, sc: StageContext) -> SubResult: ...


class QueueDispatcher:
    """Mode (a): publish an INDEPENDENT task to an L2 worker queue (governed, async)."""

    def __init__(
        self,
        broker: Broker,
        signer: Signer,
        *,
        queue_resolver: Callable[[str], str] | None = None,
        idempotency_guard: IdempotencyGuard | None = None,
        registry_loader: object | None = None,
    ) -> None:
        self._broker = broker
        self._signer = signer
        self._queue_resolver = queue_resolver or (lambda task_type: f"tasks.{task_type}")
        self._guard = idempotency_guard
        self._registry_loader = registry_loader  # registry-driven routing

    async def _resolve_queue(
        self, delegation: DelegationContract, sc: StageContext
    ) -> tuple[str, AgentManifest | None]:
        """Registry-driven routing: prefer the approved manifest's queue
        binding (honoring workflow version pins + canary cohorts); fall back to the
        ``tasks.<task_type>`` naming convention. Returns ``(queue, manifest)`` — the
        resolved manifest (``None`` on the fallback path) lets the dispatch trace
        record the agent identity so a run can later be promoted to a pinned workflow."""
        task_type = delegation.task_type or _schema_root(delegation.accepted_schema)
        fallback = self._queue_resolver(task_type)
        if self._registry_loader is None:
            return fallback, None
        candidates = await self._registry_loader.agents_for_task_type(  # type: ignore[attr-defined]
            task_type, tenant=sc.tenant
        )
        if delegation.agent:
            candidates = [m for m in candidates if m.id == delegation.agent]
        pin = delegation.pins.get("agent_version")
        if pin:
            pinned = [m for m in candidates if m.version == pin]
            if candidates and not pinned:
                from maof.errors import RegistryTrustError

                raise RegistryTrustError(
                    f"version pin {pin!r} for {task_type!r} matches no approved agent "
                    f"(available: {[m.version for m in candidates]})"
                )
            candidates = pinned
        for manifest in candidates:
            if manifest.queue:
                if manifest.canary_pct > 0:
                    from maof.registry.search import in_canary_cohort

                    if not in_canary_cohort(sc.run_id, manifest.canary_pct):
                        continue  # out of this entry's cohort -> next candidate/fallback
                return str(manifest.queue), manifest
        return fallback, None

    async def dispatch(self, delegation: DelegationContract, sc: StageContext) -> SubResult:
        task_type = delegation.task_type or _schema_root(delegation.accepted_schema)
        # Step identity must be stable across resume/replay — derived from
        # the delegation itself, never from mutable run state like dialogue length.
        ref = (
            delegation.step_ref
            or hashlib.sha256(delegation.objective.encode("utf-8")).hexdigest()[:16]
        )
        step_id = f"{sc.run_id}:{ref}:{task_type}"
        body = {
            "task_type": task_type,
            "description": delegation.objective,
            "priority": 5,
            "payload": delegation.payload,
        }
        key = make_idempotency_key(sc.run_id, step_id, task_type, body)
        task = Task(
            task_id=step_id,
            task_type=task_type,
            description=delegation.objective,
            intent_id=sc.intent.intent_id if sc.intent is not None else None,
            idempotency_key=key,
            step_ref=delegation.step_ref or ref,
            payload=dict(delegation.payload),
        )
        message = build_task_message(sc, task, schema_id=delegation.accepted_schema)
        queue, manifest = await self._resolve_queue(delegation, sc)
        await publish_task(
            self._broker,
            self._signer,
            queue=queue,
            message=message,
            idempotency_key=key,
            correlation_id=task.intent_id or sc.run_id,
            schema_id=delegation.accepted_schema,
            guard=self._guard,
        )
        if sc.run_store is not None:
            data: dict[str, Any] = {
                "mode": "queue",
                "task_type": task_type,
                "objective": delegation.objective,
            }
            if manifest is not None:
                # Identifier-only — the resolved routing so a successful run can be
                # promoted to a workflow with `pins.agent_version`. Never the
                # sensitive endpoint/metadata (see promote.py).
                data["agent_id"] = manifest.id
                data["agent_version"] = manifest.version
            await sc.run_store.append_trace(
                sc.run_id,
                TraceEntry(
                    run_id=sc.run_id,
                    seq=0,
                    kind="delegation_dispatched",
                    step=task.step_ref,
                    data=data,
                ),
            )
        return SubResult(
            delegation_objective=delegation.objective,
            summary=f"dispatched {task_type} to queue {queue}",
            status="dispatched",
        )


class InProcessSubagent:
    """Mode (b): run a subagent IN-PROCESS, sharing the run trace, returning a
    distilled summary + references to any large artifacts."""

    def __init__(
        self,
        llm: LLMProvider,
        *,
        artifacts: ArtifactStore | None = None,
        summary_chars: int = 2000,
    ) -> None:
        self._llm = llm
        self._artifacts = artifacts
        self._summary_chars = summary_chars

    async def dispatch(self, delegation: DelegationContract, sc: StageContext) -> SubResult:
        result = await self._llm.generate(
            self._build_prompt(delegation, sc), system=_SUBAGENT_SYSTEM, run_id=sc.run_id
        )
        artifacts: list[str] = []
        summary = result
        if self._artifacts is not None and len(result) > self._summary_chars:
            ref = await self._artifacts.put(
                sc.run_id, "subresult.txt", result.encode("utf-8"), "text/plain"
            )
            artifacts.append(ref)
            summary = result[: self._summary_chars]
        if sc.run_store is not None:
            data: dict[str, Any] = {"mode": "in_process", "objective": delegation.objective}
            model = getattr(self._llm, "model", None)
            if isinstance(model, str):
                data["model"] = model  # identifier-only — no keys/endpoint
            await sc.run_store.append_trace(
                sc.run_id,
                TraceEntry(
                    run_id=sc.run_id,
                    seq=0,
                    kind="subresult_received",
                    step=delegation.step_ref,
                    data=data,
                ),
            )
        return SubResult(
            delegation_objective=delegation.objective,
            summary=summary,
            artifacts=artifacts,
            tokens=len(result) // 4,
        )

    @staticmethod
    def _build_prompt(delegation: DelegationContract, sc: StageContext) -> str:
        lines = [
            f"Objective: {delegation.objective}",
            f"Output format: {delegation.output_format}",
            f"Boundaries: {'; '.join(delegation.boundaries) or 'none'}",
            f"Tool guidance: {'; '.join(delegation.tool_guidance) or 'none'}",
            f"Parent trace ref: {delegation.parent_trace_ref or sc.run_id}",
        ]
        return "\n".join(lines)


class DefaultCoordinator:
    def __init__(
        self,
        *,
        queue: QueueDispatcher | None = None,
        in_process: InProcessSubagent | None = None,
        default_mode: str = "in_process",
    ) -> None:
        self._queue = queue
        self._in_process = in_process
        self._default_mode = default_mode

    async def dispatch(self, delegation: DelegationContract, sc: StageContext) -> SubResult:
        mode = delegation.coordination_mode or self._default_mode
        if mode == "queue":
            if self._queue is None:
                raise ValueError("queue coordination requested but no QueueDispatcher configured")
            return await self._queue.dispatch(delegation, sc)
        if self._in_process is None:
            raise ValueError(
                "in_process coordination requested but no InProcessSubagent configured"
            )
        return await self._in_process.dispatch(delegation, sc)


def _schema_root(schema_id: str | None) -> str:
    return schema_id.split(".")[0] if schema_id else "generic_task"


__all__ = ["Coordinator", "QueueDispatcher", "InProcessSubagent", "DefaultCoordinator"]
