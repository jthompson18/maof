"""Default L1 driver.

Drives the workflow stage pipeline, checkpointing after each stage and skipping
already-completed stages on resume (resume-from-failure AND resume-from-waiting,
). A stage may raise :class:`NeedsWait` to park the run ``WAITING`` on
a wake condition; cancellation is cooperative and checked at every stage boundary.
Domain reasoning (planner, prompts, persona) is injected via the stages — the
driver owns only the governed loop, durability, and audit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.errors import ApprovalRequired, PolicyDenied
from maof.observability.events import AuditEvent
from maof.observability.otel import NoOpTracer
from maof.orchestrator.lifecycle import NeedsWait
from maof.types import OrchestrationResult, RunStatus, Stage, StageContext, TraceEntry

if TYPE_CHECKING:
    from maof.cost.accounting import CostLedger
    from maof.observability.events import EventSink
    from maof.orchestrator.pipeline import Pipeline
    from maof.runs.checkpoint import Checkpointer
    from maof.runs.store import RunStore
    from maof.types import TenantContext

_STAGE_VALUES = {s.value for s in Stage}


class DefaultL1:
    def __init__(
        self,
        pipeline: Pipeline,
        *,
        run_store: RunStore,
        checkpointer: Checkpointer | None = None,
        event_sink: EventSink | None = None,
        cost_ledger: CostLedger | None = None,
        waker: object | None = None,
        tracer: object | None = None,
        trajectory: object | None = None,
    ) -> None:
        self._pipeline = pipeline
        self._run_store = run_store
        self._checkpointer = checkpointer
        self._event_sink = event_sink
        self._cost_ledger = cost_ledger
        self._waker = waker
        self._tracer = tracer if tracer is not None else NoOpTracer()
        self._trajectory = trajectory

    async def run(
        self, goal: str, tenant: TenantContext, *, principal: object | None = None
    ) -> OrchestrationResult:
        run_id = await self._run_store.create(tenant, goal)
        sc = StageContext(
            run_id=run_id,
            tenant=tenant,
            goal=goal,
            run_store=self._run_store,
            cost_ledger=self._cost_ledger,
            principal=principal,
        )
        await self._emit(sc, "run_started", {"goal": goal})
        return await self._drive(sc)

    async def resume_run(self, run_id: str) -> OrchestrationResult:
        if self._checkpointer is None:
            raise RuntimeError("cannot resume without a checkpointer")
        sc = await self._checkpointer.resume(run_id)
        if sc is None:
            raise KeyError(f"no checkpoint for run {run_id!r}")
        sc.run_store = self._run_store
        sc.cost_ledger = self._cost_ledger
        await self._emit(sc, "run_resumed", {})
        return await self._drive(sc)

    async def _cancelled(self, run_id: str) -> bool:
        state = await self._run_store.get_state(run_id)
        return state.cancel_requested or state.status is RunStatus.CANCELLED

    async def _drive(self, sc: StageContext) -> OrchestrationResult:
        completed = set(sc.extras.get("completed_stages", []))
        await self._set_status(sc.run_id, RunStatus.RUNNING)
        try:
            for stage in self._pipeline.stages:
                if stage.name in completed:
                    continue
                if await self._cancelled(sc.run_id):
                    # Cooperative cancellation: stop before the next stage.
                    await self._set_status(sc.run_id, RunStatus.CANCELLED)
                    await self._emit(sc, "run_cancelled", {})
                    return self._result(sc, "cancelled", summary="run cancelled by operator")
                if stage.name in _STAGE_VALUES:
                    sc.stage = Stage(stage.name)
                with self._tracer.span(f"stage.{stage.name}", run_id=sc.run_id):  # type: ignore[attr-defined]
                    sc = await stage.execute(sc)
                if self._trajectory is not None:
                    self._trajectory.record("stage", stage.name, run_id=sc.run_id)  # type: ignore[attr-defined]
                completed.add(stage.name)
                sc.extras["completed_stages"] = sorted(completed)
                if self._checkpointer is not None:
                    await self._checkpointer.save(sc.run_id, stage.name, sc)
                await self._emit(sc, "run_checkpointed", {"step": stage.name})
                await self._run_store.append_trace(
                    sc.run_id, TraceEntry(run_id=sc.run_id, seq=0, kind=stage.name, step=stage.name)
                )
        except NeedsWait as wait:
            # Park the run: checkpoint as-is (the waiting stage is NOT in
            # completed_stages, so resume re-enters it), schedule the wake, return.
            if self._checkpointer is not None:
                await self._checkpointer.save(sc.run_id, f"waiting:{wait.condition.kind}", sc)
            if self._waker is not None:
                await self._waker.schedule(sc.run_id, wait.condition)  # type: ignore[attr-defined]
            await self._set_status(sc.run_id, RunStatus.WAITING)
            await self._emit(sc, "run_waiting", {"condition": wait.condition.kind})
            return self._result(sc, "waiting", summary=f"waiting on {wait.condition.kind}")
        except PolicyDenied as exc:
            await self._set_status(sc.run_id, RunStatus.FAILED)
            await self._emit(sc, "run_failed", {"reason": str(exc), "kind": "policy_denied"})
            return self._result(sc, "denied", summary=str(exc))
        except ApprovalRequired as exc:
            # A human denied (or the gate timed out on) a required approval — a
            # governed outcome, not a crash: clean result + FAILED state.
            await self._set_status(sc.run_id, RunStatus.FAILED)
            await self._emit(sc, "run_failed", {"reason": str(exc), "kind": "approval_denied"})
            return self._result(sc, "approval_denied", summary=str(exc))
        await self._set_status(sc.run_id, RunStatus.COMPLETED)
        await self._emit(sc, "run_completed", {})
        return self._result(sc, "completed", artifacts=list(sc.extras.get("published", [])))

    def _result(
        self,
        sc: StageContext,
        status: str,
        *,
        summary: str = "",
        artifacts: list[str] | None = None,
    ) -> OrchestrationResult:
        return OrchestrationResult(
            run_id=sc.run_id,
            status=status,
            intent_id=sc.intent.intent_id if sc.intent is not None else None,
            plan=sc.plan,
            summary=summary,
            artifacts=artifacts or [],
        )

    async def _emit(self, sc: StageContext, event_type: str, details: dict[str, object]) -> None:
        if self._event_sink is None:
            return
        actor = sc.principal.as_actor() if sc.principal is not None else None
        await self._event_sink.emit(
            AuditEvent(
                tenant_id=sc.tenant.tenant_id,
                intent_id=sc.intent.intent_id if sc.intent is not None else None,
                event_type=event_type,
                envelope={"run_id": sc.run_id},
                details=dict(details),
                actor=actor,
            )
        )

    async def _set_status(self, run_id: str, status: RunStatus) -> None:
        setter = getattr(self._run_store, "set_state", None)
        if setter is not None:
            await setter(run_id, status=status)


__all__ = ["DefaultL1"]
