"""Workflow executor: topological execution on the result path.

Each invocation is idempotent and resumable: completed steps are recomputed from
the result store (queue steps) and the checkpointed context (in-process steps);
ready steps dispatch through the Coordinator; unmet joins park the run via
``NeedsWait`` and the collector resumes it as results land. ``step_ref = step.id``
seeds the idempotency keys, so a replayed step dedupes end to end.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from maof.errors import MAOFError
from maof.orchestrator.delegation import DelegationContract
from maof.orchestrator.lifecycle import NeedsWait, WakeCondition
from maof.workflows.definition import WorkflowDefinition, WorkflowStep, bind_templates

if TYPE_CHECKING:
    from maof.orchestrator.coordinator import Coordinator
    from maof.types import StageContext


class WorkflowExecutor:
    def __init__(
        self,
        coordinator: Coordinator,
        *,
        results: Any,
        default_mode: str = "queue",
        approval_gate: Any | None = None,
    ) -> None:
        self._coordinator = coordinator
        self._results = results
        self._default_mode = default_mode
        self._approval_gate = approval_gate

    async def run(self, definition: WorkflowDefinition, sc: StageContext) -> StageContext:
        state = sc.extras.setdefault("workflow", {"dispatched": [], "approved": [], "outputs": {}})
        outputs: dict[str, dict[str, Any]] = dict(state["outputs"])
        completed: set[str] = set(outputs)

        # refresh queue-step completions from the result store
        for step in definition.steps:
            if step.id in completed:
                continue
            envelopes = await self._results.list(sc.run_id, step.id)
            if envelopes:
                outputs[step.id] = dict(envelopes[0].result.output)
                state["outputs"][step.id] = outputs[step.id]
                completed.add(step.id)

        gates_waited: list[str] = state.setdefault("gates_waited", [])
        gate_to_wait: WorkflowStep | None = None
        progressed = True
        while progressed:
            progressed = False
            for step in definition.steps:
                if step.id in completed:
                    continue
                if any(dep not in completed for dep in step.depends_on):
                    continue
                if step.kind == "gate":
                    if step.id in gates_waited:
                        # The waker fired and the run resumed: the gate is passed.
                        outputs[step.id] = {"gate": "passed"}
                        state["outputs"][step.id] = outputs[step.id]
                        completed.add(step.id)
                        progressed = True
                    elif gate_to_wait is None:
                        gate_to_wait = step  # park after dispatching other ready steps
                    continue
                mode = step.coordination_mode or self._default_mode

                if (
                    step.approval is not None
                    and step.approval.required
                    and step.id not in state["approved"]
                ):
                    if self._approval_gate is not None:
                        await self._approval_gate.wait(
                            sc,
                            reason=f"workflow {definition.name} step {step.id}",
                            required_roles=(step.approval.roles or None),
                            parties=step.approval.parties,
                        )
                    state["approved"].append(step.id)

                delegation = self._delegation(definition, step, sc, outputs)
                if mode == "in_process":
                    sub = await self._coordinator.dispatch(delegation, sc)
                    outputs[step.id] = {"summary": sub.summary, "artifacts": sub.artifacts}
                    state["outputs"][step.id] = outputs[step.id]
                    completed.add(step.id)
                    progressed = True
                elif step.id not in state["dispatched"]:
                    await self._coordinator.dispatch(delegation, sc)
                    state["dispatched"].append(step.id)

        if gate_to_wait is not None:
            gates_waited.append(gate_to_wait.id)
            raise NeedsWait(self._gate_condition(gate_to_wait, sc, outputs))

        pending = [s for s in definition.steps if s.id not in completed]
        if pending:
            waiting = next((s for s in pending if s.id in state["dispatched"]), None)
            if waiting is None:
                raise MAOFError(
                    f"workflow {definition.name!r} stalled: pending steps "
                    f"{[s.id for s in pending]} are neither ready nor dispatched"
                )
            # Park on one outstanding join; every wake re-evaluates the whole DAG,
            # so results that landed meanwhile are picked up from the store.
            raise NeedsWait(WakeCondition(kind="results_ready", step_ref=waiting.id, expected=1))

        sc.extras["workflow_completed"] = sorted(completed)
        return sc

    def _gate_condition(
        self, step: WorkflowStep, sc: StageContext, outputs: dict[str, dict[str, Any]]
    ) -> WakeCondition:
        """Map a gate step's (template-bound) input onto a wake condition."""
        spec = bind_templates(step.input, context=self._context(sc), outputs=outputs)
        if spec.get("wait") == "external_event":
            key = spec.get("event_key") or f"{sc.run_id}:{step.id}"
            return WakeCondition(kind="external_event", event_key=str(key))
        at = spec.get("at")
        if at is None:
            delay = float(spec.get("delay_s", 0))
            wake = datetime.now(UTC) + timedelta(seconds=delay)
            at = wake.strftime("%Y-%m-%dT%H:%M:%SZ")
        return WakeCondition(kind="timer", at=str(at))

    def _delegation(
        self,
        definition: WorkflowDefinition,
        step: WorkflowStep,
        sc: StageContext,
        outputs: dict[str, dict[str, Any]],
    ) -> DelegationContract:
        payload = bind_templates(step.input, context=self._context(sc), outputs=outputs)
        return DelegationContract(
            objective=f"workflow {definition.name} v{definition.version} step {step.id}",
            output_format=f"{step.task_type}.result.v1" if step.task_type else "text",
            boundaries=[f"execute only step {step.id} of workflow {definition.name}"],
            coordination_mode=step.coordination_mode or self._default_mode,
            task_type=step.task_type,
            step_ref=step.id,
            payload=dict(payload),
            pins=dict(step.pins),
            agent=step.agent,
        )

    @staticmethod
    def _context(sc: StageContext) -> dict[str, Any]:
        flags = dict(sc.envelope.policy_flags) if sc.envelope is not None else {}
        extra = dict(sc.extras.get("workflow_context", {}))
        return {"goal": sc.goal, **flags, **extra}


class WorkflowStage:
    """Pipeline adapter: run a (loaded, trust-verified) workflow as a stage."""

    name = "workflow"

    def __init__(
        self,
        executor: WorkflowExecutor,
        definition: WorkflowDefinition,
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        self._executor = executor
        self._definition = definition
        self._context = dict(context) if context else {}

    async def execute(self, sc: StageContext) -> StageContext:
        if self._context:
            sc.extras.setdefault("workflow_context", {}).update(self._context)
        return await self._executor.run(self._definition, sc)


__all__ = ["WorkflowExecutor", "WorkflowStage"]
