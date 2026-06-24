"""Promote a successful run into a draft WorkflowDefinition.

Pins the *shape* of a run that worked — its steps, dependencies, and coordination
modes — so future runs execute it under *new* goals (DESIGN.md §14.7: determinism
is a data artifact, not a model property). Not verbatim replay; the model still
runs, only the shape is fixed.

Reads already-persisted data (the result store gives per-step ``step_ref`` +
``task_type``), gated on a COMPLETED run, and emits an *unsigned* draft. The lossy
parts — input templates, parallelism, version pins — are left for the operator to
refine before the existing ``submit -> approve(sign) -> run`` pipeline. Nothing
here signs or executes anything.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

import yaml

from maof.authz import SCOPE_WORKFLOW_AUTHOR, require_scopes
from maof.types import RunStatus
from maof.workflows.definition import WorkflowDefinition, WorkflowStep

if TYPE_CHECKING:
    from maof.identity import Principal
    from maof.orchestrator.lifecycle import ResultEnvelope
    from maof.runs.store import RunStore


class PromotionError(ValueError):
    """A run cannot be promoted: missing, not COMPLETED, or no recorded task steps."""


class ResultLister(Protocol):
    """The slice of the result store promotion needs: per-run result envelopes."""

    async def list(self, run_id: str, step_ref: str | None = None) -> list[ResultEnvelope]: ...


async def promote_run(
    run_id: str,
    *,
    run_store: RunStore,
    result_store: ResultLister,
    name: str,
    version: int = 1,
    principal: Principal | None = None,
) -> WorkflowDefinition:
    """Derive a draft :class:`WorkflowDefinition` from a completed run.

    Steps come from the result store (``step_ref`` + ``task_type``), deduped and
    chained linearly for the human to relax. Raises :class:`PromotionError` if the
    run is absent, not COMPLETED, or has no results. A supplied ``principal`` must
    hold ``workflow:author`` (a trusted in-process caller may pass ``None``).
    """
    if principal is not None:
        require_scopes(principal, {SCOPE_WORKFLOW_AUTHOR})
    try:
        state = await run_store.get_state(run_id)
    except KeyError as exc:
        raise PromotionError(f"run not found: {run_id}") from exc
    if state.status != RunStatus.COMPLETED:
        raise PromotionError(
            f"can only promote COMPLETED runs; run {run_id} is {state.status.value}"
        )

    envelopes = await result_store.list(run_id)
    if not envelopes:
        raise PromotionError(f"run {run_id} has no recorded task results to promote")

    version_by_step = await _resolved_versions(run_store, run_id)
    steps: list[WorkflowStep] = []
    seen: set[str] = set()
    prev_id: str | None = None
    for env in envelopes:
        step_id = env.step_ref or env.task_id
        if step_id in seen:
            continue  # dedupe retries / fan-in onto the same logical step
        seen.add(step_id)
        agent_version = version_by_step.get(step_id)
        steps.append(
            WorkflowStep(
                id=step_id,
                task_type=env.task_type,
                depends_on=[prev_id] if prev_id is not None else [],
                coordination_mode="queue",
                pins={"agent_version": agent_version} if agent_version else {},
            )
        )
        prev_id = step_id

    return WorkflowDefinition(
        name=name,
        version=version,
        description=f"promoted from run {run_id}: {state.goal}",
        steps=steps,
    )


async def _resolved_versions(run_store: RunStore, run_id: str) -> dict[str, str]:
    """Map ``step_ref -> resolved agent_version`` from the dispatch trace.

    The coordinator records the resolved agent identity (identifier-only) at
    dispatch; promotion reads it to pin ``agent_version`` so the workflow re-routes
    to the same approved agent version. Steps with no dispatch record stay unpinned.
    """
    versions: dict[str, str] = {}
    for entry in await run_store.read_trace(run_id):
        if entry.kind != "delegation_dispatched" or not entry.step:
            continue
        version = entry.data.get("agent_version")
        if isinstance(version, str) and version:
            versions[entry.step] = version
    return versions


def to_yaml(definition: WorkflowDefinition) -> str:
    """Serialize a draft definition to YAML for review and ``maof workflow submit``."""
    data = definition.model_dump(mode="json", exclude_none=True)
    return yaml.safe_dump(data, sort_keys=False, default_flow_style=False)


__all__ = ["PromotionError", "ResultLister", "promote_run", "to_yaml"]
