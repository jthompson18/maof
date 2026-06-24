"""L2 worker pool.

The stateless consumer half of coordination mode (a): consume -> verify signature
-> validate schema -> dispatch to the registered L2 agent -> **publish the
result envelope** -> ack (failures bubble to the broker's retry/DLQ). Side-effecting
agents honor the idempotency guard (passed in the L2Context) so redelivery/replay
is safe. Tasks belonging to cancelled runs are skipped before any side
effect fires.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.observability.events import AuditEvent
from maof.orchestrator.messages import parse_message, reconstruct_l2_context

if TYPE_CHECKING:
    from maof.agents.registry_runtime import AgentRegistry
    from maof.observability.events import EventSink
    from maof.runs.artifacts import ArtifactStore
    from maof.runs.idempotency import IdempotencyGuard
    from maof.runs.store import RunStore
    from maof.schemas.registry import SchemaRegistry
    from maof.transport.base import Broker
    from maof.transport.signing import Signer
    from maof.types import IncomingMessage


class WorkerPool:
    def __init__(
        self,
        broker: Broker,
        signer: Signer,
        registry: AgentRegistry,
        *,
        schema_registry: SchemaRegistry | None = None,
        idempotency_guard: IdempotencyGuard | None = None,
        artifacts: ArtifactStore | None = None,
        require_signature: bool = True,
        allowed_task_types: list[str] | None = None,
        event_sink: EventSink | None = None,
        run_store: RunStore | None = None,
        result_queue: str | None = "results",
        agent_clients: object | None = None,
    ) -> None:
        self._broker = broker
        self._signer = signer
        self._registry = registry
        self._schema_registry = schema_registry
        self._guard = idempotency_guard
        self._artifacts = artifacts
        self._require_signature = require_signature
        self._allowed = set(allowed_task_types) if allowed_task_types is not None else None
        self._event_sink = event_sink
        self._run_store = run_store
        self._result_queue = result_queue
        self._agent_clients = agent_clients

    async def consume(self, queue: str, *, prefetch: int = 10) -> None:
        await self._broker.consume(queue, prefetch=prefetch, handler=self._handle)

    async def _run_cancelled(self, run_id: str) -> bool:
        if self._run_store is None:
            return False
        try:
            state = await self._run_store.get_state(run_id)
        except KeyError:
            return False
        from maof.types import RunStatus as _RS

        return state.cancel_requested or state.status is _RS.CANCELLED

    async def _handle(self, msg: IncomingMessage) -> None:
        if self._require_signature:
            self._signer.verify(msg.body, msg.headers)  # raises -> broker retry/DLQ
        message = parse_message(msg.body)
        task_body, ctx = reconstruct_l2_context(
            message,
            idempotency_guard=self._guard,
            artifacts=self._artifacts,
            agents=self._agent_clients,
        )
        run_id = str(message["envelope"].get("run_id", ""))
        intent_id = message["envelope"].get("intent_id")
        task_type = str(task_body.get("task_type", ""))

        if await self._run_cancelled(run_id):
            # Cooperative cancellation: skip BEFORE any side effect fires.
            await self._emit(
                "task_skipped",
                ctx.tenant.tenant_id,
                intent_id,
                {"task_type": task_type, "reason": "run_cancelled", "run_id": run_id},
            )
            return

        if self._allowed is not None and task_type not in self._allowed:
            raise ValueError(f"task type {task_type!r} not allowed on this worker")

        schema_id = message["envelope"].get("schema_id") or f"{task_type}.v1"
        if self._schema_registry is not None and self._schema_registry.is_registered(schema_id):
            self._schema_registry.validate(schema_id, task_body)

        agent = self._registry.agent_for_task_type(task_type)
        if agent is None:
            raise ValueError(f"no L2 agent registered for task type {task_type!r}")

        # Actor-level tool RBAC: strip tools whose scope neither the actor
        # nor the tenant holds BEFORE the agent sees the toolset.
        held: set[str] = set()
        actor = ctx.envelope.actor or {}
        held |= set(actor.get("scopes", []) or [])
        raw_scopes = ctx.tenant.attributes.get("scopes", "")
        held |= {scope.strip() for scope in raw_scopes.split(",") if scope.strip()}
        ctx.toolset = [t for t in ctx.toolset if t.rbac is None or t.rbac in held]

        result = await agent.handle(task_body, ctx)
        # Output conformance: a result violating <task_type>.result.v1 never
        # leaves the worker (raise -> retry/DLQ; no envelope published).
        result_schema = f"{task_type}.result.v1"
        if self._schema_registry is not None and self._schema_registry.is_registered(result_schema):
            self._schema_registry.validate(result_schema, dict(result.output))
        step_ref = str(task_body.get("step_ref") or task_body.get("task_id", ""))
        result = result.model_copy(update={"run_id": run_id, "step_ref": step_ref})

        reply_to = task_body.get("reply_to") or self._result_queue
        if reply_to:
            from maof.orchestrator.lifecycle import ResultEnvelope, publish_result

            envelope = ResultEnvelope(
                run_id=run_id,
                step_ref=step_ref,
                task_id=str(task_body.get("task_id", "")),
                task_type=task_type,
                idempotency_key=str(task_body.get("idempotency_key", "")),
                tenant_id=ctx.tenant.tenant_id,
                intent_id=intent_id,
                result=result,
            )
            await publish_result(
                self._broker,
                self._signer,
                queue=str(reply_to),
                envelope=envelope,
                guard=self._guard,
            )

        await self._emit(
            "task_completed",
            ctx.tenant.tenant_id,
            intent_id,
            {"status": result.status, "task_type": task_type},
        )

    async def _emit(
        self, event_type: str, tenant_id: str, intent_id: str | None, details: dict[str, object]
    ) -> None:
        if self._event_sink is None:
            return
        await self._event_sink.emit(
            AuditEvent(
                tenant_id=tenant_id,
                intent_id=intent_id,
                event_type=event_type,
                envelope={},
                details=dict(details),
            )
        )


__all__ = ["WorkerPool"]
