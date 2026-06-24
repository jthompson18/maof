"""Default ``publish`` stage — coordination mode (a) for the workflow plan.

Each planned task is given the deterministic idempotency key, validated
against its registered schema, signed, and published to its L2 queue — wrapped in
the IdempotencyGuard so a resumed run never double-publishes (and downstream a
consumer dedupes the same key).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from maof.orchestrator.messages import build_task_message, publish_task
from maof.runs.idempotency import make_idempotency_key

if TYPE_CHECKING:
    from maof.runs.idempotency import IdempotencyGuard
    from maof.schemas.registry import SchemaRegistry
    from maof.transport.base import Broker
    from maof.transport.signing import Signer
    from maof.types import StageContext


class PublishStage:
    name = "publish"

    def __init__(
        self,
        broker: Broker,
        signer: Signer,
        *,
        schema_registry: SchemaRegistry | None = None,
        idempotency_guard: IdempotencyGuard | None = None,
        queue_resolver: Callable[[str], str] | None = None,
        ruleset_ref: str | None = None,
    ) -> None:
        self._broker = broker
        self._signer = signer
        self._schema_registry = schema_registry
        self._guard = idempotency_guard
        self._queue_resolver = queue_resolver or (lambda task_type: f"tasks.{task_type}")
        self._ruleset_ref = ruleset_ref

    async def execute(self, sc: StageContext) -> StageContext:
        if sc.plan is None:
            return sc
        published: list[str] = []
        for task in sc.plan.tasks:
            body = task.model_dump(exclude={"idempotency_key"})
            key = make_idempotency_key(sc.run_id, task.task_id, task.task_type, body)
            task = task.model_copy(update={"idempotency_key": key})

            candidate = f"{task.task_type}.v1"
            schema_id: str | None = None
            if self._schema_registry is not None and self._schema_registry.is_registered(candidate):
                self._schema_registry.validate(candidate, task.model_dump())
                schema_id = candidate

            message = build_task_message(
                sc, task, ruleset_ref=self._ruleset_ref, schema_id=schema_id
            )
            correlation_id = sc.intent.intent_id if sc.intent is not None else sc.run_id
            await publish_task(
                self._broker,
                self._signer,
                queue=self._queue_resolver(task.task_type),
                message=message,
                idempotency_key=key,
                correlation_id=correlation_id,
                schema_id=schema_id,
                guard=self._guard,
            )
            published.append(key)
        sc.extras["published"] = published
        return sc


__all__ = ["PublishStage"]
