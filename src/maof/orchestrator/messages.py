"""Task-message construction, signing-publish, and reconstruction.

Shared by the publish stage (workflow mode) and the queue dispatcher (coordination
mode a) so the on-the-wire shape and the signing/idempotency discipline are
identical regardless of which path emits the task.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from maof.types import Envelope, L2Context, Stage, TenantContext, ToolRef, utcnow

if TYPE_CHECKING:

    from maof.runs.artifacts import ArtifactStore
    from maof.runs.idempotency import IdempotencyGuard
    from maof.transport.base import Broker
    from maof.transport.signing import Signer
    from maof.types import StageContext, Task


def build_task_message(
    sc: StageContext,
    task: Task,
    *,
    ruleset_ref: str | None = None,
    ruleset_version: int | None = None,
    schema_id: str | None = None,
) -> dict[str, Any]:
    """Assemble the task message from the stage context + a planned task."""
    env = sc.envelope
    return {
        "envelope": {
            "run_id": sc.run_id,
            "tenant_id": sc.tenant.tenant_id,
            "intent_id": sc.intent.intent_id if sc.intent is not None else None,
            "stage": Stage.PUBLISH.value,
            "ruleset_ref": ruleset_ref,
            "ruleset_version": ruleset_version,
            "schema_id": schema_id,
            "mode": sc.mode,
            "region": sc.region,
            "actor": sc.principal.as_actor() if sc.principal is not None else None,
            "timestamp": utcnow(),
        },
        "task": task.model_dump(),
        "policy_flags": dict(env.policy_flags) if env is not None else {},
        "toolset": [t.model_dump() for t in (env.toolset if env is not None else [])],
        "data_pointers": {
            dp.alias: dp.uri for dp in (env.data_pointers if env is not None else [])
        },
        "semantic_model": dict(env.semantic_model) if env is not None else {},
        "timestamp": utcnow(),
    }


def serialize_message(message: dict[str, Any]) -> bytes:
    return json.dumps(message, sort_keys=True, separators=(",", ":")).encode("utf-8")


def parse_message(body: bytes) -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(body)
    return parsed


async def publish_task(
    broker: Broker,
    signer: Signer,
    *,
    queue: str,
    message: dict[str, Any],
    idempotency_key: str,
    correlation_id: str,
    schema_id: str | None = None,
    guard: IdempotencyGuard | None = None,
) -> None:
    """Sign + publish a task message. Wrapped in the idempotency guard so a replay
    (resumed run) dedupes instead of double-firing."""
    body = serialize_message(message)

    async def _publish() -> str:
        headers = dict(signer.headers(body))
        headers["idempotency_key"] = idempotency_key
        if schema_id:
            headers["schema_id"] = schema_id
        await broker.publish(
            queue, body, headers=headers, message_id=idempotency_key, correlation_id=correlation_id
        )
        return idempotency_key

    if guard is not None:
        # Namespace the publish-dedup key so it never collides with a consumer's
        # side-effect dedup on the same task idempotency_key (publish vs commit).
        await guard.once(f"publish:{idempotency_key}", _publish)
    else:
        await _publish()


def reconstruct_l2_context(
    message: dict[str, Any],
    *,
    idempotency_guard: IdempotencyGuard | None = None,
    artifacts: ArtifactStore | None = None,
    agents: object | None = None,
) -> tuple[dict[str, Any], L2Context]:
    """Rebuild ``(task_body, L2Context)`` from a consumed message (worker side)."""
    env_data = message["envelope"]
    ctx = L2Context(
        envelope=Envelope(**env_data),
        tenant=TenantContext(tenant_id=env_data["tenant_id"]),
        data_pointers=dict(message.get("data_pointers", {})),
        policy_flags=dict(message.get("policy_flags", {})),
        toolset=[ToolRef(**t) for t in message.get("toolset", [])],
        semantic_model=dict(message.get("semantic_model", {})),
        idempotency_guard=idempotency_guard,
        artifacts=artifacts,
        agents=agents,
    )
    return message["task"], ctx


__all__ = [
    "build_task_message",
    "serialize_message",
    "parse_message",
    "publish_task",
    "reconstruct_l2_context",
]
