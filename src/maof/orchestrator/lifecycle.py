"""Execution lifecycle: results, waits/joins, timers, events, cancellation.

The result path closes the loop on queue dispatch: workers publish signed
result envelopes; the :class:`ResultCollector` persists them and wakes waiting
runs. Runs park ``WAITING`` on a :class:`WakeCondition` (raised via
:class:`NeedsWait`) — a results join, a timer, or an external event — and resume
from checkpoint exactly where they left off.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field

from maof.orchestrator.messages import parse_message, serialize_message
from maof.types import TaskResult, utcnow

if TYPE_CHECKING:
    from maof.persistence.postgres import Database
    from maof.runs.idempotency import IdempotencyGuard
    from maof.transport.base import Broker
    from maof.transport.signing import Signer


class WakeCondition(BaseModel):
    """What a WAITING run is waiting for."""

    kind: Literal["results_ready", "timer", "external_event"]
    step_ref: str | None = None  # results_ready: which step's results to await
    expected: int = 1  # results_ready: how many results constitute the join
    at: str | None = None  # timer: RFC3339 wake time
    event_key: str | None = None  # external_event: opaque key


class NeedsWait(Exception):  # noqa: N818 - control-flow signal, not an error
    """Raised by a stage/executor step: checkpoint me, park WAITING, schedule the wake."""

    def __init__(self, condition: WakeCondition) -> None:
        super().__init__(f"waiting on {condition.kind}")
        self.condition = condition


class ResultEnvelope(BaseModel):
    """The result envelope a worker publishes after handling a task."""

    run_id: str
    step_ref: str
    task_id: str
    task_type: str = "generic_task"
    idempotency_key: str
    tenant_id: str
    intent_id: str | None = None
    result: TaskResult
    timestamp: str = Field(default_factory=utcnow)


def _parse_rfc3339(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# Result store (run_results)
class InMemoryResultStore:
    def __init__(self) -> None:
        self._by_key: dict[str, ResultEnvelope] = {}

    async def save(self, envelope: ResultEnvelope) -> bool:
        """Persist; return False when this result was already recorded (dedupe)."""
        if envelope.idempotency_key in self._by_key:
            return False
        self._by_key[envelope.idempotency_key] = envelope
        return True

    async def list(self, run_id: str, step_ref: str | None = None) -> list[ResultEnvelope]:
        return [
            e
            for e in self._by_key.values()
            if e.run_id == run_id and (step_ref is None or e.step_ref == step_ref)
        ]

    async def count(self, run_id: str, step_ref: str) -> int:
        return len(await self.list(run_id, step_ref))


class PostgresResultStore:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def save(self, envelope: ResultEnvelope) -> bool:
        inserted = await self._db.fetchval(
            """
            INSERT INTO run_results (run_id, step_ref, task_id, idempotency_key, tenant_id, result)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id
            """,
            envelope.run_id,
            envelope.step_ref,
            envelope.task_id,
            envelope.idempotency_key,
            envelope.tenant_id,
            envelope.result.model_dump(),
        )
        return inserted is not None

    async def list(self, run_id: str, step_ref: str | None = None) -> list[ResultEnvelope]:
        if step_ref is None:
            rows = await self._db.fetch(
                "SELECT * FROM run_results WHERE run_id = $1 ORDER BY id ASC", run_id
            )
        else:
            rows = await self._db.fetch(
                "SELECT * FROM run_results WHERE run_id = $1 AND step_ref = $2 ORDER BY id ASC",
                run_id,
                step_ref,
            )
        return [
            ResultEnvelope(
                run_id=r["run_id"],
                step_ref=r["step_ref"],
                task_id=r["task_id"],
                idempotency_key=r["idempotency_key"],
                tenant_id=r["tenant_id"],
                result=TaskResult.model_validate(r["result"]),
                timestamp=r["created_at"].isoformat(),
            )
            for r in rows
        ]

    async def count(self, run_id: str, step_ref: str) -> int:
        n: int = await self._db.fetchval(
            "SELECT count(*) FROM run_results WHERE run_id = $1 AND step_ref = $2",
            run_id,
            step_ref,
        )
        return n


# Waker (run_wakeups)
class InMemoryRunWaker:
    def __init__(self, results: InMemoryResultStore | None = None) -> None:
        self._results = results
        #: (run_id, condition, fired)
        self.scheduled: list[list[Any]] = []

    async def schedule(self, run_id: str, condition: WakeCondition) -> None:
        self.scheduled.append([run_id, condition, False])

    async def satisfy_results(self, run_id: str, step_ref: str) -> bool:
        for entry in self.scheduled:
            rid, condition, fired = entry
            if (
                not fired
                and rid == run_id
                and condition.kind == "results_ready"
                and condition.step_ref == step_ref
            ):
                if self._results is not None:
                    ready = await self._results.count(run_id, step_ref)
                    if ready < condition.expected:
                        return False
                entry[2] = True
                return True
        return False

    async def fire_event(self, event_key: str) -> list[str]:
        woken: list[str] = []
        for entry in self.scheduled:
            rid, condition, fired = entry
            if (
                not fired
                and condition.kind == "external_event"
                and condition.event_key == event_key
            ):
                entry[2] = True
                woken.append(rid)
        return woken

    async def due_timers(self) -> list[str]:
        now = datetime.now(UTC)
        woken: list[str] = []
        for entry in self.scheduled:
            rid, condition, fired = entry
            if (
                not fired
                and condition.kind == "timer"
                and condition.at is not None
                and _parse_rfc3339(condition.at) <= now
            ):
                entry[2] = True
                woken.append(rid)
        return woken

    async def due_joins(self) -> list[str]:
        """Claim results_ready joins whose results already landed — heals the race
        where a fast worker returns BEFORE the run parks and schedules the join."""
        woken: list[str] = []
        for entry in self.scheduled:
            rid, condition, fired = entry
            if fired or condition.kind != "results_ready" or condition.step_ref is None:
                continue
            if self._results is not None:
                ready = await self._results.count(rid, condition.step_ref)
                if ready < condition.expected:
                    continue
            entry[2] = True
            woken.append(rid)
        return woken


class PostgresRunWaker:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def schedule(self, run_id: str, condition: WakeCondition) -> None:
        await self._db.execute(
            """
            INSERT INTO run_wakeups (run_id, kind, step_ref, expected, wake_at, event_key)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            run_id,
            condition.kind,
            condition.step_ref,
            condition.expected,
            _parse_rfc3339(condition.at) if condition.at else None,
            condition.event_key,
        )

    async def satisfy_results(self, run_id: str, step_ref: str) -> bool:
        """Atomically claim the join wakeup when enough results have landed."""
        row = await self._db.fetchrow(
            """
            UPDATE run_wakeups
               SET status = 'fired', fired_at = now()
             WHERE id = (
                SELECT w.id FROM run_wakeups w
                 WHERE w.status = 'pending' AND w.kind = 'results_ready'
                   AND w.run_id = $1 AND w.step_ref = $2
                   AND (SELECT count(*) FROM run_results r
                         WHERE r.run_id = w.run_id AND r.step_ref = w.step_ref) >= w.expected
                 LIMIT 1 FOR UPDATE SKIP LOCKED)
            RETURNING run_id
            """,
            run_id,
            step_ref,
        )
        return row is not None

    async def fire_event(self, event_key: str) -> list[str]:
        rows = await self._db.fetch(
            """
            UPDATE run_wakeups SET status = 'fired', fired_at = now()
             WHERE status = 'pending' AND kind = 'external_event' AND event_key = $1
            RETURNING run_id
            """,
            event_key,
        )
        return [r["run_id"] for r in rows]

    async def due_timers(self) -> list[str]:
        rows = await self._db.fetch("""
            UPDATE run_wakeups SET status = 'fired', fired_at = now()
             WHERE status = 'pending' AND kind = 'timer' AND wake_at <= now()
            RETURNING run_id
            """)
        return [r["run_id"] for r in rows]

    async def due_joins(self) -> list[str]:
        """Atomically claim results_ready joins whose results already landed —
        heals the race where a fast worker's result arrives BEFORE the run parks
        and schedules the join (the collector's satisfy_results found nothing)."""
        rows = await self._db.fetch("""
            UPDATE run_wakeups
               SET status = 'fired', fired_at = now()
             WHERE id IN (
                SELECT w.id FROM run_wakeups w
                 WHERE w.status = 'pending' AND w.kind = 'results_ready'
                   AND (SELECT count(*) FROM run_results r
                         WHERE r.run_id = w.run_id AND r.step_ref = w.step_ref) >= w.expected
                 FOR UPDATE SKIP LOCKED)
            RETURNING run_id
            """)
        return [r["run_id"] for r in rows]


# Publish + collect
async def publish_result(
    broker: Broker,
    signer: Signer,
    *,
    queue: str,
    envelope: ResultEnvelope,
    guard: IdempotencyGuard | None = None,
) -> None:
    """Sign + publish a result envelope. ``message_id = result:<key>`` is
    deterministic so a re-executed (guarded) task republishes the same result id."""
    body = serialize_message(envelope.model_dump())
    message_id = f"result:{envelope.idempotency_key}"

    async def _publish() -> str:
        headers = dict(signer.headers(body))
        headers["idempotency_key"] = envelope.idempotency_key
        await broker.publish(
            queue, body, headers=headers, message_id=message_id, correlation_id=envelope.run_id
        )
        return message_id

    if guard is not None:
        await guard.once(f"publish-result:{envelope.idempotency_key}", _publish)
    else:
        await _publish()


#: Hook the collector runs before persisting a result (post_result governance).
ResultValidator = Callable[[ResultEnvelope], Awaitable[None]]


def make_post_result_validator(
    *,
    policy: Any | None = None,
    schema_registry: Any | None = None,
) -> ResultValidator:
    """Build the collector-side conformance gate: validate the output
    against ``<task_type>.result.v1`` (when registered) and run the policy
    ``post_result`` hook. Raising quarantines the result (broker retry → DLQ);
    a denied result is never persisted, so dependent steps cannot consume it."""
    from maof.errors import PolicyDenied
    from maof.types import Envelope, Stage, Task

    async def validate(envelope: ResultEnvelope) -> None:
        task_type = envelope.task_type
        if schema_registry is not None:
            schema_id = f"{task_type}.result.v1"
            if schema_registry.is_registered(schema_id):
                schema_registry.validate(schema_id, dict(envelope.result.output))
        if policy is not None:
            env = Envelope(
                run_id=envelope.run_id, tenant_id=envelope.tenant_id, stage=Stage.PUBLISH
            )
            task = Task(
                task_id=envelope.task_id,
                task_type=task_type,
                description="post_result validation",
                idempotency_key=envelope.idempotency_key,
                step_ref=envelope.step_ref,
            )
            decision = await policy.post_result(env, task, envelope.result)
            if decision.denied:
                raise PolicyDenied(decision.denial_reason)

    return validate


class ResultCollector:
    """Consumes the results queue: verify signature → dedupe-persist → validate →
    satisfy results_ready joins → resume the woken run."""

    def __init__(
        self,
        broker: Broker,
        signer: Signer,
        *,
        results: Any,
        waker: Any,
        resume: Callable[[str], Awaitable[None]],
        queue: str = "results",
        validator: ResultValidator | None = None,
        require_signature: bool = True,
    ) -> None:
        self._broker = broker
        self._signer = signer
        self._results = results
        self._waker = waker
        self._resume = resume
        self._queue = queue
        self._validator = validator
        self._require_signature = require_signature

    async def drain(self) -> None:
        """Consume currently queued results (test/embedded; real brokers block in consume)."""
        await self._broker.consume(self._queue, prefetch=10, handler=self.handle)

    async def handle(self, msg: Any) -> None:
        if self._require_signature:
            self._signer.verify(msg.body, msg.headers)
        envelope = ResultEnvelope.model_validate(parse_message(msg.body))
        if self._validator is not None:
            await self._validator(envelope)  # may raise -> retry/DLQ per broker policy
        fresh = await self._results.save(envelope)
        if not fresh:
            return  # duplicate delivery; the join was (or will be) satisfied already
        if await self._waker.satisfy_results(envelope.run_id, envelope.step_ref):
            await self._resume(envelope.run_id)


class WakerPoller:
    """Co-located poll loop: resumes runs whose timers are due AND sweeps
    results_ready joins that were satisfied before their wakeup was scheduled
    (a fast-worker race the collector cannot see). Call :meth:`tick` from a
    scheduler, or :meth:`run_forever` as a service task."""

    def __init__(
        self,
        waker: Any,
        resume: Callable[[str], Awaitable[None]],
        *,
        interval_s: float = 1.0,
    ) -> None:
        self._waker = waker
        self._resume = resume
        self._interval = interval_s

    async def tick(self) -> list[str]:
        due: list[str] = list(await self._waker.due_timers())
        due_joins = getattr(self._waker, "due_joins", None)
        if due_joins is not None:
            due.extend(run_id for run_id in await due_joins() if run_id not in due)
        for run_id in due:
            await self._resume(run_id)
        return due

    async def run_forever(self) -> None:  # pragma: no cover - service loop
        import asyncio

        while True:
            await self.tick()
            await asyncio.sleep(self._interval)


__all__ = [
    "WakeCondition",
    "NeedsWait",
    "ResultEnvelope",
    "InMemoryResultStore",
    "PostgresResultStore",
    "InMemoryRunWaker",
    "PostgresRunWaker",
    "publish_result",
    "ResultCollector",
    "ResultValidator",
    "WakerPoller",
]
