"""The smallest governed MAOF run: publish one signed task, a worker runs your
agent under governance, you read back the signed result. Fully offline and
in-process — no database, no broker, no extras. Just ``pip install maof``.

    python -m examples.quickstart.main

For the full picture (signed workflows, policy clamps, multi-party approvals,
source-of-truth agents, kill-resume exactly-once), see ``examples/po_demo``.
"""

from __future__ import annotations

import asyncio

from maof.agents.registry_runtime import AgentRegistry
from maof.orchestrator.lifecycle import InMemoryResultStore, ResultEnvelope
from maof.orchestrator.messages import build_task_message, parse_message, publish_task
from maof.runs.idempotency import InMemoryIdempotencyGuard, make_idempotency_key
from maof.transport.fake import InMemoryBroker
from maof.transport.signing import Signer
from maof.types import IncomingMessage, QueueSpec, StageContext, Task, TenantContext
from maof.workers.pool import WorkerPool

from .agent import HelloAgent

TASKS = "tasks"
RESULTS = "results"


async def run() -> dict[str, object]:
    # 1. Wiring — all in-memory. In production these become RabbitMQ/Postgres/etc.
    broker = InMemoryBroker()
    signer = Signer({"default": "quickstart-secret"})  # any consistent key, in-process
    guard = InMemoryIdempotencyGuard()
    registry = AgentRegistry()
    registry.register_agent(HelloAgent())  # the only domain code you inject
    await broker.ensure_topology([QueueSpec(name=TASKS), QueueSpec(name=RESULTS)])

    # 2. Build one governed task. The idempotency key is what makes replay safe:
    #    a resumed or redelivered run dedupes on it instead of double-firing.
    run_id, step_ref, task_type = "run-1", "hello:0", "hello"
    payload = {"name": "world"}
    key = make_idempotency_key(run_id, step_ref, task_type, payload)
    task = Task(
        task_id="t1",
        task_type=task_type,
        description="greet someone",
        idempotency_key=key,
        step_ref=step_ref,
        reply_to=RESULTS,
        payload=payload,
    )
    sc = StageContext(run_id=run_id, tenant=TenantContext(tenant_id="demo"), goal="say hello")
    message = build_task_message(sc, task)

    # 3. Publish it — signed (HMAC) and idempotency-guarded.
    await publish_task(
        broker,
        signer,
        queue=TASKS,
        message=message,
        idempotency_key=key,
        correlation_id=run_id,
        guard=guard,
    )

    # 4. Run a worker. It verifies the signature, strips unauthorized tools, runs
    #    your agent, and publishes a signed result envelope. (The in-memory broker
    #    drains the queue and returns; real brokers consume in a loop.)
    worker = WorkerPool(broker, signer, registry, idempotency_guard=guard, result_queue=RESULTS)
    await worker.consume(TASKS)

    # 5. Collect the signed result envelope off the results queue.
    results = InMemoryResultStore()

    async def collect(msg: IncomingMessage) -> None:
        signer.verify(msg.body, msg.headers)  # results are signed too
        await results.save(ResultEnvelope.model_validate(parse_message(msg.body)))

    await broker.consume(RESULTS, prefetch=10, handler=collect)
    envelopes = await results.list(run_id)
    return dict(envelopes[0].result.output)


def main() -> None:
    output = asyncio.run(run())
    print(f"governed run complete -> {output}")


if __name__ == "__main__":
    main()
