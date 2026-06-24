"""Broker-connected reference orchestrator.

    python -m examples.po_demo.main_distributed

Unlike ``main.py`` (fully in-memory), this wires the SAME reference scenario to
the CONFIGURED broker and Postgres-backed durability: registry + workflow store,
run store/checkpoints/idempotency, the result path (``run_results`` +
``run_wakeups``). In the compose stack the orchestrator publishes signed tasks
to RabbitMQ vendor queues, the worker containers consume + commit them, and this
process runs the result collector + waker poller that resume the waiting run.
With ``EMBEDDED_L2=true`` (embedded mode) everything — including the vendor
workers — runs inline in this process over the in-memory broker.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from typing import Any

from maof.agents.client import (
    AgentClientFactory,
    ContextSourceCache,
    attach_registry_context_sources,
    attach_registry_resolvers,
)
from maof.agents.registry_runtime import AgentRegistry
from maof.approval.service import ApprovalGate
from maof.config import Settings
from maof.context.builder import ContextBuilder
from maof.context.jit import DefaultReferenceResolver
from maof.context.sources.builtins import PolicyFlagsSource
from maof.memory.pgvector import PgVectorStore
from maof.models.base import HashingEmbeddingProvider
from maof.observability.sinks.postgres_sink import PostgresEventSink
from maof.observability.sinks.stdout_sink import StdoutEventSink
from maof.orchestrator.coordinator import DefaultCoordinator, QueueDispatcher
from maof.orchestrator.l1 import DefaultL1
from maof.orchestrator.lifecycle import (
    PostgresResultStore,
    PostgresRunWaker,
    ResultCollector,
    WakerPoller,
    make_post_result_validator,
)
from maof.orchestrator.pipeline import Pipeline
from maof.orchestrator.stages import ActionPlanStage, ApprovalStage, ChatStage, IntentStage
from maof.persistence.postgres import (
    Database,
    PostgresApprovalRepo,
    PostgresCheckpointRepo,
    PostgresRegistryRepo,
    run_migrations,
)
from maof.policy.engine import NativePolicyEngine
from maof.registry.loader import RegistryLoader
from maof.registry.models import AgentManifest
from maof.registry.search import RegistrySearch
from maof.registry.store import RegistryStore
from maof.runs.checkpoint import PostgresCheckpointer
from maof.runs.idempotency import PostgresIdempotencyGuard
from maof.runs.store import PostgresRunStore
from maof.schemas.registry import SchemaRegistry
from maof.transport.consumers import load_consumers
from maof.transport.factory import build_broker
from maof.transport.signing import Signer
from maof.types import TenantContext
from maof.workers.pool import WorkerPool
from maof.workflows.definition import load_workflow_yaml
from maof.workflows.executor import WorkflowExecutor, WorkflowStage
from maof.workflows.store import PostgresWorkflowRepo, WorkflowStore

from .agents import CommitmentsAgent, FulfillmentAgent
from .scenario import (
    BUYER_LEAD,
    COMMITMENTS_MANIFEST,
    EXPEDITER_MANIFEST,
    FULFILLMENT_MANIFEST,
    GOAL,
    RESULT_SCHEMAS,
    RULES_DIR,
    TERMINAL,
    THIS_DIR,
    _certifier,
    _RulesetRepo,
    load_ruleset,
    register_schemas,
    scenario_planner,
)
from .truth import CATALOG_MANIFEST, DATASTORE_MANIFEST, CatalogClient, DatastoreClient

TASK_QUEUES = ["suppliers.commitments.v1", "suppliers.fulfillment.v1"]


def _flag(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() not in ("0", "false", "no", "")


class _FanoutSink:
    """Audit events go to compose logs (stdout) AND the audit_events table."""

    def __init__(self, sinks: list[Any]) -> None:
        self._sinks = sinks

    async def emit(self, event: Any) -> None:
        for sink in self._sinks:
            await sink.emit(event)


async def run_distributed() -> dict[str, Any]:
    settings = Settings()
    committed = int(os.getenv("DEMO_COMMITTED_SPEND_USD", "250000"))
    funds = int(os.getenv("DEMO_FUNDS_RECEIVED_USD", "250000"))
    cap = int(os.getenv("DEMO_SPEND_CAP_USD", "300000"))
    tenant = TenantContext(tenant_id=os.getenv("DEMO_TENANT_ID", "shared-buyer-001"))

    db = Database(settings.db_url, min_size=settings.db_pool_min, max_size=settings.db_pool_max)
    await db.connect()
    broker = build_broker(settings)
    connect = getattr(broker, "connect", None)
    if connect is not None:
        await connect()
    try:
        await run_migrations(db, embed_dimension=settings.embed_dimension)
        # Topology comes from the SAME consumers.yaml the workers use — declaring
        # the same queues twice with different arguments (DLX, ttl, retry) is a
        # RabbitMQ PRECONDITION_FAILED region kill.
        await broker.ensure_topology(load_consumers(str(THIS_DIR / "consumers.yaml")).queue_specs())

        signer = Signer(
            {settings.msg_signing_key_id: settings.msg_signing_secret or "demo-secret"},
            settings.msg_signing_key_id,
        )
        guard = PostgresIdempotencyGuard(db)
        sink = _FanoutSink([PostgresEventSink(db), StdoutEventSink()])
        schemas = SchemaRegistry()
        register_schemas(schemas)
        for schema_id, schema in RESULT_SCHEMAS.items():
            schemas.register(schema_id, schema)

        # trust registry on Postgres: source-of-truth + certified vendors
        repo = PostgresRegistryRepo(db)
        search = RegistrySearch(
            PgVectorStore(db), HashingEmbeddingProvider(dimension=settings.embed_dimension)
        )
        trust = RegistryStore(repo, signer, search=search, certifier=_certifier)
        loader = RegistryLoader(repo, signer)
        # DEMO_BOOTSTRAP=false skips re-submitting/approving on boot, so QA drills
        # like "revoke the workflow, restart, watch it fail closed" are real —
        # otherwise every boot self-heals the trust state (upserts are idempotent).
        bootstrap = _flag("DEMO_BOOTSTRAP", "true")
        if bootstrap:
            for manifest in (
                CATALOG_MANIFEST,
                DATASTORE_MANIFEST,
                COMMITMENTS_MANIFEST,
                FULFILLMENT_MANIFEST,
                EXPEDITER_MANIFEST,
            ):
                await trust.submit(manifest)  # upsert: safe across restarts
                await trust.approve(manifest.id)

        catalog_client = CatalogClient()
        datastore_client = DatastoreClient()

        def client_builder(manifest: AgentManifest) -> Any:
            return catalog_client if manifest.id == "catalog" else datastore_client

        policy = NativePolicyEngine(
            ruleset_ref="spend-cap",
            repo=_RulesetRepo(load_ruleset(RULES_DIR / "spend-cap.yaml")),
            event_sink=sink,
        )
        context_builder = ContextBuilder(
            [
                PolicyFlagsSource(
                    {
                        "funds_received_usd": str(funds),
                        "spend_cap_usd": str(cap),
                        "budget": str(funds),
                        "mode": "sandbox" if settings.sandbox else "live",
                    }
                )
            ],
            max_tokens=settings.context_token_budget,
        )
        await attach_registry_context_sources(
            context_builder,
            loader,
            tenant=tenant,
            client_builder=client_builder,
            cache=ContextSourceCache(),
        )
        resolver = DefaultReferenceResolver()
        await attach_registry_resolvers(resolver, loader, client_builder=client_builder)

        # the signed workflow on Postgres
        workflows = WorkflowStore(PostgresWorkflowRepo(db), signer)
        definition = load_workflow_yaml(
            (THIS_DIR / "workflows" / "po-cycle.yaml").read_text(encoding="utf-8")
        )
        if bootstrap:
            await workflows.submit(definition)
            await workflows.approve(definition.name, definition.version)
        # Always load through the trust check: a revoked/tampered workflow refuses
        # to run (RegistryTrustError) when bootstrap self-healing is off.
        workflow = await workflows.load(definition.name)

        # orchestrator: workflow over the result path
        results = PostgresResultStore(db)
        waker = PostgresRunWaker(db)
        run_store = PostgresRunStore(db)
        executor = WorkflowExecutor(
            DefaultCoordinator(
                queue=QueueDispatcher(
                    broker, signer, idempotency_guard=guard, registry_loader=loader
                )
            ),
            results=results,
            default_mode="queue",
        )
        hitl = _flag("DEMO_HITL", "false")
        approval_gate = (
            ApprovalGate(
                repo=PostgresApprovalRepo(db),
                event_sink=sink,
                timeout=float(os.getenv("DEMO_APPROVAL_TIMEOUT_S", "600")),
                poll_interval=1.0,
            )
            if hitl
            else None
        )
        pipeline = Pipeline(
            [
                ChatStage(),
                IntentStage(task_types=["funds_commit"]),
                ActionPlanStage(
                    scenario_planner(committed_spend_usd=committed),
                    policy=policy,
                    context_builder=context_builder,
                    ruleset_ref="spend-cap",
                ),
                # Without DEMO_HITL the fail-closed default denies an over-cap
                # commitment. With it, the gate is repo-backed: the approval
                # service container (or curl) resolves it cross-process.
                ApprovalStage(
                    hitl_enabled=hitl,
                    gate=approval_gate,
                    fallback=settings.approval_fallback,
                ),
                WorkflowStage(
                    executor,
                    workflow,
                    context={
                        "order_code_east": os.getenv("DEMO_ORDER_CODE_EAST", "PO_EAST_REPLENISH_A"),
                        "order_code_west": os.getenv("DEMO_ORDER_CODE_WEST", "PO_WEST_REPLENISH_A"),
                    },
                ),
            ]
        )
        l1 = DefaultL1(
            pipeline,
            run_store=run_store,
            checkpointer=PostgresCheckpointer(PostgresCheckpointRepo(db)),
            waker=waker,
            event_sink=sink,
        )

        async def resume(run_id: str) -> None:
            state = await run_store.get_state(run_id)
            if state.status in TERMINAL:
                return
            await l1.resume_run(run_id)

        collector = ResultCollector(
            broker,
            signer,
            results=results,
            waker=waker,
            resume=resume,
            queue="results",
            validator=make_post_result_validator(policy=policy, schema_registry=schemas),
        )
        poller = WakerPoller(waker, resume)

        # DEMO_AUTOSTART=false boots resume-only (distributed mode): no new run —
        # the collector + poller serve WAITING runs from a previous boot (the
        # kill→resume drill). Embedded mode always autostarts (single process).
        autostart = _flag("DEMO_AUTOSTART", "true") or settings.embedded_l2
        run_id: str | None = None
        out: dict[str, Any] = {}
        if autostart:
            result = await l1.run(GOAL, tenant, principal=BUYER_LEAD)
            run_id = result.run_id
            out = {"run_id": run_id, "status": result.status}

        if settings.embedded_l2:
            # Embedded mode: vendor workers + collector + poller inline.
            commitments = CommitmentsAgent()
            registry = AgentRegistry()
            registry.register_agent(commitments)
            registry.register_agent(FulfillmentAgent())
            worker = WorkerPool(
                broker,
                signer,
                registry,
                schema_registry=schemas,
                idempotency_guard=guard,
                run_store=run_store,
                result_queue="results",
                agent_clients=AgentClientFactory(
                    loader, tenant=tenant, client_builder=client_builder
                ),
            )
            assert run_id is not None  # embedded mode always autostarts
            for _ in range(40):
                for queue in TASK_QUEUES:
                    await worker.consume(queue)
                await collector.drain()
                await poller.tick()
                state = await run_store.get_state(run_id)
                if state.status in TERMINAL:
                    break
            out["commits"] = len(commitments.ledger)
        else:
            # Distributed mode: worker containers consume the vendor queues; this
            # process runs the collector + poller services and awaits the run.
            services = [
                asyncio.create_task(collector.drain()),  # blocking consume on real brokers
                asyncio.create_task(poller.run_forever()),
            ]
            timeout = float(os.getenv("DEMO_TIMEOUT_S", "300"))
            deadline = asyncio.get_running_loop().time() + timeout
            if run_id is not None:
                state = await run_store.get_state(run_id)
                while state.status not in TERMINAL:
                    if asyncio.get_running_loop().time() >= deadline:
                        break
                    await asyncio.sleep(1.0)
                    state = await run_store.get_state(run_id)
            else:
                # Resume-only boot: serve until no run remains in flight (or timeout).
                while asyncio.get_running_loop().time() < deadline:
                    in_flight = await db.fetchval(
                        "SELECT count(*) FROM runs "
                        "WHERE status IN ('pending', 'running', 'waiting')"
                    )
                    if not in_flight:
                        break
                    await asyncio.sleep(1.0)
            for service in services:
                service.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service

        if run_id is None:
            out["resume_only"] = True
            out["status"] = "served"
            return out

        final = await run_store.get_state(run_id)
        out["status"] = final.status.value
        reserve = await results.list(run_id, "reserve")
        invoice = await results.list(run_id, "invoice")
        out.setdefault("commits", len(reserve))
        out["committed_usd"] = reserve[0].result.output["amount_usd"] if reserve else None
        out["invoice_open"] = bool(invoice and invoice[0].result.output.get("open"))
        return out
    finally:
        close = getattr(broker, "close", None)
        if close is not None:
            await close()
        await db.close()


def main() -> None:
    out = asyncio.run(run_distributed())
    print(f"[distributed] {out}")


if __name__ == "__main__":
    main()
