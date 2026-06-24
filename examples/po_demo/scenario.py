"""Reference wiring: the full purchase-order lifecycle, offline.

A pure adopter of MAOF — zero framework edits. A buyer and its partner
share a tenant; the catalog + datastore agents (registry-approved) are the source of
truth; a signed YAML workflow drives plan → reserve → [flight-start wait] →
traffic per region → [join] → actualize → invoice across Commitments and Fulfillment,
governed by the spend-cap ruleset.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from maof.agents.client import (
    AgentClientFactory,
    ContextSourceCache,
    attach_registry_context_sources,
    attach_registry_resolvers,
)
from maof.agents.registry_runtime import AgentRegistry
from maof.context.builder import ContextBuilder
from maof.context.jit import DefaultReferenceResolver
from maof.context.sources.builtins import PolicyFlagsSource
from maof.identity import Principal
from maof.models.base import HashingEmbeddingProvider
from maof.orchestrator.coordinator import DefaultCoordinator, QueueDispatcher
from maof.orchestrator.l1 import DefaultL1
from maof.orchestrator.lifecycle import (
    InMemoryResultStore,
    InMemoryRunWaker,
    ResultCollector,
    WakerPoller,
    make_post_result_validator,
)
from maof.orchestrator.pipeline import Pipeline
from maof.orchestrator.stages import ActionPlanStage, ApprovalStage, ChatStage, IntentStage
from maof.policy.engine import NativePolicyEngine
from maof.registry.loader import RegistryLoader
from maof.registry.models import AgentManifest
from maof.registry.search import RegistrySearch
from maof.registry.store import InMemoryRegistryRepo, RegistryStore
from maof.runs.checkpoint import InMemoryCheckpointer
from maof.runs.idempotency import InMemoryIdempotencyGuard
from maof.runs.store import InMemoryRunStore
from maof.schemas.registry import SchemaRegistry
from maof.transport.fake import InMemoryBroker
from maof.transport.signing import Signer
from maof.types import (
    LoadedRuleset,
    MemorySnippet,
    OrchestrationResult,
    Plan,
    QueueSpec,
    Rule,
    RunState,
    RunStatus,
    StageContext,
    TenantContext,
)
from maof.workers.pool import WorkerPool
from maof.workflows.executor import WorkflowExecutor, WorkflowStage
from maof.workflows.store import InMemoryWorkflowRepo, WorkflowStore

from .agents import CommitmentsAgent, FulfillmentAgent
from .truth import CATALOG_MANIFEST, DATASTORE_MANIFEST, CatalogClient, DatastoreClient

THIS_DIR = Path(__file__).parent
SCHEMA_DIR = THIS_DIR / "schemas"
RULES_DIR = THIS_DIR / "rules"
GOAL = "Run the quarterly replenishment purchase cycle across the east and west regions"
TERMINAL = {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}


class _CapturingSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def emit(self, event: Any) -> None:
        self.events.append(event)


class _RulesetRepo:
    """Adopter-side PolicyRepo serving the YAML-loaded spend-cap ruleset."""

    def __init__(self, ruleset: LoadedRuleset) -> None:
        self._ruleset = ruleset

    async def load_ruleset(self, tenant: Any, ruleset_ref: str) -> LoadedRuleset | None:
        return self._ruleset if ruleset_ref == self._ruleset.ruleset_ref else None


def load_ruleset(path: Path) -> LoadedRuleset:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    rules = [
        Rule(
            rule_id=r["rule_id"],
            ruleset_ref=data["ruleset_ref"],
            version=data["version"],
            priority=r.get("priority", 100),
            stage=r.get("stage", "*"),
            when=r.get("when", {}),
            actions=r.get("actions", []),
            description=r.get("description", ""),
        )
        for r in data.get("rules", [])
    ]
    return LoadedRuleset(ruleset_ref=data["ruleset_ref"], version=data["version"], rules=rules)


def register_schemas(registry: SchemaRegistry, schema_dir: Path = SCHEMA_DIR) -> None:
    for path in sorted(schema_dir.glob("*.json")):
        registry.register(path.stem, json.loads(path.read_text(encoding="utf-8")))


# Shared tenant, two orgs: every approval/commitment is attributed.
BUYER_LEAD = Principal(id="lead-ava", kind="user", org="buyer", roles=["buyer-lead"])
BUYER_FINANCE = Principal(id="fin-frank", kind="user", org="buyer", roles=["buyer-finance"])
PARTNER_OPS = Principal(id="ops-omar", kind="user", org="partner", roles=["partner-ops"])

COMMITMENTS_MANIFEST = AgentManifest(
    id="commitments",
    kind="l2_agent",
    name="the Commitments platform",
    version="v1",
    endpoint="mcp://platform/commitments",
    accepted_task_types=["purchase_plan", "funds_commit", "reconciliation"],
    tenancy="tenant",
    queue="suppliers.commitments.v1",  # registry-driven routing
    certification={"dataset_ref": "certs/commitments.json", "min_pass_rate": 0.8},
    description=(
        "the Commitments platform: purchase planning, buying and IO commitment, "
        "budget management, billing actualization and invoice creation"
    ),
)
FULFILLMENT_MANIFEST = AgentManifest(
    id="fulfillment",
    kind="l2_agent",
    name="the Fulfillment platform",
    version="v1",
    endpoint="mcp://platform/fulfillment",
    accepted_task_types=["order_placement", "shipment_prep", "delivery_metrics"],
    tenancy="tenant",
    queue="suppliers.fulfillment.v1",
    certification={"dataset_ref": "certs/fulfillment.json", "min_pass_rate": 0.8},
    description=(
        "the Fulfillment platform: order placement, shipment preparation "
        "and tracking, cross-region delivery metrics"
    ),
)
EXPEDITER_MANIFEST = AgentManifest(
    id="expediter",
    kind="l2_agent",
    name="In-flight Expediting Agent",
    version="v1",
    endpoint="mcp://platform/expediter",
    accepted_task_types=["expediting"],
    tenancy="tenant",
    queue="suppliers.expediter.v1",
    certification={"dataset_ref": "certs/expediter.json", "min_pass_rate": 0.8},
    description="optimizes carrier selection and expedited routing for in-flight orders",
)
#: A third-party vendor that flunks its certification suite — approve refuses it.
SHADY_DSP_MANIFEST = AgentManifest(
    id="shady-broker",
    kind="l2_agent",
    name="Shady Broker",
    version="v1",
    endpoint="mcp://thirdparty/shady",
    accepted_task_types=["funds_commit"],
    tenancy="tenant",
    certification={"dataset_ref": "certs/shady.json", "min_pass_rate": 0.8},
    description="brokers supply contracts programmatically",
)

RESULT_SCHEMAS: dict[str, dict[str, Any]] = {
    "purchase_plan.result.v1": {"type": "object", "required": ["plan_id"], "properties": {}},
    "funds_commit.result.v1": {
        "type": "object",
        "required": ["committed", "amount_usd", "po_number"],
        "properties": {},
    },
    "order_placement.result.v1": {
        "type": "object",
        "required": ["catalog_ok", "region"],
        "properties": {},
    },
    "reconciliation.result.v1": {"type": "object", "properties": {}},
}


class InMemoryVectorStore:
    """Tiny cosine-similarity store for the offline registry search."""

    def __init__(self) -> None:
        self._items: list[tuple[str, MemorySnippet]] = []

    async def upsert(self, tenant: TenantContext, items: list[MemorySnippet]) -> None:
        self._items.extend((tenant.tenant_id, item) for item in items)

    async def query(
        self, tenant: TenantContext, embedding: list[float], top_k: int
    ) -> list[MemorySnippet]:
        def dot(a: list[float], b: list[float]) -> float:
            return sum(x * y for x, y in zip(a, b, strict=False))

        scored = sorted(
            (
                (dot(embedding, item.embedding or []), item)
                for tid, item in self._items
                if tid == tenant.tenant_id
            ),
            key=lambda pair: pair[0],
            reverse=True,
        )
        return [i.model_copy(update={"score": s, "embedding": None}) for s, i in scored[:top_k]]


async def _certifier(manifest: AgentManifest) -> tuple[bool, float]:
    """The certification suite (an eval harness in production): the platform vendor
    vendors pass; the shady third-party DSP flunks its gate."""
    passed = manifest.id != "shady-broker"
    return passed, 1.0 if passed else 0.25


def scenario_planner(*, committed_spend_usd: int) -> Any:
    """L1 reasoning (injected): state the requested commitment so the
    spend-cap policy can clamp/gate it; execution itself is the
    signed workflow, so the plan carries no ad-hoc tasks."""

    async def plan(sc: StageContext) -> Plan:
        if sc.envelope is not None:
            sc.envelope.policy_flags["committed_spend_usd"] = str(committed_spend_usd)
        return Plan(tasks=[], task_types=["funds_commit"])

    return plan


@dataclass
class Scenario:
    l1: DefaultL1
    broker: InMemoryBroker
    signer: Signer
    schemas: SchemaRegistry
    runtime: AgentRegistry
    trust: RegistryStore
    loader: RegistryLoader
    search: RegistrySearch
    results: InMemoryResultStore
    waker: InMemoryRunWaker
    poller: WakerPoller
    collector: ResultCollector
    worker: WorkerPool
    run_store: InMemoryRunStore
    guard: InMemoryIdempotencyGuard
    policy: NativePolicyEngine
    context_builder: ContextBuilder
    resolver: DefaultReferenceResolver
    workflows: WorkflowStore
    tenant: TenantContext
    sink: _CapturingSink
    commitments: CommitmentsAgent
    fulfillment: FulfillmentAgent
    catalog_client: CatalogClient
    datastore_client: DatastoreClient
    ledger: list[dict[str, Any]]
    task_queues: list[str]

    def event_types(self) -> list[str]:
        return [e.event_type for e in self.sink.events]


async def build_scenario(
    *,
    committed_spend_usd: int = 250_000,
    funds_received_usd: int = 250_000,
    spend_cap_usd: int = 300_000,
    order_code_east: str = "PO_EAST_REPLENISH_A",
    order_code_west: str = "PO_WEST_REPLENISH_A",
    tenant_id: str = "shared-buyer-001",
    approval_gate: Any | None = None,
    hitl_enabled: bool = False,
    catalog_down: bool = False,
) -> Scenario:
    broker = InMemoryBroker()
    signer = Signer({"default": "scenario-secret"})
    guard = InMemoryIdempotencyGuard()
    sink = _CapturingSink()
    run_store = InMemoryRunStore()
    results = InMemoryResultStore()
    waker = InMemoryRunWaker(results)
    tenant = TenantContext(tenant_id=tenant_id)

    schemas = SchemaRegistry()
    register_schemas(schemas)
    for schema_id, schema in RESULT_SCHEMAS.items():
        schemas.register(schema_id, schema)

    # trust registry: source-of-truth + vendor agents, certification-gated
    repo = InMemoryRegistryRepo()
    search = RegistrySearch(InMemoryVectorStore(), HashingEmbeddingProvider(dimension=256))
    trust = RegistryStore(repo, signer, search=search, certifier=_certifier)
    loader = RegistryLoader(repo, signer, event_sink=sink)
    for manifest in (
        CATALOG_MANIFEST,
        DATASTORE_MANIFEST,
        COMMITMENTS_MANIFEST,
        FULFILLMENT_MANIFEST,
        EXPEDITER_MANIFEST,
    ):
        await trust.submit(manifest)
        await trust.approve(manifest.id)

    catalog_client = CatalogClient(down=catalog_down)
    datastore_client = DatastoreClient()

    def client_builder(manifest: AgentManifest) -> Any:
        return catalog_client if manifest.id == "catalog" else datastore_client

    # governance: the spend-cap policy + post_result conformance
    policy = NativePolicyEngine(
        ruleset_ref="spend-cap",
        repo=_RulesetRepo(load_ruleset(RULES_DIR / "spend-cap.yaml")),
        event_sink=sink,
    )

    # context: cleared funds + registry-attached catalog slice
    context_builder = ContextBuilder(
        [
            PolicyFlagsSource(
                {
                    "funds_received_usd": str(funds_received_usd),
                    "spend_cap_usd": str(spend_cap_usd),
                    "budget": str(funds_received_usd),
                    "mode": "sandbox",
                }
            )
        ],
        max_tokens=16_000,
    )
    await attach_registry_context_sources(
        context_builder,
        loader,
        tenant=tenant,
        client_builder=client_builder,
        cache=ContextSourceCache(),
        event_sink=sink,
    )
    resolver = DefaultReferenceResolver()
    await attach_registry_resolvers(resolver, loader, client_builder=client_builder)

    # the signed workflow: submit -> approve(sign) -> load
    workflows = WorkflowStore(InMemoryWorkflowRepo(), signer)
    from maof.workflows.definition import load_workflow_yaml

    definition = load_workflow_yaml(
        (THIS_DIR / "workflows" / "po-cycle.yaml").read_text(encoding="utf-8")
    )
    await workflows.submit(definition)
    await workflows.approve(definition.name, definition.version)
    workflow = await workflows.load(definition.name)

    # execution: registry-routed dispatch + result path
    dispatcher = QueueDispatcher(broker, signer, idempotency_guard=guard, registry_loader=loader)
    executor = WorkflowExecutor(
        DefaultCoordinator(queue=dispatcher), results=results, default_mode="queue"
    )
    pipeline = Pipeline(
        [
            ChatStage(),
            IntentStage(task_types=["funds_commit"]),
            ActionPlanStage(
                scenario_planner(committed_spend_usd=committed_spend_usd),
                policy=policy,
                context_builder=context_builder,
                ruleset_ref="scenario-spend-policy",
            ),
            ApprovalStage(hitl_enabled=hitl_enabled, gate=approval_gate, fallback="deny"),
            WorkflowStage(
                executor,
                workflow,
                context={"order_code_east": order_code_east, "order_code_west": order_code_west},
            ),
        ]
    )
    l1 = DefaultL1(
        pipeline,
        run_store=run_store,
        checkpointer=InMemoryCheckpointer(),
        event_sink=sink,
        waker=waker,
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

    ledger: list[dict[str, Any]] = []
    commitments = CommitmentsAgent(ledger=ledger)
    fulfillment = FulfillmentAgent()
    runtime = AgentRegistry()
    runtime.register_agent(commitments)
    runtime.register_agent(fulfillment)
    agent_clients = AgentClientFactory(
        loader, tenant=tenant, client_builder=client_builder, event_sink=sink
    )
    worker = WorkerPool(
        broker,
        signer,
        runtime,
        schema_registry=schemas,
        idempotency_guard=guard,
        run_store=run_store,
        result_queue="results",
        agent_clients=agent_clients,
        event_sink=sink,
    )

    task_queues = ["suppliers.commitments.v1", "suppliers.fulfillment.v1"]
    await broker.ensure_topology(
        [QueueSpec(name=q, dlq_name=f"{q}.dlq") for q in task_queues]
        + [QueueSpec(name="results", dlq_name="results.dlq")]
    )

    return Scenario(
        l1=l1,
        broker=broker,
        signer=signer,
        schemas=schemas,
        runtime=runtime,
        trust=trust,
        loader=loader,
        search=search,
        results=results,
        waker=waker,
        poller=poller,
        collector=collector,
        worker=worker,
        run_store=run_store,
        guard=guard,
        policy=policy,
        context_builder=context_builder,
        resolver=resolver,
        workflows=workflows,
        tenant=tenant,
        sink=sink,
        commitments=commitments,
        fulfillment=fulfillment,
        catalog_client=catalog_client,
        datastore_client=datastore_client,
        ledger=ledger,
        task_queues=task_queues,
    )


async def start_cycle(ns: Scenario, *, principal: Principal | None = None) -> OrchestrationResult:
    """Kick off the purchase cycle as a disclosed principal (defaults to the purchasing lead)."""
    return await ns.l1.run(GOAL, ns.tenant, principal=principal or BUYER_LEAD)


async def cancel_run(ns: Scenario, run_id: str) -> None:
    """Operator cancel (what ``maof runs cancel`` / RunOps does, in-memory):
    set the cooperative flag; a parked run has nothing active to observe it,
    so finalize it immediately."""
    await ns.run_store.request_cancel(run_id)
    state = await ns.run_store.get_state(run_id)
    if state.status in (RunStatus.WAITING, RunStatus.PENDING):
        await ns.run_store.set_state(run_id, status=RunStatus.CANCELLED)


async def drive(ns: Scenario, run_id: str, *, rounds: int = 30) -> RunState:
    """Embedded pump: workers consume, the collector persists + resumes, the
    poller fires due timers — until the run reaches a terminal state."""
    state = await ns.run_store.get_state(run_id)
    for _ in range(rounds):
        for queue in ns.task_queues:
            await ns.worker.consume(queue)
        await ns.collector.drain()
        await ns.poller.tick()
        state = await ns.run_store.get_state(run_id)
        if state.status in TERMINAL:
            return state
    return state


async def run_cycle_to_completion(ns: Scenario, *, principal: Principal | None = None) -> RunState:
    out = await start_cycle(ns, principal=principal)
    return await drive(ns, out.run_id)


__all__ = [
    "Scenario",
    "build_scenario",
    "start_cycle",
    "cancel_run",
    "drive",
    "run_cycle_to_completion",
    "scenario_planner",
    "BUYER_LEAD",
    "BUYER_FINANCE",
    "PARTNER_OPS",
    "COMMITMENTS_MANIFEST",
    "FULFILLMENT_MANIFEST",
    "EXPEDITER_MANIFEST",
    "SHADY_DSP_MANIFEST",
    "TERMINAL",
]
