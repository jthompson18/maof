"""Source-of-truth agent hosting: registry-attached context
sources, RBAC-scoped agent consultation, resolver discovery, post_result governance."""

from __future__ import annotations

from typing import Any

import pytest

from maof.agents.client import (
    AgentClientFactory,
    ContextSourceCache,
    attach_registry_context_sources,
    attach_registry_resolvers,
)
from maof.context.builder import ContextBuilder
from maof.context.jit import DefaultReferenceResolver
from maof.errors import MAOFError, RegistryTrustError
from maof.orchestrator.lifecycle import (
    InMemoryResultStore,
    InMemoryRunWaker,
    ResultCollector,
    ResultEnvelope,
    make_post_result_validator,
    publish_result,
)
from maof.policy.engine import NativePolicyEngine
from maof.registry.loader import RegistryLoader
from maof.registry.models import AgentManifest, ContextDeclaration
from maof.registry.store import InMemoryRegistryRepo, RegistryStore
from maof.schemas.registry import SchemaRegistry
from maof.transport.fake import InMemoryBroker
from maof.transport.signing import Signer
from maof.types import (
    LoadedRuleset,
    Rule,
    StageContext,
    TaskResult,
    TenantContext,
)


class FakeMCP:
    """A fake catalog/datastore MCP client."""

    def __init__(self) -> None:
        self.reads: list[str] = []
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def read_resource(self, ref: str) -> dict[str, Any]:
        self.reads.append(ref)
        return {"regions": ["east", "west", "display"], "version": "tax-v3"}

    async def call_tool(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((name, args))
        return {"valid": True}


class FakeSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def emit(self, event: Any) -> None:
        self.events.append(event)


def _catalog_manifest(*, required: bool = True, mutable: bool = False) -> AgentManifest:
    return AgentManifest(
        id="catalog",
        kind="context_source",
        name="catalog",
        version="v3",
        endpoint="mcp://catalog",
        context_tags=["naming"],
        tenancy="tenant",
        description="Naming catalog: hierarchies + enumerated values with semantics",
        resolver_schemes=["catalog"],
        required=required,
        side_loaded_context=[
            ContextDeclaration(
                id="catalog_values",
                kind="lookup_table",
                description="enumerated naming values",
                scope="tenant",
                supplies=["naming"],
                mutable=mutable,
            )
        ],
    )


async def _approved_loader(
    manifest: AgentManifest, sink: FakeSink | None = None
) -> tuple[RegistryLoader, Signer]:
    signer = Signer({"default": "secret"})
    repo = InMemoryRegistryRepo()
    store = RegistryStore(repo, signer)
    await store.submit(manifest)
    await store.approve(manifest.id)
    return RegistryLoader(repo, signer, event_sink=sink), signer


def _sc() -> StageContext:
    return StageContext(run_id="r1", tenant=TenantContext(tenant_id="t"), goal="launch po")


# registry -> ContextBuilder auto-attachment
async def test_registry_context_source_auto_attaches() -> None:
    sink = FakeSink()
    loader, _ = await _approved_loader(_catalog_manifest(), sink)
    client = FakeMCP()
    builder = ContextBuilder([])
    attached = await attach_registry_context_sources(
        builder,
        loader,
        tenant=TenantContext(tenant_id="t"),
        client_builder=lambda manifest: client,
        event_sink=sink,
    )
    assert [m.id for m in attached] == ["catalog"]

    env = await builder.build(_sc())
    assert env.semantic_model["catalog"]["version"] == "tax-v3"  # truth in the envelope
    assert any(e.event_type == "context_delegated" for e in sink.events)


async def test_required_source_outage_fails_closed() -> None:
    loader, _ = await _approved_loader(_catalog_manifest(required=True))

    class DownClient:
        async def read_resource(self, ref: str) -> dict[str, Any]:
            raise ConnectionError("catalog agent unreachable")

    builder = ContextBuilder([])
    await attach_registry_context_sources(
        builder, loader, tenant=TenantContext(tenant_id="t"), client_builder=lambda m: DownClient()
    )
    with pytest.raises(MAOFError, match="required"):
        await builder.build(_sc())


async def test_immutable_source_contributions_cache_across_builds() -> None:
    loader, _ = await _approved_loader(_catalog_manifest(mutable=False))
    client = FakeMCP()
    builder = ContextBuilder([])
    await attach_registry_context_sources(
        builder,
        loader,
        tenant=TenantContext(tenant_id="t"),
        client_builder=lambda m: client,
        cache=ContextSourceCache(),
    )
    await builder.build(_sc())
    await builder.build(_sc())
    assert len(client.reads) == 1  # second build served from cache (mutable=False)


# agent->agent consultation (ctx.agents)
async def test_agent_client_factory_rbac_and_audit() -> None:
    sink = FakeSink()
    manifest = _catalog_manifest()
    manifest = manifest.model_copy(update={"rbac_scopes": ["catalog:read"]})
    loader, _ = await _approved_loader(manifest, sink)
    client = FakeMCP()

    granted = AgentClientFactory(
        loader,
        tenant=TenantContext(tenant_id="t", attributes={"scopes": "catalog:read"}),
        client_builder=lambda m: client,
        event_sink=sink,
    )
    resolved = await granted.client("catalog")
    assert (await resolved.call_tool("validate", {"name": "x"}))["valid"] is True
    assert any(e.event_type == "agent_consulted" for e in sink.events)

    denied = AgentClientFactory(
        loader,
        tenant=TenantContext(tenant_id="t2"),  # no scopes
        client_builder=lambda m: client,
    )
    with pytest.raises(RegistryTrustError):
        await denied.client("catalog")


# resolver discovery (catalog:// , datastore://)
async def test_registry_resolver_schemes_register_into_jit() -> None:
    loader, _ = await _approved_loader(_catalog_manifest())
    client = FakeMCP()
    resolver = DefaultReferenceResolver()
    await attach_registry_resolvers(resolver, loader, client_builder=lambda m: client)
    out = await resolver.resolve("catalog://order-codes", _sc())
    assert "tax-v3" in out
    assert client.reads == ["order-codes"]


# post_result governance
class _RulesetRepo:
    def __init__(self, ruleset: LoadedRuleset) -> None:
        self._ruleset = ruleset

    async def load_ruleset(self, tenant: Any, ruleset_ref: str) -> LoadedRuleset:
        return self._ruleset


def _post_result_policy() -> NativePolicyEngine:
    ruleset = LoadedRuleset(
        ruleset_ref="conformance",
        version=1,
        rules=[
            Rule(
                rule_id="deny-nonconformant-names",
                ruleset_ref="conformance",
                version=1,
                stage="post_result",
                when={"op": "eq", "lhs": "semantic.result.catalog_ok", "rhs": False},
                actions=[{"type": "deny_plan", "reason": "order code violates catalog"}],
            )
        ],
    )
    return NativePolicyEngine(ruleset_ref="conformance", repo=_RulesetRepo(ruleset))


async def test_policy_post_result_denies_nonconformant_output() -> None:
    policy = _post_result_policy()
    from maof.types import Envelope, Stage, Task

    env = Envelope(run_id="r", tenant_id="t", stage=Stage.PUBLISH)
    task = Task(task_id="t1", task_type="order_placement", description="d", idempotency_key="k")
    bad = TaskResult(status="ok", task_id="t1", output={"catalog_ok": False})
    decision = await policy.post_result(env, task, bad)
    assert decision.denied

    good = TaskResult(status="ok", task_id="t1", output={"catalog_ok": True})
    decision = await policy.post_result(env, task, good)
    assert not decision.denied


async def test_collector_validator_quarantines_denied_results() -> None:
    """A denied result never persists — dependent steps cannot consume it."""
    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    results = InMemoryResultStore()
    waker = InMemoryRunWaker(results)
    schemas = SchemaRegistry()
    schemas.register(
        "order_placement.result.v1",
        {"type": "object", "required": ["catalog_ok"], "properties": {}},
    )
    validator = make_post_result_validator(policy=_post_result_policy(), schema_registry=schemas)

    async def resume(run_id: str) -> None:
        return None

    collector = ResultCollector(
        broker, signer, results=results, waker=waker, resume=resume, validator=validator
    )

    def _envelope(output: dict[str, Any], key: str) -> ResultEnvelope:
        return ResultEnvelope(
            run_id="r1",
            step_ref="east",
            task_id="t1",
            task_type="order_placement",
            idempotency_key=key,
            tenant_id="t",
            result=TaskResult(status="ok", task_id="t1", output=output),
        )

    await broker.ensure_topology(_result_specs())
    # non-conformant (policy denial) -> handler raises -> retry -> DLQ; never persisted
    await publish_result(
        broker, signer, queue="results", envelope=_envelope({"catalog_ok": False}, "bad")
    )
    await collector.drain()
    assert await results.list("r1", "east") == []
    assert broker.depth("results.dlq") == 1

    # schema-invalid output is also quarantined
    await publish_result(
        broker, signer, queue="results", envelope=_envelope({"unexpected": 1}, "weird")
    )
    await collector.drain()
    assert await results.list("r1", "east") == []

    # conformant result persists
    await publish_result(
        broker, signer, queue="results", envelope=_envelope({"catalog_ok": True}, "good")
    )
    await collector.drain()
    assert len(await results.list("r1", "east")) == 1


def _result_specs() -> list[Any]:
    from maof.types import QueueSpec

    return [QueueSpec(name="results", dlq_name="results.dlq")]


async def test_worker_validates_result_output_schema() -> None:
    """Worker-side output validation: a result violating <task_type>.result.v1 fails the
    delivery (retry/DLQ) and publishes no envelope."""
    import json

    from maof.agents.base import BaseL2Agent
    from maof.agents.registry_runtime import AgentRegistry
    from maof.types import L2Context
    from maof.workers.pool import WorkerPool

    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    schemas = SchemaRegistry()
    schemas.register(
        "funds_commit.result.v1",
        {"type": "object", "required": ["io_number"], "properties": {}},
    )

    class BadAgent(BaseL2Agent):
        name = "bad"
        accepted_task_types = ["funds_commit"]

        async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
            return TaskResult(status="ok", task_id=task["task_id"], output={})  # missing io_number

    registry = AgentRegistry()
    registry.register_agent(BadAgent())
    pool = WorkerPool(broker, signer, registry, schema_registry=schemas, result_queue="results")

    from maof.types import QueueSpec

    await broker.ensure_topology(
        [QueueSpec(name="tasks.funds_commit", dlq_name="tasks.funds_commit.dlq")]
    )
    message = {
        "envelope": {"run_id": "r", "tenant_id": "t", "intent_id": None, "stage": "publish"},
        "task": {
            "task_id": "t1",
            "task_type": "funds_commit",
            "priority": 5,
            "description": "buy",
            "idempotency_key": "k",
            "step_ref": "buy",
        },
        "policy_flags": {},
        "toolset": [],
        "data_pointers": {},
        "semantic_model": {},
        "timestamp": "now",
    }
    body = json.dumps(message).encode()
    await broker.publish(
        "tasks.funds_commit", body, headers=signer.headers(body), message_id="k", correlation_id="c"
    )
    await pool.consume("tasks.funds_commit")  # handler raises internally -> retry/DLQ
    assert broker.depth("results") == 0  # no envelope escaped
    assert broker.depth("tasks.funds_commit.dlq") == 1  # quarantined


# L2Context.agents wiring
async def test_l2_agent_consults_catalog_mid_task() -> None:
    import json

    from maof.agents.base import BaseL2Agent
    from maof.agents.registry_runtime import AgentRegistry
    from maof.types import L2Context
    from maof.workers.pool import WorkerPool

    sink = FakeSink()
    manifest = _catalog_manifest().model_copy(update={"rbac_scopes": []})
    loader, _ = await _approved_loader(manifest, sink)
    factory = AgentClientFactory(
        loader,
        tenant=TenantContext(tenant_id="t"),
        client_builder=lambda m: FakeMCP(),
        event_sink=sink,
    )

    consulted: list[dict[str, Any]] = []

    class Trafficker(BaseL2Agent):
        name = "trafficker"
        accepted_task_types = ["shipment_prep"]

        async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
            catalog = await ctx.agents.client("catalog")  # type: ignore[union-attr]
            verdict = await catalog.call_tool("validate", {"name": "PO_EAST_RUSH_30"})
            consulted.append(verdict)
            return TaskResult(status="ok", task_id=task["task_id"], output=verdict)

    broker = InMemoryBroker()
    signer = Signer({"default": "s"})
    registry = AgentRegistry()
    registry.register_agent(Trafficker())
    pool = WorkerPool(broker, signer, registry, agent_clients=factory, result_queue=None)

    message = {
        "envelope": {"run_id": "r", "tenant_id": "t", "intent_id": None, "stage": "publish"},
        "task": {
            "task_id": "t1",
            "task_type": "shipment_prep",
            "priority": 5,
            "description": "traffic",
            "idempotency_key": "k",
        },
        "policy_flags": {},
        "toolset": [],
        "data_pointers": {},
        "semantic_model": {},
        "timestamp": "now",
    }
    body = json.dumps(message).encode()
    await broker.publish(
        "tasks.shipment_prep",
        body,
        headers=signer.headers(body),
        message_id="k",
        correlation_id="c",
    )
    await pool.consume("tasks.shipment_prep")
    assert consulted == [{"valid": True}]
    assert any(e.event_type == "agent_consulted" for e in sink.events)
