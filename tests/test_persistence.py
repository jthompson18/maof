"""Postgres adapter integration: migrations, run store, and repositories."""

from __future__ import annotations

from uuid import uuid4

from maof.persistence.postgres import (
    Database,
    PostgresApprovalRepo,
    PostgresArtifactRepo,
    PostgresCostRepo,
    PostgresEvalRepo,
    PostgresIntentRepo,
    PostgresPolicyRepo,
    PostgresPromptAuditRepo,
    PostgresRegistryRepo,
)
from maof.registry.models import AgentManifest, RegistryEntry
from maof.runs.store import PostgresRunStore
from maof.types import (
    EvalReport,
    Intent,
    LoadedRuleset,
    Rule,
    RunStatus,
    TenantContext,
    TraceEntry,
)

TABLES = [
    "intents",
    "memories",
    "approvals",
    "prompt_audit",
    "policy_rulesets",
    "policy_rules",
    "registry_entries",
    "runs",
    "run_trace",
    "checkpoints",
    "idempotency_keys",
    "artifacts",
    "notes",
    "cost_ledger",
    "eval_results",
    "audit_events",
]


async def test_migrations_create_all_tables(db: Database) -> None:
    for table in TABLES:
        exists = await db.fetchval("SELECT to_regclass($1) IS NOT NULL", f"public.{table}")
        assert exists is True, f"missing table {table}"


async def test_run_store_round_trip(db: Database) -> None:
    store = PostgresRunStore(db)
    tenant = TenantContext(tenant_id=f"t-{uuid4()}")

    run_id = await store.create(tenant, "run the replenishment cycle")
    state = await store.get_state(run_id)
    assert state.status is RunStatus.PENDING
    assert state.goal == "run the replenishment cycle"

    await store.append_trace(
        run_id, TraceEntry(run_id=run_id, seq=0, kind="chat", step="chat", data={"ok": True})
    )
    await store.append_trace(
        run_id, TraceEntry(run_id=run_id, seq=0, kind="intent", step="intent_synthesis")
    )

    entries = await store.read_trace(run_id)
    assert [e.seq for e in entries] == [1, 2]  # seq assigned monotonically by the store
    assert entries[0].kind == "chat"
    assert entries[0].data == {"ok": True}

    rest = await store.read_trace(run_id, since="1")
    assert [e.seq for e in rest] == [2]

    await store.set_state(run_id, status=RunStatus.COMPLETED, current_step="publish")
    final = await store.get_state(run_id)
    assert final.status is RunStatus.COMPLETED
    assert final.current_step == "publish"


async def test_intent_repo(db: Database) -> None:
    repo = PostgresIntentRepo(db)
    tenant = TenantContext(tenant_id=f"t-{uuid4()}")
    iid = f"i-{uuid4()}"

    await repo.save(
        tenant,
        Intent(intent_id=iid, goal="g", summary="s", task_types=["funds_commit"], details={"x": 1}),
    )
    got = await repo.get(tenant, iid)
    assert got is not None
    assert got.goal == "g"
    assert got.task_types == ["funds_commit"]
    assert got.details == {"x": 1}
    assert await repo.get(tenant, "missing") is None


async def test_approval_repo(db: Database) -> None:
    repo = PostgresApprovalRepo(db)
    tenant = TenantContext(tenant_id=f"t-{uuid4()}")

    approval_id = await repo.create(tenant, "run-1", "exceeds cleared funds")
    row = await repo.get(approval_id)
    assert row is not None
    assert row["status"] == "pending"

    await repo.resolve(approval_id, approved=True)
    resolved = await repo.get(approval_id)
    assert resolved is not None
    assert resolved["status"] == "approved"


async def test_prompt_audit_repo(db: Database) -> None:
    repo = PostgresPromptAuditRepo(db)
    run_id = f"run-{uuid4()}"
    await repo.record(TenantContext(tenant_id="t"), run_id, "the prompt", "the response")
    count = await db.fetchval("SELECT count(*) FROM prompt_audit WHERE run_id = $1", run_id)
    assert count == 1


async def test_registry_repo(db: Database) -> None:
    repo = PostgresRegistryRepo(db)
    manifest = AgentManifest(
        id=f"commitments-{uuid4()}",
        kind="l2_agent",
        name="Commitments",
        version="v1",
        endpoint="python://commitments",
        accepted_task_types=["funds_commit"],
        provided_schemas=["funds_commit.v1"],
        rbac_scopes=["buy:commit"],
        tenancy="tenant",
    )

    await repo.put(RegistryEntry(manifest=manifest, status="pending"))
    got = await repo.get(manifest.id)
    assert got is not None
    assert got.manifest.name == "Commitments"
    assert got.status == "pending"

    await repo.put(
        RegistryEntry(manifest=manifest, status="approved", signature="sig", kid="default")
    )
    approved = await repo.list_approved()
    assert any(e.manifest.id == manifest.id for e in approved)


async def test_artifact_repo(db: Database) -> None:
    repo = PostgresArtifactRepo(db)
    ref = await repo.put("run-1", "plan.json", b'{"k": 1}', "application/json")
    assert await repo.get(ref) == b'{"k": 1}'
    assert await repo.get(str(uuid4())) is None


async def test_cost_repo(db: Database) -> None:
    repo = PostgresCostRepo(db)
    run_id = f"run-{uuid4()}"
    await repo.record(run_id, "gpt", 100, 50, 0.01)
    await repo.record(run_id, "gpt", 200, 100, 0.02)
    await repo.record(run_id, "claude", 10, 5, 0.001)

    total = await repo.total(run_id)
    assert total is not None
    assert total.in_tokens == 310
    assert total.out_tokens == 155
    assert total.total_tokens == 465
    assert abs(total.cost_usd - 0.031) < 1e-9
    assert total.by_model["gpt"] == 450
    assert total.by_model["claude"] == 15


async def test_eval_repo(db: Database) -> None:
    repo = PostgresEvalRepo(db)
    dataset = f"ds-{uuid4()}"
    await repo.save_report(EvalReport(dataset=dataset, passed=8, total=10, pass_rate=0.8))
    count = await db.fetchval("SELECT count(*) FROM eval_results WHERE dataset = $1", dataset)
    assert count == 1


async def test_policy_repo_round_trip(db: Database) -> None:
    repo = PostgresPolicyRepo(db)
    ref = f"rs-{uuid4()}"
    ruleset = LoadedRuleset(
        ruleset_ref=ref,
        version=1,
        canary_pct=0.0,
        rules=[
            Rule(
                rule_id="r1",
                ruleset_ref=ref,
                version=1,
                priority=10,
                stage="action_plan",
                when={"op": "always"},
                actions=[{"type": "add_nudge", "text": "hi"}],
                description="d",
            )
        ],
    )
    await repo.save_ruleset(ruleset)

    loaded = await repo.load_ruleset(TenantContext(tenant_id="t"), ref)
    assert loaded is not None
    assert loaded.version == 1
    assert len(loaded.rules) == 1
    assert loaded.rules[0].when == {"op": "always"}
    assert loaded.rules[0].actions[0]["type"] == "add_nudge"


# concurrent trace appends must not collide on seq
async def test_concurrent_trace_appends_all_land(db: Database) -> None:
    import asyncio

    store = PostgresRunStore(db)
    run_id = await store.create(TenantContext(tenant_id=f"t-{uuid4()}"), "g")
    await asyncio.gather(
        *(
            store.append_trace(run_id, TraceEntry(run_id=run_id, seq=0, kind="evt", data={"i": i}))
            for i in range(10)
        )
    )
    entries = await store.read_trace(run_id)
    assert len(entries) == 10
    assert sorted(e.seq for e in entries) == list(range(1, 11))  # distinct, gap-free


# tenant-scoped approval resolution + vector index
async def test_approval_resolve_is_tenant_scoped(db: Database) -> None:
    repo = PostgresApprovalRepo(db)
    tenant = TenantContext(tenant_id=f"t-{uuid4()}")
    approval_id = await repo.create(tenant, "run-1", "over cap")

    await repo.resolve(approval_id, approved=True, tenant_id="some-other-tenant")
    row = await repo.get(approval_id)
    assert row is not None and row["status"] == "pending"  # cross-tenant resolve rejected

    await repo.resolve(approval_id, approved=True, tenant_id=tenant.tenant_id)
    row = await repo.get(approval_id)
    assert row is not None and row["status"] == "approved"


async def test_memories_embedding_index_exists(db: Database) -> None:
    exists = await db.fetchval("SELECT to_regclass('public.memories_embedding_idx') IS NOT NULL")
    assert exists is True
