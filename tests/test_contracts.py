"""Core contract tests.

Locks the data types, the canonical stage strings,
the interface signatures, and the Settings surface. Written before any
implementation: it must go red, then green once types.py / config.py / errors.py and
the interface stubs exist.
"""

from __future__ import annotations

import importlib

import pytest


def test_version() -> None:
    import maof

    assert maof.__version__


def test_stage_enum_exact_values() -> None:
    """These exact strings ride the wire and the audit log."""
    from maof.types import Stage

    assert Stage.CHAT == "chat"
    assert Stage.INTENT_SYNTHESIS == "intent_synthesis"
    assert Stage.ACTION_PLAN == "action_plan"
    assert Stage.APPROVAL == "approval"
    assert Stage.PUBLISH == "publish"
    assert [s.value for s in Stage] == [
        "chat",
        "intent_synthesis",
        "action_plan",
        "approval",
        "publish",
    ]


def test_core_data_types_present() -> None:
    """Every type the signatures reference must exist and resolve."""
    import maof.types as t

    for name in [
        "Envelope",
        "Task",
        "ContextEnvelope",
        "ToolRef",
        "DataPointer",
        "MemorySnippet",
        "Stage",
        "TenantContext",
        "Intent",
        "Plan",
        "TaskResult",
        "StageContext",
        "L2Context",
        "OrchestrationResult",
        "SubResult",
        "EffortBudget",
        "RunState",
        "TraceEntry",
        "Note",
        "CompactedContext",
        "CostSummary",
        "CostProjection",
        "Rubric",
        "JudgeResult",
        "EvalDataset",
        "EvalReport",
        "RunHarness",
        "DecisionTrace",
        "LoadedRuleset",
        "QueueSpec",
        "IncomingMessage",
    ]:
        assert hasattr(t, name), f"missing type maof.types.{name}"


def test_construct_wire_types() -> None:
    from maof.types import (
        ContextEnvelope,
        DataPointer,
        Envelope,
        MemorySnippet,
        Plan,
        Stage,
        Task,
        TaskResult,
        TenantContext,
        ToolRef,
    )

    tc = TenantContext(tenant_id="buyer-org-001")
    assert tc.tenant_id == "buyer-org-001"

    env = Envelope(run_id="run-1", tenant_id=tc.tenant_id, stage=Stage.ACTION_PLAN)
    assert env.stage == Stage.ACTION_PLAN
    assert env.mode  # has a default (sandbox)

    task = Task(
        task_id="t-1",
        task_type="funds_commit",
        description="Commit next-quarter east-region buy",
        idempotency_key="abc123",
    )
    assert task.priority == 5

    ce = ContextEnvelope(
        run_id="run-1",
        tenant_id=tc.tenant_id,
        stage=Stage.CHAT,
        goal="run the replenishment cycle",
        toolset=[ToolRef(name="commitments", scope="tenant", rbac="buy:commit")],
        data_pointers=[DataPointer(alias="purchase_plan", uri="s3://x")],
        memories=[MemorySnippet(kind="recall", content="prior plan")],
    )
    assert ce.toolset[0].name == "commitments"
    assert ce.data_pointers[0].alias == "purchase_plan"

    plan = Plan(tasks=[task], task_types=["funds_commit"])
    assert plan.task_types == ["funds_commit"]

    res = TaskResult(status="ok", task_id="t-1")
    assert res.status == "ok"


def test_task_requires_idempotency_key() -> None:
    """The one field MAOF mandates on every task."""
    from pydantic import ValidationError

    from maof.types import Task

    with pytest.raises(ValidationError):
        Task(task_id="t", task_type="x", description="d")  # type: ignore[call-arg]


def test_stage_context_constructs_with_optional_handles() -> None:
    from maof.types import Stage, StageContext, TenantContext

    sc = StageContext(
        run_id="run-1",
        tenant=TenantContext(tenant_id="t1"),
        goal="launch next-quarter",
    )
    assert sc.run_id == "run-1"
    assert sc.stage == Stage.CHAT
    assert sc.plan is None
    assert sc.policy_decisions == []
    assert sc.dialogue == []


def test_module_local_types() -> None:
    from maof.orchestrator.delegation import DelegationContract
    from maof.registry.models import AgentManifest, ContextDeclaration, RegistryEntry
    from maof.types import EffortBudget

    dc = DelegationContract(
        objective="commit next-quarter east-region buy capped at cleared funds",
        output_format="funds_commit.v1",
        tool_guidance=["use commitments buy:commit"],
        boundaries=["do not commit beyond cleared funds"],
        effort_budget=EffortBudget(),
        parent_trace_ref="run-1",
    )
    assert dc.accepted_schema is None

    cd = ContextDeclaration(
        id="vendor_mappings",
        kind="yaml_config",
        description="Commitments rate cards + PO templates",
        scope="tenant",
        supplies=["rate_card"],
        requires_from_l1=["budget"],
        source_ref="pkg://commitments/maps.yaml",
        mutable=False,
    )
    man = AgentManifest(
        id="commitments",
        kind="l2_agent",
        name="Commitments",
        version="v1",
        endpoint="python://commitments",
        capabilities=["funds_commit"],
        accepted_task_types=["funds_commit"],
        provided_schemas=["funds_commit.v1"],
        rbac_scopes=["buy:commit"],
        context_tags=[],
        tenancy="tenant",
        side_loaded_context=[cd],
        metadata={},
    )
    assert man.side_loaded_context[0].supplies == ["rate_card"]

    entry = RegistryEntry(manifest=man, status="pending")
    assert entry.status == "pending"
    assert entry.signature is None


def test_interface_protocols_importable() -> None:
    """The public interface signatures stay locked."""
    mods = {
        "maof.agents.base": ["Skill", "L2Agent", "L1Orchestrator"],
        "maof.orchestrator.pipeline": ["Stage", "Pipeline"],
        "maof.orchestrator.loop": ["OrchestratorLoop"],
        "maof.orchestrator.coordinator": ["Coordinator"],
        "maof.policy.engine": ["PolicyEngine"],
        "maof.transport.base": ["Broker"],
        "maof.models.base": ["LLMProvider", "EmbeddingProvider"],
        "maof.memory.base": ["VectorStore", "MemoryService"],
        "maof.persistence.base": [
            "IntentRepo",
            "ApprovalRepo",
            "PromptAuditRepo",
            "PolicyRepo",
            "RegistryRepo",
            "RunRepo",
            "CheckpointRepo",
            "ArtifactRepo",
            "CostRepo",
            "EvalRepo",
        ],
        "maof.observability.events": ["EventSink", "AuditEvent"],
        "maof.context.builder": ["ContextSource", "ContextBuilder"],
        "maof.context.budget": ["Budgeter"],
        "maof.context.compaction": ["Compactor"],
        "maof.context.jit": ["ReferenceResolver"],
        "maof.runs.store": ["RunStore"],
        "maof.runs.checkpoint": ["Checkpointer"],
        "maof.runs.idempotency": ["IdempotencyGuard"],
        "maof.runs.artifacts": ["ArtifactStore"],
        "maof.eval.judge": ["Judge"],
        "maof.eval.runner": ["EvalRunner"],
        "maof.cost.accounting": ["CostLedger", "WorthItGate"],
    }
    for mod, names in mods.items():
        m = importlib.import_module(mod)
        for n in names:
            assert hasattr(m, n), f"missing {mod}.{n}"


def test_errors_hierarchy() -> None:
    from maof import errors

    assert issubclass(errors.ConfigError, errors.MAOFError)
    assert issubclass(errors.SignatureError, errors.MAOFError)
    assert issubclass(errors.SchemaValidationError, errors.MAOFError)
    assert issubclass(errors.PolicyDenied, errors.MAOFError)


def test_settings_defaults_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from maof.config import Settings

    s = Settings()
    assert s.tenancy_mode in ("single", "multi")
    assert s.default_coordination_mode in ("queue", "in_process")
    assert s.orchestration_mode in ("workflow", "autonomous")
    assert 0.0 < s.compaction_threshold <= 1.0
    assert s.region  # has a default

    monkeypatch.setenv("REGION", "eu-west-1")
    monkeypatch.setenv("RULESET_REF", "spend-cap")
    s2 = Settings()
    assert s2.region == "eu-west-1"
    assert s2.ruleset_ref == "spend-cap"
