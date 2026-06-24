"""MAOF — Multi-Agent Orchestration Framework.

The reusable orchestration + governance layer of a hierarchical (L1 -> L2)
multi-agent system. Ships zero domain content: adopters inject their own L1
orchestrator, L2 agents, skills, task schemas, and policy rulesets.
"""

from __future__ import annotations

from maof.config import Settings
from maof.errors import (
    ApprovalRequired,
    BudgetExceeded,
    ConfigError,
    IdempotencyError,
    MAOFError,
    PolicyDenied,
    RegistryTrustError,
    SchemaValidationError,
    SignatureError,
    TenancyError,
    TransportError,
)
from maof.types import (
    ContextEnvelope,
    CoordinationMode,
    DataPointer,
    EffortBudget,
    Envelope,
    Intent,
    OrchestrationMode,
    Plan,
    Stage,
    StageContext,
    Task,
    TaskResult,
    TenantContext,
    ToolRef,
)

__version__ = "1.0.0"

__all__ = [
    "__version__",
    "Settings",
    # errors
    "MAOFError",
    "ConfigError",
    "SignatureError",
    "SchemaValidationError",
    "PolicyDenied",
    "ApprovalRequired",
    "IdempotencyError",
    "TransportError",
    "RegistryTrustError",
    "BudgetExceeded",
    "TenancyError",
    # core types
    "Stage",
    "CoordinationMode",
    "OrchestrationMode",
    "TenantContext",
    "Envelope",
    "ContextEnvelope",
    "Task",
    "TaskResult",
    "Intent",
    "Plan",
    "ToolRef",
    "DataPointer",
    "EffortBudget",
    "StageContext",
]
