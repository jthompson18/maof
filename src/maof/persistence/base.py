"""Repository interfaces.

Pluggable persistence for every stateful concern. The Postgres+pgvector adapter
is the default. Signatures are intentionally minimal here; concrete
adapters may add kwargs but not remove these.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from maof.registry.models import RegistryEntry
    from maof.types import (
        CostSummary,
        EvalReport,
        Intent,
        LoadedRuleset,
        RunState,
        TenantContext,
        TraceEntry,
    )


@runtime_checkable
class IntentRepo(Protocol):
    async def save(self, tenant: TenantContext, intent: Intent) -> str: ...

    async def get(self, tenant: TenantContext, intent_id: str) -> Intent | None: ...


@runtime_checkable
class ApprovalRepo(Protocol):
    async def create(self, tenant: TenantContext, run_id: str, reason: str) -> str: ...

    async def resolve(
        self, approval_id: str, *, approved: bool, tenant_id: str | None = None
    ) -> None: ...

    async def get(self, approval_id: str) -> dict[str, Any] | None: ...


@runtime_checkable
class PromptAuditRepo(Protocol):
    async def record(
        self, tenant: TenantContext, run_id: str, prompt: str, response: str
    ) -> None: ...


@runtime_checkable
class PolicyRepo(Protocol):
    async def load_ruleset(
        self, tenant: TenantContext, ruleset_ref: str
    ) -> LoadedRuleset | None: ...


@runtime_checkable
class RegistryRepo(Protocol):
    async def put(self, entry: RegistryEntry) -> None: ...

    async def get(self, entry_id: str) -> RegistryEntry | None: ...

    async def list_approved(self) -> list[RegistryEntry]: ...


@runtime_checkable
class RunRepo(Protocol):
    async def create(self, tenant: TenantContext, goal: str) -> str: ...

    async def get_state(self, run_id: str) -> RunState | None: ...

    async def append_trace(self, run_id: str, entry: TraceEntry) -> None: ...

    async def read_trace(self, run_id: str, *, since: str | None = None) -> list[TraceEntry]: ...


@runtime_checkable
class CheckpointRepo(Protocol):
    async def save(self, run_id: str, step: str, blob: bytes) -> None: ...

    async def latest(self, run_id: str) -> bytes | None: ...


@runtime_checkable
class ArtifactRepo(Protocol):
    async def put(self, run_id: str, name: str, data: bytes, content_type: str) -> str: ...

    async def get(self, ref: str) -> bytes | None: ...


@runtime_checkable
class CostRepo(Protocol):
    async def record(
        self, run_id: str, model: str, in_tokens: int, out_tokens: int, cost_usd: float
    ) -> None: ...

    async def total(self, run_id: str) -> CostSummary | None: ...


@runtime_checkable
class EvalRepo(Protocol):
    async def save_report(self, report: EvalReport) -> None: ...


__all__ = [
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
]
