"""Agent contracts.

First-party agents implement these Protocols and register via decorators /
entry points; the MCP adapter wraps remote MCP servers to satisfy the
same L2Agent contract. ``BaseL2Agent`` defaults
``context_delegation = []`` so a simple agent that side-loads nothing need not
declare anything.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from maof.registry.models import ContextDeclaration
    from maof.types import L2Context, OrchestrationResult, TaskResult, TenantContext


@runtime_checkable
class Skill(Protocol):
    name: str
    version: str

    async def run(self, payload: dict[str, Any], ctx: L2Context) -> dict[str, Any]: ...


@runtime_checkable
class L2Agent(Protocol):
    name: str
    accepted_task_types: list[str]
    skills: list[Skill]
    context_delegation: list[ContextDeclaration]  # declares any side-loaded context

    async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult: ...


@runtime_checkable
class L1Orchestrator(Protocol):
    async def run(self, goal: str, tenant: TenantContext) -> OrchestrationResult: ...


class BaseSkill:
    """Convenience base for a skill (an L2 agent's internal chain step)."""

    name: str = "skill"
    version: str = "v1"

    async def run(self, payload: dict[str, Any], ctx: L2Context) -> dict[str, Any]:
        raise NotImplementedError


class BaseL2Agent:
    """Convenience base for an L2 agent. Defaults ``context_delegation = []`` so a
    simple agent that side-loads nothing need not declare anything."""

    name: str = "base_l2_agent"
    accepted_task_types: list[str] = []

    def __init__(
        self,
        *,
        skills: list[Skill] | None = None,
        context_delegation: list[ContextDeclaration] | None = None,
    ) -> None:
        self.skills: list[Skill] = list(skills) if skills else []
        self.context_delegation: list[ContextDeclaration] = (
            list(context_delegation) if context_delegation else []
        )

    async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
        raise NotImplementedError


__all__ = ["Skill", "L2Agent", "L1Orchestrator", "BaseSkill", "BaseL2Agent"]
