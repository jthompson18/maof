"""Runtime agent/skill registry.

In-process map of locally injected L2 agents and skills (compile/deploy-time
trust), populated via decorators and/or ``importlib.metadata`` entry points
(groups ``maof.l2_agents`` / ``maof.skills``). Distinct from the persisted,
admin-gated discovery registry in ``maof.registry``.
"""

from __future__ import annotations

from collections.abc import Callable
from importlib import metadata
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from maof.agents.base import L2Agent, Skill

A = TypeVar("A", bound="L2Agent")
S = TypeVar("S", bound="Skill")


class AgentRegistry:
    def __init__(self) -> None:
        self._agents: dict[str, L2Agent] = {}
        self._by_task_type: dict[str, L2Agent] = {}
        self._skills: dict[str, Skill] = {}

    def register_agent(self, agent: L2Agent) -> None:
        self._agents[agent.name] = agent
        for task_type in agent.accepted_task_types:
            self._by_task_type[task_type] = agent

    def register_skill(self, skill: Skill) -> None:
        self._skills[skill.name] = skill

    def agent(self, name: str) -> L2Agent | None:
        return self._agents.get(name)

    def agent_for_task_type(self, task_type: str) -> L2Agent | None:
        return self._by_task_type.get(task_type)

    def skill(self, name: str) -> Skill | None:
        return self._skills.get(name)

    def agents(self) -> list[L2Agent]:
        return list(self._agents.values())

    def load_entry_points(self) -> None:
        """Discover adopter agents/skills declared via packaging metadata."""
        for ep in metadata.entry_points(group="maof.l2_agents"):
            factory = ep.load()
            self.register_agent(factory() if callable(factory) else factory)
        for ep in metadata.entry_points(group="maof.skills"):
            factory = ep.load()
            self.register_skill(factory() if callable(factory) else factory)


#: A process-global default registry that the decorators target.
default_registry = AgentRegistry()


def register_l2_agent(
    registry: AgentRegistry | None = None,
) -> Callable[[type[A]], type[A]]:
    """Class decorator: instantiate and register an L2 agent on import."""
    target = registry or default_registry

    def decorator(cls: type[A]) -> type[A]:
        target.register_agent(cls())
        return cls

    return decorator


def register_skill(
    registry: AgentRegistry | None = None,
) -> Callable[[type[S]], type[S]]:
    target = registry or default_registry

    def decorator(cls: type[S]) -> type[S]:
        target.register_skill(cls())
        return cls

    return decorator


__all__ = [
    "AgentRegistry",
    "default_registry",
    "register_l2_agent",
    "register_skill",
]
