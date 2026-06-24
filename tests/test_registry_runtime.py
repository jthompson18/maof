"""Runtime agent/skill registry: decorators, lookups, and
entry-point discovery (stubbed importlib.metadata)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from maof.agents.registry_runtime import AgentRegistry, register_l2_agent, register_skill


def _agent(name: str = "a1", task_types: list[str] | None = None) -> Any:
    return SimpleNamespace(
        name=name, accepted_task_types=task_types or ["t1"], skills=[], context_delegation=[]
    )


def test_register_and_lookup_agents_and_skills() -> None:
    registry = AgentRegistry()
    agent = _agent()
    registry.register_agent(agent)
    registry.register_skill(SimpleNamespace(name="s1", version="v1"))
    assert registry.agent("a1") is agent
    assert registry.agent("missing") is None
    assert registry.agent_for_task_type("t1") is agent
    assert registry.skill("s1") is not None
    assert registry.skill("missing") is None
    assert registry.agents() == [agent]


def test_decorators_register_into_a_custom_registry() -> None:
    registry = AgentRegistry()

    @register_l2_agent(registry)
    class _Agent:
        name = "deco-agent"
        accepted_task_types = ["deco_task"]
        skills: list[Any] = []
        context_delegation: list[Any] = []

    @register_skill(registry)
    class _Skill:
        name = "deco-skill"
        version = "v1"

    assert registry.agent("deco-agent") is not None
    assert registry.agent_for_task_type("deco_task") is not None
    assert registry.skill("deco-skill") is not None


def test_entry_point_discovery_loads_factories_and_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = AgentRegistry()
    instance = _agent("ep-instance", ["ep_task"])

    def fake_entry_points(*, group: str) -> list[Any]:
        if group == "maof.l2_agents":
            # one factory (callable) and one ready-made instance (not callable)
            return [
                SimpleNamespace(load=lambda: (lambda: _agent("ep-factory", ["epf_task"]))),
                SimpleNamespace(load=lambda: instance),
            ]
        return [SimpleNamespace(load=lambda: (lambda: SimpleNamespace(name="ep-skill")))]

    monkeypatch.setattr("maof.agents.registry_runtime.metadata.entry_points", fake_entry_points)
    registry.load_entry_points()
    assert registry.agent("ep-factory") is not None
    assert registry.agent("ep-instance") is instance
    assert registry.skill("ep-skill") is not None
