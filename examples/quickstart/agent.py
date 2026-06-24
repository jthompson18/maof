"""The one piece an adopter writes: an L2 agent. Everything else is MAOF machinery."""

from __future__ import annotations

from typing import Any

from maof.agents.base import BaseL2Agent
from maof.types import L2Context, TaskResult


class HelloAgent(BaseL2Agent):
    """Greets the name in the task payload. A real agent would call tools, an LLM,
    or a downstream service here; the contract is the same: take a task body + an
    ``L2Context`` and return a ``TaskResult``."""

    name = "hello"
    accepted_task_types = ["hello"]

    async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
        name = task.get("payload", {}).get("name", "world")
        return TaskResult(
            status="ok",
            task_id=task["task_id"],
            output={"greeting": f"Hello, {name}!"},
        )
