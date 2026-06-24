"""MCP verified against the official SDK + a real stdio server.

Until now the MCP adapters were exercised only against duck-typed fakes; these
tests spawn tests/fixtures/mcp_echo_server.py (a genuine FastMCP server) over
stdio and drive it through ``MCPSDKClient`` — no network, offline, gated only on
the ``mcp`` package being installed.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, cast

import pytest

pytest.importorskip("mcp")

from maof.agents.mcp_adapter import (  # noqa: E402
    MCPAgentAdapter,
    MCPContextSource,
    mcp_stdio_client,
)
from maof.errors import MAOFError  # noqa: E402
from maof.registry.models import AgentManifest  # noqa: E402
from maof.types import ContextEnvelope, Stage, StageContext, TenantContext  # noqa: E402

FIXTURE = str(Path(__file__).parent / "fixtures" / "mcp_echo_server.py")

MANIFEST = AgentManifest(
    id="echo-fixture",
    kind="mcp_server",
    name="Echo Fixture",
    version="v1",
    endpoint=f"stdio://{FIXTURE}",
    accepted_task_types=["echo_task"],
)


async def test_real_stdio_tool_call_through_agent_adapter() -> None:
    async with mcp_stdio_client(sys.executable, [FIXTURE]) as client:
        adapter = MCPAgentAdapter(MANIFEST, client)
        result = await adapter.handle(
            {"task_id": "t-1", "task_type": "echo_task", "description": "ping", "priority": 5},
            cast(Any, None),
        )
    assert result.status == "ok"
    payload = result.output["result"]
    assert payload["echoed"] == "ping"
    assert payload["task_type"] == "echo_task"


async def test_real_stdio_tool_error_raises() -> None:
    async with mcp_stdio_client(sys.executable, [FIXTURE]) as client:
        adapter = MCPAgentAdapter(MANIFEST, client)
        with pytest.raises(MAOFError):
            await adapter.handle(
                {"task_id": "t-2", "task_type": "always_fails", "description": "boom"},
                cast(Any, None),
            )


async def test_real_stdio_resource_through_context_source() -> None:
    async with mcp_stdio_client(sys.executable, [FIXTURE]) as client:
        source = MCPContextSource(MANIFEST, client, resource="catalog")
        sc = StageContext(run_id="mcp-real", tenant=TenantContext(tenant_id="t1"), goal="g")
        env = ContextEnvelope(
            run_id="mcp-real", tenant_id="t1", intent_id=None, stage=Stage.ACTION_PLAN, goal="g"
        )
        env = await source.contribute(sc, env)
    slice_ = env.semantic_model["Echo Fixture"]
    assert slice_ == {"version": "fixture-v1", "regions": ["east", "west"]}
