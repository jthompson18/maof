"""A real MCP stdio server (official SDK) used by tests/test_mcp_real.py.

One tool that echoes a MAOF task, one tool that always errors, and one JSON
resource — enough to exercise MCPAgentAdapter / MCPContextSource / MCPSDKClient
against the genuine protocol over stdio, offline.
"""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("maof-echo")


@mcp.tool()
def echo_task(
    task_id: str = "", task_type: str = "", description: str = "", priority: int = 5
) -> dict[str, object]:
    """Echo the MAOF task fields back (the adapter maps task_type -> tool name)."""
    return {"echoed": description, "task_type": task_type, "priority": priority}


@mcp.tool()
def always_fails(task_id: str = "", task_type: str = "", description: str = "") -> str:
    """Raise so the client sees an MCP error result (isError=true)."""
    raise ValueError("this tool always fails")


@mcp.resource("resource://catalog")
def catalog() -> str:
    """A JSON resource slice, as a source-of-truth server would serve it."""
    return json.dumps({"version": "fixture-v1", "regions": ["east", "west"]})


if __name__ == "__main__":
    mcp.run()
