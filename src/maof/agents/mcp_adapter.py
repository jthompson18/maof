"""MCP adapter.

Wraps a remote MCP server so it satisfies the L2Agent contract (and, separately,
the ContextSource contract). The MCP client is injected so this module imports
without the ``mcp`` extra; adopters pass a connected client (stdio or HTTP) —
either anything satisfying the duck-typed surface (``call_tool``/``read_resource``)
or :class:`MCPSDKClient`, which adapts an official ``mcp`` SDK session to it.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from maof.errors import ConfigError, MAOFError
from maof.types import TaskResult

if TYPE_CHECKING:
    from maof.registry.models import AgentManifest, ContextDeclaration
    from maof.types import ContextEnvelope, L2Context, StageContext


class MCPAgentAdapter:
    """Presents an MCP server as an L2 agent: each task maps to an MCP tool call."""

    def __init__(self, manifest: AgentManifest, client: Any) -> None:
        self.name = manifest.name
        self.accepted_task_types = list(manifest.accepted_task_types)
        self.skills: list[Any] = []
        self.context_delegation: list[ContextDeclaration] = list(manifest.side_loaded_context)
        self._client = client
        self._manifest = manifest

    async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
        tool = task.get("tool") or task.get("task_type", "")
        result = await self._client.call_tool(tool, task)
        return TaskResult(
            status="ok",
            task_id=str(task.get("task_id", "")),
            output={"mcp_tool": tool, "result": result},
        )


class MCPContextSource:
    """Uses an MCP server purely as a context provider (a read-only slice)."""

    def __init__(
        self, manifest: AgentManifest, client: Any, *, resource: str | None = None
    ) -> None:
        self.name = manifest.name
        self._client = client
        self._resource = resource or manifest.id

    async def contribute(self, sc: StageContext, env: ContextEnvelope) -> ContextEnvelope:
        data = await self._client.read_resource(self._resource)
        env.semantic_model[self.name] = data
        return env


def _parse_payload(text: str) -> Any:
    """Tool/resource payloads are JSON when the server sends JSON, else raw text."""
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return text


class MCPSDKClient:
    """Adapts an official ``mcp`` SDK ``ClientSession`` to the duck-typed surface
    the adapters above expect (``call_tool(name, args)`` / ``read_resource(ref)``).

    Importing this module never requires the ``mcp`` extra; connecting does (see
    :func:`mcp_stdio_client`). Fake clients keep satisfying the same surface.
    """

    def __init__(self, session: Any, *, default_resource_scheme: str = "resource") -> None:
        self._session = session
        self._scheme = default_resource_scheme

    async def call_tool(self, name: str, args: dict[str, Any]) -> Any:
        result = await self._session.call_tool(name, arguments=args)
        structured = getattr(result, "structuredContent", None)
        if structured:
            return structured
        parts: list[str] = []
        for block in getattr(result, "content", None) or []:
            text = getattr(block, "text", None)
            if text is not None:
                parts.append(str(text))
        text_out = "\n".join(parts)
        if getattr(result, "isError", False):
            raise MAOFError(f"MCP tool {name!r} returned an error: {text_out or 'no detail'}")
        return _parse_payload(text_out)

    async def read_resource(self, ref: str) -> Any:
        uri = ref if "://" in ref else f"{self._scheme}://{ref}"
        result = await self._session.read_resource(uri)
        for content in getattr(result, "contents", None) or []:
            text = getattr(content, "text", None)
            if text is not None:
                return _parse_payload(str(text))
            blob = getattr(content, "blob", None)
            if blob is not None:
                return blob
        return None


@contextlib.asynccontextmanager
async def mcp_stdio_client(
    command: str,
    args: list[str] | None = None,
    *,
    env: dict[str, str] | None = None,
    default_resource_scheme: str = "resource",
) -> AsyncIterator[MCPSDKClient]:
    """Spawn an MCP server over stdio and yield a connected :class:`MCPSDKClient`.

    The official SDK is imported lazily so the module works without the extra.
    """
    try:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client
    except ImportError as exc:  # pragma: no cover - depends on installed extras
        raise ConfigError(
            "mcp_stdio_client requires the 'mcp' extra (pip install maof[mcp])"
        ) from exc
    params = StdioServerParameters(command=command, args=list(args or []), env=env)
    async with (
        stdio_client(params) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        yield MCPSDKClient(session, default_resource_scheme=default_resource_scheme)


__all__ = ["MCPAgentAdapter", "MCPContextSource", "MCPSDKClient", "mcp_stdio_client"]
