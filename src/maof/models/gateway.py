"""Unified gateway adapter (LiteLLM-style) — OpenAI-compatible HTTP passthrough.

Talks to any OpenAI-compatible ``/chat/completions`` endpoint over httpx, so a
single gateway can front many providers. Uses the core httpx dependency (no extra).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx

from maof.errors import ConfigError
from maof.models.base import BaseLLMProvider

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.cost.accounting import CostLedger
    from maof.models.base import LLMProvider


class GatewayProvider(BaseLLMProvider):
    def __init__(
        self,
        model: str,
        *,
        base_url: str,
        api_key: str | None = None,
        client: httpx.AsyncClient | None = None,
        cost_ledger: CostLedger | None = None,
    ) -> None:
        super().__init__(model, cost_ledger=cost_ledger)
        self._base_url = base_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self._client = client

    async def _complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        json_schema: dict[str, Any] | None = None,
        **opts: Any,
    ) -> tuple[str, int, int]:
        messages: list[dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        payload: dict[str, Any] = {"model": self.model, "messages": messages}

        client = self._client if self._client is not None else httpx.AsyncClient()
        owns_client = self._client is None
        try:
            response = await client.post(
                f"{self._base_url}/chat/completions", json=payload, headers=self._headers
            )
            data = response.json()
        finally:
            if owns_client:
                await client.aclose()

        text: str = data["choices"][0]["message"]["content"]
        usage = data.get("usage", {})
        return text, int(usage.get("prompt_tokens", 0)), int(usage.get("completion_tokens", 0))


def build(settings: Settings, cost_ledger: CostLedger | None = None) -> LLMProvider:
    if not settings.gateway_url:
        raise ConfigError("gateway_url must be set to use the gateway provider")
    return GatewayProvider(
        settings.model_name,
        base_url=settings.gateway_url,
        api_key=settings.gateway_api_key,
        cost_ledger=cost_ledger,
    )


__all__ = ["GatewayProvider", "build"]
