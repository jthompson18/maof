"""Anthropic Claude adapter. Requires the ``anthropic`` extra."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from maof.models.base import BaseLLMProvider

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.cost.accounting import CostLedger
    from maof.models.base import LLMProvider


class AnthropicProvider(BaseLLMProvider):
    def __init__(
        self,
        model: str,
        *,
        api_key: str | None = None,
        client: Any | None = None,
        cost_ledger: CostLedger | None = None,
    ) -> None:
        super().__init__(model, cost_ledger=cost_ledger)
        if client is None:
            import anthropic

            client = anthropic.AsyncAnthropic(api_key=api_key)
        self._client = client

    async def _complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        json_schema: dict[str, Any] | None = None,
        **opts: Any,
    ) -> tuple[str, int, int]:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "max_tokens": int(opts.get("max_tokens", 1024)),
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            kwargs["system"] = system
        resp = await self._client.messages.create(**kwargs)
        text = "".join(
            getattr(block, "text", "")
            for block in resp.content
            if getattr(block, "type", "") == "text"
        )
        return text, int(resp.usage.input_tokens), int(resp.usage.output_tokens)


def build(settings: Settings, cost_ledger: CostLedger | None = None) -> LLMProvider:
    return AnthropicProvider(settings.model_name, cost_ledger=cost_ledger)


__all__ = ["AnthropicProvider", "build"]
