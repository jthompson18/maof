"""Mistral adapter. Requires the ``mistral`` extra."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from maof.models.base import BaseLLMProvider

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.cost.accounting import CostLedger
    from maof.models.base import LLMProvider


class MistralProvider(BaseLLMProvider):
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
            from mistralai import Mistral

            client = Mistral(api_key=api_key)
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
        resp = await self._client.chat.complete_async(model=self.model, messages=messages)
        text = resp.choices[0].message.content or ""
        usage = resp.usage
        return text, int(usage.prompt_tokens), int(usage.completion_tokens)


def build(settings: Settings, cost_ledger: CostLedger | None = None) -> LLMProvider:
    return MistralProvider(settings.model_name, cost_ledger=cost_ledger)


__all__ = ["MistralProvider", "build"]
