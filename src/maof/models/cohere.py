"""Cohere adapter via the v2 chat API. Requires the ``cohere`` extra."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from maof.models.base import BaseLLMProvider

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.cost.accounting import CostLedger
    from maof.models.base import LLMProvider


class CohereProvider(BaseLLMProvider):
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
            import cohere

            client = cohere.AsyncClientV2(api_key=api_key)
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
        resp = await self._client.chat(model=self.model, messages=messages)
        text = resp.message.content[0].text
        tokens = resp.usage.tokens
        return text, int(tokens.input_tokens), int(tokens.output_tokens)


def build(settings: Settings, cost_ledger: CostLedger | None = None) -> LLMProvider:
    return CohereProvider(settings.model_name, cost_ledger=cost_ledger)


__all__ = ["CohereProvider", "build"]
