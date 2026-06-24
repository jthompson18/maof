"""AWS Bedrock adapter via the Converse API. Requires the ``bedrock`` extra."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from maof.models.base import BaseLLMProvider

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.cost.accounting import CostLedger
    from maof.models.base import LLMProvider


class BedrockProvider(BaseLLMProvider):
    def __init__(
        self,
        model: str,
        *,
        region: str | None = None,
        client: Any | None = None,
        cost_ledger: CostLedger | None = None,
    ) -> None:
        super().__init__(model, cost_ledger=cost_ledger)
        self._client = client
        self._region = region

    async def _complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        json_schema: dict[str, Any] | None = None,
        **opts: Any,
    ) -> tuple[str, int, int]:
        if self._client is not None:
            return await self._converse(self._client, prompt, system)
        import aioboto3

        session = aioboto3.Session()
        async with session.client("bedrock-runtime", region_name=self._region) as client:
            return await self._converse(client, prompt, system)

    async def _converse(self, client: Any, prompt: str, system: str | None) -> tuple[str, int, int]:
        kwargs: dict[str, Any] = {
            "modelId": self.model,
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
        }
        if system:
            kwargs["system"] = [{"text": system}]
        resp = await client.converse(**kwargs)
        text = resp["output"]["message"]["content"][0]["text"]
        usage = resp.get("usage", {})
        return text, int(usage.get("inputTokens", 0)), int(usage.get("outputTokens", 0))


def build(settings: Settings, cost_ledger: CostLedger | None = None) -> LLMProvider:
    return BedrockProvider(settings.model_name, region=settings.region, cost_ledger=cost_ledger)


__all__ = ["BedrockProvider", "build"]
