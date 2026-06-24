"""Google Gemini / Vertex adapter via the google-genai SDK.

Requires the ``vertex_gemini`` extra. The SDK is imported dynamically so this
module imports cleanly without it installed.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

from maof.models.base import BaseLLMProvider

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.cost.accounting import CostLedger
    from maof.models.base import LLMProvider


class VertexGeminiProvider(BaseLLMProvider):
    def __init__(
        self,
        model: str,
        *,
        client: Any | None = None,
        cost_ledger: CostLedger | None = None,
        **client_kwargs: Any,
    ) -> None:
        super().__init__(model, cost_ledger=cost_ledger)
        if client is None:
            genai = importlib.import_module("google.genai")
            client = genai.Client(**client_kwargs)
        self._client = client

    async def _complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        json_schema: dict[str, Any] | None = None,
        **opts: Any,
    ) -> tuple[str, int, int]:
        config: dict[str, Any] = {}
        if system:
            config["system_instruction"] = system
        resp = await self._client.aio.models.generate_content(
            model=self.model, contents=prompt, config=config or None
        )
        text = resp.text or ""
        meta = resp.usage_metadata
        in_tokens = int(getattr(meta, "prompt_token_count", 0) or 0)
        out_tokens = int(getattr(meta, "candidates_token_count", 0) or 0)
        return text, in_tokens, out_tokens


def build(settings: Settings, cost_ledger: CostLedger | None = None) -> LLMProvider:
    return VertexGeminiProvider(settings.model_name, cost_ledger=cost_ledger)


__all__ = ["VertexGeminiProvider", "build"]
