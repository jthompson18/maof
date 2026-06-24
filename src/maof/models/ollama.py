"""Ollama adapter — chat + embeddings, the offline default LLM."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from maof.models.base import BaseEmbeddingProvider, BaseLLMProvider

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.cost.accounting import CostLedger
    from maof.models.base import EmbeddingProvider, LLMProvider


def _count(resp: Any, key: str) -> int:
    value = resp.get(key) if isinstance(resp, dict) else getattr(resp, key, 0)
    return int(value or 0)


class OllamaProvider(BaseLLMProvider):
    def __init__(
        self,
        model: str,
        *,
        host: str | None = None,
        client: Any | None = None,
        cost_ledger: CostLedger | None = None,
    ) -> None:
        super().__init__(model, cost_ledger=cost_ledger)
        if client is None:
            from ollama import AsyncClient

            client = AsyncClient(host=host)
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
        resp = await self._client.chat(model=self.model, messages=messages, format=json_schema)
        message = resp["message"] if isinstance(resp, dict) else resp.message
        text = message["content"] if isinstance(message, dict) else message.content
        return str(text or ""), _count(resp, "prompt_eval_count"), _count(resp, "eval_count")


class OllamaEmbeddingProvider(BaseEmbeddingProvider):
    def __init__(
        self,
        model: str,
        *,
        dimension: int,
        host: str | None = None,
        client: Any | None = None,
    ) -> None:
        if client is None:
            from ollama import AsyncClient

            client = AsyncClient(host=host)
        self._client = client
        self._model = model
        self._dim = dimension

    @property
    def dimension(self) -> int:
        return self._dim

    async def embed(self, texts: list[str]) -> list[list[float]]:
        resp = await self._client.embed(model=self._model, input=texts)
        embeddings = resp["embeddings"] if isinstance(resp, dict) else resp.embeddings
        return [list(vector) for vector in embeddings]


def build(settings: Settings, cost_ledger: CostLedger | None = None) -> LLMProvider:
    return OllamaProvider(settings.model_name, cost_ledger=cost_ledger)


def build_embeddings(settings: Settings) -> EmbeddingProvider:
    return OllamaEmbeddingProvider(settings.embed_model, dimension=settings.embed_dimension)


__all__ = ["OllamaProvider", "OllamaEmbeddingProvider", "build", "build_embeddings"]
