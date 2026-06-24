"""OpenAI adapter — chat + embeddings. Requires the ``openai`` extra."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from maof.models.base import BaseEmbeddingProvider, BaseLLMProvider

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.cost.accounting import CostLedger
    from maof.models.base import EmbeddingProvider, LLMProvider


class OpenAIProvider(BaseLLMProvider):
    def __init__(
        self,
        model: str,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        client: Any | None = None,
        cost_ledger: CostLedger | None = None,
    ) -> None:
        super().__init__(model, cost_ledger=cost_ledger)
        if client is None:
            import openai

            client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url)
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
        kwargs: dict[str, Any] = {"model": self.model, "messages": messages}
        if json_schema is not None:
            kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "out", "schema": json_schema},
            }
        resp = await self._client.chat.completions.create(**kwargs)
        text = resp.choices[0].message.content or ""
        usage = resp.usage
        return text, int(usage.prompt_tokens), int(usage.completion_tokens)


class OpenAIEmbeddingProvider(BaseEmbeddingProvider):
    def __init__(
        self,
        model: str,
        *,
        dimension: int,
        api_key: str | None = None,
        base_url: str | None = None,
        client: Any | None = None,
    ) -> None:
        if client is None:
            import openai

            client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url)
        self._client = client
        self._model = model
        self._dim = dimension

    @property
    def dimension(self) -> int:
        return self._dim

    async def embed(self, texts: list[str]) -> list[list[float]]:
        resp = await self._client.embeddings.create(model=self._model, input=texts)
        return [list(item.embedding) for item in resp.data]


def build(settings: Settings, cost_ledger: CostLedger | None = None) -> LLMProvider:
    return OpenAIProvider(settings.model_name, cost_ledger=cost_ledger)


def build_embeddings(settings: Settings) -> EmbeddingProvider:
    return OpenAIEmbeddingProvider(settings.embed_model, dimension=settings.embed_dimension)


__all__ = ["OpenAIProvider", "OpenAIEmbeddingProvider", "build", "build_embeddings"]
