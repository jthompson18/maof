"""Azure OpenAI adapter — reuses the OpenAI SDK's Azure client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from maof.models.openai import OpenAIEmbeddingProvider, OpenAIProvider

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.cost.accounting import CostLedger
    from maof.models.base import EmbeddingProvider, LLMProvider


class AzureOpenAIProvider(OpenAIProvider):
    def __init__(
        self,
        model: str,
        *,
        api_key: str | None = None,
        azure_endpoint: str | None = None,
        api_version: str = "2024-06-01",
        client: Any | None = None,
        cost_ledger: CostLedger | None = None,
    ) -> None:
        if client is None:
            import openai

            # openai>=2.41 types azure_endpoint as str (no None): omit it when unset
            # so the SDK falls back to AZURE_OPENAI_ENDPOINT / raises its clear error.
            if azure_endpoint is None:
                client = openai.AsyncAzureOpenAI(api_key=api_key, api_version=api_version)
            else:
                client = openai.AsyncAzureOpenAI(
                    api_key=api_key, azure_endpoint=azure_endpoint, api_version=api_version
                )
        super().__init__(model, client=client, cost_ledger=cost_ledger)


def build(settings: Settings, cost_ledger: CostLedger | None = None) -> LLMProvider:
    return AzureOpenAIProvider(settings.model_name, cost_ledger=cost_ledger)


def build_embeddings(settings: Settings) -> EmbeddingProvider:
    import openai

    client = openai.AsyncAzureOpenAI()
    return OpenAIEmbeddingProvider(
        settings.embed_model, dimension=settings.embed_dimension, client=client
    )


__all__ = ["AzureOpenAIProvider", "build", "build_embeddings"]
