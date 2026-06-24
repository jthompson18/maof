"""Provider-agnostic LLM + embedding interfaces + base classes + registry.

Shipped adapters (Anthropic, OpenAI, Azure, Bedrock, Gemini/Vertex, Mistral,
Cohere, Ollama) and a unified gateway implement these; adopters can bring their
own via :func:`register_llm_provider`. ``BaseLLMProvider`` centralizes the
cost-ledger recording so **every** ``generate`` call ledgers tokens. The
embedding ``dimension`` flows into the vector-store schema.
"""

from __future__ import annotations

import hashlib
import importlib
import math
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from maof.errors import ConfigError

if TYPE_CHECKING:
    from maof.config import Settings
    from maof.cost.accounting import CostLedger


@runtime_checkable
class LLMProvider(Protocol):
    async def generate(
        self,
        prompt: str,
        *,
        system: str | None = None,
        json_schema: dict[str, Any] | None = None,
        **opts: Any,
    ) -> str: ...


@runtime_checkable
class EmbeddingProvider(Protocol):
    async def embed(self, texts: list[str]) -> list[list[float]]: ...

    @property
    def dimension(self) -> int: ...


class BaseLLMProvider(ABC):
    """Adapter base. Subclasses implement :meth:`_complete`; this class records
    token usage to the cost ledger on every call (when a ``run_id`` is supplied)."""

    def __init__(
        self,
        model: str,
        *,
        cost_ledger: CostLedger | None = None,
        prompt_audit: object | None = None,
    ) -> None:
        self.model = model
        self._cost_ledger = cost_ledger
        self._prompt_audit = prompt_audit  # PromptAuditRepo: automatic redacted capture

    async def generate(
        self,
        prompt: str,
        *,
        system: str | None = None,
        json_schema: dict[str, Any] | None = None,
        **opts: Any,
    ) -> str:
        run_id = opts.pop("run_id", None)
        tenant_id = opts.pop("tenant_id", None)
        text, in_tokens, out_tokens = await self._complete(
            prompt, system=system, json_schema=json_schema, **opts
        )
        if self._cost_ledger is not None and run_id is not None:
            await self._cost_ledger.record(
                str(run_id), model=self.model, in_tokens=in_tokens, out_tokens=out_tokens
            )
        if self._prompt_audit is not None and run_id is not None:
            # Compliance capture: every prompt/response persists, PII-redacted.
            from maof.context.redactor import scrub_text
            from maof.types import TenantContext

            await self._prompt_audit.record(  # type: ignore[attr-defined]
                TenantContext(tenant_id=str(tenant_id or "unknown")),
                str(run_id),
                scrub_text(prompt),
                scrub_text(text),
            )
        return text

    @abstractmethod
    async def _complete(
        self,
        prompt: str,
        *,
        system: str | None,
        json_schema: dict[str, Any] | None,
        **opts: Any,
    ) -> tuple[str, int, int]:
        """Return ``(text, input_tokens, output_tokens)`` from the provider SDK."""


class BaseEmbeddingProvider(ABC):
    @abstractmethod
    async def embed(self, texts: list[str]) -> list[list[float]]: ...

    @property
    @abstractmethod
    def dimension(self) -> int: ...


class HashingEmbeddingProvider(BaseEmbeddingProvider):
    """Deterministic, dependency-free embeddings — the offline default for dev and
    tests. Hashes whitespace tokens into buckets and L2-normalizes (cosine-ready)."""

    def __init__(self, dimension: int = 768) -> None:
        self._dim = dimension

    @property
    def dimension(self) -> int:
        return self._dim

    async def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_one(text) for text in texts]

    def _embed_one(self, text: str) -> list[float]:
        vec = [0.0] * self._dim
        for token in text.lower().split():
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            bucket = int.from_bytes(digest[:8], "big") % self._dim
            vec[bucket] += 1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]


# Provider registry (custom / bring-your-own)
LLMFactory = Callable[["Settings", "CostLedger | None"], LLMProvider]
EmbeddingFactory = Callable[["Settings"], EmbeddingProvider]

_CUSTOM_LLM: dict[str, LLMFactory] = {}
_CUSTOM_EMBED: dict[str, EmbeddingFactory] = {}

# name -> (module, factory attribute). Lazily imported so an unused provider's SDK
# is never imported.
_BUILTIN_LLM: dict[str, tuple[str, str]] = {
    "anthropic": ("maof.models.anthropic", "build"),
    "openai": ("maof.models.openai", "build"),
    "azure_openai": ("maof.models.azure_openai", "build"),
    "bedrock": ("maof.models.bedrock", "build"),
    "vertex_gemini": ("maof.models.vertex_gemini", "build"),
    "mistral": ("maof.models.mistral", "build"),
    "cohere": ("maof.models.cohere", "build"),
    "ollama": ("maof.models.ollama", "build"),
    "gateway": ("maof.models.gateway", "build"),
}


def register_llm_provider(name: str, factory: LLMFactory) -> None:
    _CUSTOM_LLM[name] = factory


def register_embedding_provider(name: str, factory: EmbeddingFactory) -> None:
    _CUSTOM_EMBED[name] = factory


def build_llm_provider(settings: Settings, *, cost_ledger: CostLedger | None = None) -> LLMProvider:
    name = settings.model_provider
    if name in _CUSTOM_LLM:
        return _CUSTOM_LLM[name](settings, cost_ledger)
    if name in _BUILTIN_LLM:
        module_name, attr = _BUILTIN_LLM[name]
        factory: LLMFactory = getattr(importlib.import_module(module_name), attr)
        return factory(settings, cost_ledger)
    raise ConfigError(f"unknown model_provider: {name!r}")


def build_embedding_provider(settings: Settings) -> EmbeddingProvider:
    name = settings.embed_provider
    if name in _CUSTOM_EMBED:
        return _CUSTOM_EMBED[name](settings)
    if name == "hashing":
        return HashingEmbeddingProvider(settings.embed_dimension)
    builtin = {
        "openai": ("maof.models.openai", "build_embeddings"),
        "azure_openai": ("maof.models.azure_openai", "build_embeddings"),
        "ollama": ("maof.models.ollama", "build_embeddings"),
    }
    if name in builtin:
        module_name, attr = builtin[name]
        factory: EmbeddingFactory = getattr(importlib.import_module(module_name), attr)
        return factory(settings)
    raise ConfigError(f"unknown embed_provider: {name!r}")


__all__ = [
    "LLMProvider",
    "EmbeddingProvider",
    "BaseLLMProvider",
    "BaseEmbeddingProvider",
    "HashingEmbeddingProvider",
    "register_llm_provider",
    "register_embedding_provider",
    "build_llm_provider",
    "build_embedding_provider",
]
