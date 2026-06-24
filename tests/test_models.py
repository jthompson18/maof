"""Model adapters (mock clients), the cost-ledger recording contract, registry,
and the offline hashing embedder."""

from __future__ import annotations

import json
import math
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from maof.config import Settings
from maof.errors import ConfigError
from maof.models.base import (
    BaseLLMProvider,
    HashingEmbeddingProvider,
    build_embedding_provider,
    build_llm_provider,
    register_llm_provider,
)
from maof.types import CostSummary


class FakeLedger:
    def __init__(self) -> None:
        self.records: list[tuple[str, str, int, int]] = []

    async def record(self, run_id: str, *, model: str, in_tokens: int, out_tokens: int) -> None:
        self.records.append((run_id, model, in_tokens, out_tokens))

    async def total(self, run_id: str) -> CostSummary:
        return CostSummary(run_id=run_id)


class StubProvider(BaseLLMProvider):
    async def _complete(
        self, prompt: str, *, system: Any = None, json_schema: Any = None, **opts: Any
    ) -> tuple[str, int, int]:
        return f"echo:{prompt}", 7, 11


# the shared ledger-recording contract (all adapters inherit this)
async def test_base_provider_records_every_generate() -> None:
    ledger = FakeLedger()
    provider = StubProvider("stub-model", cost_ledger=ledger)
    out = await provider.generate("hi", run_id="r1")
    assert out == "echo:hi"
    assert ledger.records == [("r1", "stub-model", 7, 11)]


async def test_base_provider_skips_ledger_without_run_id() -> None:
    ledger = FakeLedger()
    provider = StubProvider("m", cost_ledger=ledger)
    await provider.generate("hi")
    assert ledger.records == []


# individual adapters behind mock clients
async def test_anthropic_adapter() -> None:
    from maof.models.anthropic import AnthropicProvider

    class FakeAnthropic:
        def __init__(self) -> None:
            self.messages = self
            self.last: dict[str, Any] = {}

        async def create(self, **kw: Any) -> Any:
            self.last = kw
            return SimpleNamespace(
                content=[SimpleNamespace(type="text", text="hello")],
                usage=SimpleNamespace(input_tokens=12, output_tokens=8),
            )

    client = FakeAnthropic()
    ledger = FakeLedger()
    provider = AnthropicProvider("claude-x", client=client, cost_ledger=ledger)
    out = await provider.generate("q", system="be brief", run_id="r1")
    assert out == "hello"
    assert client.last["system"] == "be brief"
    assert ledger.records == [("r1", "claude-x", 12, 8)]


async def test_openai_adapter() -> None:
    from maof.models.openai import OpenAIProvider

    class FakeOpenAI:
        def __init__(self) -> None:
            self.chat = SimpleNamespace(completions=self)
            self.last: dict[str, Any] = {}

        async def create(self, **kw: Any) -> Any:
            self.last = kw
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="oai"))],
                usage=SimpleNamespace(prompt_tokens=5, completion_tokens=9),
            )

    client = FakeOpenAI()
    ledger = FakeLedger()
    provider = OpenAIProvider("gpt", client=client, cost_ledger=ledger)
    out = await provider.generate("q", system="sys", json_schema={"type": "object"}, run_id="r1")
    assert out == "oai"
    assert client.last["response_format"]["type"] == "json_schema"
    assert ledger.records == [("r1", "gpt", 5, 9)]


def test_azure_adapter_constructs_against_real_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    # Regression: openai>=2.41 types azure_endpoint as str (None is rejected), so the
    # adapter must omit the kwarg when unset and let the SDK's env fallback apply.
    pytest.importorskip("openai")
    from maof.models.azure_openai import AzureOpenAIProvider

    explicit = AzureOpenAIProvider(
        "gpt", api_key="k", azure_endpoint="https://unit.test.azure.example"
    )
    assert explicit is not None
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://unit.test.azure.example")
    from_env = AzureOpenAIProvider("gpt", api_key="k")
    assert from_env is not None


async def test_ollama_adapter() -> None:
    from maof.models.ollama import OllamaProvider

    class FakeOllama:
        async def chat(self, **kw: Any) -> dict[str, Any]:
            return {"message": {"content": "olla"}, "prompt_eval_count": 3, "eval_count": 4}

    provider = OllamaProvider("llama", client=FakeOllama(), cost_ledger=(ledger := FakeLedger()))
    assert await provider.generate("q", run_id="r1") == "olla"
    assert ledger.records == [("r1", "llama", 3, 4)]


async def test_bedrock_adapter() -> None:
    from maof.models.bedrock import BedrockProvider

    class FakeBedrock:
        def __init__(self) -> None:
            self.last: dict[str, Any] = {}

        async def converse(self, **kw: Any) -> dict[str, Any]:
            self.last = kw
            return {
                "output": {"message": {"content": [{"text": "bed"}]}},
                "usage": {"inputTokens": 6, "outputTokens": 2},
            }

    client = FakeBedrock()
    ledger = FakeLedger()
    provider = BedrockProvider("anthropic.claude", client=client, cost_ledger=ledger)
    out = await provider.generate("q", system="s", run_id="r1")
    assert out == "bed"
    assert client.last["modelId"] == "anthropic.claude"
    assert ledger.records == [("r1", "anthropic.claude", 6, 2)]


async def test_mistral_adapter() -> None:
    from maof.models.mistral import MistralProvider

    class FakeMistral:
        def __init__(self) -> None:
            self.chat = self

        async def complete_async(self, **kw: Any) -> Any:
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="mis"))],
                usage=SimpleNamespace(prompt_tokens=2, completion_tokens=3),
            )

    provider = MistralProvider(
        "mistral", client=FakeMistral(), cost_ledger=(ledger := FakeLedger())
    )
    assert await provider.generate("q", run_id="r1") == "mis"
    assert ledger.records == [("r1", "mistral", 2, 3)]


async def test_cohere_adapter() -> None:
    from maof.models.cohere import CohereProvider

    class FakeCohere:
        async def chat(self, **kw: Any) -> Any:
            return SimpleNamespace(
                message=SimpleNamespace(content=[SimpleNamespace(text="coh")]),
                usage=SimpleNamespace(tokens=SimpleNamespace(input_tokens=1, output_tokens=2)),
            )

    provider = CohereProvider("command", client=FakeCohere(), cost_ledger=(ledger := FakeLedger()))
    assert await provider.generate("q", run_id="r1") == "coh"
    assert ledger.records == [("r1", "command", 1, 2)]


async def test_vertex_adapter() -> None:
    from maof.models.vertex_gemini import VertexGeminiProvider

    class FakeVertex:
        def __init__(self) -> None:
            self.aio = SimpleNamespace(models=self)

        async def generate_content(self, **kw: Any) -> Any:
            return SimpleNamespace(
                text="gem",
                usage_metadata=SimpleNamespace(prompt_token_count=5, candidates_token_count=6),
            )

    provider = VertexGeminiProvider(
        "gemini", client=FakeVertex(), cost_ledger=(ledger := FakeLedger())
    )
    assert await provider.generate("q", system="s", run_id="r1") == "gem"
    assert ledger.records == [("r1", "gemini", 5, 6)]


async def test_gateway_adapter() -> None:
    from maof.models.gateway import GatewayProvider

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["model"] == "gw-model"
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": "pong"}}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 2},
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    ledger = FakeLedger()
    provider = GatewayProvider(
        "gw-model", base_url="http://gw.local", api_key="k", client=client, cost_ledger=ledger
    )
    out = await provider.generate("ping", run_id="r1")
    assert out == "pong"
    assert ledger.records == [("r1", "gw-model", 4, 2)]
    await client.aclose()


# embeddings + registry
async def test_hashing_embeddings_deterministic_and_normalized() -> None:
    embedder = HashingEmbeddingProvider(dimension=16)
    assert embedder.dimension == 16
    out = await embedder.embed(["hello world", "hello world"])
    assert out[0] == out[1]  # deterministic
    assert len(out[0]) == 16
    assert abs(math.sqrt(sum(x * x for x in out[0])) - 1.0) < 1e-9  # L2-normalized
    other = await embedder.embed(["a totally unrelated phrase"])
    assert other[0] != out[0]


def test_build_unknown_provider_raises() -> None:
    with pytest.raises(ConfigError):
        build_llm_provider(Settings(model_provider="nope"))


async def test_build_byo_provider() -> None:
    class MyProvider(BaseLLMProvider):
        async def _complete(
            self, prompt: str, *, system: Any = None, json_schema: Any = None, **opts: Any
        ) -> tuple[str, int, int]:
            return "custom", 1, 1

    register_llm_provider(
        "myprov", lambda settings, ledger: MyProvider(settings.model_name, cost_ledger=ledger)
    )
    provider = build_llm_provider(Settings(model_provider="myprov", model_name="x"))
    assert await provider.generate("hi") == "custom"


def test_build_gateway_provider() -> None:
    from maof.models.gateway import GatewayProvider

    provider = build_llm_provider(
        Settings(model_provider="gateway", model_name="m", gateway_url="http://gw")
    )
    assert isinstance(provider, GatewayProvider)


def test_build_embedding_hashing_default() -> None:
    embedder = build_embedding_provider(Settings(embed_provider="hashing", embed_dimension=32))
    assert embedder.dimension == 32


async def test_ollama_adapter_system_and_schema_branches() -> None:
    from maof.models.ollama import OllamaProvider

    class FakeOllama:
        def __init__(self) -> None:
            self.last: dict[str, Any] = {}

        async def chat(self, **kw: Any) -> dict[str, Any]:
            self.last = kw
            return {"message": {"content": "ok"}, "prompt_eval_count": 2, "eval_count": 3}

    client = FakeOllama()
    provider = OllamaProvider("llama", client=client)
    out = await provider.generate("q", system="be terse", json_schema={"type": "object"})
    assert out == "ok"
    assert client.last["messages"][0] == {"role": "system", "content": "be terse"}
    assert client.last["format"] == {"type": "object"}


async def test_ollama_embedding_provider() -> None:
    from maof.models.ollama import OllamaEmbeddingProvider

    class FakeOllamaEmbed:
        async def embed(self, **kw: Any) -> dict[str, Any]:
            assert kw["input"] == ["a", "b"]
            return {"embeddings": [[0.1, 0.2], [0.3, 0.4]]}

    provider = OllamaEmbeddingProvider("nomic", dimension=2, client=FakeOllamaEmbed())
    assert provider.dimension == 2
    assert await provider.embed(["a", "b"]) == [[0.1, 0.2], [0.3, 0.4]]


async def test_openai_embedding_provider() -> None:
    from maof.models.openai import OpenAIEmbeddingProvider

    class FakeEmbeddings:
        async def create(self, **kw: Any) -> Any:
            assert kw["model"] == "text-embedding-x"
            return SimpleNamespace(
                data=[SimpleNamespace(embedding=[1.0, 0.0]), SimpleNamespace(embedding=[0.0, 1.0])]
            )

    client = SimpleNamespace(embeddings=FakeEmbeddings())
    provider = OpenAIEmbeddingProvider("text-embedding-x", dimension=2, client=client)
    assert provider.dimension == 2
    assert await provider.embed(["x", "y"]) == [[1.0, 0.0], [0.0, 1.0]]
