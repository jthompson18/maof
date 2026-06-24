"""Single configuration surface.

One ``Settings`` object sourced from environment + optional YAML. Everything an
adopter wires lives here, with safe, offline-capable defaults. Precedence:
init kwargs > environment > .env > YAML (``MAOF_CONFIG_YAML``) > secrets.

Uses the reference's conventional env names where sensible (``REGION``,
``RULESET_REF``, ``SANDBOX``, ``MSG_SIGNING_SECRET``, ``EMBED_MODEL``, ...) plus
the new surfaces (coordination, context engineering, durability, cost, eval,
protocols).
"""

from __future__ import annotations

import os
from typing import Literal

from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

try:  # YAML source added in pydantic-settings >= 2.2
    from pydantic_settings import YamlConfigSettingsSource

    _HAS_YAML_SOURCE = True
except ImportError:  # pragma: no cover - depends on installed version
    _HAS_YAML_SOURCE = False


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )

    # transport / persistence
    broker_kind: Literal["rabbitmq", "kafka", "redis", "sqs", "memory"] = "rabbitmq"
    broker_url: str = "amqp://guest:guest@localhost:5672/"
    db_url: str = "postgresql://maof:maof@localhost:5432/maof"
    db_pool_min: int = 1
    db_pool_max: int = 10  # keep above worker concurrency (guards hold a connection)
    task_queue: str = "tasks"
    audit_queue: str = "audit"
    approval_host: str = "0.0.0.0"  # noqa: S104 - container service binds all interfaces
    approval_port: int = 8080

    # models / embeddings
    model_provider: str = "ollama"  # offline default
    model_name: str = "llama3.1"
    embed_provider: str = "hashing"  # offline-safe default; ollama/openai/... for real embeddings
    embed_model: str = "nomic-embed-text"
    embed_dimension: int = 768
    gateway_url: str | None = None  # unified-gateway (LiteLLM-style) base URL
    gateway_api_key: str | None = None

    # signing
    msg_signing_secret: str = ""
    msg_signing_key_id: str = "default"
    # Verify signatures on inbound task messages and result envelopes. True
    # (default) is the safe posture: workers and the result collector reject
    # unsigned or forged messages. Set False ONLY for a trusted single-process /
    # offline deployment (e.g. embedded mode) with no shared broker.
    require_signature: bool = True
    registry_signing_key: str = ""
    # Key sourcing: "env" (the fields above), "file" (mounted JSON secret),
    # or "vault" (HashiCorp KV-v2 via the 'vault' extra). See transport/signing.py.
    signing_key_provider: Literal["env", "file", "vault"] = "env"
    signing_keys_file: str = ""
    vault_url: str = ""
    vault_token: str = ""
    vault_secret_path: str = ""
    vault_mount: str = "secret"

    # governance / tenancy / HITL
    tenancy_mode: Literal["single", "multi"] = "multi"
    # Operator identity for CLI/admin actions — bring-your-own auth wires these
    # (e.g. CI/gateway sets PRINCIPAL_ID/PRINCIPAL_SCOPES). With none set,
    # single-tenant trusts the local operator; multi-tenant fails closed.
    principal_id: str = ""
    principal_scopes: str = ""  # comma-separated scopes (e.g. "workflow:author,runs:read")
    principal_roles: str = ""  # comma-separated roles
    principal_org: str = ""  # intra-tenant attribution (e.g. "buyer")
    hitl_enabled: bool = True
    # When a plan requires approval but HITL is off / no gate is wired:
    # "deny" fails closed (default), "allow" publishes with only the audit flag.
    approval_fallback: Literal["allow", "deny"] = "deny"
    ruleset_ref: str = "default"

    # run mode / placement
    sandbox: bool = True
    dry_run: bool = True
    region: str = "us-east-1"
    embedded_l2: bool = False
    consumers_yaml_path: str | None = None

    # observability
    otel_endpoint: str | None = None
    event_sinks: list[str] = Field(default_factory=lambda: ["stdout"])

    # coordination / orchestration
    default_coordination_mode: Literal["queue", "in_process"] = "queue"
    orchestration_mode: Literal["workflow", "autonomous"] = "workflow"
    max_subagents: int = 3
    default_max_tool_calls: int = 10

    # context engineering
    model_context_window: int = 200_000
    context_token_budget: int = 100_000
    compaction_threshold: float = 0.85  # fraction of the window
    compaction_model: str | None = None
    redaction_enabled: bool = True

    # durable execution
    checkpoint_backend: Literal["postgres", "memory"] = "postgres"
    run_store_dsn: str | None = None
    artifact_backend: Literal["pg", "s3"] = "pg"
    artifact_bucket: str | None = None
    idempotency_key_ttl_s: int = 86_400
    trace_retention_days: int = 30
    audit_retention_days: int = 90

    # cost / worth-it gate
    run_token_budget: int = 1_000_000
    run_cost_budget_usd: float = 50.0
    model_prices: dict[str, float] = Field(default_factory=dict)  # usd per 1k tokens
    worth_it_fanout_cap: int = 10
    worth_it_cost_cap_usd: float = 10.0
    worth_it_action: Literal["deny", "require_approval", "cap"] = "require_approval"

    # eval
    eval_dataset_path: str | None = None
    judge_model: str | None = None
    eval_min_pass_rate: float = 0.8

    # protocols
    a2a_enabled: bool = False
    a2a_card_endpoint: str | None = None

    def security_warnings(self) -> list[str]:
        """Insecure-for-production settings to surface at startup (dev defaults).

        These are warnings, not failures: the offline dev stack relies on them.
        Override them in any shared/production deployment (see the "Production
        hardening" section of docs/deployment.md).
        """
        out: list[str] = []
        if not self.require_signature:
            out.append("require_signature=false: inbound messages are NOT signature-verified.")
        elif self.signing_key_provider == "env" and not self.msg_signing_secret:
            out.append("MSG_SIGNING_SECRET is empty: message signing is not effective.")
        if "guest:guest@" in self.broker_url:
            out.append("broker_url uses the default guest:guest credentials.")
        if "maof:maof@" in self.db_url:
            out.append("db_url uses the default maof:maof credentials.")
        return out

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """env overrides YAML; YAML is loaded only when ``MAOF_CONFIG_YAML`` points
        at an existing file."""
        sources: list[PydanticBaseSettingsSource] = [
            init_settings,
            env_settings,
            dotenv_settings,
        ]
        yaml_path = os.getenv("MAOF_CONFIG_YAML")
        if _HAS_YAML_SOURCE and yaml_path and os.path.isfile(yaml_path):
            sources.append(YamlConfigSettingsSource(settings_cls, yaml_file=yaml_path))
        sources.append(file_secret_settings)
        return tuple(sources)


__all__ = ["Settings"]
