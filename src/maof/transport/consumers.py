"""Declarative worker/queue config — the ``consumers.yaml`` model.

Parsed + validated by Pydantic, with ``${ENV}`` interpolation for secrets. The
worker-pool *runner* that consumes these and dispatches to L2 agents lives in
``workers/pool.py``; this module owns parsing and the topology/signer helpers
the runner uses.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from maof.errors import ConfigError
from maof.transport.signing import Signer
from maof.types import QueueSpec


class HmacKey(BaseModel):
    key_id: str
    value: str


class SecretsConfig(BaseModel):
    hmac_keys: list[HmacKey] = Field(default_factory=list)


class WorkerConfig(BaseModel):
    name: str
    queue: str
    concurrency: int = 1
    prefetch: int = 10
    require_signature: bool = True
    allowed_task_types: list[str] = Field(default_factory=list)
    tags: dict[str, str] = Field(default_factory=dict)


class DLQConfig(BaseModel):
    name: str
    ttl: str | None = None
    max_len: int | None = None


class RetryConfig(BaseModel):
    steps: list[str] = Field(default_factory=list)


class QueueConfig(BaseModel):
    name: str
    dlq: DLQConfig | None = None
    retry: RetryConfig | None = None

    def to_queue_spec(self) -> QueueSpec:
        return QueueSpec(
            name=self.name,
            dlq_name=self.dlq.name if self.dlq is not None else None,
            dlq_ttl=self.dlq.ttl if self.dlq is not None else None,
            dlq_max_len=self.dlq.max_len if self.dlq is not None else None,
            retry_steps=self.retry.steps if self.retry is not None else [],
        )


class ResultsConfig(BaseModel):
    """The results queue the ResultCollector consumes."""

    queue: str = "results"
    dlq: DLQConfig | None = None
    retry: RetryConfig | None = None

    def to_queue_spec(self) -> QueueSpec:
        return QueueSpec(
            name=self.queue,
            dlq_name=self.dlq.name if self.dlq is not None else None,
            dlq_ttl=self.dlq.ttl if self.dlq is not None else None,
            dlq_max_len=self.dlq.max_len if self.dlq is not None else None,
            retry_steps=self.retry.steps if self.retry is not None else [],
        )


class ConsumersConfig(BaseModel):
    version: int = 1
    secrets: SecretsConfig = Field(default_factory=SecretsConfig)
    workers: list[WorkerConfig] = Field(default_factory=list)
    queues: list[QueueConfig] = Field(default_factory=list)
    results: ResultsConfig = Field(default_factory=ResultsConfig)

    def queue_specs(self) -> list[QueueSpec]:
        return [q.to_queue_spec() for q in self.queues] + [self.results.to_queue_spec()]

    def signer(self, active_kid: str | None = None) -> Signer:
        keys = {k.key_id: k.value for k in self.secrets.hmac_keys}
        if not keys:
            raise ConfigError("no hmac_keys configured in consumers secrets")
        kid = active_kid if active_kid is not None else next(iter(keys))
        return Signer(keys, active_kid=kid)


_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _interpolate(value: Any, env: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        return _ENV_RE.sub(lambda m: env.get(m.group(1), ""), value)
    if isinstance(value, list):
        return [_interpolate(v, env) for v in value]
    if isinstance(value, dict):
        return {k: _interpolate(v, env) for k, v in value.items()}
    return value


def load_consumers(path: str | Path, *, env: Mapping[str, str] | None = None) -> ConsumersConfig:
    """Load + validate a consumers.yaml, interpolating ``${ENV}`` references."""
    environ: Mapping[str, str] = os.environ if env is None else env
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    data = _interpolate(raw or {}, environ)
    return ConsumersConfig.model_validate(data)


__all__ = [
    "HmacKey",
    "SecretsConfig",
    "WorkerConfig",
    "DLQConfig",
    "RetryConfig",
    "QueueConfig",
    "ConsumersConfig",
    "load_consumers",
]
