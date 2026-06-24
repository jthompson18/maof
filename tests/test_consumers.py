"""consumers.yaml parsing + env interpolation + queue_specs/signer helpers."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from maof.transport.consumers import ConsumersConfig, load_consumers

YAML = textwrap.dedent("""
    version: 1
    secrets:
      hmac_keys:
        - {key_id: default, value: "${MSG_SIGNING_SECRET}"}
    workers:
      - name: commitments-1
        queue: tasks.funds_commit
        concurrency: 2
        prefetch: 20
        require_signature: true
        allowed_task_types: [funds_commit, reconciliation]
        tags: {vendor: commitments}
      - name: fulfillment-1
        queue: tasks.order_placement
        concurrency: 2
        prefetch: 20
        require_signature: true
        allowed_task_types: [order_placement, shipment_prep, delivery_metrics]
        tags: {vendor: fulfillment}
    queues:
      - name: tasks.funds_commit
        dlq: {name: tasks.funds_commit.dlq, ttl: 10m, max_len: 20000}
        retry: {steps: ["5s","30s","2m"]}
      - name: tasks.order_placement
        dlq: {name: tasks.order_placement.dlq}
        retry: {steps: ["10s","1m"]}
    """)


def test_load_and_interpolate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MSG_SIGNING_SECRET", "topsecret")
    p = tmp_path / "consumers.yaml"
    p.write_text(YAML)

    cfg = load_consumers(p)
    assert cfg.version == 1
    assert cfg.secrets.hmac_keys[0].value == "topsecret"  # ${ENV} interpolated
    assert len(cfg.workers) == 2
    assert cfg.workers[0].queue == "tasks.funds_commit"
    assert cfg.workers[0].allowed_task_types == ["funds_commit", "reconciliation"]
    assert cfg.workers[0].tags["vendor"] == "commitments"
    assert cfg.workers[0].require_signature is True


def test_queue_specs() -> None:
    cfg = ConsumersConfig.model_validate(
        {
            "version": 1,
            "queues": [
                {
                    "name": "q",
                    "dlq": {"name": "q.dlq", "ttl": "10m", "max_len": 100},
                    "retry": {"steps": ["5s", "30s"]},
                }
            ],
        }
    )
    specs = cfg.queue_specs()
    assert specs[0].name == "q"
    assert specs[0].dlq_name == "q.dlq"
    assert specs[0].dlq_ttl == "10m"
    assert specs[0].dlq_max_len == 100
    assert specs[0].retry_steps == ["5s", "30s"]


def test_signer_from_config() -> None:
    cfg = ConsumersConfig.model_validate(
        {"version": 1, "secrets": {"hmac_keys": [{"key_id": "default", "value": "s3cret"}]}}
    )
    signer = cfg.signer()
    body = b"abc"
    headers = signer.headers(body)
    assert headers["kid"] == "default"
    signer.verify(body, headers)
