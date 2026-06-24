# Contributing to MAOF

## Development setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"          # add ,postgres etc. for adapter work
```

Optional live infra for integration tests (skipped if absent):

```bash
docker run -d --name maof-pg -e POSTGRES_USER=maof -e POSTGRES_PASSWORD=maof \
  -e POSTGRES_DB=maof -p 5432:5432 pgvector/pgvector:pg16
export MAOF_TEST_DATABASE_URL=postgresql://maof:maof@127.0.0.1:5432/maof
```

## Quality bar (must stay green)

```bash
mypy src/maof          # --strict, clean
ruff check src tests examples
black --check src tests examples
pytest -q              # DB/broker integration tests skip when infra is absent
```

## Integration tests (live brokers)

The broker adapters have live integration tests (`tests/test_{rabbitmq,kafka,redis,sqs}.py`),
marked `live` and deselected by the default offline run. To run them against real brokers:

```bash
docker compose -f docker-compose.test.yml up -d --wait
export MAOF_TEST_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
export MAOF_TEST_REDIS_URL=redis://127.0.0.1:16379/0
export MAOF_TEST_KAFKA_URL=127.0.0.1:19092
export MAOF_TEST_SQS_URL=http://127.0.0.1:14566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
pytest -m live tests/test_rabbitmq.py tests/test_kafka.py tests/test_redis.py tests/test_sqs.py
docker compose -f docker-compose.test.yml down -v
```

CI runs the same suite in the `integration` job.

## Methodology

- **TDD.** Write the failing test first (from the acceptance criteria), then implement to green.
- **Lock contracts first.** `types.py` and the core service/adapter interface signatures are the
  contract; keep them stable.
- **Zero domain content in `src/maof`.** Agents, schemas, rules, and prompts are always injected
  by the adopter. The only in-repo domain example is `examples/po_demo`.

## Extending MAOF

New adapters implement the relevant Protocol and are wired behind config/extras so the boundary
stays swappable:

- **Broker** (`transport.base.Broker`): publish/consume/ensure_topology with uniform retry/DLQ
  (`transport.retry.RetryPolicy`, per-attempt headers). Add an optional extra; register in
  `transport.factory.build_broker`. See `transport/rabbitmq.py` (default) and the Kafka/Redis/SQS
  adapters for the pattern.
- **LLM / Embedding provider** (`models.base`): subclass `BaseLLMProvider` (it records every call
  to the cost ledger) / `BaseEmbeddingProvider`; register via `register_llm_provider`. Guard the
  SDK import and accept an injected client so the module imports without the extra.
- **Persistence** (`persistence.base` Repos, `memory.base.VectorStore`): a new backend implements
  the repo Protocols; Postgres+pgvector is the default.
- **Event sink / tracer** (`observability`): implement `EventSink.emit`; OTel is no-op until a
  collector is configured.
- **L2 agent / skill** (`agents.base`): subclass `BaseL2Agent`; declare any side-loaded context via
  `context_delegation`. Register with `@register_l2_agent` or the `maof.l2_agents` entry point.

Every adapter that mutates external state must wrap side effects in `IdempotencyGuard.once` so
replay/redelivery is safe, and (for transport) carry the deterministic `idempotency_key`.

## Never weaken governance

Durable resume, subagent dispatch, and registry/A2A imports all remain subject to policy, signing,
RBAC, and audit. Layer new features underneath governance, not around it.

## Known limitations

See "Known limitations" in `docs/deployment.md` before extending the transport or
registry layers; several behaviors there (at-least-once delivery, revocation replay,
Redis reclaim) are deliberate trade-offs that extensions must preserve or improve, not
accidentally regress.
