# Changelog

All notable changes to MAOF are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-06-24

Initial public release of MAOF: the reusable orchestration and governance layer of a hierarchical
(L1 → L2) multi-agent system, shipping zero domain content.

### Added

- Two coordination modes: governed async queue dispatch and in-process context-shared subagents.
- Workflow-as-data: a pluggable workflow pipeline, an autonomous orchestrator loop, and signed YAML
  workflow definitions; plus `maof runs promote` to derive a reusable workflow from a successful run
  (resolved agent versions captured as per-step pins).
- Full execution lifecycle: signed result envelopes, wait/resume from checkpoint, cooperative
  cancellation, and a runs API/CLI (`list|show|trace|cancel|wake|promote`).
- Governance: policy-as-code (pre/post-result hooks, versioned + canary rulesets), N-of-M role-bound
  multi-party HITL approvals, multi-tenancy, a `Principal` identity threaded through runs, audit, and
  approvals, and scope-based bring-your-own RBAC (`require_scopes`, `PRINCIPAL_*`, an injectable
  `principal_resolver`) gating every admin mutation across runs, workflows, and the registry.
- Source-of-truth agent hosting, context-engineering layer (budgeting, compaction, JIT retrieval),
  durable execution (checkpoint/resume, idempotency keys, artifact store).
- Admin-gated signed discovery registry (MCP + A2A) with semantic capability search and per-version
  canary cohorts.
- Provider-agnostic LLM/embedding support (all major SDKs + gateway + BYO), OpenTelemetry-based
  observability, cost/token ledger, and an evaluation harness with a CI gate.
- `examples/po_demo` reference scenario running end-to-end with zero edits to `src/maof`, and
  `examples/quickstart/` — a minimal, offline, in-process governed run (one agent).
- Startup security warnings for insecure dev defaults; graceful worker shutdown on `SIGTERM`/`SIGINT`.
- CI: CodeQL (SAST), a Python 3.11/3.12 test matrix, and live broker integration tests (RabbitMQ,
  Kafka, Redis, SQS) via `docker-compose.test.yml`.
- Open-source release under the Apache License 2.0, published to PyPI via Trusted Publishing.

### Security

- Message signing is safe-by-default: `Settings.require_signature` defaults to `true`, and
  `maof run-worker` refuses to start when signing is required but no key is configured.

### Changed

- Core dependencies carry upper-version caps (`pydantic<3`, `httpx<1`, `jsonschema<5`, …) for
  reproducible installs.

[1.0.0]: https://github.com/jthompson18/maof/releases/tag/v1.0.0
