# MAOF — Multi-Agent Orchestration Framework

[![PyPI](https://img.shields.io/pypi/v/maof.svg)](https://pypi.org/project/maof/)
[![Python versions](https://img.shields.io/pypi/pyversions/maof.svg)](https://pypi.org/project/maof/)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

MAOF is a reusable, installable Python library that abstracts the **orchestration and governance** layer of a hierarchical (L1 → L2) multi-agent system. Adopters inject their own L1 orchestrator, L2 agents, skills, task schemas, and policy rulesets; MAOF provides everything else:

- Two coordination modes: governed async queue dispatch (independent tasks) and in-process context-shared subagents (interdependent decisions)
- A pluggable workflow pipeline, an autonomous orchestrator loop, **and signed YAML workflow definitions** (versioned DAGs: submit → approve and sign → execute; templates, joins, gates, per-step approvals, and version pins) — or **promote a successful run** into a draft definition (`maof runs promote`) to reuse a proven process under new goals
- A full **execution lifecycle**: workers return signed result envelopes; runs **wait** on results, timers, and external events, then resume from checkpoint; cooperative **cancellation**; a runs API/CLI (`maof runs list|show|trace|cancel|wake|promote`)
- Policy-as-code (pre- **and post-result** hooks, versioned and canary rulesets), HITL approval including **N-of-M role-bound multi-party approvals**, multi-tenancy, a **Principal** identity threaded through runs, audit, and approvals, and **scope-based RBAC** (bring-your-own auth: `PRINCIPAL_*` env or an injectable resolver)
- **Source-of-truth agent hosting**: registry-approved context sources auto-attach to planning context (required sources fail closed); agents consult other agents via RBAC-scoped clients (`ctx.agents`, audited); registry-declared JIT resolvers (`catalog://`, `datastore://`)
- A context-engineering layer (token budgeting, compaction, structured note-taking, just-in-time retrieval)
- Durable execution (checkpoint/resume, deterministic idempotency keys, artifact store)
- An admin-gated, signed discovery registry (MCP + A2A) with **semantic capability search**, **certification-gated approval**, registry-driven routing, and per-version canary cohorts
- Provider-agnostic LLM/embedding support (all major SDKs + gateway + BYO)
- Bring-your-own observability (OpenTelemetry + structured event sink + trajectory capture + redacted prompt audit), retention pruning (`maof prune`)
- A cost/token ledger with a "worth-it" policy gate
- An evaluation harness (LLM-as-judge, end-state grading, CI gate)

## Quickstart

Install from PyPI:

```bash
pip install maof                                       # core (lean, offline-installable)
pip install "maof[all]"                                # everything: all adapters + providers
pip install "maof[postgres,rabbitmq,anthropic,api]"    # or only the extras you need
```

Or work from a local checkout (for contributing or running the example):

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"             # core + dev tooling (offline); add extras as needed
pytest -q                           # unit suite runs offline; DB/broker tests skip if absent
python -m examples.quickstart.main  # smallest governed run — one agent, offline
python -m examples.po_demo.main     # the full reference scenario (in-memory, offline)
```

Bring up the full dev stack (Postgres+pgvector, RabbitMQ, MinIO, services) with Docker:

```bash
docker compose up
```

## Worked example

`examples/po_demo/` runs MAOF as a **pure adopter, with zero edits to `src/maof`**. In the reference scenario, a buyer and its partner **share a tenant** (org-attributed principals), and a **catalog agent** and **shared datastore agent**, both injected through the trust registry, act as the source of truth.

A **signed YAML workflow** (`workflows/po-cycle.yaml`) drives the full purchase-order lifecycle:

```
plan → reserve → [wait: window open] → order per region → [join] → actualize → invoice
```

The run spans two platform agents: **Commitments** (planning, funding, billing; funds-committing) and **Fulfillment** (ordering, shipment, delivery metrics). A **spend-cap** ruleset governs it, and the scenario exercises:

- spend clamped to cleared client funds;
- an over-cap commitment that needs a **two-party approval** (buyer finance and partner ops);
- a catalog-violating order code **denied post-result** before anything downstream consumes it;
- a redelivered commitment that commits **exactly once**;
- a clean mid-flight run cancellation;
- an expediting agent selected by **semantic capability search**.

See `tests/test_scenario.py` (the headline test) and `tests/test_po_demo.py`.

## Implementing your own

MAOF ships **zero domain content**. You inject everything domain-specific:

| You implement | Against | Register via |
|---|---|---|
| L2 agents + skills | `maof.agents.base.{L2Agent,Skill}` (or `BaseL2Agent`) | `@register_l2_agent` / entry point `maof.l2_agents` |
| L1 planner / orchestrator | inject a planner into `ActionPlanStage`, or use `OrchestratorLoop` | — |
| Task schemas | `SchemaRegistry.register(schema_id, json_schema)` | runtime |
| Policy rulesets | the condition DSL (YAML) or `CallableRule` | `PolicyRepo` / `NativePolicyEngine(callable_rules=...)` |
| LLM / embedding provider | `maof.models.base.{LLMProvider,EmbeddingProvider}` | `register_llm_provider` / config |
| Third-party agents / MCP | `AgentManifest` → admin-gated, signed discovery registry | `maof registry submit/approve` |
| Workflow definitions | YAML DAGs over registry agents (`maof.workflows`) | `maof workflow submit/approve/run` |
| Source-of-truth agents | `AgentManifest(kind="context_source", required=..., resolver_schemes=[...])` | registry + `attach_registry_context_sources` |

## Docs

- [`examples/quickstart/`](examples/quickstart/): the smallest end-to-end governed run (one agent, offline) — start here.
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): architecture overview, with the reference scenario walked through every layer.
- [`docs/QA.md`](docs/QA.md): manual QA runbook, with tiered local setup (offline → Docker stack → Ollama models), interactive governance drills, and a scenario matrix.
- [`docs/coordination-modes.md`](docs/coordination-modes.md): how to choose a coordination mode, and the one rule that governs it.
- [`docs/deployment.md`](docs/deployment.md): Docker, embedded mode, and rainbow/gradual deploys.

## Toolchain

pip + `pyproject.toml` (hatchling), `src/` layout with per-adapter extras, published to [PyPI](https://pypi.org/project/maof/). Python 3.11+, Pydantic v2, `mypy --strict` / `ruff` / `black`. Release flow and Trusted Publishing setup live in [`docs/publishing.md`](docs/publishing.md).

## Security

Found a vulnerability? Please report it privately via [`SECURITY.md`](SECURITY.md). Do not open a public issue for security reports.

## License

MAOF is licensed under the **Apache License 2.0**; see [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE). It is permissive and embeddable in proprietary products, with an explicit patent grant.
