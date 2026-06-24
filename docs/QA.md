# MAOF manual QA runbook

A complete local setup to test and evaluate MAOF by hand. Three tiers, each
independently runnable:

- **Tier 0: offline, in-process** (no Docker, no Ollama): the test suite, the
  demo walkthrough, and interactive governance drills.
- **Tier 1: distributed stack** (Docker): the same scenario over real
  RabbitMQ + Postgres with separate orchestrator/worker/approval containers
  (durability, HITL-over-HTTP, kill→resume, DLQs, run ops).
- **Tier 2: model paths** (Ollama): every injection point where a real LLM
  matters (planning, subagents, judge, embeddings, compaction).

All commands run from the repo root.

---

## 0. Prerequisites

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,postgres,api,ollama]"
docker --version           # Docker Desktop running (Tier 1)
ollama --version           # https://ollama.com (Tier 2)
```

### Ollama models (Tier 2)

| Role in MAOF | Model | Size | Why |
|---|---|---|---|
| **L1 planner + LLM judge** | `qwen3:8b` (or `qwen3:14b` on 32 GB+ machines) | 5.2 / 9.3 GB | strongest local structured-JSON output for delegation planning and judge scoring |
| **Subagents (mode b) + compaction** | `llama3.2:3b` | 2.0 GB | fast; subagent outputs get distilled into summaries anyway |
| **Embeddings (registry semantic search)** | `nomic-embed-text` | 274 MB | 768-dim, matching `EMBED_DIMENSION`'s default exactly |
| Single-model fallback | `llama3.1:8b` | 4.9 GB | matches `Settings.model_name`'s default |

```bash
ollama serve &                      # if not already running
ollama pull qwen3:8b llama3.2:3b nomic-embed-text
```

16 GB Macs: the three recommended models coexist comfortably (Ollama loads one
at a time). If qwen3:8b is too heavy, everything below also runs on
`llama3.2:3b` (`QA_PLANNER_MODEL=llama3.2:3b`); expect noticeably weaker judge
discrimination, and comparing the two **is itself a QA scenario**.

### Suggested 90-minute session

Kick off `ollama pull ...` and `docker compose build` in the background first.
Then: Tier 0 (10 min) → interactive drills (20) → embedded bridge (5) →
`docker compose up` (5) → Tier 1 drills in table order (35) → Tier 2 + eval
gate (15) → `docker compose down -v`.

---

## 1. Tier 0: offline smoke + interactive governance drills

```bash
pytest -q                            # the full suite, offline (uses the dev PG on :55432 if present)
python -m examples.po_demo.main     # the whole reference lifecycle, narrated, exit 0
```

Then drive the governance by hand. Each drill states what it proves, pauses
where a human decision matters, and prints the evidence:

```bash
python -m examples.po_demo.qa_interactive            # menu
python -m examples.po_demo.qa_interactive --drill 2  # one drill
python -m examples.po_demo.qa_interactive --all --yes  # non-interactive smoke
```

| # | Drill | You exercise |
|---|---|---|
| 1 | Spend-cap clamp | $500k ask vs $250k cleared → exactly $250k committed |
| 2 | Two-party role-bound approval | wrong role rejected; finance alone insufficient; finance+ops proceeds; deny fails closed; full attribution |
| 3 | Mid-flight cancellation | queued task skipped before any side effect |
| 4 | Catalog quarantine | violating placement → result DLQ'd, join never satisfied |
| 5 | Kill→resume exactly-once | redelivered commitment dedupes; one ledger entry |
| 6 | Certification gate | `shady-broker` flunks its suite; approve refused |
| 7 | Required-source outage | catalog down → run fails closed |
| 8 | Semantic search (hashing baseline) | "tune in-flight pacing" → expediter ranks first |

### Bridge: embedded distributed entry against the dev Postgres

Runs the FULL workflow (registry, signed workflow, result path, wakeups) on
Postgres durability with the in-memory broker, in one process:

```bash
docker start maof-pg 2>/dev/null || docker run -d --name maof-pg -p 55432:5432 \
  -e POSTGRES_USER=maof -e POSTGRES_PASSWORD=maof -e POSTGRES_DB=maof pgvector/pgvector:pg16
DB_URL=postgresql://maof:maof@127.0.0.1:55432/maof EMBEDDED_L2=true \
  MSG_SIGNING_SECRET=demo-secret python -m examples.po_demo.main_distributed
# expect: {'run_id': ..., 'status': 'completed', 'commits': 1, 'committed_usd': 250000, 'invoice_open': True}
```

---

## 2. Tier 1: the distributed stack

```bash
docker compose up        # postgres, rabbitmq, minio, migrate, orchestrator, worker-commitments, worker-fulfillment, approval
```

**Consoles:** RabbitMQ `http://localhost:15672` (guest/guest), to watch
`suppliers.commitments.v1`, `suppliers.fulfillment.v1`, `results` and their `.dlq`s. MinIO
`http://localhost:9001`. Approval API `http://localhost:8080`.

**Host-side CLI + SQL against the stack** (export once per shell):

```bash
export DB_URL=postgresql://maof:maof@localhost:5432/maof MSG_SIGNING_SECRET=change-me-in-prod
export PRINCIPAL_SCOPES=runs:read,runs:write,workflow:author,workflow:approve,registry:author,registry:approve  # CLI admin actions are scope-gated in multi-tenant mode
maof runs list                          # run ops
maof runs promote <id> -o draft.yaml    # promote a successful run -> draft workflow
maof workflow list                      # signed workflow versions
maof registry list                      # approved trust registry
docker compose exec postgres psql -U maof -d maof   # SQL console
```

Useful SQL during any drill:

```sql
SELECT run_id, status, cancel_requested FROM runs ORDER BY created_at DESC LIMIT 5;
SELECT step_ref, idempotency_key FROM run_results WHERE run_id='<id>' ORDER BY id;
SELECT kind, step_ref, status FROM run_wakeups WHERE run_id='<id>';
SELECT event_type, actor->>'id' AS actor, ts FROM audit_events ORDER BY ts DESC LIMIT 20;
SELECT approval_id, status, reason FROM approvals ORDER BY created_at DESC LIMIT 5;
```

The orchestrator container runs **one purchase cycle per boot** (no restart policy);
re-run a scenario with `docker compose up orchestrator` plus env overrides, e.g.
`DEMO_COMMITTED_SPEND_USD=500000 docker compose up orchestrator`.

### Scenario matrix

| Scenario | How to run | Expected | Verify |
|---|---|---|---|
| **Happy path** | `docker compose up` | orchestrator log: `status: completed, commits 1, committed_usd 250000, invoice_open True` | `maof runs trace <id>`; 6 rows in `run_results` (plan→invoice); audit events in logs AND `audit_events`; vendor queues drained in RabbitMQ UI |
| **Clamp** | `DEMO_COMMITTED_SPEND_USD=500000 docker compose up orchestrator` | committed exactly 250000; clamp fires *before* the over-cap rule, so no approval | `run_results` reserve row; `policy_decision` audit event with the clamp nudge |
| **HITL over-cap (curl approve)** | `DEMO_HITL=true DEMO_COMMITTED_SPEND_USD=400000 DEMO_FUNDS_RECEIVED_USD=400000 docker compose up orchestrator` (funds raised so the clamp doesn't pre-empt the 300k cap) | run **blocks**; `approvals` row pending; after curl, run proceeds to completed | mint + curl: `TOKEN=$(python -c "from maof.approval.tokens import mint_approval_token; print(mint_approval_token('<approval_id>','change-me-in-prod'))")` then `curl -X POST "http://localhost:8080/approvals/<approval_id>/approve?token=$TOKEN"` (note: cross-process resolution is **single-party**; role/parties enforcement is in-process, and drill 2 covers it) |
| **Catalog quarantine** | `DEMO_ORDER_CODE_EAST="Bad Name!!" docker compose up orchestrator` | order_east result denied post_result → `results.dlq` grows; run stuck WAITING on the join | RabbitMQ `results.dlq`; `maof runs list` shows waiting → `maof runs cancel <id>` finalizes it |
| **Kill→resume exactly-once** | ① `docker compose stop worker-commitments worker-fulfillment` ② `docker compose up orchestrator` → parks WAITING on the reserve result ③ `docker compose kill orchestrator` ④ `DEMO_AUTOSTART=false docker compose up orchestrator` (resume-only boot: serves collector+poller, starts **no** new run) ⑤ `docker compose start worker-commitments worker-fulfillment` | the **old** run resumes and completes; exactly one run; exactly one reserve result | `maof runs list` (one run, completed); `SELECT count(*) FROM run_results WHERE run_id='<id>' AND step_ref='reserve'` = 1 |
| **Worker kill redelivery** | `docker compose kill worker-commitments` mid-run, then `docker compose start worker-commitments` | RabbitMQ redelivers; the idempotency guard + `run_results` UNIQUE dedupe; run completes | single reserve row; RabbitMQ unacked → redelivered |
| **Workflow signature tamper** | `psql`: `UPDATE workflows SET signature='00ff' WHERE name='po-cycle';` then `maof workflow run po-cycle --module examples.po_demo.qa_interactive:describe_workflow` | **RegistryTrustError**; tampered definitions refuse to run | restore: `maof workflow approve po-cycle 1` (re-signs) |
| **Revoke fails closed** | `maof workflow revoke po-cycle 1`; then `DEMO_BOOTSTRAP=false docker compose up orchestrator` (bootstrap off so boot doesn't self-heal) | orchestrator exits with RegistryTrustError; nothing dispatched | logs; restore with `maof workflow approve po-cycle 1` |
| **Forged message rejected** | RabbitMQ UI → `suppliers.commitments.v1` → Publish message with junk payload/no signature | worker rejects (signature) → retry chain (5s/30s/2m) → `suppliers.commitments.v1.dlq` | worker-commitments logs; DLQ depth after ~2.5 min |
| **Registry CLI lifecycle** | `maof registry submit examples/po_demo/qa/shady-broker.manifest.json && maof registry approve shady-broker && maof registry revoke shady-broker` | submit/approve/revoke round-trip | **known gap:** the CLI store wires no certifier, so approve succeeds here; certification *enforcement* is drill 6 in qa_interactive |
| **Retention prune** | `maof prune --trace-days 0 --audit-days 0` | per-table deleted counts > 0 after a few runs | CLI output; row counts drop |
| **Promote a run** | after a happy path: `maof runs promote <id> -o draft.yaml` | a draft workflow YAML mirroring the run (plan→…→invoice steps, queue mode, `agent_version` pins) | inspect `draft.yaml`; `maof workflow submit draft.yaml` then `maof workflow approve <name> 1` re-signs it for reuse under a new goal |
| **RBAC scope gate** | `PRINCIPAL_SCOPES=runs:read maof workflow approve po-cycle 1` (lacks `workflow:approve`) | **AuthzError**; version stays unsigned | restore full scopes and re-run; `audit_events` then shows `workflow_approved` with `details->>'approver'` |

**Reset everything:** `docker compose down -v` (drops Postgres/queue state).

---

## 3. Tier 2: model paths on Ollama

```bash
python -m examples.po_demo.qa_llm                 # all scenarios (a–f)
python -m examples.po_demo.qa_llm --scenario d    # just the autonomous loop
QA_PLANNER_MODEL=llama3.2:3b python -m examples.po_demo.qa_llm   # small-model variant
```

| Scenario | What it proves | Watch for |
|---|---|---|
| (a) connectivity | server reachable, models pulled | friendly failure + pull hint otherwise |
| (b) semantic search | real `nomic-embed-text` embeddings vs the hashing baseline over the registered manifests | `expediter` ranks first for "expedite carrier selection and routing for urgent orders" under both |
| (c) mode-b subagent | `InProcessSubagent` generates the reconciliation narrative | the model states the $250k committed amount (boundary honored in prose) |
| (d) autonomous loop | the planner model emits **JSON delegations**; subagents execute under the policy + effort budget | 2 objectives proposed, 2 subagents run, distilled summaries |
| (e) LLM-as-judge | rubric-scored eval over the spend-policy dataset | `clamped-overask` passes ≈0.9; `overcommit-bad` fails ≈0.0 |
| (f) compaction | `LLMCompactor` digests 46 dialogue lines | the money facts (clamp, reservation, IO) survive; telemetry noise dropped |

The eval **gate** end-to-end via the CLI (uses `MODEL_*` env, `--criteria` sets
the rubric):

```bash
MODEL_PROVIDER=ollama MODEL_NAME=qwen3:8b EVAL_MIN_PASS_RATE=0.6 \
  maof eval run examples/po_demo/qa/eval_spendcap.json \
  --criteria spend_policy_honored,disclosed_principal_asserted --pass-threshold 0.6
```

Expected: the three `*-bad` violation cases fail, the seven good cases pass →
7/10, gate PASS with a capable judge. **A 3B judge typically lands ~5/10**: it
under-grades the "good but negative-sounding" cases (fail-closed denial, clean
cancellation). Comparing judge models on the same dataset is exactly the
judge-quality evaluation this drill is for.

---

## 4. Troubleshooting

- **Port 5432 busy**: a local Postgres is running; stop it or remap the compose port. (The dev test container `maof-pg` uses 55432 and never conflicts.)
- **`cannot reach Ollama`**: `ollama serve` isn't running, or `OLLAMA_HOST` points elsewhere.
- **Model OOM / very slow**: drop to `QA_PLANNER_MODEL=llama3.2:3b`; close other model consumers (Ollama keeps one model resident).
- **Orchestrator exits immediately with RegistryTrustError**: a previous drill revoked the workflow; `maof workflow approve po-cycle 1` (or boot once without `DEMO_BOOTSTRAP=false`).
- **Stale runs cluttering `maof runs list`**: `docker compose down -v` for a clean slate.

### Documented limitations (by design, verify they hold)

- Cross-process approval resolution is **single-party**: one authorized curl resolves the row. N-of-M **role-bound** approvals are enforced by the in-process gate (drill 2).
- `maof runs wake` fires `external_event` waits; the po-cycle workflow uses a **timer** gate, so it's a no-op here (the surface is exercised by `tests/test_lifecycle.py`).
- `maof registry approve` runs no certification suite (no certifier wired in the CLI); the gate is enforced wherever `RegistryStore(..., certifier=...)` is constructed (see drill 6).

## Env knob reference (demo)

| Knob | Default | Purpose |
|---|---|---|
| `DEMO_COMMITTED_SPEND_USD` / `DEMO_FUNDS_RECEIVED_USD` / `DEMO_SPEND_CAP_USD` | 250000 / 250000 / 300000 | the spend-policy triangle |
| `DEMO_ORDER_CODE_EAST` / `DEMO_ORDER_CODE_WEST` | catalog-conformant names | set a violating name to trigger quarantine |
| `DEMO_HITL` (+ `DEMO_APPROVAL_TIMEOUT_S`) | false / 600 | repo-backed approval gate for curl-approve |
| `DEMO_AUTOSTART` | true | false = resume-only boot (kill→resume drill) |
| `DEMO_BOOTSTRAP` | true | false = no registry/workflow self-heal on boot (revocation drill) |
| `DEMO_TIMEOUT_S` | 300 | distributed-mode serve window |
| `QA_PLANNER_MODEL` / `QA_FAST_MODEL` / `QA_EMBED_MODEL` | qwen3:8b / llama3.2:3b / nomic-embed-text | qa_llm model knobs |
