# Deployment

## Docker dev stack

`docker compose up` brings up Postgres+pgvector, RabbitMQ (management UI on
:15672), MinIO (artifact store, console on :9001), runs DB migrations once
(`maof migrate`), then starts the orchestrator, a worker, and the approval service
(:8080). Defaults run offline, with no cloud credentials required (point a local Ollama
at `host.docker.internal` for the local-model path; cloud providers need only env
creds).

Images (multi-stage, non-root, healthchecked):

- `docker/orchestrator.Dockerfile`: runs the adopter's L1 workload (the demo by default).
- `docker/worker.Dockerfile`: `maof run-worker --queue ... --consumers consumers.yaml`.
- `docker/approval.Dockerfile`: `maof run-approval` (FastAPI).

Migrations run on startup via the one-shot `migrate` service; other services wait on
`service_completed_successfully`.

## Embedded mode

For simple deployments, run the orchestrator and workers in one process: set
`EMBEDDED_L2=true` and use the in-process / in-memory adapters (`broker_kind=memory`,
the in-memory run store / checkpointer / idempotency guard). The `po_demo` runs this
way end-to-end with zero external infrastructure.

## Configuration

Everything an adopter wires lives in one `Settings` object (`maof.config`), sourced
from environment variables + optional YAML (`MAOF_CONFIG_YAML`). Conventional env
names: `DB_URL`, `BROKER_KIND`/`BROKER_URL`, `MSG_SIGNING_SECRET`/`MSG_SIGNING_KEY_ID`,
`TENANCY_MODE`, `PRINCIPAL_ID`/`PRINCIPAL_SCOPES`/`PRINCIPAL_ROLES`/`PRINCIPAL_ORG`,
`REGION`, `EMBED_PROVIDER`/`EMBED_MODEL`, `ARTIFACT_BACKEND`/`ARTIFACT_BUCKET`,
`RUN_TOKEN_BUDGET`/`RUN_COST_BUDGET_USD`, `EVAL_MIN_PASS_RATE`, and the tuning surfaces
(coordination, context budgeting, compaction threshold, worth-it thresholds).

## Production hardening

The defaults run **offline for local development** and are deliberately permissive. Override them
before any shared or production deployment:

- **Message signing (required).** Set `MSG_SIGNING_SECRET` to a strong random value (32+ bytes), or use
  `SIGNING_KEY_PROVIDER=file|vault`. `REQUIRE_SIGNATURE` defaults to `true`; `maof run-worker` **refuses
  to start** when signing is required but no key is configured, and the worker and result collector
  reject unsigned or forged messages. Set `REQUIRE_SIGNATURE=false` only for a trusted single-process /
  embedded deployment with no shared broker. For cross-org trust use `Ed25519Signer` (the `crypto` extra).
- **Credentials.** Replace the dev defaults: `BROKER_URL` (not `guest:guest`) and `DB_URL` (not
  `maof:maof`). The worker prints a startup warning when it detects these.
- **DB pool sizing.** The idempotency guard holds a connection for each in-flight side effect, so
  `DB_POOL_MAX` must exceed a worker's concurrency (prefetch) or workers starve on the pool.
- **Approval service.** `APPROVAL_HOST` binds `0.0.0.0` for containers; put it behind your ingress and
  authn, never directly on the internet.
- **Tenancy & approvals.** Keep `TENANCY_MODE=multi` unless single-tenant is intended, and leave
  `APPROVAL_FALLBACK=deny` (fail closed) so a missing HITL gate cannot wave plans through.
- **Operator identity & RBAC.** Run / workflow / registry CLI and API mutations are scope-gated when a
  `Principal` is asserted (bring-your-own auth). Set `PRINCIPAL_ID` + `PRINCIPAL_SCOPES` (full admin:
  `runs:read,runs:write,workflow:author,workflow:approve,registry:author,registry:approve`) for CLI/CI,
  or pass an injectable `principal_resolver` to `create_runs_app` / `create_registry_admin_app` (verify a
  bearer token via `verify_oidc_token`, a gateway assertion, …); without one the mutating API routes are
  unauthenticated. Single-tenant trusts the local operator; multi-tenant fails closed until a principal
  is wired.
- **Retention.** Cron `maof prune` so trace/audit/idempotency tables stay bounded.
- **Graceful deploys.** `SIGTERM`/`SIGINT` drains a worker (it stops intake and disconnects cleanly);
  with redelivery + idempotency, in-flight work is safe across restarts.

See [`SECURITY.md`](../SECURITY.md) for vulnerability reporting.

## Run operations & retention

- `maof runs {list,show,trace,cancel,wake,promote}` operates the runs tables; `create_runs_app`
  exposes the same surface as a FastAPI service (the console backend, with an optional
  `principal_resolver` auth seam). `promote` derives a draft signed workflow from a completed
  run — reuse a proven process under new goals. `cancel` is
  cooperative, checked at stage boundaries and by workers before side effects; a run
  parked `WAITING` finalizes immediately.
- A **waker poller** (`WakerPoller.run_forever`) must run somewhere (orchestrator
  sidecar or its own service) to resume timer waits; the **result collector** consumes
  the `results` queue and resumes joins.
- `maof prune` (cron it) applies the retention windows: `TRACE_RETENTION_DAYS`,
  `AUDIT_RETENTION_DAYS`, `IDEMPOTENCY_KEY_TTL_S`.

## Identity & signing keys

- **Principals from your IdP**: map verified token claims to a `Principal` with
  `maof.identity.principal_from_claims` (presets: `OKTA_CLAIMS`, `ENTRA_CLAIMS`;
  or a custom `ClaimsMapping`). To verify tokens in-process, install the `oidc`
  extra and use `verify_oidc_token(token, key=..., issuer=..., audience=...)`;
  invalid/expired/tampered tokens raise and never become principals. For JWKS
  issuers, resolve the key with PyJWT's `PyJWKClient` against the IdP's JWKS URL.
- **Key sourcing**: `build_signer(settings)` constructs the message `Signer` from
  `SIGNING_KEY_PROVIDER`: `env` (default: `MSG_SIGNING_SECRET`), `file`
  (`SIGNING_KEYS_FILE`, a mounted JSON secret of `{kid: secret}` that supports
  rotation via multiple kids), or `vault` (`VAULT_URL`/`VAULT_TOKEN`/
  `VAULT_SECRET_PATH`, HashiCorp KV-v2 via the `vault` extra). File/Vault
  providers fail closed on misconfiguration.
- **Approval notifications**: approvals are audit events, so pushing them to chat
  is just an event sink: wire `WebhookEventSink` (Slack Block Kit by default,
  `format_teams` for Teams) into your sink fanout (`FanoutEventSink`) with
  `approval_base_url` pointing at the approval service for one-click
  approve/deny links. Delivery is best-effort by design: a webhook outage can
  never block or break the governance path.

## Signing across org boundaries

The default `Signer` is HMAC (shared secret). For cross-org trust (vendor agents,
A2A, "platform-verified" verification) install the `crypto` extra and use
`Ed25519Signer`: registries sign with the private key, and verifying parties hold only
public keys and cannot forge approvals.

## Rainbow / gradual deploys (do not orphan running agents)

Orchestrations are **durable runs** with checkpoint/resume and idempotency keys, but a
long-running run can still be in flight when you deploy. **Do not hard-cut deploys.** Use
a rainbow (a.k.a. gradual) strategy:

1. Stand up the **new** orchestrator/worker versions alongside the old ones.
2. Stop routing **new** runs to the old version (drain).
3. Let the old version **finish or checkpoint** its in-flight runs; on the new version a
   resumed run picks up from the last checkpoint (`resume_run(run_id)`) without re-running
   completed steps, and idempotency keys ensure no side effect double-fires.
4. Retire the old version once it is drained.

Because every side effect is wrapped in `IdempotencyGuard.once` and keyed by
`sha256(run_id, step_id, task_type, canonical(body))`, a run that migrates mid-flight
(or is redelivered) commits **exactly once**.

## Known limitations

- **Messaging is at-least-once; exactly-once lives in the consumer guard.** All broker
  adapters can deliver duplicates (e.g. a crash in the republish→ack window). Side
  effects stay exactly-once because consumers wrap them in `IdempotencyGuard.once`
  (race-free via a per-key Postgres advisory lock). Do not bypass the guard.
- **Registry revocation vs. DB replay.** Revocation destroys the entry's signature, so a
  DB writer cannot resurrect it by flipping status. A *backup/replica replay* of the
  pre-revocation row would still verify, so rotate the registry signing key when a
  revocation must be unforgeable against replay (a signed CRL is a future option).
- **Redis processing-list reclaim.** The Redis adapter parks in-flight messages in
  `<queue>.processing` (BLMOVE). A crash mid-handler leaves the message there; an
  operator (or a startup reclaim job) must move it back to the main queue.
- **Kafka retry backoff is in-process.** Retries sleep inline before re-producing
  (bounded by the configured steps); offsets commit only after handling, so messages
  are not lost, but a long backoff stalls that partition's consumer. Prefer short
  steps or a dedicated retry topic for high-throughput Kafka deployments.
- **SQS delays cap at 900s** (native `DelaySeconds`); configure retry steps accordingly.
- **Broker adapters are exercised against live brokers.** RabbitMQ, Kafka, Redis, and SQS
  each have a `live`-marked integration suite (`tests/test_{rabbitmq,kafka,redis,sqs}.py`),
  run by the CI `integration` job and locally via `docker compose -f docker-compose.test.yml up`
  + `pytest -m live` (see CONTRIBUTING). The default offline run still mocks them; pin and
  re-run the suite against your own broker version before relying on it in production.
- **Migrations are a single idempotent schema file** (`0001_init.sql`); adopters with
  existing schemas should layer a migration tool (e.g. alembic) on top.
- **HITL approvals across processes use DB polling** (`ApprovalGate(poll_interval=...)`,
  default 0.5s): wire the same `ApprovalRepo` (Postgres) into the orchestrator gate and
  the approval service.
- **Tenant isolation engages with an asserted `TenantContext`.** `runs show/trace/cancel` enforce
  per-tenant isolation when a tenant is supplied (the CLI resolves one); `runs list` and the shipped
  FastAPI runs app are scope-gated (`runs:read`/`runs:write`) but resolve only a `Principal`, not a
  tenant, so per-tenant filtering at the HTTP edge is the adopter's job — front the admin/runs APIs with
  a gateway, or extend `principal_resolver` to scope by tenant. Don't expose those routes unscoped in a
  multi-tenant deployment.
