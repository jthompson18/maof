-- MAOF initial schema.
-- Core tables. The memories embedding dimension is
-- templated from config (EmbeddingProvider.dimension) via the __EMBED_DIM__ token.

CREATE EXTENSION IF NOT EXISTS vector;

-- intents -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS intents (
    intent_id   TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    goal        TEXT NOT NULL,
    summary     TEXT NOT NULL DEFAULT '',
    task_types  JSONB NOT NULL DEFAULT '[]',
    details     JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS intents_tenant_idx ON intents (tenant_id);

-- memories (vector recall) -------------------------------------------------
CREATE TABLE IF NOT EXISTS memories (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    kind        TEXT NOT NULL,
    content     TEXT NOT NULL,
    prov        TEXT NOT NULL DEFAULT '',
    embedding   vector(__EMBED_DIM__),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS memories_tenant_idx ON memories (tenant_id);
CREATE INDEX IF NOT EXISTS memories_embedding_idx
    ON memories USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- approvals ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS approvals (
    approval_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    tenant_id   TEXT NOT NULL,
    run_id      TEXT,
    reason      TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending | approved | denied
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS approvals_tenant_idx ON approvals (tenant_id);

-- prompt_audit -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prompt_audit (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    run_id      TEXT,
    prompt      TEXT NOT NULL,
    response    TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- policy rulesets + rules --------------------------------------------------
CREATE TABLE IF NOT EXISTS policy_rulesets (
    id          BIGSERIAL PRIMARY KEY,
    ruleset_ref TEXT NOT NULL,
    version     INT NOT NULL,
    tenant_id   TEXT NOT NULL DEFAULT '',          -- '' = global
    canary_pct  DOUBLE PRECISION NOT NULL DEFAULT 0,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ruleset_ref, version, tenant_id)
);
CREATE TABLE IF NOT EXISTS policy_rules (
    ruleset_ref TEXT NOT NULL,
    version     INT NOT NULL,
    rule_id     TEXT NOT NULL,
    priority    INT NOT NULL DEFAULT 100,
    stage       TEXT NOT NULL DEFAULT '*',
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    when_json   JSONB NOT NULL DEFAULT '{}',
    actions     JSONB NOT NULL DEFAULT '[]',
    description TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (ruleset_ref, version, rule_id)
);

-- signed workflow definitions ----------------------------------------------
CREATE TABLE IF NOT EXISTS workflows (
    name         TEXT NOT NULL,
    version      INT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending | approved | revoked
    definition   JSONB NOT NULL,
    signature    TEXT,
    kid          TEXT,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_at  TIMESTAMPTZ,
    PRIMARY KEY (name, version)
);

-- discovery registry -------------------------------------------------------
CREATE TABLE IF NOT EXISTS registry_entries (
    id           TEXT PRIMARY KEY,
    kind         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending | approved | revoked
    manifest     JSONB NOT NULL,
    signature    TEXT,
    kid          TEXT,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_at  TIMESTAMPTZ
);

-- durable runs + append-only trace ----------------------------------------
CREATE TABLE IF NOT EXISTS runs (
    run_id           TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    tenant_id        TEXT NOT NULL,
    goal             TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    current_step     TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    principal_id     TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- idempotent column guards for pre-existing dev databases (schema is regenerated
-- wholesale; nothing deployed depends on the old shape)
ALTER TABLE runs ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS principal_id TEXT;

-- result envelopes + wake conditions ----------------------------------------
CREATE TABLE IF NOT EXISTS run_results (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    step_ref        TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    tenant_id       TEXT NOT NULL,
    result          JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS run_results_join_idx ON run_results (run_id, step_ref);

CREATE TABLE IF NOT EXISTS run_wakeups (
    id         BIGSERIAL PRIMARY KEY,
    run_id     TEXT NOT NULL,
    kind       TEXT NOT NULL,                     -- results_ready | timer | external_event
    step_ref   TEXT,
    expected   INT NOT NULL DEFAULT 1,
    wake_at    TIMESTAMPTZ,
    event_key  TEXT,
    status     TEXT NOT NULL DEFAULT 'pending',   -- pending | fired | cancelled
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    fired_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS run_wakeups_due_idx ON run_wakeups (status, wake_at);
CREATE INDEX IF NOT EXISTS run_wakeups_event_idx ON run_wakeups (status, event_key);
CREATE INDEX IF NOT EXISTS run_wakeups_results_idx ON run_wakeups (status, run_id, step_ref);
CREATE TABLE IF NOT EXISTS run_trace (
    run_id TEXT NOT NULL,
    seq    BIGINT NOT NULL,
    kind   TEXT NOT NULL,
    step   TEXT,
    data   JSONB NOT NULL DEFAULT '{}',
    ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);

-- checkpoints (resume-from-failure) ---------------------------------------
CREATE TABLE IF NOT EXISTS checkpoints (
    id         BIGSERIAL PRIMARY KEY,
    run_id     TEXT NOT NULL,
    step       TEXT NOT NULL,
    blob       BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS checkpoints_run_idx ON checkpoints (run_id, id DESC);

-- idempotency keys (replay-safe side effects) -----------------------------
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key        TEXT PRIMARY KEY,
    result     JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- artifacts (reference passing) -------------------------------------------
CREATE TABLE IF NOT EXISTS artifacts (
    ref          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    run_id       TEXT NOT NULL,
    name         TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    data         BYTEA NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- notes (agentic memory) ---------------------------------------------------
CREATE TABLE IF NOT EXISTS notes (
    id         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    run_id     TEXT NOT NULL,
    content    TEXT NOT NULL,
    tags       JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS notes_run_idx ON notes (run_id);

-- cost ledger --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cost_ledger (
    id         BIGSERIAL PRIMARY KEY,
    run_id     TEXT NOT NULL,
    model      TEXT NOT NULL,
    in_tokens  INT NOT NULL DEFAULT 0,
    out_tokens INT NOT NULL DEFAULT 0,
    cost_usd   DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cost_ledger_run_idx ON cost_ledger (run_id);

-- eval results -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS eval_results (
    id         BIGSERIAL PRIMARY KEY,
    dataset    TEXT NOT NULL,
    report     JSONB NOT NULL,
    pass_rate  DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- audit events (EventSink sink table) -------------------------------------
CREATE TABLE IF NOT EXISTS audit_events (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    intent_id  TEXT,
    event_type TEXT NOT NULL,
    severity   TEXT NOT NULL DEFAULT 'info',
    kind       TEXT NOT NULL DEFAULT '',
    envelope   JSONB NOT NULL DEFAULT '{}',
    details    JSONB NOT NULL DEFAULT '{}',
    ts         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS audit_events_tenant_idx ON audit_events (tenant_id, ts);
