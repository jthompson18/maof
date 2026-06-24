"""Postgres adapter — the default persistence backend.

A thin asyncpg pool wrapper (:class:`Database`), a migration runner, and Postgres
implementations of the repository Protocols. A JSONB type codec is registered on
every connection so dict/list params and results cross the boundary as Python
objects (no manual ``::jsonb`` casts).
"""

from __future__ import annotations

import importlib.resources
import json
from datetime import UTC, datetime
from typing import Any

import asyncpg

from maof.registry.models import AgentManifest, RegistryEntry
from maof.types import CostSummary, Intent, LoadedRuleset, Rule, TenantContext


async def _init_connection(conn: Any) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog", format="text"
    )


class Database:
    """Owns the asyncpg connection pool and exposes small query helpers.

    Size the pool above worker concurrency: idempotency guards hold one
    connection across each side effect's critical section."""

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 5) -> None:
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._pool: Any | None = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._dsn,
                min_size=self._min_size,
                max_size=self._max_size,
                init=_init_connection,
            )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> Any:
        if self._pool is None:
            raise RuntimeError("Database is not connected; call connect() first")
        return self._pool

    async def execute(self, sql: str, *args: Any) -> str:
        async with self.pool.acquire() as conn:
            status: str = await conn.execute(sql, *args)
            return status

    async def fetch(self, sql: str, *args: Any) -> list[Any]:
        async with self.pool.acquire() as conn:
            rows: list[Any] = await conn.fetch(sql, *args)
            return rows

    async def fetchrow(self, sql: str, *args: Any) -> Any:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(sql, *args)


def _migration_sql(embed_dimension: int) -> str:
    sql = (
        importlib.resources.files("maof.persistence.migrations")
        .joinpath("0001_init.sql")
        .read_text(encoding="utf-8")
    )
    return sql.replace("__EMBED_DIM__", str(int(embed_dimension)))


async def run_migrations(db: Database, *, embed_dimension: int) -> None:
    """Apply the schema (idempotent). The memories vector dimension is templated
    from the configured embedding dimension."""
    await db.execute(_migration_sql(embed_dimension))


# Repository implementations
class PostgresIntentRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def save(self, tenant: TenantContext, intent: Intent) -> str:
        await self._db.execute(
            """
            INSERT INTO intents (intent_id, tenant_id, goal, summary, task_types, details)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (intent_id) DO UPDATE
              SET goal = EXCLUDED.goal, summary = EXCLUDED.summary,
                  task_types = EXCLUDED.task_types, details = EXCLUDED.details
            """,
            intent.intent_id,
            tenant.tenant_id,
            intent.goal,
            intent.summary,
            intent.task_types,
            intent.details,
        )
        return intent.intent_id

    async def get(self, tenant: TenantContext, intent_id: str) -> Intent | None:
        row = await self._db.fetchrow(
            "SELECT * FROM intents WHERE intent_id = $1 AND tenant_id = $2",
            intent_id,
            tenant.tenant_id,
        )
        if row is None:
            return None
        return Intent(
            intent_id=row["intent_id"],
            goal=row["goal"],
            summary=row["summary"],
            task_types=list(row["task_types"]),
            details=dict(row["details"]),
        )


class PostgresApprovalRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def create(self, tenant: TenantContext, run_id: str, reason: str) -> str:
        approval_id: str = await self._db.fetchval(
            """
            INSERT INTO approvals (tenant_id, run_id, reason)
            VALUES ($1, $2, $3) RETURNING approval_id
            """,
            tenant.tenant_id,
            run_id,
            reason,
        )
        return approval_id

    async def resolve(
        self, approval_id: str, *, approved: bool, tenant_id: str | None = None
    ) -> None:
        """Resolve an approval. When ``tenant_id`` is provided, the update is
        tenant-scoped — a caller from another tenant cannot resolve it."""
        await self._db.execute(
            """
            UPDATE approvals SET status = $2, resolved_at = now()
             WHERE approval_id = $1 AND ($3::text IS NULL OR tenant_id = $3)
            """,
            approval_id,
            "approved" if approved else "denied",
            tenant_id,
        )

    async def get(self, approval_id: str) -> dict[str, Any] | None:
        row = await self._db.fetchrow("SELECT * FROM approvals WHERE approval_id = $1", approval_id)
        return dict(row) if row is not None else None


class PostgresPromptAuditRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def record(self, tenant: TenantContext, run_id: str, prompt: str, response: str) -> None:
        await self._db.execute(
            """
            INSERT INTO prompt_audit (tenant_id, run_id, prompt, response)
            VALUES ($1, $2, $3, $4)
            """,
            tenant.tenant_id,
            run_id,
            prompt,
            response,
        )


class PostgresPolicyRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def save_ruleset(self, ruleset: LoadedRuleset, *, tenant_id: str = "") -> None:
        """Persist a ruleset + its rules (used by the policy engine / loaders)."""
        await self._db.execute(
            """
            INSERT INTO policy_rulesets (ruleset_ref, version, tenant_id, canary_pct)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (ruleset_ref, version, tenant_id)
              DO UPDATE SET canary_pct = EXCLUDED.canary_pct
            """,
            ruleset.ruleset_ref,
            ruleset.version,
            tenant_id,
            ruleset.canary_pct,
        )
        for rule in ruleset.rules:
            await self._db.execute(
                """
                INSERT INTO policy_rules
                  (ruleset_ref, version, rule_id, priority, stage, enabled, when_json,
                   actions, description)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (ruleset_ref, version, rule_id) DO UPDATE
                  SET priority = EXCLUDED.priority, stage = EXCLUDED.stage,
                      enabled = EXCLUDED.enabled, when_json = EXCLUDED.when_json,
                      actions = EXCLUDED.actions, description = EXCLUDED.description
                """,
                rule.ruleset_ref,
                rule.version,
                rule.rule_id,
                rule.priority,
                rule.stage,
                rule.enabled,
                rule.when,
                rule.actions,
                rule.description,
            )

    async def load_ruleset(self, tenant: TenantContext, ruleset_ref: str) -> LoadedRuleset | None:
        head = await self._db.fetchrow(
            """
            SELECT version, canary_pct FROM policy_rulesets
            WHERE ruleset_ref = $1 AND tenant_id IN ($2, '')
            ORDER BY (tenant_id = $2) DESC, version DESC
            LIMIT 1
            """,
            ruleset_ref,
            tenant.tenant_id,
        )
        if head is None:
            return None
        version = int(head["version"])
        rule_rows = await self._db.fetch(
            """
            SELECT rule_id, priority, stage, enabled, when_json, actions, description
            FROM policy_rules WHERE ruleset_ref = $1 AND version = $2
            ORDER BY priority ASC
            """,
            ruleset_ref,
            version,
        )
        rules = [
            Rule(
                rule_id=r["rule_id"],
                ruleset_ref=ruleset_ref,
                version=version,
                priority=r["priority"],
                stage=r["stage"],
                enabled=r["enabled"],
                when=dict(r["when_json"]),
                actions=list(r["actions"]),
                description=r["description"],
            )
            for r in rule_rows
        ]
        return LoadedRuleset(
            ruleset_ref=ruleset_ref,
            version=version,
            rules=rules,
            canary_pct=float(head["canary_pct"]),
        )


def _parse_rfc3339(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _canonical_rfc3339(value: datetime | None) -> str | None:
    """Format exactly like maof.types.utcnow — the registry signature binds the
    approved_at STRING, so the DB round-trip must be byte-identical."""
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class PostgresRegistryRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def put(self, entry: RegistryEntry) -> None:
        await self._db.execute(
            """
            INSERT INTO registry_entries (id, kind, status, manifest, signature, kid, approved_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE
              SET kind = EXCLUDED.kind, status = EXCLUDED.status,
                  manifest = EXCLUDED.manifest, signature = EXCLUDED.signature,
                  kid = EXCLUDED.kid, approved_at = EXCLUDED.approved_at
            """,
            entry.manifest.id,
            entry.manifest.kind,
            entry.status,
            entry.manifest.model_dump(),
            entry.signature,
            entry.kid,
            _parse_rfc3339(entry.approved_at),
        )

    async def get(self, entry_id: str) -> RegistryEntry | None:
        row = await self._db.fetchrow("SELECT * FROM registry_entries WHERE id = $1", entry_id)
        return self._to_entry(row) if row is not None else None

    async def list_approved(self) -> list[RegistryEntry]:
        rows = await self._db.fetch("SELECT * FROM registry_entries WHERE status = 'approved'")
        return [self._to_entry(r) for r in rows]

    @staticmethod
    def _to_entry(row: Any) -> RegistryEntry:
        return RegistryEntry(
            manifest=AgentManifest.model_validate(row["manifest"]),
            status=row["status"],
            signature=row["signature"],
            kid=row["kid"],
            submitted_at=row["submitted_at"].isoformat(),
            approved_at=_canonical_rfc3339(row["approved_at"]),
        )


class PostgresCheckpointRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def save(self, run_id: str, step: str, blob: bytes) -> None:
        await self._db.execute(
            "INSERT INTO checkpoints (run_id, step, blob) VALUES ($1, $2, $3)",
            run_id,
            step,
            blob,
        )

    async def latest(self, run_id: str) -> bytes | None:
        row = await self._db.fetchrow(
            "SELECT blob FROM checkpoints WHERE run_id = $1 ORDER BY id DESC LIMIT 1",
            run_id,
        )
        return bytes(row["blob"]) if row is not None else None


class PostgresArtifactRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def put(self, run_id: str, name: str, data: bytes, content_type: str) -> str:
        ref: str = await self._db.fetchval(
            """
            INSERT INTO artifacts (run_id, name, data, content_type)
            VALUES ($1, $2, $3, $4) RETURNING ref
            """,
            run_id,
            name,
            data,
            content_type,
        )
        return ref

    async def get(self, ref: str) -> bytes | None:
        row = await self._db.fetchrow("SELECT data FROM artifacts WHERE ref = $1", ref)
        return bytes(row["data"]) if row is not None else None


class PostgresCostRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def record(
        self, run_id: str, model: str, in_tokens: int, out_tokens: int, cost_usd: float
    ) -> None:
        await self._db.execute(
            """
            INSERT INTO cost_ledger (run_id, model, in_tokens, out_tokens, cost_usd)
            VALUES ($1, $2, $3, $4, $5)
            """,
            run_id,
            model,
            in_tokens,
            out_tokens,
            cost_usd,
        )

    async def total(self, run_id: str) -> CostSummary | None:
        rows = await self._db.fetch(
            "SELECT model, in_tokens, out_tokens, cost_usd FROM cost_ledger WHERE run_id = $1",
            run_id,
        )
        summary = CostSummary(run_id=run_id)
        for r in rows:
            summary.in_tokens += int(r["in_tokens"])
            summary.out_tokens += int(r["out_tokens"])
            summary.cost_usd += float(r["cost_usd"])
            summary.by_model[r["model"]] = (
                summary.by_model.get(r["model"], 0) + int(r["in_tokens"]) + int(r["out_tokens"])
            )
        summary.total_tokens = summary.in_tokens + summary.out_tokens
        return summary


class PostgresEvalRepo:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def save_report(self, report: Any) -> None:
        await self._db.execute(
            "INSERT INTO eval_results (dataset, report, pass_rate) VALUES ($1, $2, $3)",
            report.dataset,
            report.model_dump(),
            report.pass_rate,
        )


__all__ = [
    "Database",
    "run_migrations",
    "PostgresIntentRepo",
    "PostgresApprovalRepo",
    "PostgresPromptAuditRepo",
    "PostgresPolicyRepo",
    "PostgresRegistryRepo",
    "PostgresCheckpointRepo",
    "PostgresArtifactRepo",
    "PostgresCostRepo",
    "PostgresEvalRepo",
]
