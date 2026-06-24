"""Shared test fixtures.

The ``db`` fixture connects to a test Postgres (default: the local pgvector
container on port 55432) and applies migrations. Integration tests that request
it are skipped automatically when no database is reachable, so the offline unit
suite stays green.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio

from maof.persistence.postgres import Database, run_migrations

TEST_DSN = os.getenv("MAOF_TEST_DATABASE_URL", "postgresql://maof:maof@127.0.0.1:55432/maof")
TEST_EMBED_DIM = 8


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    database = Database(TEST_DSN)
    try:
        await database.connect()
    except (OSError, asyncpg.PostgresError) as exc:  # pragma: no cover - env dependent
        pytest.skip(f"Postgres not available at {TEST_DSN}: {exc}")
    await run_migrations(database, embed_dimension=TEST_EMBED_DIM)
    try:
        yield database
    finally:
        await database.close()
