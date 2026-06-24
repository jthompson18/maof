"""Run operations — RunOps over a scripted fake Database +
the FastAPI runs app (the console backend)."""

from __future__ import annotations

from typing import Any

import pytest

from maof.runs.ops import RunOps, create_runs_app


class _FakeDatabase:
    """Duck-typed maof.persistence.postgres.Database: scripted rows + SQL capture."""

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.row: dict[str, Any] | None = None
        self.executed: list[str] = []

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        self.executed.append(sql.split()[0])
        return list(self.rows)

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        self.executed.append(sql.split()[0])
        return self.row

    async def execute(self, sql: str, *args: Any) -> None:
        self.executed.append(" ".join(sql.split())[:60])


class _FakeWaker:
    async def fire_event(self, event_key: str) -> list[str]:
        return [f"run-for-{event_key}"]


async def test_list_show_trace_round_trip() -> None:
    db = _FakeDatabase()
    db.rows = [{"run_id": "r1", "status": "completed"}]
    db.row = {"run_id": "r1", "status": "completed"}
    ops = RunOps(db)  # type: ignore[arg-type]
    assert (await ops.list_runs(tenant_id="t1", status="completed"))[0]["run_id"] == "r1"
    assert (await ops.show("r1"))["run_id"] == "r1"  # type: ignore[index]
    db.rows = [{"seq": 1, "kind": "stage", "step": "chat", "data": {}, "ts": "now"}]
    assert (await ops.trace("r1"))[0]["seq"] == 1


async def test_show_missing_run_returns_none() -> None:
    db = _FakeDatabase()
    ops = RunOps(db)  # type: ignore[arg-type]
    assert await ops.show("absent") is None


async def test_cancel_finalizes_waiting_runs_immediately() -> None:
    db = _FakeDatabase()
    db.row = {"status": "waiting"}
    ops = RunOps(db)  # type: ignore[arg-type]
    await ops.cancel("r1")
    cancels = [sql for sql in db.executed if sql.startswith("UPDATE")]
    assert any("cancel_requested = TRUE" in sql for sql in cancels)
    assert any("status = 'cancelled'" in sql for sql in cancels)  # finalized now
    assert any("run_wakeups" in sql for sql in cancels)  # pending waits cancelled


async def test_cancel_running_run_stays_cooperative() -> None:
    db = _FakeDatabase()
    db.row = {"status": "running"}
    ops = RunOps(db)  # type: ignore[arg-type]
    await ops.cancel("r1")
    assert not any("status = 'cancelled'" in sql for sql in db.executed)


async def test_wake_requires_a_waker() -> None:
    db = _FakeDatabase()
    assert await RunOps(db).wake("evt") == []  # type: ignore[arg-type]
    assert await RunOps(db, waker=_FakeWaker()).wake("evt") == ["run-for-evt"]  # type: ignore[arg-type]


async def test_runs_ops_show_enforces_scope_and_tenant() -> None:
    from maof.authz import SCOPE_RUNS_READ
    from maof.errors import AuthzError
    from maof.identity import Principal
    from maof.types import TenantContext

    db = _FakeDatabase()
    db.row = {"run_id": "r1", "tenant_id": "t1", "status": "completed"}
    ops = RunOps(db)  # type: ignore[arg-type]
    t1 = TenantContext(tenant_id="t1")
    reader = Principal(id="u", scopes=[SCOPE_RUNS_READ])

    assert (await ops.show("r1", principal=reader, tenant=t1))["run_id"] == "r1"
    with pytest.raises(AuthzError):  # missing runs:read
        await ops.show("r1", principal=Principal(id="u", scopes=[]), tenant=t1)
    with pytest.raises(AuthzError):  # cross-tenant read denied
        await ops.show("r1", principal=reader, tenant=TenantContext(tenant_id="t2"))
    assert (await ops.show("r1"))["run_id"] == "r1"  # no principal -> ungated trusted path


def test_runs_app_over_http() -> None:
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    db = _FakeDatabase()
    db.rows = [{"run_id": "r1", "status": "waiting"}]
    db.row = {"run_id": "r1", "status": "waiting"}
    client = TestClient(create_runs_app(RunOps(db, waker=_FakeWaker())))  # type: ignore[arg-type]

    assert client.get("/runs").json()["runs"][0]["run_id"] == "r1"
    assert client.get("/runs/r1").json()["run_id"] == "r1"
    db.rows = []
    assert client.get("/runs/r1/trace").json() == {"trace": []}
    assert client.post("/runs/r1/cancel").json()["status"] == "cancel_requested"
    assert client.post("/runs/wake/creative_approved").json() == {
        "woken": ["run-for-creative_approved"]
    }
    db.row = None
    assert client.get("/runs/missing").status_code == 404
