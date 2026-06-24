"""Registry admin API: submit → approve → list → revoke
over the in-memory repo, driven through the real FastAPI app."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402

from maof.registry.admin_api import create_registry_admin_app  # noqa: E402
from maof.registry.loader import RegistryLoader  # noqa: E402
from maof.registry.store import InMemoryRegistryRepo, RegistryStore  # noqa: E402
from maof.transport.signing import Signer  # noqa: E402

MANIFEST = {
    "id": "vendor-x",
    "kind": "l2_agent",
    "name": "Vendor X",
    "version": "v1",
    "endpoint": "https://vendor.example.com/a2a",
    "accepted_task_types": ["order_placement"],
}


def _client() -> TestClient:
    repo = InMemoryRegistryRepo()
    signer = Signer({"default": "admin-api-test-secret-0123456789abcd"})
    store = RegistryStore(repo, signer)
    loader = RegistryLoader(repo, signer)
    return TestClient(create_registry_admin_app(store, loader))


def test_full_admin_lifecycle_over_http() -> None:
    client = _client()

    submitted = client.post("/registry/submit", json=MANIFEST)
    assert submitted.status_code == 200
    assert submitted.json() == {"id": "vendor-x", "status": "pending"}

    # pending entries are invisible to the trust loader
    assert client.get("/registry").json() == {"entries": []}

    approved = client.post("/registry/vendor-x/approve")
    assert approved.json() == {"id": "vendor-x", "status": "approved"}
    entries = client.get("/registry").json()["entries"]
    assert [e["id"] for e in entries] == ["vendor-x"]

    revoked = client.post("/registry/vendor-x/revoke")
    assert revoked.json() == {"id": "vendor-x", "status": "revoked"}
    assert client.get("/registry").json() == {"entries": []}
