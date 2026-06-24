"""Registry admin API — submit / approve / revoke / list.

FastAPI app (requires the ``api`` extra), constructed lazily so the module imports
without it. The operator-facing surface for the admin-gated, signed lifecycle.
``principal_resolver`` is the bring-your-own-auth seam: given the Authorization
header it returns a Principal (or raises), so the mutating routes are scope-gated.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from maof.identity import Principal
    from maof.registry.loader import RegistryLoader
    from maof.registry.store import RegistryStore


def create_registry_admin_app(
    store: RegistryStore,
    loader: RegistryLoader,
    *,
    principal_resolver: Callable[[str | None], Awaitable[Principal | None]] | None = None,
) -> Any:
    from fastapi import FastAPI, Header
    from fastapi.responses import JSONResponse

    from maof.errors import AuthzError
    from maof.registry.models import AgentManifest

    app = FastAPI(title="MAOF Registry Admin")

    @app.exception_handler(AuthzError)
    async def _on_authz_error(_: Any, exc: AuthzError) -> JSONResponse:
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    async def _principal(authorization: str | None) -> Principal | None:
        return await principal_resolver(authorization) if principal_resolver is not None else None

    async def submit(
        manifest: dict[str, Any], authorization: str | None = Header(default=None)
    ) -> dict[str, str]:
        entry = await store.submit(
            AgentManifest.model_validate(manifest), principal=await _principal(authorization)
        )
        return {"id": entry.manifest.id, "status": entry.status}

    async def approve(
        entry_id: str, authorization: str | None = Header(default=None)
    ) -> dict[str, str]:
        entry = await store.approve(entry_id, principal=await _principal(authorization))
        return {"id": entry.manifest.id, "status": entry.status}

    async def revoke(
        entry_id: str, authorization: str | None = Header(default=None)
    ) -> dict[str, str]:
        entry = await store.revoke(entry_id, principal=await _principal(authorization))
        return {"id": entry.manifest.id, "status": entry.status}

    async def list_approved() -> dict[str, Any]:
        return {"entries": [m.model_dump() for m in await loader.manifests()]}

    app.add_api_route("/registry/submit", submit, methods=["POST"])
    app.add_api_route("/registry/{entry_id}/approve", approve, methods=["POST"])
    app.add_api_route("/registry/{entry_id}/revoke", revoke, methods=["POST"])
    app.add_api_route("/registry", list_approved, methods=["GET"])
    return app


__all__ = ["create_registry_admin_app"]
