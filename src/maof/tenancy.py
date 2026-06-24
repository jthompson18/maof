"""Tenant isolation threading + single-tenant mode.

Tenancy is threaded through every interface but can run single-tenant (the
``TenantContext`` is still present, just defaulted).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.errors import TenancyError
from maof.types import TenantContext

if TYPE_CHECKING:
    from maof.config import Settings

DEFAULT_TENANT_ID = "default"


def resolve_tenant(settings: Settings, tenant_id: str | None = None) -> TenantContext:
    """Resolve the effective tenant. Single-tenant mode defaults the id; multi-tenant
    mode requires an explicit ``tenant_id`` (fail fast otherwise)."""
    if settings.tenancy_mode == "single":
        return TenantContext(tenant_id=tenant_id or DEFAULT_TENANT_ID, multi_tenant=False)
    if not tenant_id:
        raise TenancyError("multi-tenant mode requires an explicit tenant_id")
    return TenantContext(tenant_id=tenant_id, multi_tenant=True)


__all__ = ["resolve_tenant", "DEFAULT_TENANT_ID"]
