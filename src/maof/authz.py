"""Authorization primitives — scope checks over a Principal.

MAOF is bring-your-own auth/identity/RBAC: adopters resolve a
:class:`~maof.identity.Principal` however they like (OIDC, a gateway, env); the
framework ships only the enforcement primitive. The scope names below are
conventions granted via ``Principal.scopes`` or tenant ``attributes["scopes"]``.

:func:`require_scopes` fails closed. Governed actions enforce it only when a
principal is asserted, so trusted in-process callers pass ``None``; asserting
identity at the CLI/API boundary is what fails multi-tenant access closed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from maof.errors import AuthzError

if TYPE_CHECKING:
    from maof.identity import Principal
    from maof.types import TenantContext

# Governed-action scopes. Adopters grant these to principals (or tenants).
SCOPE_RUNS_READ = "runs:read"
SCOPE_RUNS_WRITE = "runs:write"
SCOPE_WORKFLOW_AUTHOR = "workflow:author"
SCOPE_WORKFLOW_APPROVE = "workflow:approve"
SCOPE_REGISTRY_AUTHOR = "registry:author"
SCOPE_REGISTRY_APPROVE = "registry:approve"
ADMIN_SCOPES = frozenset(
    {
        SCOPE_RUNS_READ,
        SCOPE_RUNS_WRITE,
        SCOPE_WORKFLOW_AUTHOR,
        SCOPE_WORKFLOW_APPROVE,
        SCOPE_REGISTRY_AUTHOR,
        SCOPE_REGISTRY_APPROVE,
    }
)


def held_scopes(principal: Principal | None, tenant: TenantContext | None = None) -> set[str]:
    """The scopes a principal effectively holds: its own, plus tenant grants as a
    fallback (``tenant.attributes["scopes"]``, comma-separated) — the same
    precedence the registry uses."""
    held: set[str] = set()
    if principal is not None:
        held |= set(principal.scopes)
    if tenant is not None:
        raw = str(tenant.attributes.get("scopes", "") or "")
        held |= {scope.strip() for scope in raw.split(",") if scope.strip()}
    return held


def require_scopes(
    principal: Principal | None,
    required: set[str],
    *,
    tenant: TenantContext | None = None,
) -> None:
    """Raise :class:`~maof.errors.AuthzError` unless every scope in ``required`` is
    held by ``principal`` (or its ``tenant``). Fails closed: an anonymous principal
    holds nothing."""
    if not required:
        return
    missing = required - held_scopes(principal, tenant)
    if missing:
        who = principal.id if principal is not None else "<anonymous>"
        raise AuthzError(f"principal {who!r} lacks required scope(s): {sorted(missing)}")


def require_run_read(
    run_tenant_id: str,
    principal: Principal | None,
    *,
    tenant: TenantContext | None = None,
) -> None:
    """Authorize reading a run: the ``runs:read`` scope plus tenant isolation (a
    principal scoped to one tenant cannot read another tenant's run)."""
    require_scopes(principal, {SCOPE_RUNS_READ}, tenant=tenant)
    if tenant is not None and tenant.tenant_id != run_tenant_id:
        who = principal.id if principal is not None else "<anonymous>"
        raise AuthzError(
            f"principal {who!r} (tenant {tenant.tenant_id!r}) may not read a run "
            f"in tenant {run_tenant_id!r}"
        )


def require_run_write(
    run_tenant_id: str,
    principal: Principal | None,
    *,
    tenant: TenantContext | None = None,
) -> None:
    """Authorize mutating a run (e.g. cancel): the ``runs:write`` scope plus tenant
    isolation (a principal scoped to one tenant cannot modify another tenant's run)."""
    require_scopes(principal, {SCOPE_RUNS_WRITE}, tenant=tenant)
    if tenant is not None and tenant.tenant_id != run_tenant_id:
        who = principal.id if principal is not None else "<anonymous>"
        raise AuthzError(
            f"principal {who!r} (tenant {tenant.tenant_id!r}) may not modify a run "
            f"in tenant {run_tenant_id!r}"
        )


__all__ = [
    "SCOPE_RUNS_READ",
    "SCOPE_RUNS_WRITE",
    "SCOPE_WORKFLOW_AUTHOR",
    "SCOPE_WORKFLOW_APPROVE",
    "SCOPE_REGISTRY_AUTHOR",
    "SCOPE_REGISTRY_APPROVE",
    "ADMIN_SCOPES",
    "held_scopes",
    "require_scopes",
    "require_run_read",
    "require_run_write",
]
