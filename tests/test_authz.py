"""Authorization primitives: scope checks over a Principal (bring-your-own RBAC).

require_scopes fails closed; governed actions enforce it only when a Principal is
asserted. These cover the pure enforcement logic the CLI/API/library gates reuse.
"""

from __future__ import annotations

import pytest

from maof.authz import (
    SCOPE_RUNS_READ,
    SCOPE_WORKFLOW_AUTHOR,
    held_scopes,
    require_run_read,
    require_scopes,
)
from maof.errors import AuthzError
from maof.identity import Principal
from maof.types import TenantContext


def _principal(*scopes: str) -> Principal:
    return Principal(id="u1", scopes=list(scopes))


def test_require_scopes_allows_when_all_held() -> None:
    require_scopes(_principal(SCOPE_WORKFLOW_AUTHOR, "extra"), {SCOPE_WORKFLOW_AUTHOR})


def test_require_scopes_denies_when_missing() -> None:
    with pytest.raises(AuthzError, match="workflow:author"):
        require_scopes(_principal("other"), {SCOPE_WORKFLOW_AUTHOR})


def test_require_scopes_fails_closed_for_anonymous() -> None:
    with pytest.raises(AuthzError):
        require_scopes(None, {SCOPE_RUNS_READ})


def test_require_scopes_empty_required_is_noop() -> None:
    require_scopes(None, set())  # nothing required -> allowed even with no principal


def test_tenant_scopes_are_a_fallback() -> None:
    tenant = TenantContext(tenant_id="t", attributes={"scopes": "runs:read, workflow:approve"})
    # principal carries none; the tenant grant satisfies the requirement
    require_scopes(Principal(id="u"), {SCOPE_RUNS_READ}, tenant=tenant)


def test_held_scopes_unions_principal_and_tenant() -> None:
    tenant = TenantContext(tenant_id="t", attributes={"scopes": "a, b"})
    assert held_scopes(_principal("c"), tenant) == {"a", "b", "c"}


def test_require_run_read_enforces_scope_and_tenant_isolation() -> None:
    reader = Principal(id="u", scopes=[SCOPE_RUNS_READ])
    tenant = TenantContext(tenant_id="t1")

    require_run_read("t1", reader, tenant=tenant)  # same tenant + scope -> ok

    with pytest.raises(AuthzError):
        require_run_read("t2", reader, tenant=tenant)  # cross-tenant read denied

    with pytest.raises(AuthzError):
        require_run_read("t1", Principal(id="u"), tenant=tenant)  # missing runs:read


def test_require_run_read_without_tenant_checks_scope_only() -> None:
    reader = Principal(id="u", scopes=[SCOPE_RUNS_READ])
    require_run_read("whatever", reader)  # no tenant asserted -> scope check only


def test_operator_principal_trusts_local_in_single_tenant() -> None:
    from maof.authz import ADMIN_SCOPES
    from maof.config import Settings
    from maof.identity import resolve_operator_principal

    principal = resolve_operator_principal(Settings(tenancy_mode="single", principal_id=""))
    assert set(principal.scopes) == set(ADMIN_SCOPES)  # local operator trusted


def test_operator_principal_fails_closed_in_multi_tenant() -> None:
    from maof.config import Settings
    from maof.identity import resolve_operator_principal

    principal = resolve_operator_principal(Settings(tenancy_mode="multi", principal_id=""))
    assert principal.scopes == []  # scopeless -> governed actions deny until identity wired


def test_operator_principal_uses_configured_fields() -> None:
    from maof.config import Settings
    from maof.identity import resolve_operator_principal

    principal = resolve_operator_principal(
        Settings(
            tenancy_mode="multi",
            principal_id="alice",
            principal_scopes="workflow:author, runs:read",
            principal_roles="admin",
            principal_org="buyer",
        )
    )
    assert principal.id == "alice"
    assert set(principal.scopes) == {"workflow:author", "runs:read"}
    assert principal.roles == ["admin"]
    assert principal.org == "buyer"
