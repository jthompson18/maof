"""Principal identity.

A tenant is not an actor: principals from different business parties can share
a tenant while carrying distinct orgs, roles, and scopes. The Principal threads
through runs, audit events, approvals, and the task envelope so authorization
and attribution work at the actor level everywhere money moves.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field

from maof.errors import ConfigError, SignatureError
from maof.tenancy import resolve_tenant
from maof.types import TenantContext

if TYPE_CHECKING:
    from maof.config import Settings

#: The default identity for unattended/system runs.
SERVICE_PRINCIPAL_ID = "maof-service"


class Principal(BaseModel):
    id: str
    kind: Literal["user", "service", "agent"] = "user"
    org: str = ""  # e.g. "buyer" | "supplier" — intra-tenant attribution
    roles: list[str] = Field(default_factory=list)
    scopes: list[str] = Field(default_factory=list)  # checked before tenant scopes

    def as_actor(self) -> dict[str, object]:
        """The wire shape carried on envelopes and audit events."""
        return {
            "id": self.id,
            "kind": self.kind,
            "org": self.org,
            "roles": list(self.roles),
            "scopes": list(self.scopes),
        }


def service_principal() -> Principal:
    return Principal(id=SERVICE_PRINCIPAL_ID, kind="service")


def resolve_identity(
    settings: Settings,
    tenant_id: str | None = None,
    principal: Principal | None = None,
) -> tuple[TenantContext, Principal]:
    """Resolve the effective (tenant, principal) pair. Multi-tenant mode requires an
    explicit tenant; an absent principal defaults to the service identity."""
    tenant = resolve_tenant(settings, tenant_id)
    return tenant, principal if principal is not None else service_principal()


def _split_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def resolve_operator_principal(settings: Settings) -> Principal:
    """The local operator's Principal for CLI/admin actions — bring-your-own identity.

    Uses the env-provided ``PRINCIPAL_*`` fields when ``PRINCIPAL_ID`` is set (a
    CI/gateway can inject a real identity that way). With none configured,
    single-tenant mode trusts the local operator (granted the admin scopes); multi-
    tenant mode returns a scopeless service identity, so governed actions fail closed
    until a real identity is wired."""
    from maof.authz import ADMIN_SCOPES

    if settings.principal_id:
        return Principal(
            id=settings.principal_id,
            org=settings.principal_org,
            roles=_split_csv(settings.principal_roles),
            scopes=_split_csv(settings.principal_scopes),
        )
    if settings.tenancy_mode == "single":
        return Principal(id=SERVICE_PRINCIPAL_ID, kind="service", scopes=sorted(ADMIN_SCOPES))
    return service_principal()


class ClaimsMapping(BaseModel):
    """Where Principal fields live inside an identity provider's token claims.

    Scope/role claim values may be lists or space-delimited strings (RFC 8693
    ``scope`` style); both are accepted. ``org_claim`` is optional — many IdPs
    carry no org concept, and intra-tenant org attribution can also be derived
    by the adopter after mapping.
    """

    subject_claim: str = "sub"
    roles_claim: str = "roles"
    scopes_claim: str = "scope"
    org_claim: str = ""
    kind: Literal["user", "service", "agent"] = "user"


#: Okta access tokens: groups carry authorization, ``scp`` is a list of scopes.
OKTA_CLAIMS = ClaimsMapping(roles_claim="groups", scopes_claim="scp")

#: Microsoft Entra ID access tokens: app roles in ``roles``, delegated scopes in
#: ``scp`` (space-delimited), tenant id in ``tid``.
ENTRA_CLAIMS = ClaimsMapping(roles_claim="roles", scopes_claim="scp", org_claim="tid")


def _claim_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [part for part in value.split() if part]
    if isinstance(value, Sequence):
        return [str(item) for item in value]
    return [str(value)]


def principal_from_claims(
    claims: dict[str, Any], mapping: ClaimsMapping | None = None
) -> Principal:
    """Map verified IdP token claims to a :class:`Principal` (pure; no deps).

    Verification is the caller's job — pair with :func:`verify_oidc_token`, or
    map claims your gateway already validated.
    """
    m = mapping if mapping is not None else ClaimsMapping()
    subject = claims.get(m.subject_claim)
    if not subject:
        raise SignatureError(f"token claims missing subject claim {m.subject_claim!r}")
    org = str(claims.get(m.org_claim, "") or "") if m.org_claim else ""
    return Principal(
        id=str(subject),
        kind=m.kind,
        org=org,
        roles=_claim_values(claims.get(m.roles_claim)),
        scopes=_claim_values(claims.get(m.scopes_claim)),
    )


def verify_oidc_token(
    token: str,
    *,
    key: Any,
    issuer: str | None = None,
    audience: str | None = None,
    algorithms: Sequence[str] = ("RS256",),
    mapping: ClaimsMapping | None = None,
    leeway: float = 0.0,
) -> Principal:
    """Verify an OIDC/JWT access token and map its claims to a Principal.

    ``key`` is the issuer's verification key — a PEM public key, a shared
    secret for HS*, or a key resolved via PyJWT's ``PyJWKClient`` against the
    IdP's JWKS endpoint (``PyJWKClient(jwks_url).get_signing_key_from_jwt(token).key``).
    Requires the ``oidc`` extra (PyJWT). Invalid, expired, or tampered tokens
    raise :class:`~maof.errors.SignatureError` — fail closed, never map unverified claims.
    """
    try:
        import jwt
    except ImportError as exc:  # pragma: no cover - depends on installed extras
        raise ConfigError(
            "verify_oidc_token requires the 'oidc' extra (pip install maof[oidc])"
        ) from exc
    try:
        claims = jwt.decode(
            token,
            key=key,
            algorithms=list(algorithms),
            issuer=issuer,
            audience=audience,
            leeway=leeway,
            options={"verify_aud": audience is not None, "verify_iss": issuer is not None},
        )
    except jwt.PyJWTError as exc:
        raise SignatureError(f"OIDC token rejected: {exc}") from exc
    return principal_from_claims(claims, mapping)


__all__ = [
    "Principal",
    "resolve_identity",
    "resolve_operator_principal",
    "service_principal",
    "SERVICE_PRINCIPAL_ID",
    "ClaimsMapping",
    "OKTA_CLAIMS",
    "ENTRA_CLAIMS",
    "principal_from_claims",
    "verify_oidc_token",
]
