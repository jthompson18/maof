"""Registry entry signing.

On approval MAOF computes a canonical serialization of the manifest **plus the
trust state** (status, approved_at) and signs it (HMAC by default, key-id'd —
pluggable). The loader only trusts entries that are approved AND whose signature
verifies over that same canonical state, and revocation **destroys the
signature** — so a DB writer flipping a revoked row back to ``approved`` cannot
resurrect it without the signing key. (Replaying a pre-revocation row captured
from a backup still verifies — rotate the registry signing key for hard
revocations; see docs/deployment.md known limitations.)
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from maof.errors import RegistryTrustError, SignatureError
from maof.types import utcnow

if TYPE_CHECKING:
    from maof.registry.models import AgentManifest, RegistryEntry
    from maof.transport.signing import Signer


def canonical_bytes(
    manifest: AgentManifest, *, status: str = "approved", approved_at: str | None = None
) -> bytes:
    """Deterministic serialization signed over: manifest + trust state."""
    payload = {
        "manifest": manifest.model_dump(),
        "status": status,
        "approved_at": approved_at,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_entry(entry: RegistryEntry, signer: Signer) -> RegistryEntry:
    approved_at = utcnow()
    headers = signer.headers(
        canonical_bytes(entry.manifest, status="approved", approved_at=approved_at)
    )
    return entry.model_copy(
        update={
            "status": "approved",
            "signature": headers["sig"],
            "kid": headers["kid"],
            "approved_at": approved_at,
        }
    )


def verify_entry(entry: RegistryEntry, signer: Signer) -> None:
    """Raise :class:`RegistryTrustError` unless the entry is approved, signed, and
    the signature covers its current (manifest, status, approved_at) state."""
    if entry.status != "approved":
        raise RegistryTrustError(
            f"registry entry {entry.manifest.id!r} is not approved (status={entry.status!r})"
        )
    if not entry.signature or not entry.kid:
        raise RegistryTrustError(f"registry entry {entry.manifest.id!r} is unsigned")
    try:
        signer.verify(
            canonical_bytes(entry.manifest, status=entry.status, approved_at=entry.approved_at),
            {"kid": entry.kid, "sig": entry.signature},
        )
    except SignatureError as exc:
        raise RegistryTrustError(
            f"registry entry {entry.manifest.id!r} failed verification"
        ) from exc


__all__ = ["canonical_bytes", "sign_entry", "verify_entry"]
