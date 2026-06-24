"""OIDC claims → Principal mapping + token verification.

Pure claim mapping needs no dependencies; token tests mint real JWTs locally
with PyJWT (the ``oidc`` extra) — offline, no IdP. Verification fails closed:
tampered/expired/wrong-audience tokens never become Principals.
"""

from __future__ import annotations

import time

import pytest

from maof.errors import SignatureError
from maof.identity import (
    ENTRA_CLAIMS,
    OKTA_CLAIMS,
    ClaimsMapping,
    principal_from_claims,
)

# pure claim mapping


def test_default_mapping_with_space_delimited_scope() -> None:
    principal = principal_from_claims(
        {"sub": "user-1", "roles": ["buyer-finance"], "scope": "buy:commit read:plans"}
    )
    assert principal.id == "user-1"
    assert principal.kind == "user"
    assert principal.roles == ["buyer-finance"]
    assert principal.scopes == ["buy:commit", "read:plans"]


def test_okta_shaped_claims() -> None:
    claims = {
        "sub": "00u1abcd",
        "groups": ["buyer-finance", "Everyone"],
        "scp": ["buy:commit", "openid"],
    }
    principal = principal_from_claims(claims, OKTA_CLAIMS)
    assert principal.id == "00u1abcd"
    assert principal.roles == ["buyer-finance", "Everyone"]
    assert principal.scopes == ["buy:commit", "openid"]


def test_entra_shaped_claims() -> None:
    claims = {
        "sub": "AAAA-1",
        "roles": ["partner-ops"],
        "scp": "buy:commit audit:read",
        "tid": "tenant-guid-1",
    }
    principal = principal_from_claims(claims, ENTRA_CLAIMS)
    assert principal.roles == ["partner-ops"]
    assert principal.scopes == ["buy:commit", "audit:read"]
    assert principal.org == "tenant-guid-1"


def test_missing_subject_fails_closed() -> None:
    with pytest.raises(SignatureError):
        principal_from_claims({"roles": ["x"]})


def test_service_kind_mapping() -> None:
    mapping = ClaimsMapping(subject_claim="client_id", kind="service")
    principal = principal_from_claims({"client_id": "svc-1"}, mapping)
    assert principal.kind == "service"


# token verification (PyJWT, minted locally)

jwt = pytest.importorskip("jwt")

# >=32 bytes: HS256 keys below that trip PyJWT's InsecureKeyLengthWarning (RFC 7518 §3.2).
SECRET = "unit-test-oidc-secret-0123456789-abcdef"
HS = ("HS256",)


def _token(claims: dict[str, object]) -> str:
    return jwt.encode(claims, SECRET, algorithm="HS256")


def test_verify_token_maps_principal() -> None:
    from maof.identity import verify_oidc_token

    token = _token(
        {
            "sub": "user-7",
            "roles": ["buyer-finance"],
            "scope": "buy:commit",
            "iss": "https://idp.example.com",
            "aud": "maof",
            "exp": int(time.time()) + 60,
        }
    )
    principal = verify_oidc_token(
        token, key=SECRET, issuer="https://idp.example.com", audience="maof", algorithms=HS
    )
    assert principal.id == "user-7"
    assert principal.roles == ["buyer-finance"]


def test_verify_rejects_tampered_token() -> None:
    from maof.identity import verify_oidc_token

    token = _token({"sub": "user-7", "exp": int(time.time()) + 60})
    header, payload, sig = token.split(".")
    tampered = f"{header}.{payload}x.{sig}"
    with pytest.raises(SignatureError):
        verify_oidc_token(tampered, key=SECRET, algorithms=HS)


def test_verify_rejects_expired_token() -> None:
    from maof.identity import verify_oidc_token

    token = _token({"sub": "user-7", "exp": int(time.time()) - 10})
    with pytest.raises(SignatureError):
        verify_oidc_token(token, key=SECRET, algorithms=HS)


def test_verify_rejects_wrong_audience() -> None:
    from maof.identity import verify_oidc_token

    token = _token({"sub": "user-7", "aud": "someone-else", "exp": int(time.time()) + 60})
    with pytest.raises(SignatureError):
        verify_oidc_token(token, key=SECRET, audience="maof", algorithms=HS)


def test_rs256_round_trip_when_cryptography_available() -> None:
    pytest.importorskip("cryptography")
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    from maof.identity import verify_oidc_token

    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    public_pem = private.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    token = jwt.encode(
        {"sub": "user-rs", "scope": "buy:commit", "exp": int(time.time()) + 60},
        private_pem,
        algorithm="RS256",
    )
    principal = verify_oidc_token(token, key=public_pem, algorithms=("RS256",))
    assert principal.id == "user-rs"
    assert principal.scopes == ["buy:commit"]
