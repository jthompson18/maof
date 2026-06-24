"""HMAC signing round-trip, tamper detection, rotation, header aliases."""

from __future__ import annotations

import pytest

from maof.errors import ConfigError, SignatureError
from maof.transport.signing import Signer, compute_signature

BODY = b'{"task":"funds_commit","amount":250000}'


def test_compute_signature_is_hex_sha256() -> None:
    sig = compute_signature(BODY, "secret")
    assert len(sig) == 64
    int(sig, 16)  # valid hex


def test_sign_verify_round_trip() -> None:
    signer = Signer({"default": "s3cret"}, active_kid="default")
    headers = signer.headers(BODY)
    assert headers["kid"] == "default"
    signer.verify(BODY, headers)  # does not raise


def test_tamper_rejected() -> None:
    signer = Signer({"default": "s3cret"})
    headers = signer.headers(BODY)
    with pytest.raises(SignatureError):
        signer.verify(BODY + b"x", headers)


def test_rotation_multi_key() -> None:
    producer = Signer({"k1": "old"}, active_kid="k1")
    headers = producer.headers(BODY)
    # consumer that knows both keys verifies a k1-signed message
    Signer({"k1": "old", "k2": "new"}, active_kid="k2").verify(BODY, headers)
    # consumer that only knows k2 cannot (unknown kid)
    with pytest.raises(SignatureError):
        Signer({"k2": "new"}, active_kid="k2").verify(BODY, headers)


@pytest.mark.parametrize(
    "kid_h,sig_h",
    [("kid", "sig"), ("x-hmac-kid", "x-hmac-sig"), ("x-sign-kid", "x-signature")],
)
def test_header_aliases_accepted(kid_h: str, sig_h: str) -> None:
    signer = Signer({"default": "s3cret"})
    base = signer.headers(BODY)
    signer.verify(BODY, {kid_h: base["kid"], sig_h: base["sig"]})


def test_header_names_case_insensitive() -> None:
    signer = Signer({"default": "s3cret"})
    base = signer.headers(BODY)
    signer.verify(BODY, {"KID": base["kid"], "SIG": base["sig"]})


def test_missing_headers_rejected() -> None:
    signer = Signer({"default": "s3cret"})
    with pytest.raises(SignatureError):
        signer.verify(BODY, {})


def test_active_kid_must_exist() -> None:
    with pytest.raises(ConfigError):
        Signer({"k1": "x"}, active_kid="missing")


def test_is_valid_returns_bool() -> None:
    signer = Signer({"default": "s3cret"})
    headers = signer.headers(BODY)
    assert signer.is_valid(BODY, headers) is True
    assert signer.is_valid(BODY + b"!", headers) is False
