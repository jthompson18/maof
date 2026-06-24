"""Approval tokens — HMAC-signed, tamper-evident grant tokens."""

from __future__ import annotations

import hashlib
import hmac

from maof.errors import SignatureError


def mint_approval_token(approval_id: str, secret: str) -> str:
    sig = hmac.new(secret.encode("utf-8"), approval_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{approval_id}.{sig}"


def verify_approval_token(token: str, secret: str) -> str:
    approval_id, _, sig = token.rpartition(".")
    if not approval_id or not sig:
        raise SignatureError("malformed approval token")
    expected = hmac.new(
        secret.encode("utf-8"), approval_id.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise SignatureError("invalid approval token")
    return approval_id


__all__ = ["mint_approval_token", "verify_approval_token"]
