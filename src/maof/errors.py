"""MAOF exception hierarchy.

All framework errors derive from :class:`MAOFError` so adopters can catch the
whole family with one ``except``. Subclasses map to the failure domains called
out in the design spec (config, signing, schema, policy, idempotency, transport,
registry trust, budget/cost).
"""

from __future__ import annotations


class MAOFError(Exception):
    """Base class for every error raised by MAOF."""


class ConfigError(MAOFError):
    """Missing or invalid configuration (fail fast on startup)."""


class SignatureError(MAOFError):
    """Message signing/verification failure (HMAC mismatch, unknown kid)."""


class SchemaValidationError(MAOFError):
    """A task body failed validation against its registered JSON Schema."""

    def __init__(self, message: str, *, schema_id: str | None = None) -> None:
        super().__init__(message)
        self.schema_id = schema_id


class PolicyDenied(MAOFError):
    """A policy rule denied a plan/action (`deny_plan`)."""

    def __init__(self, reason: str, *, rule_id: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.rule_id = rule_id


class ApprovalRequired(MAOFError):
    """A policy rule routed the action to the HITL approval gate."""

    def __init__(self, reason: str, *, rule_id: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.rule_id = rule_id


class IdempotencyError(MAOFError):
    """A side effect could not be made replay-safe."""


class TransportError(MAOFError):
    """Broker publish/consume/topology failure."""


class RegistryTrustError(MAOFError):
    """A registry entry is unapproved, unsigned, tampered, or revoked."""


class BudgetExceeded(MAOFError):
    """A token/cost/effort budget was exceeded."""


class TenancyError(MAOFError):
    """A tenant-isolation invariant was violated."""


class AuthzError(MAOFError):
    """A principal lacks the scope(s) required for an action (authorization denied)."""
