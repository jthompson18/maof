"""Safe-by-default signing + security warnings (production-hardening review)."""

from __future__ import annotations

import pytest

from maof.config import Settings
from maof.errors import ConfigError
from maof.transport.signing import ensure_signing_configured


def test_require_signature_defaults_true() -> None:
    assert Settings(msg_signing_secret="x").require_signature is True


def test_ensure_signing_refuses_required_but_empty_env_key() -> None:
    settings = Settings(require_signature=True, signing_key_provider="env", msg_signing_secret="")
    with pytest.raises(ConfigError):
        ensure_signing_configured(settings)


def test_ensure_signing_passes_with_secret() -> None:
    settings = Settings(
        require_signature=True, signing_key_provider="env", msg_signing_secret="s3cr3t"
    )
    ensure_signing_configured(settings)


def test_ensure_signing_opt_out_allows_empty_key() -> None:
    settings = Settings(require_signature=False, signing_key_provider="env", msg_signing_secret="")
    ensure_signing_configured(settings)


def test_security_warnings_flags_dev_defaults() -> None:
    settings = Settings(
        require_signature=False,
        msg_signing_secret="",
        broker_url="amqp://guest:guest@localhost:5672/",
        db_url="postgresql://maof:maof@localhost:5432/maof",
    )
    warnings = settings.security_warnings()
    assert any("require_signature" in w for w in warnings)
    assert any("guest:guest" in w for w in warnings)
    assert any("maof:maof" in w for w in warnings)


def test_security_warnings_quiet_when_hardened() -> None:
    settings = Settings(
        require_signature=True,
        msg_signing_secret="s3cr3t",
        broker_url="amqps://user:pass@broker.internal:5671/",
        db_url="postgresql://app:pass@db.internal:5432/app",
    )
    assert settings.security_warnings() == []
