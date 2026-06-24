"""Duration parsing + retry policy (backoff schedule -> DLQ on exhaustion)."""

from __future__ import annotations

import pytest

from maof.transport.retry import RetryPolicy, parse_duration


@pytest.mark.parametrize(
    "text,seconds",
    [("5s", 5.0), ("30s", 30.0), ("2m", 120.0), ("10m", 600.0), ("1h", 3600.0), ("500ms", 0.5)],
)
def test_parse_duration(text: str, seconds: float) -> None:
    assert parse_duration(text) == seconds


def test_parse_duration_invalid() -> None:
    with pytest.raises(ValueError):
        parse_duration("nope")


def test_retry_policy_delays() -> None:
    p = RetryPolicy(["5s", "30s", "2m"])
    assert p.max_attempts == 3
    assert p.delay_for_attempt(1) == 5.0
    assert p.delay_for_attempt(2) == 30.0
    assert p.delay_for_attempt(3) == 120.0
    assert p.delay_for_attempt(4) is None  # exhausted -> DLQ


def test_retry_policy_empty() -> None:
    p = RetryPolicy([])
    assert p.max_attempts == 0
    assert p.delay_for_attempt(1) is None
