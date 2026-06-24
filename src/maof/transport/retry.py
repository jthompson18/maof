"""Retry-with-backoff schedule + duration parsing.

Shared by every Broker adapter so retry/DLQ semantics are uniform (per-attempt
headers + delayed requeue), independent of the underlying transport.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

#: Header carrying the 1-based delivery attempt count.
ATTEMPT_HEADER = "x-maof-attempt"
#: Header stamped on dead-lettered messages explaining why.
DEATH_REASON_HEADER = "x-maof-death-reason"

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$")
_UNIT_SECONDS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86_400.0}


def parse_duration(text: str) -> float:
    """Parse ``"5s"`` / ``"30s"`` / ``"2m"`` / ``"10m"`` / ``"1h"`` / ``"500ms"`` -> seconds."""
    match = _DURATION_RE.match(text)
    if match is None:
        raise ValueError(f"invalid duration: {text!r}")
    value, unit = match.group(1), match.group(2)
    return float(value) * _UNIT_SECONDS[unit]


class RetryPolicy:
    """A fixed backoff schedule. ``steps[i]`` is the delay before retry ``i+1``."""

    def __init__(self, steps: Sequence[str]) -> None:
        self._delays: list[float] = [parse_duration(s) for s in steps]

    @property
    def max_attempts(self) -> int:
        """Number of retries (i.e. number of configured backoff steps)."""
        return len(self._delays)

    def delay_for_attempt(self, attempt: int) -> float | None:
        """Delay before the next retry after delivery #``attempt`` (1-based) failed.

        Returns ``None`` when the retry budget is exhausted -> dead-letter."""
        if 1 <= attempt <= len(self._delays):
            return self._delays[attempt - 1]
        return None


__all__ = ["parse_duration", "RetryPolicy", "ATTEMPT_HEADER", "DEATH_REASON_HEADER"]
