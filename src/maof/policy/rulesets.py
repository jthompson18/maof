"""Ruleset version + canary selection.

Canary routing is deterministic in a stable key (run_id / tenant_id) so a given
run consistently sees the same ruleset. Tenant-scoped rulesets beat global ones
(handled in the repo query); this module handles stable-vs-canary selection.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from maof.types import LoadedRuleset


def in_canary(key: str, canary_pct: float) -> bool:
    """Deterministically decide if ``key`` falls in the canary cohort (pct 0-100)."""
    if canary_pct <= 0:
        return False
    if canary_pct >= 100:
        return True
    bucket = int.from_bytes(hashlib.sha256(key.encode("utf-8")).digest()[:4], "big") % 100
    return bucket < canary_pct


def choose_ruleset(
    stable: LoadedRuleset, canary: LoadedRuleset | None, *, key: str
) -> LoadedRuleset:
    if canary is not None and in_canary(key, canary.canary_pct):
        return canary
    return stable


__all__ = ["in_canary", "choose_ruleset"]
