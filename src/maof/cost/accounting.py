"""Cost/token accounting + the worth-it gate.

Every LLMProvider.generate records to the ledger; the orchestrator loop consults
the worth-it gate before spawning subagents. Multi-agent fan-out is ~15x chat
token cost — reserve it for high-value, parallelizable work.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from maof.policy.engine import RuleDecision
from maof.types import CostSummary

if TYPE_CHECKING:
    from maof.persistence.base import CostRepo
    from maof.types import CostProjection, StageContext


@runtime_checkable
class CostLedger(Protocol):
    async def record(self, run_id: str, *, model: str, in_tokens: int, out_tokens: int) -> None: ...

    async def total(self, run_id: str) -> CostSummary: ...


@runtime_checkable
class WorthItGate(Protocol):
    async def check(self, sc: StageContext, projected: CostProjection) -> RuleDecision: ...


class RepoCostLedger:
    """Default CostLedger: prices token usage from a per-model table (usd per 1k
    tokens) and persists via any :class:`CostRepo` (Postgres by default)."""

    def __init__(self, cost_repo: CostRepo, *, prices: dict[str, float] | None = None) -> None:
        self._repo = cost_repo
        self._prices = dict(prices) if prices is not None else {}

    def _cost_for(self, model: str, in_tokens: int, out_tokens: int) -> float:
        price_per_1k = self._prices.get(model, 0.0)
        return (in_tokens + out_tokens) / 1000.0 * price_per_1k

    async def record(self, run_id: str, *, model: str, in_tokens: int, out_tokens: int) -> None:
        cost = self._cost_for(model, in_tokens, out_tokens)
        await self._repo.record(run_id, model, in_tokens, out_tokens, cost)

    async def total(self, run_id: str) -> CostSummary:
        summary = await self._repo.total(run_id)
        return summary if summary is not None else CostSummary(run_id=run_id)


class DefaultWorthItGate:
    """Feeds projected fan-out/cost into a deny/require_approval/cap decision.

    Multi-agent fan-out is ~15x chat cost — this gate is the brake. ``action``
    selects the response when over budget; ``cap`` and ``deny`` both halt further
    fan-out (capping the subagent count at what has run), ``require_approval`` routes
    to HITL."""

    def __init__(
        self,
        ledger: CostLedger,
        *,
        fanout_cap: int = 10,
        cost_cap_usd: float = 10.0,
        action: str = "require_approval",
        price_per_1k: float = 0.0,
    ) -> None:
        self._ledger = ledger
        self._fanout_cap = fanout_cap
        self._cost_cap_usd = cost_cap_usd
        self._action = action
        self._price_per_1k = price_per_1k

    async def check(self, sc: StageContext, projected: CostProjection) -> RuleDecision:
        summary = await self._ledger.total(sc.run_id)
        projected_usd = projected.projected_usd
        if projected_usd == 0.0 and projected.projected_tokens and self._price_per_1k:
            # Price the token projection so the cost cap can fire PROSPECTIVELY,
            # before the spend happens — not just on ledgered actuals.
            projected_usd = projected.projected_tokens / 1000.0 * self._price_per_1k
        projected_cost = summary.cost_usd + projected_usd
        over_fanout = projected.projected_subagents > self._fanout_cap
        over_cost = projected_cost > self._cost_cap_usd
        if not (over_fanout or over_cost):
            return RuleDecision()
        reason = (
            f"worth-it gate: subagents {projected.projected_subagents} (cap {self._fanout_cap}), "
            f"projected ${projected_cost:.2f} (cap ${self._cost_cap_usd})"
        )
        if self._action in ("deny", "cap"):
            return RuleDecision(denied=True, denial_reason=reason)
        return RuleDecision(require_approval=True, approval_reason=reason)


if TYPE_CHECKING:

    def _assert_cost_ledger(repo: CostRepo) -> CostLedger:
        return RepoCostLedger(repo)  # structural conformance check

    def _assert_worth_it_gate(ledger: CostLedger) -> WorthItGate:
        return DefaultWorthItGate(ledger)  # structural conformance check


__all__ = ["CostLedger", "WorthItGate", "RepoCostLedger", "DefaultWorthItGate"]
