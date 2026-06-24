"""Default ``approval`` stage — the HITL gate (toggleable).

If the post-plan policy decision required approval and HITL is enabled, block on
the injected gate until a human approves (denial raises, handled cleanly by the
L1 driver). When HITL is disabled or no gate is wired, behavior follows
``fallback``: **"deny" (the default) fails closed** — an approval-requiring plan
without a human in the loop is denied — while "allow" preserves the advisory
posture (publish with only the audit flag).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from maof.errors import PolicyDenied

if TYPE_CHECKING:
    from maof.types import StageContext


class ApprovalStage:
    name = "approval"

    def __init__(
        self,
        *,
        gate: Any | None = None,
        hitl_enabled: bool = True,
        fallback: str = "deny",
    ) -> None:
        self._gate = gate
        self._hitl_enabled = hitl_enabled
        self._fallback = fallback

    async def execute(self, sc: StageContext) -> StageContext:
        policy = sc.extras.get("policy", {})
        if not policy.get("require_approval"):
            return sc
        reason = policy.get("approval_reason", "")
        if self._hitl_enabled and self._gate is not None:
            await self._gate.wait(
                sc,
                reason=reason,
                required_roles=policy.get("approval_roles") or None,
                parties=int(policy.get("approval_parties", 1)),
            )
            return sc
        if self._fallback == "deny":
            raise PolicyDenied(f"approval required but HITL is unavailable (fail closed): {reason}")
        return sc


__all__ = ["ApprovalStage"]
