"""Injected purchasing lead L1 planner + Commitments/Fulfillment L2 agents.

Pure adopter code: imports MAOF, injects domain agents/skills. Touches no
framework code. Commitments is funds-committing (its funds_commit creates financial
spend-policy); Fulfillment executes (ordering/shipment/delivery-metrics, not spend-policy).
"""

from __future__ import annotations

from typing import Any

from maof.agents.base import BaseL2Agent
from maof.registry.models import ContextDeclaration
from maof.types import L2Context, TaskResult

# Commitments side-loads its own platform context (rate cards, PO templates, regions)
# and DECLARES it so the L1 can de-duplicate + record the delegation.
COMMITMENTS_CONTEXT = ContextDeclaration(
    id="commitments_platform",
    kind="yaml_config",
    description="Commitments rate cards, PO templates, supported regions/formats",
    scope="tenant",
    supplies=["rate_card", "po_template"],
    requires_from_l1=["budget"],
    source_ref="pkg://po_demo/commitments_platform.yaml",
    mutable=False,
)


class CommitmentsAgent(BaseL2Agent):
    """the Commitments platform: purchase planning, buying/commitment, billing. The
    funds_commit commitment is a real financial side effect, wrapped in the
    idempotency guard so a resumed run commits exactly once (no double spend)."""

    name = "commitments"
    accepted_task_types = ["purchase_plan", "funds_commit", "reconciliation"]

    def __init__(self, *, ledger: list[dict[str, Any]] | None = None) -> None:
        super().__init__(context_delegation=[COMMITMENTS_CONTEXT])
        self.ledger: list[dict[str, Any]] = ledger if ledger is not None else []

    async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
        task_type = task["task_type"]
        payload = dict(task.get("payload", {}))
        if task_type == "funds_commit":
            return await self._commit(task, ctx)
        if task_type == "purchase_plan":
            # Plan against the catalog: order codes arrive from the workflow
            # context (sourced from the catalog agent / console surface).
            output = {
                "planned": True,
                "plan_id": f"PLAN-{task['task_id'][:12]}",
                "east_name": payload.get("east_name") or "PO_EAST_REPLENISH_A",
                "west_name": payload.get("west_name") or "PO_WEST_REPLENISH_A",
                "total_usd": int(payload.get("budget") or 0),
            }
            return TaskResult(status="ok", task_id=task["task_id"], output=output)
        # reconciliation: actualize spend, then open the invoice.
        if payload.get("kind") == "invoice":
            amount = int(payload.get("actual_usd") or 0)
            output = {
                "reconciled": True,
                "invoice_id": f"INV-{task['task_id'][:12]}",
                "open": True,
                "amount_usd": amount,
            }
            return TaskResult(status="ok", task_id=task["task_id"], output=output)
        committed = int(payload.get("committed_usd") or 0)
        return TaskResult(
            status="ok",
            task_id=task["task_id"],
            output={"reconciled": True, "actual_usd": committed},
        )

    async def _commit(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
        payload = dict(task.get("payload", {}))
        amount = int(payload.get("amount") or ctx.policy_flags.get("committed_spend_usd", "0"))
        funds = int(ctx.policy_flags.get("funds_received_usd", "0"))

        async def commit() -> dict[str, Any]:
            # The commitment position written to the (replay-safe) ledger.
            record = {
                "task_id": task["task_id"],
                "amount_usd": amount,
                "funds_received_usd": funds,
                "disclosed_principal": ctx.policy_flags.get("disclosed_principal"),
                "parties": {
                    "buyer": ctx.tenant.tenant_id,
                    "partner": "procurement-partner",
                    "vendor": "commitments",
                },
            }
            self.ledger.append(record)
            return {
                "committed": True,
                "amount_usd": amount,
                "po_number": f"PO-{task['task_id'][:12]}",
            }

        key = task["idempotency_key"]
        if ctx.idempotency_guard is not None:
            result = await ctx.idempotency_guard.once(key, commit)
        else:
            result = await commit()
        return TaskResult(status="ok", task_id=task["task_id"], output=result)


class FulfillmentAgent(BaseL2Agent):
    """the Fulfillment platform: order placement, shipment preparation, delivery metrics."""

    name = "fulfillment"
    accepted_task_types = ["order_placement", "shipment_prep", "delivery_metrics"]

    def __init__(self) -> None:
        super().__init__()
        self.executed: list[str] = []

    async def handle(self, task: dict[str, Any], ctx: L2Context) -> TaskResult:
        self.executed.append(task["task_type"])
        output: dict[str, Any] = {"executed": task["task_type"]}
        payload = dict(task.get("payload", {}))
        if task["task_type"] == "order_placement" and "order_code" in payload:
            # Reference path: consult the catalog agent mid-task and
            # report conformance — post_result policy denies the result if False.
            name = str(payload["order_code"])
            output["region"] = payload.get("region", "")
            output["order_code"] = name
            verdict: dict[str, Any] = {"valid": True}
            if ctx.agents is not None:
                catalog = await ctx.agents.client("catalog")
                verdict = await catalog.call_tool("validate", {"name": name})
            output["catalog_ok"] = bool(verdict.get("valid", False))
        return TaskResult(status="ok", task_id=task["task_id"], output=output)


__all__ = ["CommitmentsAgent", "FulfillmentAgent", "COMMITMENTS_CONTEXT"]
