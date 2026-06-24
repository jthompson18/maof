"""Reference integration: the full purchase-order lifecycle as a pure
adopter — shared buyer/partner tenant, catalog + datastore source-of-truth agents,
a signed YAML workflow over Commitments/Fulfillment, the spend-cap policy — offline, with
zero edits to ``src/maof``."""

from __future__ import annotations

import asyncio

import pytest

from examples.po_demo.scenario import (
    BUYER_FINANCE,
    BUYER_LEAD,
    PARTNER_OPS,
    SHADY_DSP_MANIFEST,
    build_scenario,
    cancel_run,
    drive,
    run_cycle_to_completion,
    start_cycle,
)
from maof.errors import MAOFError, RegistryTrustError
from maof.types import RunStatus, StageContext, TenantContext


# the headline: full lifecycle, offline, registry-routed
async def test_full_lifecycle_completes_offline() -> None:
    ns = await build_scenario()
    state = await run_cycle_to_completion(ns)
    assert state.status is RunStatus.COMPLETED

    # exactly one funds-committing commitment, disclosed principal asserted
    assert len(ns.ledger) == 1
    position = ns.ledger[0]
    assert position["amount_usd"] == 250_000
    assert position["disclosed_principal"] == "true"
    assert position["parties"]["vendor"] == "commitments"

    # both regions placed after the flight-start gate; budgets actualized;
    # the invoice opened
    assert ns.fulfillment.executed.count("order_placement") == 2
    all_envelopes = await ns.results.list(state.run_id)
    by_step = {e.step_ref: e.result.output for e in all_envelopes}
    assert by_step["reserve"]["committed"] is True
    assert by_step["actualize"]["actual_usd"] == 250_000
    assert by_step["invoice"]["open"] is True and by_step["invoice"]["amount_usd"] == 250_000

    # registry-driven routing: vendor queues, not the naming convention
    assert ns.broker.depth("tasks.purchase_plan") == 0
    assert ns.broker.depth("tasks.order_placement") == 0

    # lifecycle events with the disclosed principal as actor
    types = ns.event_types()
    assert "run_started" in types and "run_waiting" in types and "run_completed" in types
    started = next(e for e in ns.sink.events if e.event_type == "run_started")
    assert started.actor is not None and started.actor["id"] == BUYER_LEAD.id
    # source-of-truth context delegation + mid-task consultation audited
    assert "context_delegated" in types
    assert "agent_consulted" in types
    assert ns.catalog_client.validations  # fulfillment consulted the catalog mid-task


async def test_catalog_slice_attaches_to_planning_context() -> None:
    ns = await build_scenario()
    sc = StageContext(run_id="ctx-probe", tenant=ns.tenant, goal="probe")
    env = await ns.context_builder.build(sc)
    assert env.semantic_model["catalog"]["version"] == "tax-v3"
    assert env.semantic_model["catalog"]["regions"] == ["east", "west", "central"]


# the spend-cap policy: clamp + fail-closed approvals
async def test_overcommit_is_clamped_to_cleared_funds() -> None:
    ns = await build_scenario(
        committed_spend_usd=500_000, funds_received_usd=250_000, spend_cap_usd=600_000
    )
    state = await run_cycle_to_completion(ns)
    assert state.status is RunStatus.COMPLETED
    assert len(ns.ledger) == 1
    assert ns.ledger[0]["amount_usd"] == 250_000  # never beyond client funding


async def test_overcap_requires_two_party_approval_with_attribution() -> None:
    from maof.approval.service import ApprovalGate

    gate = ApprovalGate(timeout=10.0)
    ns = await build_scenario(
        committed_spend_usd=400_000,
        funds_received_usd=500_000,
        spend_cap_usd=300_000,
        hitl_enabled=True,
        approval_gate=gate,
    )
    run_task = asyncio.create_task(start_cycle(ns))
    for _ in range(200):
        await asyncio.sleep(0.01)
        if gate._pending:  # noqa: SLF001
            break
    approval_id = next(iter(gate._pending))  # noqa: SLF001
    assert not run_task.done()  # blocked on the two-party gate

    # a wrong-role principal cannot resolve it
    with pytest.raises(PermissionError):
        await gate.resolve(approval_id, approved=True, principal=BUYER_LEAD)

    # buyer finance alone is not enough — both sides must sign
    await gate.resolve(approval_id, approved=True, principal=BUYER_FINANCE)
    await asyncio.sleep(0.05)
    assert not run_task.done()

    await gate.resolve(approval_id, approved=True, principal=PARTNER_OPS)
    out = await asyncio.wait_for(run_task, timeout=5.0)
    assert out.status == "waiting"  # proceeded into the workflow

    resolutions = gate.resolutions(approval_id)
    assert {r["principal_id"] for r in resolutions} == {BUYER_FINANCE.id, PARTNER_OPS.id}
    assert {r["org"] for r in resolutions} == {"buyer", "partner"}

    state = await drive(ns, out.run_id)
    assert state.status is RunStatus.COMPLETED
    assert ns.ledger[0]["amount_usd"] == 400_000


async def test_overcap_without_gate_fails_closed() -> None:
    ns = await build_scenario(
        committed_spend_usd=400_000, funds_received_usd=500_000, spend_cap_usd=300_000
    )
    out = await start_cycle(ns)
    assert out.status == "denied"  # fail closed: no gate, no commitment
    assert ns.ledger == []
    kinds = [e.details.get("kind") for e in ns.sink.events if e.event_type == "run_failed"]
    assert "policy_denied" in kinds


# source-of-truth enforcement
async def test_catalog_violating_placement_is_quarantined() -> None:
    ns = await build_scenario(order_code_east="po east lot!!")  # violates the grammar
    out = await start_cycle(ns)
    state = await drive(ns, out.run_id, rounds=6)

    # the non-conformant trafficking result was denied post_result: quarantined to
    # the DLQ, never persisted — the actualize join cannot consume it
    assert ns.broker.depth("results.dlq") >= 1
    assert await ns.results.list(out.run_id, "order_east") == []
    assert await ns.results.list(out.run_id, "actualize") == []
    assert state.status is RunStatus.WAITING  # still parked on the join, not completed


async def test_required_catalog_outage_fails_run_closed() -> None:
    ns = await build_scenario(catalog_down=True)
    with pytest.raises(MAOFError, match="required"):
        await start_cycle(ns)


# durability: kill -> resume commits exactly once
async def test_redelivered_commitment_commits_exactly_once() -> None:
    ns = await build_scenario()
    out = await start_cycle(ns)
    assert out.status == "waiting"

    # plan executes; its result resumes the run, which dispatches the reserve task
    await ns.worker.consume("suppliers.commitments.v1")
    await ns.collector.drain()
    redelivery = ns.broker.peek("suppliers.commitments.v1")[0]

    await ns.worker.consume("suppliers.commitments.v1")  # first delivery -> commits
    assert len(ns.ledger) == 1

    body, headers, message_id, correlation_id = redelivery
    await ns.broker.publish(  # kill-before-ack: the broker redelivers
        "suppliers.commitments.v1",
        body,
        headers=headers,
        message_id=message_id,
        correlation_id=correlation_id,
    )
    await ns.worker.consume("suppliers.commitments.v1")
    assert len(ns.ledger) == 1  # the guard deduped the replayed commitment

    state = await drive(ns, out.run_id)
    assert state.status is RunStatus.COMPLETED
    assert len(ns.ledger) == 1


# execution lifecycle: cancellation
async def test_cancel_mid_flight_stops_side_effects() -> None:
    ns = await build_scenario()
    out = await start_cycle(ns)
    assert out.status == "waiting"

    await cancel_run(ns, out.run_id)  # operator cancels while parked
    state = await ns.run_store.get_state(out.run_id)
    assert state.status is RunStatus.CANCELLED

    # the already-queued plan task is skipped before any side effect
    await ns.worker.consume("suppliers.commitments.v1")
    assert "task_skipped" in ns.event_types()
    assert ns.ledger == []
    assert await ns.results.list(out.run_id) == []


# registry intelligence
async def test_semantic_search_selects_the_expediting_agent() -> None:
    ns = await build_scenario()
    hits = await ns.search.search(
        "expedite carrier selection and routing for urgent orders", ns.loader, tenant=ns.tenant
    )
    assert hits and hits[0].id == "expediter"


async def test_certification_gate_blocks_uncertified_vendor() -> None:
    ns = await build_scenario()
    await ns.trust.submit(SHADY_DSP_MANIFEST)
    with pytest.raises(RegistryTrustError, match="certification"):
        await ns.trust.approve("shady-broker")
    approved = {m.id for m in await ns.loader.manifests()}
    assert "shady-broker" not in approved


# source-of-truth data layer + signed workflow lifecycle
async def test_datastore_resolver_serves_references_jit() -> None:
    ns = await build_scenario()
    sc = StageContext(run_id="jit", tenant=TenantContext(tenant_id=ns.tenant.tenant_id), goal="g")
    out = await ns.resolver.resolve("datastore://rate-card/east", sc)
    assert "unit_price_usd" in out
    assert ns.datastore_client.reads == ["rate-card/east"]


async def test_revoked_workflow_refuses_to_run() -> None:
    ns = await build_scenario()
    await ns.workflows.revoke("po-cycle", 1)
    with pytest.raises(RegistryTrustError):
        await ns.workflows.load("po-cycle")
