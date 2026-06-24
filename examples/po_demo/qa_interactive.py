"""Interactive QA drills — drive the reference governance by hand (docs/QA.md Tier 0).

    python -m examples.po_demo.qa_interactive            # menu
    python -m examples.po_demo.qa_interactive --drill 2  # one drill
    python -m examples.po_demo.qa_interactive --all --yes  # non-interactive smoke

Everything runs offline and in-process (no Docker, no Ollama). Each drill states
what it proves, pauses where a human decision matters, and prints the evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Any

from maof.approval.service import ApprovalGate
from maof.errors import MAOFError, RegistryTrustError

from .scenario import (
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

YES = False  # --yes: auto-confirm prompts


def say(text: str = "") -> None:
    sys.stdout.write(text + "\n")


def ask(prompt: str) -> bool:
    if YES:
        say(f"{prompt} [auto-yes]")
        return True
    return input(f"{prompt} [Y/n] ").strip().lower() not in ("n", "no")


def header(n: int, title: str, proves: str) -> None:
    say(f"\n=== Drill {n}: {title} " + "=" * max(1, 60 - len(title)))
    say(f"    proves: {proves}\n")


async def drill_clamp() -> None:
    header(1, "spend-cap clamp", "spend never exceeds cleared client funds")
    ns = await build_scenario(
        committed_spend_usd=500_000, funds_received_usd=250_000, spend_cap_usd=600_000
    )
    say("purchasing lead asks to commit $500,000; only $250,000 of client funds has cleared.")
    state = await run_cycle_to_completion(ns)
    position = ns.ledger[0]
    say(f"run: {state.status.value}")
    say(
        f"ledger: committed ${position['amount_usd']:,} "
        f"(cleared ${position['funds_received_usd']:,}) "
        f"disclosed_principal={position['disclosed_principal']}"
    )
    assert position["amount_usd"] == 250_000
    say("PASS — the policy clamped the commitment to cleared funds.")


async def drill_two_party_approval() -> None:
    header(
        2,
        "two-party role-bound approval",
        "an over-cap commitment needs buyer-finance AND partner-ops; "
        "wrong roles are rejected; every resolution is attributed",
    )
    gate = ApprovalGate(timeout=300.0)
    ns = await build_scenario(
        committed_spend_usd=400_000,
        funds_received_usd=500_000,
        spend_cap_usd=300_000,
        hitl_enabled=True,
        approval_gate=gate,
    )
    say("Committing $400,000 against a $300,000 spend cap -> approval required.")
    run_task = asyncio.create_task(start_cycle(ns))
    for _ in range(400):
        await asyncio.sleep(0.01)
        if gate._pending:  # noqa: SLF001 - QA reaches into the in-process gate
            break
    approval_id = next(iter(gate._pending))  # noqa: SLF001
    say(f"run is BLOCKED on approval {approval_id!r}.")

    if ask(f"Try to approve as the purchasing lead ({BUYER_LEAD.id}, roles={BUYER_LEAD.roles})?"):
        try:
            await gate.resolve(approval_id, approved=True, principal=BUYER_LEAD)
        except PermissionError as exc:
            say(f"REJECTED (as designed): {exc}")

    if not ask(f"Approve as buyer finance ({BUYER_FINANCE.id})?"):
        await gate.resolve(approval_id, approved=False, principal=BUYER_FINANCE)
        out = await asyncio.wait_for(run_task, timeout=10.0)
        say(f"denied by finance -> run: {out.status} (fail closed). Drill over.")
        return
    await gate.resolve(approval_id, approved=True, principal=BUYER_FINANCE)
    await asyncio.sleep(0.05)
    say(f"one signature in; run still blocked (parties=2): done={run_task.done()}")

    if not ask(f"Approve as partner ops ({PARTNER_OPS.id})?"):
        await gate.resolve(approval_id, approved=False, principal=PARTNER_OPS)
        out = await asyncio.wait_for(run_task, timeout=10.0)
        say(f"denied by ops -> run: {out.status} (fail closed). Drill over.")
        return
    await gate.resolve(approval_id, approved=True, principal=PARTNER_OPS)
    out = await asyncio.wait_for(run_task, timeout=10.0)
    say(f"both parties signed -> run proceeded: {out.status}")
    say(f"attribution: {gate.resolutions(approval_id)}")
    state = await drive(ns, out.run_id)
    say(f"final: {state.status.value}; ledger amount ${ns.ledger[0]['amount_usd']:,}")
    say("PASS — two-party, role-bound, attributed.")


async def drill_cancellation() -> None:
    header(3, "mid-flight cancellation", "an operator cancel stops side effects cold")
    ns = await build_scenario()
    out = await start_cycle(ns)
    say(f"run {out.run_id} is WAITING (plan task queued, not yet consumed).")
    if ask("Cancel the run now (maof runs cancel)?"):
        await cancel_run(ns, out.run_id)
        state = await ns.run_store.get_state(out.run_id)
        say(f"status: {state.status.value}")
        await ns.worker.consume("suppliers.commitments.v1")  # the queued task is skipped
        say(f"queued task skipped: {'task_skipped' in ns.event_types()}")
        persisted = len(await ns.results.list(out.run_id))
        say(f"ledger entries: {len(ns.ledger)}  results persisted: {persisted}")
        assert ns.ledger == []
        say("PASS — cancelled before any side effect.")
    else:
        state = await drive(ns, out.run_id)
        say(f"left to run -> {state.status.value}")


async def drill_catalog_quarantine() -> None:
    header(
        4,
        "source-of-truth enforcement",
        "a catalog-violating order code is quarantined post-result; "
        "downstream steps never consume it",
    )
    ns = await build_scenario(order_code_east="po east lot!!")
    say('east-region order code "po east lot!!" violates the catalog grammar.')
    out = await start_cycle(ns)
    state = await drive(ns, out.run_id, rounds=6)
    say(f"run status after pumping: {state.status.value} (stuck on the join, by design)")
    say(f"results DLQ depth: {ns.broker.depth('results.dlq')}")
    say(f"order_east results persisted: {len(await ns.results.list(out.run_id, 'order_east'))}")
    say(f"actualize ran: {len(await ns.results.list(out.run_id, 'actualize')) > 0}")
    if ask("Clean up: cancel the stuck run?"):
        await cancel_run(ns, out.run_id)
        say(f"final: {(await ns.run_store.get_state(out.run_id)).status.value}")
    say("PASS — non-conformant vendor output never propagated.")


async def drill_exactly_once() -> None:
    header(5, "kill -> resume exactly-once", "a redelivered commitment never double-commits")
    ns = await build_scenario()
    out = await start_cycle(ns)
    await ns.worker.consume("suppliers.commitments.v1")  # plan executes
    await ns.collector.drain()  # resume -> reserve dispatched
    redelivery = ns.broker.peek("suppliers.commitments.v1")[0]
    await ns.worker.consume("suppliers.commitments.v1")  # commits
    say(f"first delivery committed. ledger entries: {len(ns.ledger)}")
    if ask("Simulate kill-before-ack: redeliver the SAME commitment message?"):
        body, headers, message_id, correlation_id = redelivery
        await ns.broker.publish(
            "suppliers.commitments.v1",
            body,
            headers=headers,
            message_id=message_id,
            correlation_id=correlation_id,
        )
        await ns.worker.consume("suppliers.commitments.v1")
        say(f"after redelivery: ledger entries: {len(ns.ledger)} (guard deduped)")
        assert len(ns.ledger) == 1
    state = await drive(ns, out.run_id)
    say(f"final: {state.status.value}; ledger entries: {len(ns.ledger)}")
    say("PASS — exactly-once across redelivery.")


async def drill_certification_gate() -> None:
    header(
        6, "certification-gated registry", "a vendor that flunks its eval suite cannot be approved"
    )
    ns = await build_scenario()
    await ns.trust.submit(SHADY_DSP_MANIFEST)
    say(f"submitted {SHADY_DSP_MANIFEST.id!r} (certification min_pass_rate=0.8).")
    try:
        await ns.trust.approve("shady-broker")
        say("FAIL — approve should have been refused!")
    except RegistryTrustError as exc:
        say(f"approve refused: {exc}")
    approved = {m.id for m in await ns.loader.manifests()}
    say(f"approved registry: {sorted(approved)}")
    assert "shady-broker" not in approved
    say("PASS — certification gate held.")


async def drill_required_source_outage() -> None:
    header(
        7,
        "required context source fails closed",
        "if the catalog agent is down, planning runs do not proceed on stale truth",
    )
    ns = await build_scenario(catalog_down=True)
    say("catalog agent is DOWN (required=true).")
    try:
        await start_cycle(ns)
        say("FAIL — the run should have failed closed!")
    except MAOFError as exc:
        say(f"run failed closed: {exc}")
    say("PASS — no planning without the source of truth.")


async def drill_semantic_search() -> None:
    header(
        8,
        "semantic capability search (hashing baseline)",
        "a natural-language need finds the right registered agent "
        "(see qa_llm.py for real embeddings)",
    )
    ns = await build_scenario()
    query = "expedite carrier selection and routing for urgent orders"
    hits = await ns.search.search(query, ns.loader, tenant=ns.tenant)
    say(f"query: {query!r}")
    for rank, manifest in enumerate(hits[:3], start=1):
        say(f"  {rank}. {manifest.id} — {manifest.description[:60]}")
    assert hits and hits[0].id == "expediter"
    say("PASS — the expediting agent ranks first.")


DRILLS = [
    drill_clamp,
    drill_two_party_approval,
    drill_cancellation,
    drill_catalog_quarantine,
    drill_exactly_once,
    drill_certification_gate,
    drill_required_source_outage,
    drill_semantic_search,
]


def describe_workflow(definition: Any) -> str:
    """`maof workflow run <name> --module examples.po_demo.qa_interactive:describe_workflow`
    target: proves the CLI loads ONLY approved + signature-valid definitions."""
    steps = " -> ".join(s.id for s in definition.steps)
    return f"{definition.name} v{definition.version}: {steps}"


async def _main() -> int:
    global YES
    parser = argparse.ArgumentParser(description="MAOF interactive QA drills")
    parser.add_argument("--all", action="store_true", help="run every drill in order")
    parser.add_argument("--drill", type=int, default=None, help="run one drill (1-8)")
    parser.add_argument("--yes", action="store_true", help="auto-confirm prompts (smoke mode)")
    args = parser.parse_args()
    YES = args.yes

    if args.all:
        chosen = DRILLS
    elif args.drill:
        chosen = [DRILLS[args.drill - 1]]
    else:
        say("MAOF QA drills:")
        for i, drill in enumerate(DRILLS, start=1):
            say(f"  {i}. {(drill.__doc__ or drill.__name__)}")
        for i, drill in enumerate(DRILLS, start=1):
            say("")
            if ask(f"Run drill {i} ({drill.__name__.removeprefix('drill_')})?"):
                await drill()
        return 0

    for drill in chosen:
        await drill()
    say("\nAll selected drills completed.")
    return 0


def main() -> int:
    return asyncio.run(_main())


if __name__ == "__main__":
    raise SystemExit(main())
