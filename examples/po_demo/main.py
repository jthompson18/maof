"""Run the po_demo — the reference scenario — end-to-end, offline.

    python -m examples.po_demo.main

A buyer + partner shared tenant; catalog + datastore source-of-truth agents
injected through the trust registry; the signed po-cycle workflow driving
plan -> reserve -> [the ordering window opens] -> traffic per region -> [join] -> actualize
-> invoice across Commitments and Fulfillment under the spend-cap policy — plus the
auxiliary scenarios (coordination modes, declared context delegation, the
autonomous loop, the eval gate). Zero edits to ``src/maof``.
"""

from __future__ import annotations

import asyncio
import sys

from . import demo
from .scenario import build_scenario, run_cycle_to_completion


async def _main() -> int:
    out = sys.stdout
    out.write("== MAOF po_demo (reference scenario) ==\n\n")

    # 1) The full lifecycle: the purchasing lead asks for 500k, only 250k of client funds
    #    cleared -> the spend-cap policy clamps the commitment.
    ns = await build_scenario(
        committed_spend_usd=500_000, funds_received_usd=250_000, spend_cap_usd=600_000
    )
    state = await run_cycle_to_completion(ns)
    out.write(f"[lifecycle] run {state.run_id}: {state.status.value}\n")
    for position in ns.ledger:
        out.write(
            f"[spend-policy] committed ${position['amount_usd']:,} "
            f"(cleared funds ${position['funds_received_usd']:,}) "
            f"disclosed_principal={position['disclosed_principal']} "
            f"parties={position['parties']}\n"
        )
    for envelope in await ns.results.list(state.run_id):
        out.write(f"[workflow]  {envelope.step_ref}: {envelope.result.output}\n")
    out.write(f"[catalog]  consulted mid-task for: {ns.catalog_client.validations}\n")
    run_events = [e.event_type for e in ns.sink.events if e.event_type.startswith("run_")]
    out.write(f"[events]    {' -> '.join(run_events)}\n\n")

    # 2) Registry intelligence: select the expediting agent semantically.
    hits = await ns.search.search(
        "expedite carrier selection and routing for urgent orders", ns.loader, tenant=ns.tenant
    )
    out.write(f"[search] 'tune in-flight pacing' -> {[m.id for m in hits]}\n")

    # 3) Both coordination modes on the same system.
    modes = await demo.run_both_coordination_modes(ns)
    out.write(
        f"[coordination] mode a -> {modes['queue_name']} "
        f"depth={modes['order_placement_queue_depth']}"
        f"  mode b -> {modes['in_process_summary']!r}\n"
    )

    # 4) Declared context delegation.
    env = await demo.run_context_delegation(ns)
    out.write(f"[delegation] delegated_context={env.extras.get('delegated_context')}\n")
    out.write(f"[delegation] data_pointers after de-dup={[dp.alias for dp in env.data_pointers]}\n")

    # 5) The autonomous loop (subagents under delegation contracts).
    subresults = await demo.run_autonomous_loop(ns)
    refs = [bool(s["artifacts"]) for s in subresults]
    out.write(f"[autonomous] subagents={len(subresults)} artifact_refs={refs}\n")

    # 6) Eval gate: the spend-policy chain is honored.
    report, passed = await demo.run_eval_gate(min_pass_rate=0.6)
    out.write(f"[eval] pass_rate={report.pass_rate:.2f} gate_passed={passed}\n")

    return 0 if state.status.value == "completed" else 1


def main() -> int:
    return asyncio.run(_main())


if __name__ == "__main__":
    raise SystemExit(main())
