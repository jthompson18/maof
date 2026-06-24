"""LLM-path QA scenarios on local Ollama models (docs/QA.md Tier 2).

    ollama pull qwen3:8b llama3.2:3b nomic-embed-text   # one-time
    python -m examples.po_demo.qa_llm                  # all scenarios
    python -m examples.po_demo.qa_llm --scenario d     # one scenario

Exercises every injection point where a real model matters: registry semantic
search (real embeddings vs the hashing baseline), mode-b in-process subagents,
an LLM-driven autonomous planning loop, the LLM-as-judge eval gate, and context
compaction. Model knobs (env):

    QA_PLANNER_MODEL  planner + judge   (default qwen3:8b)
    QA_FAST_MODEL     subagents + compaction (default llama3.2:3b)
    QA_EMBED_MODEL    embeddings        (default nomic-embed-text, 768-dim)
    OLLAMA_HOST       server            (default http://127.0.0.1:11434)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx

from maof.eval.judge import LLMJudge
from maof.eval.rubrics import make_rubric
from maof.eval.runner import load_dataset
from maof.models.base import HashingEmbeddingProvider
from maof.models.ollama import OllamaEmbeddingProvider, OllamaProvider
from maof.orchestrator.coordinator import DefaultCoordinator, InProcessSubagent
from maof.orchestrator.delegation import DelegationContract
from maof.orchestrator.loop import OrchestratorLoop
from maof.policy.engine import NativePolicyEngine
from maof.registry.loader import RegistryLoader
from maof.registry.search import RegistrySearch
from maof.registry.store import InMemoryRegistryRepo, RegistryStore
from maof.runs.store import InMemoryRunStore
from maof.transport.signing import Signer
from maof.types import EffortBudget, StageContext, TenantContext

from .scenario import (
    CATALOG_MANIFEST,
    COMMITMENTS_MANIFEST,
    DATASTORE_MANIFEST,
    EXPEDITER_MANIFEST,
    FULFILLMENT_MANIFEST,
    RULES_DIR,
    InMemoryVectorStore,
    _RulesetRepo,
    load_ruleset,
)

PLANNER_MODEL = os.getenv("QA_PLANNER_MODEL", "qwen3:8b")
FAST_MODEL = os.getenv("QA_FAST_MODEL", "llama3.2:3b")
EMBED_MODEL = os.getenv("QA_EMBED_MODEL", "nomic-embed-text")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
MANIFESTS = (
    CATALOG_MANIFEST,
    DATASTORE_MANIFEST,
    COMMITMENTS_MANIFEST,
    FULFILLMENT_MANIFEST,
    EXPEDITER_MANIFEST,
)
QUERY = "expedite carrier selection and routing for urgent orders"
DATASET = Path(__file__).parent / "qa" / "eval_spendcap.json"


def say(text: str = "") -> None:
    sys.stdout.write(text + "\n")


def header(key: str, title: str) -> None:
    say(f"\n=== ({key}) {title} " + "=" * max(1, 56 - len(title)))


async def scenario_a_connectivity() -> None:
    header("a", f"Ollama connectivity @ {OLLAMA_HOST}")
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            tags = (await client.get(f"{OLLAMA_HOST}/api/tags")).json()
    except Exception as exc:  # noqa: BLE001 - QA-friendly diagnostics
        say(f"FAIL — cannot reach Ollama at {OLLAMA_HOST}: {exc}")
        say("start it with:  ollama serve")
        raise SystemExit(1) from exc
    pulled = {m["name"] for m in tags.get("models", [])}
    say(f"server up; {len(pulled)} model(s) pulled.")
    for role, model in (
        ("planner/judge", PLANNER_MODEL),
        ("fast", FAST_MODEL),
        ("embed", EMBED_MODEL),
    ):
        ok = any(name == model or name.startswith(f"{model}:") for name in pulled)
        say(f"  {role:14} {model:18} {'OK' if ok else 'MISSING -> ollama pull ' + model}")
        if not ok:
            raise SystemExit(1)


async def scenario_b_semantic_search() -> None:
    header("b", "registry semantic search: real embeddings vs hashing baseline")
    tenant = TenantContext(tenant_id="qa")

    async def rank(embedder: Any) -> list[str]:
        search = RegistrySearch(InMemoryVectorStore(), embedder)
        repo = InMemoryRegistryRepo()
        store = RegistryStore(repo, Signer({"k": "qa"}, "k"), search=search)
        loader = RegistryLoader(repo, Signer({"k": "qa"}, "k"))
        for manifest in MANIFESTS:
            await store.submit(manifest)
            await store.approve(manifest.id)
        return [m.id for m in await search.search(QUERY, loader, tenant=tenant, top_k=3)]

    hashing = await rank(HashingEmbeddingProvider(dimension=768))
    real = await rank(OllamaEmbeddingProvider(EMBED_MODEL, dimension=768, host=OLLAMA_HOST))
    say(f"query: {QUERY!r}")
    say(f"  hashing baseline: {hashing}")
    say(f"  {EMBED_MODEL}: {real}")
    say("PASS" if real and real[0] == "expediter" else "CHECK — expected 'expediter' first")


async def scenario_c_in_process_subagent() -> None:
    header("c", f"mode-b in-process subagent on {FAST_MODEL}")
    coordinator = DefaultCoordinator(in_process=InProcessSubagent(OllamaProvider(FAST_MODEL)))
    sc = StageContext(
        run_id="qa-llm-c",
        tenant=TenantContext(tenant_id="qa"),
        goal="launch the next-quarter purchase cycle within cleared client funds",
        run_store=InMemoryRunStore(),
    )
    delegation = DelegationContract(
        objective=(
            "In 3 sentences: reconcile a purchase plan that requests $500,000 against "
            "$250,000 of cleared client funds under the spend-cap policy. State the "
            "committed amount."
        ),
        output_format="text",
        coordination_mode="in_process",
        boundaries=["do not commit beyond cleared funds"],
    )
    sub = await coordinator.dispatch(delegation, sc)
    say(f"status: {sub.status}")
    say(f"subagent says: {sub.summary[:400]}")
    say("PASS" if sub.status == "ok" and sub.summary else "CHECK — empty subagent output")


async def scenario_d_autonomous_loop() -> None:
    header("d", f"autonomous loop: {PLANNER_MODEL} plans, {FAST_MODEL} executes")
    planner_llm = OllamaProvider(PLANNER_MODEL)
    rounds = {"n": 0}

    async def llm_planner(sc: StageContext, subresults: list[Any]) -> list[DelegationContract]:
        if rounds["n"]:  # one planning round is enough for QA
            return []
        rounds["n"] += 1
        raw = await planner_llm.generate(
            "You are a procurement orchestrator. Goal: research the regional supply market "
            "before committing budget. Propose exactly 2 short research objectives.",
            json_schema={
                "type": "object",
                "properties": {
                    "objectives": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["objectives"],
            },
        )
        objectives = [str(o) for o in json.loads(raw).get("objectives", [])][:2]
        say(f"planner proposed: {objectives}")
        return [
            DelegationContract(objective=objective, output_format="text")
            for objective in objectives
        ]

    policy = NativePolicyEngine(
        ruleset_ref="spend-cap",
        repo=_RulesetRepo(load_ruleset(RULES_DIR / "spend-cap.yaml")),
    )
    loop = OrchestratorLoop(
        planner_llm,
        DefaultCoordinator(in_process=InProcessSubagent(OllamaProvider(FAST_MODEL))),
        EffortBudget(max_subagents=2),
        policy,
        planner=llm_planner,
        max_iterations=2,
    )
    sc = StageContext(
        run_id="qa-llm-d",
        tenant=TenantContext(tenant_id="qa"),
        goal="research the regional supply market",
        run_store=InMemoryRunStore(),
    )
    out = await loop.run(sc)
    subresults = list(out.extras.get("subresults", []))
    say(f"subagents ran: {len(subresults)}")
    for sub in subresults:
        say(f"  - {str(sub.get('summary', ''))[:120]}")
    say("PASS" if len(subresults) >= 1 else "CHECK — planner produced no delegations")


async def scenario_e_llm_judge() -> None:
    header("e", f"LLM-as-judge eval gate on {PLANNER_MODEL}")
    judge = LLMJudge(OllamaProvider(PLANNER_MODEL))
    rubric = make_rubric(
        "spend-cap",
        criteria=["spend_policy_honored", "disclosed_principal_asserted"],
        pass_threshold=0.7,
    )
    dataset = load_dataset(str(DATASET))
    picks = [c for c in dataset.cases if c.id in ("clamped-overask", "overcommit-bad")]
    for case in picks:
        result = await judge.score(output=case.input, reference=case.reference, rubric=rubric)
        say(f"  {case.id:<18} overall={result.overall:.2f} passed={result.passed}")
        say(f"    scores: {result.scores}")
    say("expected: the clamped case passes; the overcommit violation fails.")
    say(
        "CLI variant:  MODEL_PROVIDER=ollama MODEL_NAME="
        f"{PLANNER_MODEL} maof eval run examples/po_demo/qa/eval_spendcap.json"
    )


async def scenario_f_compaction() -> None:
    header("f", f"context compaction digest on {FAST_MODEL}")
    from maof.context.compaction import LLMCompactor

    sc = StageContext(
        run_id="qa-llm-f",
        tenant=TenantContext(tenant_id="qa"),
        goal="run the purchase cycle",
        run_store=InMemoryRunStore(),
    )
    sc.dialogue = [
        "user: run the next-quarter replenishment cycle across the east and west regions",
        "planner: requested commitment is $500,000; cleared client funds are $250,000",
        "policy: committed spend CLAMPED to cleared client funds (the spend-cap policy)",
        "commitments: reserved $250,000 against PLAN-1; PO-1 issued",
        "fulfillment: placed PO_EAST_REPLENISH_A and PO_WEST_REPLENISH_A",
        "delivery_metrics: early unit cost tracking 8% under target on east-region",
    ] + [f"telemetry: heartbeat {i} nominal, no action required" for i in range(40)]
    digest = await LLMCompactor(OllamaProvider(FAST_MODEL)).make_digest(sc, target_tokens=160)
    say(
        f"original lines: {len(sc.dialogue)} -> digest tokens: {digest.token_count} "
        f"(dropped ~{digest.dropped_tokens})"
    )
    say(f"digest:\n{digest.digest[:500]}")
    say("CHECK that the clamp + commitments survived compaction (money facts are salient).")


SCENARIOS = {
    "a": scenario_a_connectivity,
    "b": scenario_b_semantic_search,
    "c": scenario_c_in_process_subagent,
    "d": scenario_d_autonomous_loop,
    "e": scenario_e_llm_judge,
    "f": scenario_f_compaction,
}


async def _main() -> int:
    parser = argparse.ArgumentParser(description="MAOF LLM-path QA scenarios (Ollama)")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default=None)
    args = parser.parse_args()
    say(f"models: planner/judge={PLANNER_MODEL} fast={FAST_MODEL} embed={EMBED_MODEL}")
    if args.scenario:
        await scenario_a_connectivity()  # always gate on connectivity first
        if args.scenario != "a":
            await SCENARIOS[args.scenario]()
        return 0
    for scenario in SCENARIOS.values():
        await scenario()
    say("\nAll LLM scenarios completed.")
    return 0


def main() -> int:
    return asyncio.run(_main())


if __name__ == "__main__":
    raise SystemExit(main())
