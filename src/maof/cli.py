"""The ``maof`` CLI entry point.

Implemented: ``maof registry {submit,list,approve,revoke}``. Other subcommands
(run-orchestrator, run-worker, run-approval, eval, runs) land in later phases.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import sys
from pathlib import Path
from typing import Any


def _ensure_cwd_on_path() -> None:
    """Console scripts (unlike ``python -m``) do not put the working directory on
    ``sys.path`` — adopter modules referenced by ``--agents``/``--module``/
    ``--harness`` are resolved relative to where ``maof`` is invoked."""
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="maof", description="Multi-Agent Orchestration Framework")
    sub = parser.add_subparsers(dest="command")

    registry = sub.add_parser("registry", help="discovery registry admin")
    rsub = registry.add_subparsers(dest="registry_command")
    submit = rsub.add_parser("submit", help="submit a manifest (pending)")
    submit.add_argument("manifest", help="path to a manifest JSON file")
    rsub.add_parser("list", help="list approved + signed entries")
    approve = rsub.add_parser("approve", help="approve + sign an entry")
    approve.add_argument("entry_id")
    revoke = rsub.add_parser("revoke", help="revoke an entry")
    revoke.add_argument("entry_id")

    ev = sub.add_parser("eval", help="evaluation harness")
    evsub = ev.add_subparsers(dest="eval_command")
    run = evsub.add_parser("run", help="score an eval dataset and gate on EVAL_MIN_PASS_RATE")
    run.add_argument("dataset", help="path to an eval dataset JSON file")
    run.add_argument(
        "--harness",
        help="module:attr of an async (EvalCase) -> str harness; default grades case.input",
    )
    run.add_argument(
        "--criteria",
        help="comma-separated rubric criteria (default: the generic built-in rubric)",
    )
    run.add_argument(
        "--pass-threshold", type=float, default=0.7, help="per-case pass threshold (default 0.7)"
    )

    sub.add_parser("migrate", help="apply database migrations")

    prune = sub.add_parser("prune", help="apply retention windows (trace/audit/idempotency)")
    prune.add_argument("--trace-days", type=int, default=None, help="override TRACE_RETENTION_DAYS")
    prune.add_argument("--audit-days", type=int, default=None, help="override AUDIT_RETENTION_DAYS")

    orch = sub.add_parser("run-orchestrator", help="run an adopter orchestrator entry point")
    orch.add_argument("--module", required=True, help="module exposing the entry callable")
    orch.add_argument("--attr", default="main", help="callable attribute (default: main)")

    worker = sub.add_parser("run-worker", help="run an L2 worker pool")
    worker.add_argument("--queue", required=True, help="queue to consume")
    worker.add_argument("--consumers", help="path to a consumers.yaml (for topology)")
    worker.add_argument("--agents", help="module to import that registers L2 agents")

    sub.add_parser("run-approval", help="run the HITL approval service (api extra)")

    runs = sub.add_parser("runs", help="run operations")
    runs_sub = runs.add_subparsers(dest="runs_command")
    runs_list = runs_sub.add_parser("list", help="list runs")
    runs_list.add_argument("--tenant", default=None)
    runs_list.add_argument("--status", default=None)
    runs_show = runs_sub.add_parser("show", help="show a run")
    runs_show.add_argument("run_id")
    runs_show.add_argument("--tenant", default=None, help="tenant (required in multi-tenant mode)")
    runs_trace = runs_sub.add_parser("trace", help="show a run's trace")
    runs_trace.add_argument("run_id")
    runs_trace.add_argument("--tenant", default=None, help="tenant (required in multi-tenant mode)")
    runs_cancel = runs_sub.add_parser("cancel", help="cooperatively cancel a run")
    runs_cancel.add_argument("run_id")
    runs_wake = runs_sub.add_parser("wake", help="fire an external wake event")
    runs_wake.add_argument("event_key")
    runs_promote = runs_sub.add_parser(
        "promote", help="promote a completed run to a draft workflow definition"
    )
    runs_promote.add_argument("run_id")
    runs_promote.add_argument("--name", default=None, help="workflow name (default: run-<id8>)")
    runs_promote.add_argument(
        "-o", "--out", default=None, help="write the draft YAML here (default: stdout)"
    )

    wf = sub.add_parser("workflow", help="signed workflow definitions")
    wf_sub = wf.add_subparsers(dest="workflow_command")
    wf_submit = wf_sub.add_parser("submit", help="submit a workflow YAML (pending)")
    wf_submit.add_argument("file", help="path to a workflow YAML file")
    wf_approve = wf_sub.add_parser("approve", help="approve + sign a workflow version")
    wf_approve.add_argument("name")
    wf_approve.add_argument("version", type=int)
    wf_revoke = wf_sub.add_parser("revoke", help="revoke a workflow version")
    wf_revoke.add_argument("name")
    wf_revoke.add_argument("version", type=int)
    wf_sub.add_parser("list", help="list workflow definitions")
    wf_run = wf_sub.add_parser("run", help="execute an approved workflow via a wiring module")
    wf_run.add_argument("name")
    wf_run.add_argument(
        "--module",
        required=True,
        help="module:attr of an async (WorkflowDefinition) -> result runner (adopter wiring)",
    )
    return parser


async def _run_workflow_cmd(args: argparse.Namespace, definition_data: Any | None) -> int:
    from maof.config import Settings
    from maof.identity import resolve_operator_principal
    from maof.observability.sinks.postgres_sink import PostgresEventSink
    from maof.persistence.postgres import Database
    from maof.transport.signing import build_signer
    from maof.workflows.definition import WorkflowDefinition
    from maof.workflows.store import PostgresWorkflowRepo, WorkflowStore

    settings = Settings()
    db = Database(settings.db_url, min_size=settings.db_pool_min, max_size=settings.db_pool_max)
    await db.connect()
    try:
        signer = build_signer(settings)
        # event_sink records the approver (workflow_approved) on approve.
        store = WorkflowStore(PostgresWorkflowRepo(db), signer, event_sink=PostgresEventSink(db))
        principal = resolve_operator_principal(settings)  # bring-your-own identity
        if args.workflow_command == "submit":
            definition = WorkflowDefinition.model_validate(definition_data)
            await store.submit(definition, principal=principal)
            sys.stdout.write(f"submitted {definition.name} v{definition.version} (pending)\n")
        elif args.workflow_command == "approve":
            entry = await store.approve(args.name, args.version, principal=principal)
            sys.stdout.write(f"approved + signed {entry.definition.name} v{args.version}\n")
        elif args.workflow_command == "revoke":
            await store.revoke(args.name, args.version, principal=principal)
            sys.stdout.write(f"revoked {args.name} v{args.version}\n")
        elif args.workflow_command == "list":
            for entry in await store.repo.list_entries():  # type: ignore[attr-defined]
                sys.stdout.write(
                    f"{entry.definition.name}\tv{entry.definition.version}\t{entry.status}\n"
                )
        elif args.workflow_command == "run":
            import importlib
            import inspect

            _ensure_cwd_on_path()

            definition = await store.load(args.name)  # approved + signature-valid only
            module_name, _, attr = args.module.partition(":")
            runner = getattr(importlib.import_module(module_name), attr or "run_workflow")
            outcome = runner(definition)
            if inspect.iscoroutine(outcome):
                outcome = await outcome
            sys.stdout.write(f"workflow {args.name} -> {outcome}\n")
        else:
            sys.stdout.write("usage: maof workflow {submit,approve,revoke,list,run}\n")
            return 2
        return 0
    finally:
        await db.close()


async def _run_runs(args: argparse.Namespace) -> int:
    from maof.config import Settings
    from maof.identity import resolve_operator_principal
    from maof.orchestrator.lifecycle import PostgresRunWaker
    from maof.persistence.postgres import Database
    from maof.runs.ops import RunOps
    from maof.tenancy import resolve_tenant

    settings = Settings()
    db = Database(settings.db_url, min_size=settings.db_pool_min, max_size=settings.db_pool_max)
    await db.connect()
    try:
        ops = RunOps(db, waker=PostgresRunWaker(db))
        principal = resolve_operator_principal(settings)  # bring-your-own identity
        if args.runs_command == "list":
            for row in await ops.list_runs(tenant_id=args.tenant, status=args.status):
                sys.stdout.write(
                    f"{row['run_id']}\t{row['tenant_id']}\t{row['status']}"
                    f"\t{row['goal'][:60]}\n"
                )
        elif args.runs_command == "show":
            tenant = resolve_tenant(settings, args.tenant)
            shown = await ops.show(args.run_id, principal=principal, tenant=tenant)
            sys.stdout.write(f"{shown}\n" if shown else "run not found\n")
            return 0 if shown else 1
        elif args.runs_command == "trace":
            tenant = resolve_tenant(settings, args.tenant)
            for entry in await ops.trace(args.run_id, principal=principal, tenant=tenant):
                sys.stdout.write(f"{entry['seq']}\t{entry['kind']}\t{entry['step']}\n")
        elif args.runs_command == "cancel":
            await ops.cancel(args.run_id)
            sys.stdout.write(f"cancel requested for {args.run_id}\n")
        elif args.runs_command == "wake":
            woken = await ops.wake(args.event_key)
            sys.stdout.write(f"woken: {woken}\n")
        elif args.runs_command == "promote":
            from maof.orchestrator.lifecycle import PostgresResultStore
            from maof.runs.store import PostgresRunStore
            from maof.workflows.promote import PromotionError, promote_run, to_yaml

            name = args.name or f"run-{args.run_id[:8]}"
            try:
                definition = await promote_run(
                    args.run_id,
                    run_store=PostgresRunStore(db),
                    result_store=PostgresResultStore(db),
                    name=name,
                    principal=principal,
                )
            except PromotionError as exc:
                sys.stderr.write(f"cannot promote run {args.run_id}: {exc}\n")
                return 1
            draft = to_yaml(definition)
            if args.out:
                # One-shot CLI write just before exit — not a server event-loop hot path.
                Path(args.out).write_text(draft, encoding="utf-8")  # noqa: ASYNC240
                sys.stdout.write(
                    f"promoted {args.run_id} -> {definition.name} v{definition.version} "
                    f"({len(definition.steps)} steps) -> {args.out}\n"
                    "review the draft (deps + input templates), then: "
                    f"maof workflow submit {args.out} && "
                    f"maof workflow approve {definition.name} {definition.version}\n"
                )
            else:
                sys.stdout.write(draft)
        else:
            sys.stdout.write("usage: maof runs {list,show,trace,cancel,wake,promote}\n")
            return 2
        return 0
    finally:
        await db.close()


async def _run_eval(args: argparse.Namespace) -> int:
    """Score the dataset with the configured judge LLM and gate on the pass rate."""
    import importlib

    from maof.config import Settings
    from maof.eval.judge import LLMJudge
    from maof.eval.runner import CallableHarness, DefaultEvalRunner, load_dataset
    from maof.models.base import build_llm_provider
    from maof.types import EvalCase

    settings = Settings()
    dataset = load_dataset(args.dataset)

    if args.harness:
        _ensure_cwd_on_path()
        module_name, _, attr = args.harness.partition(":")
        harness_fn = getattr(importlib.import_module(module_name), attr or "harness")
        harness = CallableHarness(harness_fn)
    else:

        async def _identity(case: EvalCase) -> str:
            return case.input  # end-state grading of recorded outputs

        harness = CallableHarness(_identity)

    rubric = None
    if args.criteria:
        from maof.eval.rubrics import make_rubric

        rubric = make_rubric(
            "cli",
            criteria=[c.strip() for c in args.criteria.split(",") if c.strip()],
            pass_threshold=args.pass_threshold,
        )
    runner = DefaultEvalRunner(LLMJudge(build_llm_provider(settings)), rubric=rubric)
    report = await runner.run_dataset(dataset, harness)
    passed = runner.gate(report, min_pass_rate=settings.eval_min_pass_rate)
    sys.stdout.write(
        f"dataset {report.dataset!r}: {report.passed}/{report.total} passed "
        f"(pass_rate={report.pass_rate:.2f}, min={settings.eval_min_pass_rate:.2f}) "
        f"gate={'PASS' if passed else 'FAIL'}\n"
    )
    return 0 if passed else 1


def _run_orchestrator(args: argparse.Namespace) -> int:
    """Import and invoke the adopter's orchestrator entry point."""
    import importlib
    import inspect

    _ensure_cwd_on_path()

    module = importlib.import_module(args.module)
    entry = getattr(module, args.attr)
    result = entry()
    if inspect.iscoroutine(result):
        asyncio.run(result)
    return 0


async def _migrate() -> int:
    from maof.config import Settings
    from maof.persistence.postgres import Database, run_migrations

    settings = Settings()
    db = Database(settings.db_url, min_size=settings.db_pool_min, max_size=settings.db_pool_max)
    await db.connect()
    try:
        await run_migrations(db, embed_dimension=settings.embed_dimension)
        sys.stdout.write("migrations applied\n")
        return 0
    finally:
        await db.close()


async def _prune(args: argparse.Namespace) -> int:
    from maof.config import Settings
    from maof.persistence.postgres import Database
    from maof.runs.retention import prune

    settings = Settings()
    db = Database(settings.db_url, min_size=settings.db_pool_min, max_size=settings.db_pool_max)
    await db.connect()
    try:
        summary = await prune(
            db,
            trace_retention_days=args.trace_days or settings.trace_retention_days,
            audit_retention_days=args.audit_days or settings.audit_retention_days,
            idempotency_ttl_s=settings.idempotency_key_ttl_s,
        )
        for table, deleted in summary.items():
            sys.stdout.write(f"{table}\t{deleted} deleted\n")
        return 0
    finally:
        await db.close()


async def _run_worker(args: argparse.Namespace) -> int:
    import importlib

    from maof.agents.registry_runtime import default_registry
    from maof.config import Settings
    from maof.transport.consumers import load_consumers
    from maof.transport.factory import build_broker
    from maof.transport.signing import build_signer, ensure_signing_configured
    from maof.workers.pool import WorkerPool

    settings = Settings()
    default_registry.load_entry_points()
    if args.agents:
        _ensure_cwd_on_path()
        importlib.import_module(args.agents)  # side effect: registers agents

    broker = build_broker(settings)
    connect = getattr(broker, "connect", None)
    if connect is not None:
        await connect()
    if args.consumers:
        await broker.ensure_topology(load_consumers(args.consumers).queue_specs())
    ensure_signing_configured(settings)  # refuse to start unsigned when signing is required
    for warning in settings.security_warnings():
        sys.stderr.write(f"warning: {warning}\n")
    signer = build_signer(settings)
    pool = WorkerPool(
        broker, signer, default_registry, require_signature=settings.require_signature
    )
    sys.stdout.write(f"worker consuming {args.queue}\n")

    # Graceful shutdown: on SIGINT/SIGTERM stop intake and disconnect cleanly
    # instead of a hard kill. Redelivery + idempotency keep an interrupted
    # in-flight handler safe.
    loop = asyncio.get_running_loop()
    consume_task = asyncio.ensure_future(pool.consume(args.queue))

    def _request_stop() -> None:
        if not consume_task.done():
            sys.stderr.write("shutdown signal received; stopping intake\n")
            consume_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_stop)
    try:
        await consume_task
    except asyncio.CancelledError:
        pass
    finally:
        close = getattr(broker, "close", None)
        if close is not None:
            with contextlib.suppress(Exception):
                await close()
    return 0


def _run_approval() -> int:
    """Run the HITL approval service. When the postgres extra is installed the
    gate is repo-backed, so resolutions land in the ``approvals`` table and
    unblock orchestrators in OTHER processes (cross-process HITL,)."""
    import uvicorn

    from maof.approval.service import ApprovalGate, create_approval_app
    from maof.config import Settings

    settings = Settings()
    db = None
    try:
        from maof.persistence.postgres import Database, PostgresApprovalRepo
    except ImportError:
        gate = ApprovalGate()  # in-process only (postgres extra not installed)
    else:
        db = Database(settings.db_url, min_size=settings.db_pool_min, max_size=settings.db_pool_max)
        gate = ApprovalGate(repo=PostgresApprovalRepo(db))
    app = create_approval_app(
        gate,
        signing_secret=settings.msg_signing_secret,
        # the asyncpg pool must bind to uvicorn's event loop, not this one
        on_startup=db.connect if db is not None else None,
        on_shutdown=db.close if db is not None else None,
    )
    uvicorn.run(app, host=settings.approval_host, port=settings.approval_port)
    return 0


async def _run_registry(args: argparse.Namespace, manifest_data: dict[str, Any] | None) -> int:
    from maof.config import Settings
    from maof.identity import resolve_operator_principal
    from maof.persistence.postgres import Database, PostgresRegistryRepo
    from maof.registry.loader import RegistryLoader
    from maof.registry.models import AgentManifest
    from maof.registry.store import RegistryStore
    from maof.transport.signing import build_signer

    settings = Settings()
    db = Database(settings.db_url, min_size=settings.db_pool_min, max_size=settings.db_pool_max)
    await db.connect()
    try:
        repo = PostgresRegistryRepo(db)
        signer = build_signer(settings)
        store = RegistryStore(repo, signer)
        loader = RegistryLoader(repo, signer)
        principal = resolve_operator_principal(settings)  # bring-your-own identity

        if args.registry_command == "submit":
            entry = await store.submit(
                AgentManifest.model_validate(manifest_data), principal=principal
            )
            sys.stdout.write(f"submitted {entry.manifest.id} (status={entry.status})\n")
        elif args.registry_command == "approve":
            entry = await store.approve(args.entry_id, principal=principal)
            sys.stdout.write(f"approved {entry.manifest.id}\n")
        elif args.registry_command == "revoke":
            entry = await store.revoke(args.entry_id, principal=principal)
            sys.stdout.write(f"revoked {entry.manifest.id}\n")
        elif args.registry_command == "list":
            for manifest in await loader.manifests():
                sys.stdout.write(
                    f"{manifest.id}\t{manifest.kind}\t{manifest.accepted_task_types}\n"
                )
        else:
            sys.stdout.write("usage: maof registry {submit,list,approve,revoke}\n")
            return 2
        return 0
    finally:
        await db.close()


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv if argv is not None else sys.argv[1:])
    if args.command == "registry":
        # File I/O happens in this sync context, not inside the async runner.
        manifest_data = (
            json.loads(Path(args.manifest).read_text(encoding="utf-8"))
            if getattr(args, "registry_command", None) == "submit"
            else None
        )
        return asyncio.run(_run_registry(args, manifest_data))
    if args.command == "eval":
        if getattr(args, "eval_command", None) == "run":
            return asyncio.run(_run_eval(args))
        sys.stdout.write("usage: maof eval run <dataset.json> [--harness module:attr]\n")
        return 2
    if args.command == "migrate":
        return asyncio.run(_migrate())
    if args.command == "prune":
        return asyncio.run(_prune(args))
    if args.command == "run-orchestrator":
        return _run_orchestrator(args)
    if args.command == "run-worker":
        return asyncio.run(_run_worker(args))
    if args.command == "run-approval":
        return _run_approval()
    if args.command == "runs":
        return asyncio.run(_run_runs(args))
    if args.command == "workflow":
        definition_data = None
        if getattr(args, "workflow_command", None) == "submit":
            import yaml

            definition_data = yaml.safe_load(Path(args.file).read_text(encoding="utf-8"))
        return asyncio.run(_run_workflow_cmd(args, definition_data))
    sys.stdout.write("maof — Multi-Agent Orchestration Framework CLI\n")
    sys.stdout.write(
        "commands: registry, workflow, eval, migrate, prune, run-orchestrator, run-worker, "
        "run-approval, runs\n"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
