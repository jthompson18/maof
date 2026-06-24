# MAOF quickstart

The smallest possible **governed run**: define one L2 agent, dispatch one task, read
the result. Fully offline and in-process — no database, no broker, no extras.

```bash
pip install maof
python -m examples.quickstart.main
# governed run complete -> {'greeting': 'Hello, world!'}
```

## What it shows

- **You write the agent.** [`agent.py`](agent.py) is a `BaseL2Agent` with one task type
  and a `handle()` method. That is the only domain code.
- **MAOF governs the run.** [`main.py`](main.py) wires the in-memory broker, signer, and
  idempotency guard, then publishes a **signed** task that a `WorkerPool` consumes:
  signature verified, agent invoked, a **signed result envelope** returned. The
  **idempotency key** makes a replayed/redelivered task dedupe instead of double-firing.
- **Swapping in real infrastructure is config, not code.** Replace `InMemoryBroker` /
  in-memory stores with RabbitMQ + Postgres via `Settings` and the same agent runs unchanged.

## Next

[`examples/po_demo`](../po_demo) is the full reference scenario — signed YAML workflows,
policy-as-code clamps, N-of-M approvals, source-of-truth agents, and a kill→resume that
commits exactly once. See also [`docs/ARCHITECTURE.md`](../../docs/ARCHITECTURE.md).

When a run's process works well, **reuse it**: `maof runs promote <run_id>` derives a
draft signed workflow from a completed run, ready to execute under new goals.
