# Choosing a coordination mode

MAOF supports two coordination modes. Picking the right one is the single most
important design decision in a multi-agent system.

> **The one rule.** Parallelize across agents only when subtasks are genuinely
> **independent**. Whenever actions carry decisions that depend on each other, the
> context and trace must stay **unified**.

## Mode (a): governed async queue dispatch

`QueueDispatcher` publishes a signed, schema-valid task to an L2 worker queue.

Use it when subtasks are **independent**: fan-out, parallel, read-heavy,
separately governable/auditable. Examples: fulfilment + delivery metrics across regions,
parallel research over disjoint sources.

- Stateless workers scale horizontally.
- Each task is signed, schema-validated, retried, and dead-lettered uniformly.
- Side effects are idempotent (deterministic `idempotency_key`), so redelivery and
  replay never double-fire.

It is a *distributed governed job system*: it discards the orchestrator's reasoning
trace, which is fine for independent work and **fatal** for interdependent work.

## Mode (b): in-process context-shared subagents

`InProcessSubagent` runs a subagent in-process, sharing the run trace, and returns a
**distilled ~1–2k-token summary** plus references to any large artifacts.

Use it when actions carry **interdependent decisions**, where a shared context/trace
is required for correctness. Subagents fail when they act on conflicting *implicit*
assumptions they never reconciled; a unified trace is the defense.

## How to choose

The deciding axis: **do the subtasks' actions carry decisions that depend on each
other?**

- **No → mode (a)** (queue).
- **Yes → mode (b)** (in-process, shared trace).

Express the choice per delegation: `DelegationContract(coordination_mode="queue" | "in_process")`.
The `Coordinator` routes accordingly. **Do not use the queue as a substitute for context
sharing**: fanning interdependent steps out as independent signed tasks is the failure
mode the rule warns against.

## Workflow first, autonomy when warranted

The predictable, governed **workflow pipeline** is the default and is easier to audit
and approve. Reserve the **autonomous orchestrator-loop** for genuinely open-ended tasks
whose path can't be predicted. Multi-agent fan-out costs ~15× chat tokens, so the worth-it
gate (`DefaultWorthItGate`) keeps it reserved for high-value, parallelizable work.
