# Workflows — durable orchestration

A workflow is a DAG of steps the **runtime** interprets: it stores every step's state, decides which steps are ready, holds timers, event/signal waits and child runs, applies retries with backoff and runs compensations. The SDK declares the definition and executes the **tasks** the runtime leases to it: `local` functions, `call`s, `publish`es and compensations.

There are two sides: the **owner** declares the workflow (`workflow.handle`) and executes its tasks; a **caller** starts and steers runs. A workflow is addressed by `(service, name)`.

## Declare (owner side)

```ts
sb.workflow.handle(name: string, def: WorkflowDef): void

interface WorkflowDef {
  steps: Step[];
  version?: string;                  // version of the local-step code; bump it when a fn changes
  input?: Record<string, unknown>;   // JSON Schema of the run input
  retry?: RetryPolicy;               // default retry of task steps
  maxParallelism?: number;           // concurrent task steps per run (0 = unlimited)
  timeoutMs?: number;                // whole-run timeout → status timed_out
}
```

Declare before `await sb.start()`. The definition travels in the registration; the runtime validates it (ids, `waitFor` references, no cycles, depth ≤ 10, ≤ 500 steps, path syntax) and refuses an invalid one. A name another live service already declares is refused. Runs keep the frozen plan they started with.

## Step types

Every step shares:

```ts
interface StepControl {
  id: string;            // ^[a-z0-9_]+$, unique in the graph, not "input"
  waitFor?: string[];    // sibling ids that must succeed first
  when?: Predicate;      // false → step skipped, output null
  timeoutMs?: number;    // step deadline → step fails with TIMEOUT, run stops
  retry?: RetryPolicy;   // task steps: { maxAttempts, baseDelayMs, factor, maxDelayMs, jitter }
}
```

| `type` | Extra fields | Does |
|---|---|---|
| `"call"` | `service`, `method`, `input?`, `opts?`, `compensate?` | `rpc.call` on the owner instance; output = reply |
| `"publish"` | `event`, `input?`, `opts?`, `compensate?` | publish; output = `{ eventId }` |
| `"local"` | `fn: (state, ctx) => unknown` | your function; `ctx = { signal, runId, stepId, attempt }` |
| `"sleep"` | `durationMs` | durable timer held by the runtime |
| `"wait_event"` | `event`, `filter?` | park until a matching event; output = payload |
| `"wait_signal"` | `signal` | park until `sb.workflow.signal(runId, signal, …)`; output = payload |
| `"workflow"` | `workflow`, `service?`, `input?`, `idempotencyKey?`, `childTimeoutMs?` | the runtime starts a child run and waits; output = child output |
| `"parallel"` / `"sequence"` | `steps`, `forEach?: { from, as }` | group; output = map of child ids → outputs |

`call` opts: `timeoutMs`, `transport`, `idempotencyKey`, `requestId`, `retry` (RPC-level). `publish` opts: `idempotencyKey`, `partitionKey`, `headers`. `local` must stop on `ctx.signal` — it fires when the lease is lost, the step deadline passes or the client stops.

## Expressions

Resolved by the runtime when a step activates:

- `"$.input.userId"`, `"$.reserve.token"`, `"$.items[0]"`, `"$.items[*].id"` — paths into run state (`input` + every step output by id).
- `{ literal: "$.x" }` — a value as-is; objects and arrays resolve member by member; anything else is a literal.
- Inside a `forEach` iteration the `as` name and the plain ids of iteration siblings are in scope.

```ts
type Predicate = string | { not: Predicate } | { equals: [JsonExpression, JsonExpression] }
  | { in: [JsonExpression, JsonExpression] } | { and: Predicate[] } | { or: Predicate[] };
```

`wait_event.filter`: `{ "$.orderId": "$.input.orderId" }` — payload path → expected value, all must match.

## Compensation

```ts
compensate: { method: "Release", input: { token: "$.reserve.token" }, retry: { maxAttempts: 3 } }
// type?: "call" | "publish" (default: same as the step); empty service/method/event reuse the step's
```

When a step fails for good, the run is cancelled or it times out, the runtime stops in-flight tasks and waits, cancels unfinished child runs and runs compensations of successful steps in reverse completion order. Outcome: `failed` / `cancelled` / `timed_out` if all compensations succeeded, `failed_compensated` if one exhausted its retries — then `sb.workflow.retryCompensation(runId)`.

## Launch & observe (caller side)

```ts
await sb.workflow.start(service, name, input, opts?)           // opts: { idempotencyKey?, timeoutMs? } → { runId }
await sb.workflow.await(runId)                                 // output on success; WorkflowRunFailedError otherwise
await sb.workflow.query(runId)                                 // { status, stopReason, waitingReason, output, steps, signals, ... }
await sb.workflow.signal(runId, name, payload, { signalId? })  // FIFO queue → { duplicate }
await sb.workflow.cancel(runId)
await sb.workflow.replay(runId, { fromStepId? })               // new run; copies steps independent of fromStepId
await sb.workflow.retryCompensation(runId)
```

Run statuses: `active | compensating | success | failed | cancelled | timed_out | failed_compensated`. Step statuses: `pending | leased | parked | success | failed | compensated`. `waitingReason`: `no_instance | retry | sleep | signal | event | child`.

Signal / query / await / cancel / replay / retryCompensation are allowed for the owner service, the service that started the run and callers with an explicit `workflow.run` egress rule; `start` passes the bilateral `workflow.run` / `workflow.handle` policy.

## Recipe — saga with compensation

```ts
// owner ("orders")
sb.service("inventory", { rpc: ["Reserve", "Release"] });
sb.service("billing", { rpc: ["Charge"] });
sb.workflow.handle("checkout", {
  version: "1",
  input: { type: "object", required: ["userId", "item"] },
  steps: [
    { type: "call", id: "reserve", service: "inventory", method: "Reserve",
      input: { item: "$.input.item" },
      compensate: { method: "Release", input: { token: "$.reserve.token" } } },
    { type: "call", id: "charge", service: "billing", method: "Charge",
      input: { userId: "$.input.userId" }, waitFor: ["reserve"], retry: { maxAttempts: 3 } },
    { type: "publish", id: "notify", event: "order.placed",
      input: { userId: "$.input.userId" }, waitFor: ["charge"] },
  ],
});
await sb.start();

// caller
const { runId } = await sb.workflow.start("orders", "checkout", { userId: "u-1", item: "sku-9" });
const output = await sb.workflow.await(runId);
```

## Errors

```ts
import { WorkflowAccessDeniedError, WorkflowNotFoundError, WorkflowTerminalError, WorkflowRunFailedError } from "service-bridge";
```

- `WorkflowNotFoundError` — no such workflow for the service, or no such run.
- `WorkflowAccessDeniedError` — policy denied, or the caller may not touch this run.
- `WorkflowTerminalError` — signal/cancel on a finished run, retryCompensation on a run that is not `failed_compensated`.
- `WorkflowRunFailedError` — `await` on a run that ended `failed`, `cancelled`, `timed_out` or `failed_compensated`.
