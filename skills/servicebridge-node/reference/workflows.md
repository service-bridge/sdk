# Workflows — durable orchestration

A workflow is a DAG of steps persisted in the runtime: steps run in parallel by default and order only by their `waitFor` dependencies. The runtime drives execution, survives restarts, supports compensation (saga rollback), external signals, and replay.

There are two sides: the **owner** registers the definition (`workflow.handle`); a **caller** launches runs (`workflow.start`). They can be the same service or different ones.

## Define & register (owner side)

```ts
sb.workflow.handle(name: string, def: WorkflowDef, opts?: { input?: Record<string, unknown> }): void

interface WorkflowDef {
  input?: Record<string, unknown>;   // JSON-Schema-ish shape for run input
  steps: Step[];                      // the DAG
  retry?: Partial<RetryOpts>;         // default retry for steps
  maxParallelism?: number;            // cap concurrent steps (0 = unlimited)
  timeoutSec?: number;                // whole-run wall-clock timeout
}
```

Register before `await sb.start()`. The definition is canonicalized and fingerprinted; the runtime rejects re-registering the same name with a different shape while runs exist (in-flight runs keep their frozen plan).

## Step types

Every step shares these control fields:

```ts
interface StepControlFields {
  id: string;                    // unique within the workflow, ^[a-z0-9_]+$
  waitFor?: string[];            // ids that must finish first (this is what orders the DAG)
  when?: Predicate;              // run only if predicate is true (see below)
  compensate?: CompensateSpec;   // rollback action if the run later fails (call/publish steps)
  timeoutSec?: number;           // workflow-control timeout for this step
  retry?: Partial<RetryOpts>;
}
```

| `type` | Extra fields | Does |
|---|---|---|
| `"call"` | `service`, `method`, `input`, `opts?` | `rpc.call` to another service |
| `"publish"` | `event`, `input`, `opts?` | publish an event |
| `"sleep"` | `durationSec` | durable timer |
| `"wait_event"` | `event`, `filter?` | park until a matching event is ingested |
| `"wait_signal"` | `signal` | park until `sb.workflow.signal(runId, signal, …)` |
| `"workflow"` | `workflow`, `input`, `opts?` | start a child workflow and wait for it |
| `"parallel"` | `steps`, `forEach?` | group; all inner steps start at once |
| `"sequence"` | `steps`, `forEach?` | group; inner steps run one after another |
| `"local"` | `fn: (state) => Promise<unknown>` | arbitrary JS in the SDK process — use sparingly |

`forEach?: { from: JsonExpression; as: string }` on parallel/sequence fans the group out over an array.

## Expressions (JSONPath-lite)

Step `input`, `when`, `idempotencyKey`, `forEach.from`, etc. accept declarative expressions:

- `"$.input.userId"` — a path into run input or accumulated state. Each completed step's output is stored under its `id`, so `"$.reserve.token"` reads the `reserve` step's output field `token`.
- A string **not** starting with `$.` is a literal. Use `{ literal: "$.x" }` to force a literal that looks like a path.
- Objects/arrays are evaluated recursively.

`Predicate` for `when`:

```ts
type Predicate =
  | string                                  // truthy expression, e.g. "$.input.enabled"
  | { not: Predicate }
  | { equals: [JsonExpression, JsonExpression] }
  | { in: [JsonExpression, JsonExpression] }   // [value, array]
  | { and: Predicate[] }
  | { or: Predicate[] };
```

`CompensateSpec` (rollback for a `call`/`publish` step, run in reverse if a later step fails):

```ts
interface CompensateSpec {
  type?: "call" | "publish";
  service?: string; method?: string;   // for call
  event?: string;                       // for publish
  input: JsonExpression;
  retry?: Partial<RetryOpts>;
  idempotencyKey?: string;
}
```

## Launch & observe (caller side)

```ts
await sb.workflow.start(name, input, opts?): Promise<{ runId: string }>   // opts: { idempotencyKey?, timeoutSec?, parentRunId? }
await sb.workflow.await(runId): Promise<Record<string, unknown>>          // resolves on success, rejects on failed/cancelled
await sb.workflow.query(runId): Promise<{ status; state; steps }>         // point-in-time snapshot
await sb.workflow.signal(runId, signalName, payload): Promise<void>       // deliver to a wait_signal step
await sb.workflow.cancel(runId): Promise<void>                            // cooperative cancel → compensating → cancelled
await sb.workflow.replay(runId, opts?): Promise<{ runId: string }>        // fork a new run; opts: { fromStepId? }
```

Caller ops require `await sb.start()` first. `query().status` is one of `pending | running | waiting | success | failed | cancelling | cancelled | compensating | failed_compensated`.

> When the **same instance** both `handle()`s a workflow and `start()`s it, the definition registers asynchronously after `start()` — a `start()` issued microseconds later can throw `WorkflowNotFoundError`. Wait for the `connected` event (or briefly settle) first. With a separate owner service (the usual case), this doesn't apply.

## Recipe — saga with compensation

```ts
// owner
sb.workflow.handle("checkout", {
  input: { userId: "string", item: "string", quantity: "number", amount: "number" },
  steps: [
    {
      type: "call", id: "reserve",
      service: "inventory", method: "Reserve",
      input: { item: "$.input.item", quantity: "$.input.quantity" },
      compensate: { service: "inventory", method: "Release", input: { token: "$.reserve.token" } },
    },
    {
      type: "call", id: "charge",
      service: "billing", method: "Charge",
      input: { userId: "$.input.userId", amount: "$.input.amount" },
      waitFor: ["reserve"],
      // if "charge" fails, the runtime runs "reserve"'s compensate (Release) automatically
    },
    {
      type: "publish", id: "notify",
      event: "order.placed",
      input: { userId: "$.input.userId", token: "$.reserve.token" },
      waitFor: ["charge"],
    },
  ],
  retry: { maxAttempts: 3 },
});
await sb.start();
```

```ts
// caller
await sb.start();
const { runId } = await sb.workflow.start("checkout", {
  userId: "u-1", item: "sku-9", quantity: 2, amount: 100,
});
const finalState = await sb.workflow.await(runId);
```

## Errors

```ts
import { WorkflowAccessDeniedError, WorkflowNotFoundError, WorkflowTerminalError } from "service-bridge";
```

- `WorkflowNotFoundError` — `start()` on an unregistered name.
- `WorkflowAccessDeniedError` — bilateral access policy denied the start.
- `WorkflowTerminalError` — `signal()`/`cancel()` on an already-terminal run.
- `await(runId)` rejects when the run ends in `failed`/`cancelled`/`failed_compensated`.
