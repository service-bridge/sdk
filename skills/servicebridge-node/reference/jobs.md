# Jobs — scheduled work

Cron, delayed, and interval jobs driven by the runtime. The runtime fires the job on schedule, leases it to one instance, and retries on failure. Jobs have no incoming caller and no payload.

## Register a job

```ts
sb.job.handle(name: string, opts: JobOpts, fn: (ctx: JobHandlerCtx) => Promise<void>): void
```

Register before `await sb.start()`. Names must be unique per service (duplicate throws).

```ts
interface JobOpts {
  trigger: Trigger;            // required — exactly one shape below
  catchup?: CatchupPolicy;     // "skip" (default) | "fire_once" | "fire_all"
  overlap?: OverlapPolicy;     // "skip" (default) | "allow" | "buffer_one"
  deps?: DeclaredDep[];        // declared downstream dependencies
  maxAttempts?: number;
  leaseTtlMs?: number;
  maxConcurrent?: number;      // with overlap "allow"
  retry?: RetryPolicy;         // { initialMs, maxMs, multiplier, jitter }
}

type Trigger =
  | { cron: string; tz?: string }              // 5-field cron, no seconds; e.g. "0 9 * * 1"
  | { delayed: { at: Date | string | number } }
  | { interval: number };                       // milliseconds

type DeclaredDep = { rpc: string } | { event: string } | { workflow: string };  // rpc form: "service.Method"
```

## Handler context

```ts
interface JobHandlerCtx {
  jobName: string;
  executionId: string;
  scheduledAt: Date;        // UTC fire time
  localScheduledAt: Date;   // in the cron tz (UTC for interval/delayed)
  attempt: number;          // 1,2,3… diagnostic only — DO NOT dedup on this
  idempotencyKey: string;   // stable per (job, scheduled tick) — DEDUP ON THIS
  signal: AbortSignal;      // aborts on lease loss / reconnect
}
```

**Idempotency:** a tick may be delivered more than once (retry, failover). Dedup on `ctx.idempotencyKey` (e.g. a DB unique constraint or `SET NX`), never on `ctx.attempt`. Respect `ctx.signal` for long jobs.

## Policies

- `catchup` — after the runtime was down across scheduled ticks: `skip` (ignore them), `fire_once` (one catch-up run), `fire_all` (replay all missed, capped by runtime budget).
- `overlap` — when the previous run is still going: `skip` (drop this tick), `buffer_one` (queue one), `allow` (run up to `maxConcurrent` concurrently).

## Recipe — cron job that calls an RPC

```ts
import { ServiceBridge } from "service-bridge";
const sb = new ServiceBridge("localhost:14445", process.env.JOBS_KEY!);

sb.service("billing", { rpc: ["Reconcile"] }); // declare the downstream call
sb.job.handle(
  "nightly-reconcile",
  {
    trigger: { cron: "0 2 * * *", tz: "Europe/Moscow" },  // 02:00 Moscow daily
    catchup: "fire_once",
    overlap: "skip",
    maxAttempts: 3,
    deps: [{ rpc: "billing.Reconcile" }],
  },
  async (ctx) => {
    if (!(await claimOnce(ctx.idempotencyKey))) return; // idempotency guard
    await sb.rpc.call("billing", "Reconcile", { date: ctx.localScheduledAt.toISOString() });
  },
);

await sb.start();
```

## Recipe — one-shot delayed job

```ts
sb.job.handle(
  "send-reminder",
  { trigger: { delayed: { at: Date.now() + 60_000 } }, maxAttempts: 2 },
  async (ctx) => { await sendReminder(ctx.idempotencyKey); },
);
await sb.start();
```

Per-service job rate limits are enforced by the runtime at registration time; tune them in the dashboard, not the SDK.
