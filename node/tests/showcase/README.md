# Showcase fixture

Comprehensive workflow + cron job + abandoned-RPC scenarios that exercise every
visual primitive in the canonical-tracing UI waterfall. One trace, one walk —
visual acceptance gate for the Phase 5 tracing rebuild.

## Zone of responsibility

- `showcase-workflow.ts` — main script. Boots 8 ServiceBridge instances in one
  Node process, registers workflow + cron job + delayed job definitions, seeds
  policy rules, fires the workflow via HTTP `POST /run-showcase`, awaits
  completion, prints trace URLs.
- `abandoned-scenario.ts` — standalone script. Spawns a callee in a child
  process, calls a 60 s handler from the parent, SIGKILLs the callee after 3 s
  → runtime grace+sweep flips RPC.HANDLE → ABANDONED.
- `showcase-workflow.test.ts` — automated integration test (Bun test). Runs the
  same workflow path, queries `operations` table, asserts every channel/kind
  combo the visual checklist expects is present.
- `showcase.proto` — single proto file with messages + services for all RPCs
  and events the fixture uses.
- `showcase-keys.ts` — provisions 8 distinct bootstrap keys via
  `runtime/cmd/sbkey-gen`, caches them in `<repo>/.env.showcase` (gitignored).
- `policy-helpers.ts` — showcase-shaped grants (`grantRpcWildcard`,
  `grantEventFlow`, `grantWorkflowFlow`) over the shared SQL surface in
  `../e2e/_helpers/policy-db.ts`.

## How to run

```bash
# Prerequisites
docker ps | grep servicebridge2-pg                 # PG18 on :5433
go run -C runtime ./cmd/runtime -pg-url postgres://servicebridge:servicebridge@localhost:5433/service-bridge?sslmode=disable &  # runtime on :14444 + :14445

# First time only — provisions 8 services (~5 s) and caches keys.
# Re-runs reuse .env.showcase.
cd sdk/node
bun run tests/showcase/showcase-workflow.ts
# ~30–60 s. Prints trace URLs to stdout. Open the workflow URL.

# Abandoned scenario (separate trace):
bun run tests/showcase/abandoned-scenario.ts
# ~40 s (3 s of work + 30 s sweep + 5 s buffer). Prints one trace URL.
```

## Acceptance checklist

Walk the workflow trace top-down. Every checkbox below must visually match the
description. If any item fails the rebuild is not done.

In the **main workflow waterfall**:

- [ ] Root WORKFLOW.RUN (`showcase-flow`) — orange bar, holds RUNNING for the
      full duration.
- [ ] Sequence steps (`charge`, `publish_order`, `fanout` group) render in
      order with no overlap.
- [ ] Parallel `fanout` group — branches A, B, C render as three independent
      lanes under the parent, with overlapping time ranges visible.
- [ ] Branch A `reserve` — RetryGroupBar with 2 red + 1 green segment, gaps
      sized to the exponential backoff (1 s, then up to 3 s).
- [ ] Each `reserve` attempt expands into a RpcTripleBar (CALL + FORWARD +
      HANDLE) via the hopCollapser. "Show mesh details" toggle expands to 3
      rows.
- [ ] Branch A nested `shipping-flow` — separate WORKFLOW.RUN under
      `step.workflow`, indented via NestingGuide, header badge reads
      "child workflow run".
- [ ] Branch B `wait_15s` — gray striped WaitBar, label `sleeping until
      <T+15s>`. On wake → green right edge with the checkmark.
- [ ] `publish_order` → fan-out × 3 — FanoutLanes: 1 publish + 3 deliver lanes,
      dashed connectors. Collapse summary shows `3 deliveries OK`.
- [ ] `publish_audit` → fan-out × 1 — single deliver lane.
- [ ] Branch C `manager_approval` — WaitBar `waiting for signal:
      manager_approval`. After 10 s the right edge turns orange with the
      hourglass and status flips to TIMEOUT.
- [ ] Branch C `notify` — 3 RPC.CALL attempts under one subject. The 3rd is
      red-700 with DlqBadge `DLQ: max attempts (3/3) exceeded`.
- [ ] `charge` compensation — a USER.SUBOP bar with subject `compensate:charge`
      under the compensation phase divider, plus the actual `billing/Refund`
      RPC.CALL under it. CompensationArrow connects the USER.SUBOP back to the
      forward `charge` step.
- [ ] `publish_order` compensation — analogous USER.SUBOP +
      `EVENT.PUBLISH` under the compensation phase, dashed CompensationArrow
      to the forward publish.
- [ ] Vertical PhaseDivider `compensation triggered: step_failure` between
      forward phase and compensation phase.
- [ ] `delayed-cleanup` job — separate JOB.EXEC row, attached to the workflow
      run via correlated trace, TemporalGap of `~5 s gap` rendered as the
      ellipsis bar.

In the **`/traces`** table:

- [ ] Showcase trace appears as a root row with channel=WORKFLOW, kind=RUN,
      status=ERROR (DLQ + compensation), business_key = workflow run id.
- [ ] Attempts column on the DLQ'd op shows `3/3 💀`.
- [ ] `channel=WORKFLOW` filter hides HTTP/RPC/EVENT/JOB roots and leaves
      workflow.

In **`/http`, `/rpc`, `/events`, `/jobs`**:

- [ ] `/http` — root HTTP.HANDLE on `/run-showcase` with a link to the trace.
- [ ] `/rpc` — every RPC call surfaces (billing × 2, inventory × 3 attempts,
      external × 3 attempts, shipping nested × 2). RpcTripleBar inline.
- [ ] `/events` — `order_created` publish + 3 deliveries, `audit_log` publish
      + 1 delivery. DeliveryStatusBar primary visual.
- [ ] `/jobs` — `cleanup-job` (4 attempts → DLQ), `delayed-cleanup` (1 attempt
      → OK). DeliveryRetryChain expand works.
- [ ] `/dlq` — `external-api/Notify` op + `cleanup-job` op both present with
      DlqBadge + MetaTable containing `reason`.
- [ ] `/services` — health colors: `external-api-service` shows ERROR-mixed
      (3 errors in last minute), `inventory-service` shows healthy (transient
      retries succeed → final OK).
- [ ] `/workflows` — `showcase-flow` and `shipping-flow` registered. DAG view
      not broken.
- [ ] `/dashboard` — channel-breakdown mini-pies are populated.

From `abandoned-scenario.ts`:

- [ ] Separate trace under `/traces` with an RPC.HANDLE in ABANDONED status.
- [ ] Bar is striped gray-red, tooltip reads `instance disconnected at
      <time>; original status unknown`.
- [ ] Counter `sb_telemetry_late_end_after_abandoned_total` stays 0
      (`kill -9` never let the callee flush an END frame).

## Definition of Done for showcase

Owner ticks every box manually after walking through the UI. Any single
failure is a blocker — the executor returns to the UI delta and fixes it.
