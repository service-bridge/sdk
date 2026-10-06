# Workflows — Go SDK reference

Durable DAGs. Declare the graph once; the runtime interprets it — readiness, timers, waits, child runs, retries with backoff and compensation — and leases task steps (`Local`, `Call`, `Publish`, compensations) to an instance of the owner service.

## Signatures

```go signature
func (d *WorkflowDomain) Handle(name string, def wf.Definition) error
func (d *WorkflowDomain) Start(ctx context.Context, service, name string, input any, opts ...StartOption) (string, error)
func (d *WorkflowDomain) Signal(ctx context.Context, runID, signal string, payload any, opts ...SignalOption) (duplicate bool, err error)
func (d *WorkflowDomain) Cancel(ctx context.Context, runID string) error
func (d *WorkflowDomain) Await(ctx context.Context, runID string) (map[string]any, error)
func (d *WorkflowDomain) Query(ctx context.Context, runID string) (RunSnapshot, error)
func (d *WorkflowDomain) Replay(ctx context.Context, runID, fromStepID string) (string, error)
func (d *WorkflowDomain) RetryCompensation(ctx context.Context, runID string) error

func WithRunIdempotencyKey(key string) StartOption
func WithRunTimeout(d time.Duration) StartOption
func WithSignalID(id string) SignalOption
```

Import: `wf "github.com/service-bridge/sdk/go/workflow"`.

## The mental model

- **Run state is JSON.** The run input lives under `input`; each step's output lives under that step's `ID`. Steps are written with plain Go values — maps, slices, strings, numbers.
- **A `call` step reaches an ordinary typed handler.** Its JSON tree is read into the callee's protobuf request and its reply comes back as JSON, through the pair of types declared with `NewMethod`. See "Call steps need a declared dependency".
- **Top-level steps start in parallel.** `WaitFor` declares the dependencies that create the execution levels.
- **The step set is closed** — the `wf.Step` marker method is unexported, so a graph can never carry a kind the runtime does not know.
- **The runtime holds the run.** Sleeps, waits and child runs are runtime-held; an instance crash loses nothing. A task is leased per attempt and renewed by heartbeat; a lost instance's task goes to another one when the lease expires.
- **`wf.Local` is found by `ID` and `Definition.Version`.** The closure never reaches the runtime — bump `Version` when it changes.

## Step kinds

| Kind | Own fields |
|---|---|
| `wf.Call` | `Service`, `Method` (`Target`), `Input`, `Opts *CallOpts` |
| `wf.Publish` | `Event` (`Target`), `Input`, `Opts *PublishOpts` |
| `wf.Sleep` | `Duration time.Duration` (runtime-held durable timer) |
| `wf.WaitEvent` | `Event string`, `Filter map[string]any` (payload path → value or `Path`) |
| `wf.WaitSignal` | `Signal string` |
| `wf.SubWorkflow` | `Service` (`Target`, nil = own), `Workflow` (`Target`), `Input`, `IdempotencyKey`, `Timeout` |
| `wf.Parallel` | `Steps []Step`, `ForEach *ForEach` |
| `wf.Sequence` | `Steps []Step`, `ForEach *ForEach` |
| `wf.Local` | `Fn LocalFunc` |

Every kind embeds `wf.Control{ID, WaitFor, When, Compensate, Timeout, Retry}`.

`Control.Timeout` is the **step** deadline (expiry fails the step and stops the run). `CallOpts.Timeout` bounds the underlying RPC. `Control.Retry` (else `Definition.Retry`) is applied by the runtime between task attempts.

## Call steps need a declared dependency

The callee is an ordinary `Handle[Req, Resp]` handler. A `call` step reaches it only if the same service and method were declared with their types:

```go
inventory := sb.NewClient(c, "inventory-svc")
_, err := sb.NewMethod[*pb.ReserveRequest, *pb.ReserveReply](inventory, "Reserve")
```

Version routing matches the contract hash of the `(Req, Resp)` pair exactly, and the step itself carries only a method name and a JSON tree. The pair supplies both the hash and the encoding.

| What | Where it comes from |
|---|---|
| Request bytes | the step's `Input` tree read into `Req` |
| Contract hash | the `(Req, Resp)` pair from `NewMethod` |
| Step output in run state | `Resp` rendered as its JSON mirror |

The JSON mirror is `protojson` output, not the Go struct: 64-bit integers are strings (`"9007199254740993"`), enums are value names (`"STATUS_ACTIVE"`), `bytes` is base64. `Input` is written in that form and outputs land in run state in that form, so a value leaving one step enters the next unchanged. A field the message has no room for fails the step rather than being dropped.

An undeclared target with literal `wf.Name` service and method is refused at `Start` with `CodeConfig`, naming the workflow, the step and the fix. A target computed with `wf.Path` has no name until the step runs, so it fails the same way inside the run.

`c.Service(name, sb.ServiceDeps{...})` declares only the mesh edge, without types, and is not enough for a `call` step.

## Paths versus literals

Two string types keep expressions and data apart, so a literal that looks like a path needs no escaping:

- `wf.Path("$.charge.transactionId")` — read from run state when the step executes.
- `wf.Name("payment-svc")` — a literal written at declaration.

Grammar: `$` followed by any number of `.field`, `[N]` and `[*]`. `[*].field` collects that field from every element into an array. The runtime resolves paths when the step activates. A path that leads nowhere is "no value" — a step skipped by its condition has output `nil`. Inside a `ForEach` iteration the `As` name and iteration siblings' plain ids are in scope.

`Path` resolves at any depth inside a value tree, so `Input` can be a `map[string]any` mixing literals and paths.

## Predicates

`wf.Truthy(Path)`, `wf.Not(Predicate)`, `wf.Equals(any, any)`, `wf.In(any, any)`, `wf.And(...Predicate)`, `wf.Or(...Predicate)`. The set is closed — these constructors are the only way to build one.

## Complete program

```go
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	sb "github.com/service-bridge/sdk/go"
	wf "github.com/service-bridge/sdk/go/workflow"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"),
		sb.WithAdvertise(os.Getenv("POD_IP"), 50051))
	if err != nil {
		log.Fatal(err)
	}

	if err := c.Workflow.Handle("checkout", wf.Definition{
		Input: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"orderId": map[string]any{"type": "string"},
				"email":   map[string]any{"type": "string"},
			},
			"required": []any{"orderId"},
		},
		Version: "1",
		Timeout: 15 * time.Minute,
		Steps: []wf.Step{
			// Compensated call: the reverse action reads THIS step's output.
			wf.Call{
				Control: wf.Control{
					ID: "reserve",
					Compensate: &wf.Compensation{
						Kind:     wf.CompensateCall,
						Service:  wf.Name("inventory-svc"),
						Method:   wf.Name("Release"),
						Input:    wf.Path("$.reserve"),
						Retry:    &wf.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second},
						CallOpts: &wf.CallOpts{IdempotencyKey: wf.Path("$.input.orderId")},
					},
				},
				Service: wf.Name("inventory-svc"),
				Method:  wf.Name("Reserve"),
				Input:   wf.Path("$.input"),
			},
			wf.Call{
				Control: wf.Control{ID: "charge", WaitFor: []string{"reserve"}, Timeout: 30 * time.Second},
				Service: wf.Name("payment-svc"),
				Method:  wf.Name("Charge"),
				Input:   wf.Path("$.input"),
			},
			// A Go closure that runs in THIS process, found by ID and Version.
			wf.Local{
				Control: wf.Control{ID: "score", WaitFor: []string{"charge"}},
				Fn: func(ctx context.Context, state map[string]any) (any, error) {
					input, _ := state["input"].(map[string]any)
					orderID, _ := input["orderId"].(string)
					return map[string]any{"risk": len(orderID) % 7}, nil
				},
			},
			// Conditional publish.
			wf.Publish{
				Control: wf.Control{
					ID:      "announce",
					WaitFor: []string{"charge"},
					When:    wf.Truthy(wf.Path("$.charge.ok")),
				},
				Event: wf.Name("order.placed"),
				Input: wf.Path("$.input"),
			},
			// Fan out over a list only known at run time.
			wf.Parallel{
				Control: wf.Control{ID: "notify_all", WaitFor: []string{"announce"}},
				ForEach: &wf.ForEach{From: wf.Path("$.input.recipients"), As: "recipient"},
				Steps: []wf.Step{
					wf.Call{
						Control: wf.Control{ID: "send"},
						Service: wf.Name("mail-svc"),
						Method:  wf.Name("Send"),
						// A value tree mixing literals and paths.
						Input: map[string]any{
							"to":       wf.Path("$.recipient"),
							"template": "order_placed",
							"vars":     map[string]any{"order": wf.Path("$.input.orderId")},
						},
					},
				},
			},
			// Park on a durable timer, then wait for a human.
			wf.Sleep{
				Control:  wf.Control{ID: "cooldown", WaitFor: []string{"notify_all"}},
				Duration: 5 * time.Minute,
			},
			wf.WaitSignal{
				Control: wf.Control{ID: "await_approval", WaitFor: []string{"cooldown"}},
				Signal:  "approval",
			},
		},
	}); err != nil {
		log.Fatal(err) // only what cannot be encoded; the runtime validates at Start
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	runID, err := c.Workflow.Start(ctx, "orders-svc", "checkout",
		map[string]any{"orderId": "o-1", "recipients": []any{"a@example.com"}},
		sb.WithRunIdempotencyKey("checkout-o-1"), // a repeat returns the same run
		sb.WithRunTimeout(10*time.Minute),
	)
	if err != nil {
		log.Fatal(err)
	}

	snap, err := c.Workflow.Query(ctx, runID)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("status:", snap.Status, "waiting:", snap.WaitingReason)

	if _, err := c.Workflow.Signal(ctx, runID, "approval", map[string]any{"ok": true}, sb.WithSignalID("approve-o-1")); err != nil {
		log.Fatal(err)
	}

	// Await returns the output ONLY for a successful run; any other end is an
	// error wrapping *sb.RunFailedError.
	state, err := c.Workflow.Await(ctx, runID)
	var failed *sb.RunFailedError
	switch {
	case errors.As(err, &failed):
		log.Println("run ended", failed.Status, failed.ErrorCode)
	case err != nil:
		log.Fatal(err)
	default:
		log.Println("output:", state)
	}
}
```

## Driving runs

| Call | Behaviour |
|---|---|
| `Start` | Returns the run id of `(service, name)`. `WithRunIdempotencyKey` makes a repeat return the existing run; `WithRunTimeout` ends it `timed_out`. |
| `Query` | `RunSnapshot`: `Status`, `StopReason`, `WaitingReason` (`no_instance`, `retry`, `sleep`, `signal`, `event`, `child`), `Output`, steps (`Status`, `Attempt`, `ErrorCode`, `ErrorMessage`, `WaitKey`, `ChildRunID`, `CompensatesStepID`) and queued signals. |
| `Signal` | FIFO queue per run; `WithSignalID` dedups a resend (`duplicate == true`). At most 1000 unconsumed signals. |
| `Cancel` | Compensates what was already done, in reverse; ends `cancelled`. |
| `Await` | Blocks until terminal (only `ctx` bounds it). Output on success, `*sb.RunFailedError` (code `CodeTerminal`) otherwise. |
| `Replay` | A **new** run from the frozen plan and input; with `fromStepID` (top-level step) the successful steps that do not depend on it are copied. |
| `RetryCompensation` | Re-runs the failed compensations of a `failed_compensated` run. |

Run statuses: `active`, `compensating`, `success`, `failed`, `cancelled`, `timed_out`, `failed_compensated`. Signal/Query/Await/Cancel/Replay/RetryCompensation are allowed for the owner, the starter and callers with an explicit `workflow.run` rule.

Codes: `CodeNotFound`, `CodeAccessDenied`, `CodeTerminal` (signal/cancel on a finished run, retry of a run that is not `failed_compensated`, non-success `Await`).

## Validation

`c.Workflow.Handle` only encodes the graph; the runtime validates it at registration and `c.Start` returns its refusal (`InvalidArgument` naming the step and the rule). Checked by the runtime: `ID` matching `^[a-z0-9_]+$`, not `input`, unique across the graph; `WaitFor` naming siblings with no cycle; compensation only on `Call` / `Publish`; every `Path` parsing; no direct self-referencing `SubWorkflow`; positive `Sleep.Duration`; `MaxParallelism` ≤ 1024; depth ≤ 10; ≤ 500 steps. A workflow name another live service declares is refused (`AlreadyExists`).

## Gotchas

- `c.Workflow.Handle` after `Start` → `CodeState`.
- Step ids allow only `[a-z0-9_]` — no dashes, no camelCase.
- Do not put a protobuf message into a step input: run state is JSON.
- Keep every `Definition.Version` with live runs deployed somewhere; a task for a missing version fails with `UNSUPPORTED_VERSION`.
- `wf.Local` must stop on `ctx.Done()`: a lost lease or step deadline cancels it.
