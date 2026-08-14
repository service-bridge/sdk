# Workflows — Go SDK reference

Durable DAGs. Declare the graph once; the runtime executes it, persists state between steps, survives restarts and compensates on failure or cancel.

## Signatures

```go signature
func (d *WorkflowDomain) Handle(name string, def wf.Definition) error
func (d *WorkflowDomain) Start(ctx context.Context, name string, input any, opts ...StartOption) (string, error)
func (d *WorkflowDomain) Signal(ctx context.Context, runID, signal string, payload any) error
func (d *WorkflowDomain) Cancel(ctx context.Context, runID string) error
func (d *WorkflowDomain) Await(ctx context.Context, runID string) (map[string]any, error)
func (d *WorkflowDomain) Query(ctx context.Context, runID string) (RunSnapshot, error)
func (d *WorkflowDomain) Replay(ctx context.Context, runID, fromStepID string) (string, error)

func WithRunIdempotencyKey(key string) StartOption
func WithRunTimeoutSec(sec int) StartOption
```

Import: `wf "github.com/service-bridge/sdk/go/workflow"`.

## The mental model

- **Run state is JSON.** The run input lives under `input`; each step's output lives under that step's `ID`. Steps are written with plain Go values — maps, slices, strings, numbers.
- **A `call` step reaches an ordinary typed handler.** Its JSON tree is read into the callee's protobuf request and its reply comes back as JSON, through the pair of types declared with `NewMethod`. See "Call steps need a declared dependency".
- **Top-level steps start in parallel.** `WaitFor` declares the dependencies that create the execution levels.
- **The step set is closed** — the `wf.Step` marker method is unexported, so a graph can never carry a kind the runtime does not know.
- **The graph executes in the declaring process.** The runtime assigns a step; the body comes from the locally declared graph. That is what makes `wf.Local` possible.

## Step kinds

| Kind | Own fields |
|---|---|
| `wf.Call` | `Service`, `Method` (`Target`), `Input`, `Opts *CallOpts` |
| `wf.Publish` | `Event` (`Target`), `Input`, `Opts *PublishOpts` |
| `wf.Sleep` | `DurationSec int64` (runtime-held durable timer) |
| `wf.WaitEvent` | `Event` (`Target`), `Filter map[string]any` |
| `wf.WaitSignal` | `Signal string` |
| `wf.SubWorkflow` | `Workflow` (`Target`), `Input`, `Opts *StartOpts` |
| `wf.Parallel` | `Steps []Step`, `ForEach *ForEach` |
| `wf.Sequence` | `Steps []Step`, `ForEach *ForEach` |
| `wf.Local` | `Fn LocalFunc` |

Every kind embeds `wf.Control{ID, WaitFor, When, Compensate, TimeoutSec, Retry}`.

`Control.TimeoutSec` bounds the **step** (expiry starts compensation). `CallOpts.Timeout` bounds the underlying RPC. They are different things.

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

Grammar: `$` followed by any number of `.field`, `[N]` and `[*]`. `[*].field` collects that field from every element into an array. A path that leads nowhere resolves to `nil`, not an error — a step skipped by its condition leaves nothing behind.

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
		TimeoutSec: 900,
		Steps: []wf.Step{
			// Compensated call: the reverse action reads THIS step's output.
			wf.Call{
				Control: wf.Control{
					ID: "reserve",
					Compensate: &wf.Compensation{
						Kind:           wf.CompensateCall,
						Service:        wf.Name("inventory-svc"),
						Method:         wf.Name("Release"),
						Input:          wf.Path("$.reserve"),
						IdempotencyKey: wf.Path("$.input.orderId"),
					},
				},
				Service: wf.Name("inventory-svc"),
				Method:  wf.Name("Reserve"),
				Input:   wf.Path("$.input"),
			},
			wf.Call{
				Control: wf.Control{ID: "charge", WaitFor: []string{"reserve"}, TimeoutSec: 30},
				Service: wf.Name("payment-svc"),
				Method:  wf.Name("Charge"),
				Input:   wf.Path("$.input"),
			},
			// A Go closure that runs in THIS process. Not part of the frozen
			// graph or the fingerprint — the step is identified by its ID.
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
				Control:     wf.Control{ID: "cooldown", WaitFor: []string{"notify_all"}},
				DurationSec: 300,
			},
			wf.WaitSignal{
				Control: wf.Control{ID: "await_approval", WaitFor: []string{"cooldown"}},
				Signal:  "approval",
			},
		},
	}); err != nil {
		log.Fatal(err) // CodeValidation names the step and the field
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	runID, err := c.Workflow.Start(ctx, "checkout",
		map[string]any{"orderId": "o-1", "recipients": []any{"a@example.com"}},
		sb.WithRunIdempotencyKey("checkout-o-1"), // a repeat returns the same run
		sb.WithRunTimeoutSec(600),
	)
	if err != nil {
		log.Fatal(err)
	}

	snap, err := c.Workflow.Query(ctx, runID)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("status:", snap.Status, "steps:", len(snap.Steps))

	if err := c.Workflow.Signal(ctx, runID, "approval", map[string]any{"ok": true}); err != nil {
		log.Fatal(err)
	}

	// Await returns state ONLY for a successful run. Cancelled or compensated
	// comes back as CodeTerminal — there is no result to hand back.
	state, err := c.Workflow.Await(ctx, runID)
	switch {
	case errors.Is(err, sb.ErrTerminal):
		log.Println("run ended without success")
	case err != nil:
		log.Fatal(err)
	default:
		log.Println("final state:", state)
	}
}
```

## Driving runs

| Call | Behaviour |
|---|---|
| `Start` | Returns the run id. `WithRunIdempotencyKey` makes a repeat return the existing run. |
| `Query` | One request, no waiting: `RunSnapshot{RunID, Status, State, Steps}`; each `StepSnapshot` has `StepID`, `Status`, `Output`, `LastError`, `CompensatedBy`. Status strings come from the runtime. |
| `Signal` | Delivers to a run parked on a matching `WaitSignal`. |
| `Cancel` | Compensates what was already done, in reverse. |
| `Await` | Blocks until terminal. **No SDK-side timeout** — only your `ctx` bounds it. Returns state on success, `CodeTerminal` otherwise. |
| `Replay` | Forks a finished run into a **new** one from `fromStepID` onward; an empty id replays the whole run. Returns the new run id. |

Codes: `CodeNotFound` (unknown workflow), `CodeAccessDenied`, `CodeTerminal` (signal/cancel on a finished run, or a non-success `Await`), `CodeValidation` (refused declaration).

## Validation at declaration

`c.Workflow.Handle` freezes the graph and refuses an invalid one before anything reaches the runtime. Checked: non-empty name and at least one step; `ID` matching `^[a-z0-9_]+$` and unique across the whole graph including nesting; `WaitFor` resolving with no cycle; compensation only on `Call` and `Publish`; non-empty targets; every `Path` parsing (in values, filters, options and predicates); no self-referencing `SubWorkflow`; non-negative `Sleep.DurationSec`; non-nil `Local.Fn`; non-empty groups; `ForEach.As` in the step-id alphabet; depth ≤ 10; ≤ 500 steps.

## Gotchas

- `c.Workflow.Handle` after `Start` → `CodeState`.
- Compensation on anything but `Call` / `Publish` → validation error.
- Step ids allow only `[a-z0-9_]` — no dashes, no camelCase.
- Do not put a protobuf message into a step input: run state is JSON.
- `wf.Local` bodies must be declared in every process that may execute the run.
