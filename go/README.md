<!--
Keywords: service-bridge, ServiceBridge, microservices, Go SDK, golang, gRPC, mTLS,
RPC framework, durable events, pub/sub, message broker alternative, RabbitMQ alternative,
workflow engine, saga, orchestration, Temporal alternative, job scheduler, cron,
distributed tracing, observability, OpenTelemetry alternative, Jaeger alternative,
service mesh alternative, Istio alternative, self-hosted, PostgreSQL, chi, gin,
circuit breaker, idempotency, retries, load balancing, protobuf, iter.Seq2, slog.
-->

# service-bridge (Go)

[![Go Reference](https://pkg.go.dev/badge/github.com/service-bridge/sdk/go.svg)](https://pkg.go.dev/github.com/service-bridge/sdk/go)
[![Go 1.26.6+](https://img.shields.io/badge/go-%E2%89%A51.26.6-00ADD8.svg)](https://go.dev/dl/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](../LICENSE)

**The Go SDK for [ServiceBridge](https://servicebridge.dev) — RPC, durable events, workflows, jobs, streaming and full observability over one self-hosted runtime. No broker. No sidecar. No tracing stack. Just one Go binary plus PostgreSQL.**

You declare what your service handles and what it calls. ServiceBridge does the rest: provisions an mTLS identity, opens the connection, registers your handlers, and routes every RPC, event, job and workflow step — with tracing, metrics and access policy built in.

```
        BEFORE                                       AFTER

  ┌─────────────────────┐
  │  Istio + Envoy      │  ← mesh / mTLS
  │  RabbitMQ / Kafka   │  ← events                 ┌──────────────────────┐
  │  Temporal           │  ← workflows              │                      │
  │  a cron scheduler   │  ← jobs                   │   ServiceBridge      │
  │  gRPC plumbing      │  ← RPC          ═══►      │   runtime (1 binary) │
  │  Jaeger / Tempo     │  ← tracing                │          +           │
  │  Prometheus wiring  │  ← metrics                │      PostgreSQL      │
  │  Loki               │  ← logs                   │                      │
  │  a load balancer    │  ← LB / retries           └──────────────────────┘
  │  service registry   │  ← discovery
  └─────────────────────┘
     10+ moving parts                                  2 things to run
```

---

## Table of contents

- [Install](#install)
- [Schemas come from generated types](#schemas-come-from-generated-types)
- [Quick start](#quick-start)
- [Runtime setup](#runtime-setup)
- [Shape of the API](#shape-of-the-api)
- [API reference](#api-reference)
  - [RPC](#rpc)
  - [Streaming](#streaming)
  - [Events](#events)
  - [Jobs](#jobs)
  - [Workflows](#workflows)
  - [Telemetry](#telemetry)
  - [HTTP](#http)
  - [Introspection and lifecycle callbacks](#introspection-and-lifecycle-callbacks)
  - [Testing](#testing)
- [Configuration](#configuration)
- [Lifecycle](#lifecycle)
- [Error handling](#error-handling)
- [Units of time](#units-of-time)
- [Go syntax next to the Node SDK](#go-syntax-next-to-the-node-sdk)
- [Platform features](#platform-features)
- [FAQ](#faq)
- [Community](#community)
- [License](#license)

---

## Install

```sh
go get github.com/service-bridge/sdk/go
```

- **Go:** 1.26.6 or newer (patched standard library).
- **Backend:** a running ServiceBridge runtime (gRPC control plane on `:14445`) backed by PostgreSQL 18+. See [Runtime setup](#runtime-setup).
- **Cgo:** not required.

The module path ends in `/go`; the package it declares is `servicebridge`. Every example here aliases it to `sb`:

```go
import sb "github.com/service-bridge/sdk/go"
```

The SDK reads **no environment variables** — the runtime address, the service key and every knob are arguments to `sb.New`, so you decide where configuration comes from.

Two more packages ship alongside it, and one separate module:

| Import | What it is |
|---|---|
| `github.com/service-bridge/sdk/go/job` | Job triggers, options and the handler contract. |
| `github.com/service-bridge/sdk/go/workflow` | The workflow graph vocabulary: steps, predicates, paths. |
| `github.com/service-bridge/sdk/go/sbhttp` | `net/http` and chi integration: route publication plus one span per request. |
| `github.com/service-bridge/sdk/go/sbtest` | In-memory doubles for unit-testing your handlers. |
| `github.com/service-bridge/sdk/go/sbgin` | gin integration. Its own module — see [HTTP](#http). |

---

## Schemas come from generated types

There is no schema file to point the SDK at and no schema to register. A handler's request and response types **are** the contract: the SDK reads the protobuf descriptor out of the generated struct, derives the JSON Schema and the contract hash from it, and sends those in the registration.

Write the messages, generate the Go types the usual way, and use them:

```proto
// payment.proto
syntax = "proto3";
package demo;
option go_package = "example.com/orders/paymentpb";

message ChargeRequest { string user_id = 1; int64 amount = 2; }
message ChargeReply   { bool ok = 1; }
```

```sh
protoc -I . --go_out=. --go_opt=module=example.com/orders payment.proto
```

Only the messages matter. You do not need a `service` block and you do not need `protoc-gen-go-grpc`: routing is the runtime's job, and a method is named by the string you register it under.

Because the contract hash comes from the types, two deployments compiled against different message shapes are different contracts. The runtime routes a caller only to callees advertising the exact hash it asks for, so a blue-green rollout routes `v1→v1` and `v2→v2` instead of failing to decode.

---

## Quick start

**Worker** — register the handler, then start.

```go
package main

import (
	"context"
	"log"
	"os"

	"example.com/orders/paymentpb"
	sb "github.com/service-bridge/sdk/go"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("PAYMENT_KEY"),
		sb.WithAdvertise("127.0.0.1", 50051))
	if err != nil {
		log.Fatal(err)
	}

	err = sb.Handle(c, "Charge",
		func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
			return &paymentpb.ChargeReply{Ok: req.GetAmount() > 0}, nil
		})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	select {}
}
```

**Caller** — in another process, declare the dependency and call it.

```go
package main

import (
	"context"
	"log"
	"os"

	"example.com/orders/paymentpb"
	sb "github.com/service-bridge/sdk/go"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"), sb.WithCallerOnly())
	if err != nil {
		log.Fatal(err)
	}

	payment := sb.NewClient(c, "payment-svc")
	charge, err := sb.NewMethod[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](payment, "Charge")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	res, err := charge.Call(ctx, &paymentpb.ChargeRequest{UserId: "u-1", Amount: 100})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("charged:", res.GetOk())
}
```

Declare everything — handlers, dependencies, events, jobs, workflows — **before** `Start`. Declarations ride along in the first registration, and after `Start` the set is sealed: a handler added later would exist in your process and nowhere in the mesh. Declaring late returns `CodeState` rather than failing silently.

---

## Runtime setup

The SDK needs a running ServiceBridge runtime. Spin one up with the one-line installer:

```sh
bash <(curl -fsSL https://servicebridge.dev/install.sh)
```

It pulls the runtime container, wires it to PostgreSQL 18+, and exposes the gRPC control plane on `:14445` and the dashboard on `:14444`. Open the dashboard, create a service, and copy its **bootstrap service key** — an `sb.…` string that is the second argument to `sb.New`. The key carries the CA certificate, so the SDK trusts exactly one root and nothing from the system store.

Each instance authenticates with its key: the SDK provisions a short-lived leaf certificate, opens an mTLS gRPC channel and registers. Certificates rotate automatically with overlap, so long-running instances never drop traffic at renewal.

Full self-hosting docs live at **[servicebridge.dev/docs](https://servicebridge.dev/docs)**.

---

## Shape of the API

Go has no generic methods, so anything that needs a type parameter is a **free function taking the client first**:

```
sb.Handle(c, name, fn)                  sb.Call[Req, Resp](ctx, c, service, method, req)
sb.HandleStream(c, name, fn)            sb.Stream[Req, Chunk](ctx, c, service, method, req)
sb.DefineEvent[T](c, name)              sb.PublishEvent[T](ctx, c, name, payload)
sb.SubscribeEvent[T](c, pattern, fn)    sb.NewMethod[Req, Resp](serviceClient, method)
sb.SubscribeEventRaw(c, pattern, fn)    sb.CallInfoFromContext(ctx)
```

Everything that needs no type parameter stays a method on the domain it belongs to:

```
c.Job.Handle(...)        c.Workflow.Handle/Start/Signal/Cancel/Await/Query/Replay(...)
c.Telemetry.StartOp/Logger/Counter/Gauge/Histogram(...)
c.Identity()  c.ServiceMap()  c.PolicyEvaluation()  c.Start(ctx)  c.Ready(ctx)  c.Stop(ctx)
```

Type inference does most of the work: `sb.Handle` and `sb.SubscribeEvent` infer both parameters from the function you pass. `sb.Call` and `sb.Stream` cannot infer the response type from the arguments, so write it out — or declare the method once with `sb.NewMethod` and call `.Call` / `.Stream` on it.

---

## API reference

### RPC

Request/response over mTLS, with load balancing, retries and circuit breaking on the caller side.

```go
// Serve a method.
err := sb.Handle(c, "Charge",
	func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
		return &paymentpb.ChargeReply{Ok: req.GetAmount() > 0}, nil
	})
```

Calling — the declared method (preferred), or the one-off form:

```go
payment := sb.NewClient(c, "payment-svc")
charge, err := sb.NewMethod[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](payment, "Charge")
res, err := charge.Call(ctx, &paymentpb.ChargeRequest{UserId: "u-1", Amount: 100})

res2, err := sb.Call[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
	ctx, c, "payment-svc", "Charge", req,
	sb.WithTimeout(5*time.Second), sb.WithIdempotencyKey("order-42"))
```

`sb.NewMethod` is the whole declaration: it registers the outgoing dependency on `payment-svc.Charge` and binds the schema from its type parameters. There is no second "load the schema" step to forget.

If you would rather declare dependencies in one block — for a coarse service map, or for methods you only call through the untyped form — use `c.Service`:

```go
err := c.Service("payment-svc", sb.ServiceDeps{
	RPC:       []string{"Charge", "Refund"},
	Workflows: []string{"checkout"},
	HTTP:      []string{"POST /orders"},
})
```

| `CallOption` | Default | What it does |
|---|---|---|
| `WithTimeout(d)` | `30s` (`sb.DefaultCallTimeout`) | Bounds one call; an earlier `ctx` deadline still wins. |
| `WithTransport(t)` | `TransportAuto` | `TransportAuto` dials the picked instance over mTLS and, if that path fails before the request was sent, retries through the runtime proxy. `TransportDirect` never proxies; `TransportProxy` always goes through the runtime. |
| `WithIdempotencyKey(k)` | none | Your dedup key; the callee reads it from `sb.CallInfoFromContext`. It does not make the SDK retry more — the SDK never invents a key. |
| `WithBusinessKey(k)` | none | Labels the call in the trace view with a domain id (an order id, a customer id). |

`sb.WithCallDefaults(...)` at construction applies the same options to every call path — `sb.Call`, `sb.Stream`, declared methods and workflow call steps — unless a call overrides them.

**Retries.** A call is repeated (up to `WithCallAttempts`, backoff 200 ms × 2 up to 5 s with jitter) only when the SDK can prove the handler never ran: no instance was selectable, the channel to the picked instance did not become ready before the request was written, or the callee refused with the `x-sb-not-dispatched` proof (draining, not ready, overloaded). Anything else — a timeout, a dropped connection mid-call, a handler failure — is returned as is, because the effect may already have happened. Streams are never retried.

**Load balancing and breaking.** Power-of-two-choices on in-flight calls over instances that advertise an endpoint, are not revoked, pass the per-instance circuit breaker and are not flagged unhealthy by the runtime; if the health hint would exclude everyone, it is ignored. The breaker counts `CONNECTION`, `TIMEOUT`, `OVERLOADED` and internal-status failures; a handler's answer counts as success.

**Serving side.** A handler's `ctx` is cancelled when the caller cancels or its deadline passes, and `sb.CallInfoFromContext(ctx)` gives the `RequestID`, `IdempotencyKey`, `CallerServiceID`, `CallerInstanceID` and `Deadline`. Return `&sb.HandlerError{Code: "OUT_OF_STOCK", Message: "..."}` to answer with your own code; the caller gets `CodeHandler` and reaches the same `*sb.HandlerError` with `errors.As`. Any other error, and a panic, answers `INTERNAL`.

### Streaming

Server-side streaming is a first-class shape. Handlers send with a callback; callers get an `iter.Seq2` and use a plain `range`.

```go
err := sb.HandleStream(c, "Generate",
	func(ctx context.Context, req *genpb.GenRequest, send func(*genpb.Token) error) error {
		for _, word := range strings.Fields(req.GetPrompt()) {
			if err := send(&genpb.Token{Text: word}); err != nil {
				return err
			}
		}
		return nil
	})
```

```go
req := &genpb.GenRequest{Prompt: "write a haiku"}
for tok, err := range sb.Stream[*genpb.GenRequest, *genpb.Token](ctx, c, "gen-svc", "Generate", req) {
	if err != nil {
		log.Println("stream failed:", err)
		break
	}
	fmt.Print(tok.GetText(), " ")
}
```

Sending blocks while the caller is behind — that is the backpressure — and fails once the caller is gone. Leaving the loop (`break`, `return`, or an error) tears the stream down: the iterator's cleanup runs by construction, so an abandoned stream cannot leak the callee's handler. Streams are never retried; a repeat would re-deliver chunks the caller already consumed.

### Events

At-least-once publish/subscribe. `PublishEvent` returns once the runtime acknowledged the event — it is in the runtime's store by then. While the runtime is unreachable the event waits in an in-memory queue: the queue is bounded (`CodeQueueFull`, see `WithMaxPendingPublishes`) and so is the wait (`CodeTimeout` after `WithPublishTimeout`, saying whether the event was ever sent). Nothing is written to disk; an event not yet acknowledged dies with the process.

```go
placed, err := sb.DefineEvent[*orderpb.OrderPlaced](c, "order.placed")

err = sb.SubscribeEvent(c, "order.*",
	func(ctx context.Context, e *orderpb.OrderPlaced) error {
		info, _ := sb.DeliveryFromContext(ctx)
		return sendReceipt(ctx, e.GetOrderId(), info.EventName)
	},
	sb.WithFilter(map[string]any{"$.region": "eu"}))

// after Start
id, err := placed.Publish(ctx,
	&orderpb.OrderPlaced{OrderId: "o-1", Total: 4200},
	sb.WithPartitionKey("o-1"),
	sb.WithEventIdempotencyKey("order-o-1-placed"),
)
```

`sb.PublishEvent[T](ctx, c, name, payload, opts...)` is the same thing without the declared handle. `DefineEvent` declares what this service publishes; a subscriber never needs it — its own type parameter is the schema it decodes with. `sb.SubscribeEventRaw(c, pattern, fn)` hands the payload over undecoded.

A subscription names one event or a pattern: `*` covers exactly one segment, `#` zero or more. Routing is the runtime's: each delivery lists the patterns of this service it matched, and the handlers of exactly those patterns run — the SDK does no wildcard matching of its own. One pattern has one handler per process (a second one is `CodeValidation`). `sb.WithFilter` narrows the subscription on the runtime with equality on JSON paths of the payload; a filter the runtime refuses stops the client at registration.

Ids are UUIDv7, increasing in publish order. Events sharing a partition key reach consumers in publication order. A publish takes a name, never a pattern; names must match `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$`, anything else is `CodeInvalidEventName`.

Returning an error from a subscriber nacks the delivery and the runtime redelivers it later — make handlers idempotent. `sb.DeliveryFromContext(ctx)` gives `EventID`, `EventName`, `Attempt`, `DeliveryID`, `LeaseToken`, `PartitionKey`, `Headers` and `OccurredAtMs`.

| `PublishOption` | What it does |
|---|---|
| `WithEventIdempotencyKey(k)` | Dedup key at the runtime: a repeat returns the original event's id. |
| `WithPartitionKey(k)` | Puts the event on a FIFO lane. |
| `WithFireAndForget()` | Returns the id as soon as the event is queued, without waiting for the acknowledgement. Accepts loss: until acknowledged the event lives only in process memory, and a failure is only logged. A full queue still fails. |
| `WithHeaders(map[string]string)` | Envelope metadata. |
| `WithOccurredAt(unixMs)` | The moment the event happened. Defaults to the moment of publication. |

| Runtime verdict | Result |
|---|---|
| accepted / duplicate | the event id (for a duplicate, the original one) |
| conflict (same id, other content) | `CodeConflict` |
| invalid name | `CodeInvalidEventName` |
| forbidden by policy | `CodeAccessDenied`, and `OnPolicyViolation` fires |
| no verdict / transport failure | retried with the same id (100 ms … 5 s), immediately on reconnect |

### Jobs

Scheduled work: cron, fixed interval, or one-shot. The runtime owns the schedule, the lease and the retries.

```go
import "github.com/service-bridge/sdk/go/job"

nightly, err := job.Cron("0 3 * * *", "UTC") // five fields, no seconds
err = c.Job.Handle("nightly-rollup",
	job.NewSpec(nightly,
		job.WithVersion("v1"),
		job.WithOverlap(job.OverlapSkip),
		job.WithCatchup(job.CatchupFireOnce),
		job.WithMaxAttempts(5),
		job.WithDeps(job.RPC("billing-svc.Rollup")),
	),
	func(ctx context.Context, exec job.Execution) error {
		return rollup(ctx, exec.IdempotencyKey)
	})

beat, err := job.Interval(30 * time.Second)
err = c.Job.Handle("heartbeat", job.NewSpec(beat, job.WithVersion("v1")), ping)
```

A trigger comes only from `job.Cron`, `job.Interval` or `job.At`, so a job carries exactly one by construction. The cron expression is parsed at declaration by the same parser the runtime registers with — a typo fails where you wrote it instead of never firing.

The handler receives `job.Execution`: `Name`, `ID`, `ScheduledAtUnixMs`, `LocalScheduledAtUnixMs`, `Attempt`, `IdempotencyKey`. Jobs carry no input and no output. **Be idempotent by `IdempotencyKey`, not by `Attempt`**: the key is the same across every attempt of one scheduled fire, while `Attempt` changes on each retry, so keying on it makes every retry look like new work. Return `sb.NonRetryable(err)` to stop the runtime from spending the remaining attempts on it: the execution goes to the dead-letter queue at once.

Options left unset are decided by the runtime — the SDK keeps no copy of the defaults to drift from. The full option list is in [`job/README.md`](./job/README.md).

### Workflows

Durable DAGs. Declare the graph once; the runtime executes it, persists state between steps, survives restarts, and compensates on failure or cancel.

```go
import wf "github.com/service-bridge/sdk/go/workflow"

err := c.Workflow.Handle("checkout", wf.Definition{
    Version: "v1",
	Input: map[string]any{
		"type":       "object",
		"properties": map[string]any{"orderId": map[string]any{"type": "string"}},
	},
	Steps: []wf.Step{
		wf.Call{
			Control: wf.Control{
				ID: "reserve",
				Compensate: &wf.Compensation{
					Service: wf.Name("inventory-svc"),
					Method:  wf.Name("Release"),
					Input:   wf.Path("$.reserve"),
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
		wf.Publish{
			Control: wf.Control{
				ID:      "announce",
				WaitFor: []string{"charge"},
				When:    wf.Truthy(wf.Path("$.charge.ok")),
			},
			Event: wf.Name("order.placed"),
			Input: wf.Path("$.input"),
		},
	},
})
```

Top-level steps start in parallel; `WaitFor` declares the dependencies that define the execution levels. Step kinds: `Call`, `Publish`, `Sleep`, `WaitEvent`, `WaitSignal`, `SubWorkflow`, `Parallel`, `Sequence`, `Local`. The set is closed — the marker method is unexported — so a graph can never carry a kind the runtime does not know.

A `wf.Call` step reaches an ordinary `sb.Handle[Req, Resp]` handler, so the method it names must also be declared with `sb.NewMethod`: run state is JSON while the callee takes protobuf, and the declared pair of types is both the encoding and the contract hash the step routes at. The step's `Input` is written as the message's JSON mirror — 64-bit integers are strings, enums are value names — and the reply lands in run state in the same form. A target named literally and never declared is refused at `Start`, with the workflow, the step and the missing declaration in the message; a target computed from run state fails the same way inside the run, because its name does not exist any earlier.

Two string types keep expressions and data apart: `wf.Path("$.reserve.id")` is read from run state when the step executes, `wf.Name("payment-svc")` is a literal written at declaration. A literal that happens to look like a path needs no escaping here; the type says which is which.

`wf.Local` runs a Go closure in the declaring process. The closure is not part of the frozen graph or the fingerprint — the step is identified by its `ID`, and the locally declared graph supplies the function the assignment cannot carry.

Driving a run:

```go
runID, err := c.Workflow.Start(ctx, "checkout",
	map[string]any{"orderId": "o-1"},
	sb.WithRunIdempotencyKey("checkout-o-1"),
	sb.WithRunTimeoutSec(600),
)

state, err := c.Workflow.Await(ctx, runID)   // blocks until terminal
snap, err := c.Workflow.Query(ctx, runID)    // RunSnapshot: Status, State, Steps
err = c.Workflow.Signal(ctx, runID, "approval", map[string]any{"ok": true})
err = c.Workflow.Cancel(ctx, runID)          // compensates in reverse
forked, err := c.Workflow.Replay(ctx, runID, "charge")
```

An unknown workflow name is `CodeNotFound`, a refusal by the access policy is `CodeAccessDenied`, and signalling or cancelling a finished run is `CodeTerminal`. Run state is JSON throughout (that is what `Path` reads and what `Await` returns), so step inputs and outputs are plain Go values, not protobuf messages.

The full vocabulary — predicates, `ForEach`, compensation, retry policies — is in [`workflow/README.md`](./workflow/README.md).

### Telemetry

Every RPC, event, job, workflow step and HTTP request already emits a span and propagates the trace across hops. `c.Telemetry` adds your own; anything opened inside a handler nests under that handler.

```go
ctx, op := c.Telemetry.StartOp(ctx, "reprice-cart", sb.WithOpBusinessKey(cartID))
if err := reprice(ctx, cartID); err != nil {
	op.Fail(err)
	return err
}
op.End()

c.Telemetry.Logger().Info("cart repriced", "cart", cartID, "items", 7)
c.Telemetry.Counter("carts_repriced_total", map[string]string{"tier": "gold"}).Inc()
c.Telemetry.Gauge("queue_depth", "", nil).Set(42)
c.Telemetry.Histogram("reprice_ms", "ms", nil, []float64{1, 5, 10, 50, 100}).Observe(12.5)
```

`StartOp` returns a context carrying the operation as the parent — pass **that** context down, or the calls made underneath start their own trace root and one request becomes two trees. `WithOpPeer(serviceID)` names the service the operation talks to.

`Logger()` is an ordinary `*slog.Logger` whose handler writes into the telemetry buffer. It is not a logger to learn: take `c.Telemetry.Logger().Handler()` and put it in your own `slog` chain if you want application logs in both places.

```go
handler := c.Telemetry.Logger().Handler()
app := slog.New(handler)
```

Metric handles re-resolve when the identity rotates, so a handle held for the lifetime of the process keeps reporting under the live instance instead of one the runtime already tore down. Anything recorded before `Start` waits in an in-memory ring and drains once connected.

`sb.WithLogger(*slog.Logger)` is a different knob: it sets where the SDK writes its own diagnostics.

### HTTP

ServiceBridge does **not** proxy your business HTTP. You run your own server; the integration publishes its routes to the Service Map and wraps each request in one `HTTP.HANDLE` span, so an HTTP request and the RPCs and events it triggers land in the same trace.

`net/http` and chi share one middleware shape, so both use `sbhttp` directly:

```go
import "github.com/service-bridge/sdk/go/sbhttp"

integration, err := sbhttp.New(c)

mux := sbhttp.NewMux()
mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusCreated)
})

err = integration.PublishMux(mux, sbhttp.Endpoint{Host: "10.0.0.4", Port: 3000})

srv := &http.Server{Addr: ":3000", Handler: integration.Middleware(mux)}
log.Fatal(srv.ListenAndServe())
```

`sbhttp.NewMux` is a thin wrapper over `http.ServeMux` that remembers the patterns as you register them. With chi, keep your own router and call `integration.PublishChi(router, endpoint)`; with anything else, hand `integration.Publish(routes, endpoint)` the list yourself. Publishing before `Start` is fine — the endpoint rides along in the first registration; after `Start` it reopens the registry stream so the routes arrive now rather than at the next reconnect.

**gin** lives in its own module, because Go has no optional dependencies and gin in the main module would land in the dependency graph of everyone using the SDK:

```sh
go get github.com/service-bridge/sdk/go/sbgin
```

```go
integration, err := sbhttp.New(c)

engine := gin.New()
engine.Use(sbgin.Middleware(integration)) // before the routes: gin runs handlers in registration order
engine.POST("/orders", func(ctx *gin.Context) { ctx.JSON(http.StatusCreated, gin.H{"ok": true}) })

err = sbgin.Publish(integration, engine, sbhttp.Endpoint{Host: "10.0.0.4", Port: 3000})
log.Fatal(engine.Run(":3000"))
```

Each request becomes one span named by its route template — `http.handle:POST//orders/{id}`, never the raw path; a request that matched no route is `*`. The template is found before the handler runs: for a wrapped `sbhttp.Mux` or `http.ServeMux`, from chi's routing context, from gin's `c.FullPath()`, or through `sbhttp.WithRouteResolver` for anything else. The span carries `{method, route, status}`; its business key is the `Idempotency-Key` header or `"<METHOD> <route>"`, never with a query string. An incoming `X-SB-Trace` header is ignored unless the integration is built with `sbhttp.WithTrustTraceHeader()` — a public edge must not let clients graft requests into arbitrary traces. Bodies are captured as-is when the runtime's capture mode asks for them; the runtime masks them on ingest.

The host is explicit or it is loopback: guessing an address from the environment is wrong more often than right inside a container. Details and the capture rules are in [`sbhttp/README.md`](./sbhttp/README.md).

### Introspection and lifecycle callbacks

```go
c.OnConnected(func(id sb.Identity) { log.Println("connected as", id.ServiceName, id.InstanceID) })
c.OnReconnecting(func(attempt int, cause error) { log.Println("reconnecting", attempt, cause) })
c.OnDraining(func(reason string) { log.Println("runtime is draining:", reason) })
c.OnDisconnected(func(cause error) { log.Println("disconnected:", cause) })
c.OnPolicyViolation(func(v sb.PolicyViolation) {
	log.Println("policy refused", v.Declaration, v.Value, v.Reason)
})

log.Println("identity:", c.Identity().ServiceID)
log.Println("instances in the mesh:", len(c.ServiceMap().Instances))
log.Println("capabilities:", c.PolicyEvaluation().Capabilities)
```

Callbacks run on the client's own goroutines and must not block; a panic in one is recovered and logged, and the others still run. The instance id is stable across certificate renewals. `OnDraining` reports a runtime shutting down; the client reconnects by itself when the runtime closes the session.

A policy violation deserves attention even though it is not an error: the runtime registers what it can and warns about the rest, so on the wire a half-wired service looks like a healthy one. `sb.WithFailOnPolicyViolation()` turns that warning into a stop.

### Testing

`sbtest` builds a real client whose network edges are in memory: you declare handlers, subscriptions and dependencies on it exactly as in production, and calls, deliveries and publications go through the same encoding, error mapping, publish queue and event routing.

```go
import "github.com/service-bridge/sdk/go/sbtest"

func TestCharge(t *testing.T) {
	h := sbtest.New(t)
	if err := sb.Handle(h.Client, "Charge", chargeHandler); err != nil {
		t.Fatal(err)
	}
	_ = sbtest.Respond(h, "fraud-svc", "Score", func(ctx context.Context, req *fraudpb.ScoreRequest) (*fraudpb.Score, error) {
		return &fraudpb.Score{Risk: 0.1}, nil
	})
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	res, err := sbtest.Invoke[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h, "Charge", &paymentpb.ChargeRequest{Amount: 100},
		sbtest.WithCaller("orders-svc-id", "inst-1"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetOk() || len(h.Published()) != 1 {
		t.Fatal("expected an accepted charge and one event")
	}
}
```

`Invoke` / `InvokeStream` return errors in the caller's form (`CodeHandler` with the handler's `*sb.HandlerError`), `h.Calls()` and `h.Published()` read back the outbound traffic, and `h.Deliver(ctx, name, payload)` delivers an event with the matched patterns the runtime would compute. Access policy, subscription filters, retries, leases, workflows and jobs are the runtime's and stay with end-to-end tests. See [`sbtest/README.md`](./sbtest/README.md).

---

## Configuration

Everything is a functional option on `sb.New(url, key, opts...)`. Every wrong bound is reported there, with `CodeConfig`, before any I/O — a misconfigured limit must never look like a network condition and feed the reconnect ladder.

```go
c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"),
	sb.WithAdvertise(os.Getenv("POD_IP"), 50051),
	sb.WithCallDefaults(sb.WithTimeout(10*time.Second)),
	sb.WithCallAttempts(3),
	sb.WithMaxPendingPublishes(50_000),
	sb.WithInboundLimits(256, 256),
	sb.WithReconnectLadder(time.Second, 5*time.Second, 30*time.Second),
)
```

| Option | Default | What it does |
|---|---|---|
| `WithAdvertise(host, port)` | `127.0.0.1`, port `0` | The address peers dial for direct RPC. Port `0` asks the OS for a free one and announces what it hands back. Pass a real address in a container: the default is announced as-is. |
| `WithCallerOnly()` | off | Outbound-only instance: no inbound listener, and registering a handler is refused. Contradicts `WithAdvertise`. |
| `WithCallDefaults(opts...)` | timeout `30s`, `TransportAuto` | `CallOption`s applied under every call path that does not override them. |
| `WithCallAttempts(n)` | `3` | Total tries of one logical call, counting the first. Only proven pre-dispatch failures are retried. |
| `WithFailOnPolicyViolation()` | off | Stop the client when the runtime reports a policy violation instead of only surfacing it. |
| `WithMaxPendingPublishes(n)` | `10000` | Events waiting for the runtime's acknowledgement; past it `PublishEvent` fails at once with `CodeQueueFull`. |
| `WithPublishTimeout(d)` | `30s` | How long one publication may wait for the acknowledgement before `CodeTimeout`. |
| `WithMaxInFlightEvents(n)` | `32` | Concurrently processed inbound deliveries. At the cap the delivery stream stops being read, which is what the runtime feels as backpressure. |
| `WithInboundLimits(calls, queued)` | `256` / `256` | Handlers running at once, and calls waiting for one. Past both a caller gets `RESOURCE_EXHAUSTED` with the not-dispatched proof and retries elsewhere. HTTP/2 streams per connection are capped at the sum. |
| `WithReconnectAttempts(n)` | `0` — unlimited | Cap on consecutive failed reconnects; the count resets on every successful session. |
| `WithReconnectLadder(rungs...)` | `1s, 5s, 15s, 30s, 60s` | Reconnect delays. The last rung repeats forever and every rung is jittered ±20 %. |
| `WithTelemetryDropHandler(fn)` | none | Called with the telemetry lost since the previous report (`ServerDrops`, `RingDrops`, `BackpressureLevel`). The same counts reach the runtime as `sb_sdk_telemetry_dropped_total{source}`. |
| `WithLogger(log)` | `slog.Default()` | Where the SDK writes its own diagnostics. |

The defaults are exported as constants — `sb.DefaultCallTimeout`, `sb.DefaultCallAttempts`, `sb.DefaultMaxPendingPublishes`, `sb.DefaultPublishTimeout`, `sb.DefaultMaxInFlightEvents`, `sb.DefaultMaxConcurrentCalls`, `sb.DefaultMaxQueuedCalls`, `sb.DefaultStopTimeout`, `sb.DefaultAdvertiseHost`.

---

## Lifecycle

```go
c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"))

// Declare: handlers, dependencies, events, jobs, workflows.
err = sb.Handle(c, "Ship", shipHandler)
payment := sb.NewClient(c, "payment-svc")
charge, err := sb.NewMethod[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](payment, "Charge")

err = c.Start(ctx) // provision, connect, register, subscribe

// ... serve ...

err = c.Stop(ctx) // drain, flush, release; idempotent
```

`Start` seals the declarations, provisions the mTLS identity, binds the inbound server, opens the control session, waits for the runtime's `Welcome` and the first registry snapshot (30 s at most), and only then starts the publisher and the subscriptions. A failure stops everything it started and is returned. `c.Ready(ctx)` blocks while the client is reconnecting and returns once the current session is live and has applied a snapshot.

The client outlives the context passed to `Start`: that context's cancellation is dropped and its values are kept. Reconnects follow the ladder; credentials the runtime refuses, an unknown service, a refused registration (for example an invalid subscription filter) or a protocol version the runtime does not speak end the client instead (`OnDisconnected`). A runtime `Drain` is routine: the client reconnects once the runtime closes the session. Certificates renew in place 30 minutes before expiry: nothing reconnects, new connections simply present the new leaf.

`Stop` shuts down in an order that loses nothing it can keep:

1. it withdraws the instance — re-registers without a call endpoint, the inbound server refuses new calls with the not-dispatched proof, subscribers stop taking new work;
2. waits for in-flight calls, event and job handlers;
3. sends what is still in the publish queue; what is left fails with `CodeConnection` "client stopped";
4. flushes telemetry and waits up to 2 s for its last acknowledgement;
5. closes the session, the streams, the channels and the server.

Steps 2 and 3 share the deadline of `ctx` (10 s when it has none). Go cannot stop code that ignores its context: past the deadline `Stop` cancels the handlers and returns `CodeTimeout`.

---

## Error handling

`*sb.Error` is the only error type the SDK returns, so one `errors.As` against it catches every SDK failure and cannot go stale when a code is added. The taxonomy lives in the `Code` field; the sentinels match on code alone, ignoring `Op`, `Msg` and the wrapped cause.

```go
_, err := charge.Call(ctx, req)

var sbErr *sb.Error
if errors.As(err, &sbErr) {
	log.Printf("%s failed with %s: %s", sbErr.Op, sbErr.Code, sbErr.Msg)
}

switch {
case errors.Is(err, sb.ErrAccessDenied):
	// the access policy refuses this call
case errors.Is(err, sb.ErrNoLiveInstance):
	// nothing serves this contract right now
case errors.Is(err, sb.ErrHandler):
	var he *sb.HandlerError
	if errors.As(err, &he) && he.Code == "OUT_OF_STOCK" {
		// the callee's own business answer
	}
}
```

| Code | Sentinel | Raised when | `Retryable()` |
|---|---|---|---|
| `CodeConfig` | `ErrConfig` | A configuration the SDK refuses to run with, or a runtime speaking another protocol version. | no |
| `CodeState` | `ErrState` | An operation in the wrong lifecycle phase: declaring after `Start`, publishing before it, using a stopped client. | no |
| `CodeConnection` | `ErrConnection` | The runtime or the callee could not be reached (`UNAVAILABLE`), a channel that never became ready, a publish left over at `Stop`. | yes |
| `CodeTimeout` | `ErrTimeout` | A deadline passed with the outcome unknown. Repeat only with an idempotency key. | no |
| `CodeCancelled` | `ErrCancelled` | The caller cancelled. | no |
| `CodeAccessDenied` | `ErrAccessDenied` | The access policy, a revoked identity or rejected credentials. | no |
| `CodeNotFound` | `ErrNotFound` | A name the mesh has no definition for. | no |
| `CodeValidation` | `ErrValidation` | A declaration or an argument that is refused. | no |
| `CodeConflict` | `ErrConflict` | An id already used with other content. | no |
| `CodeTerminal` | `ErrTerminal` | A workflow run that has already finished. | no |
| `CodeNoLiveInstance` | `ErrNoLiveInstance` | A call with nowhere to go: nothing publishes the contract or advertises an address, or every instance is circuit-open. | yes |
| `CodeOverloaded` | `ErrOverloaded` | The callee or the runtime is shedding load. | yes |
| `CodeQueueFull` | `ErrQueueFull` | The publish queue is at its cap. | yes |
| `CodeInvalidEventName` | `ErrInvalidEventName` | A name the event grammar rejects. | no |
| `CodeHandler` | `ErrHandler` | The callee's handler answered with a failure; `errors.As(err, &*sb.HandlerError)` gives its own code. | no |
| `CodeInternal` | `ErrInternal` | Everything else. | no |

`(*sb.Error).Retryable()` is true exactly for the rows marked yes. A gRPC status from the far side maps as: `CANCELLED`→`CANCELLED`; `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`→`VALIDATION`; `DEADLINE_EXCEEDED`→`TIMEOUT`; `NOT_FOUND`, `UNIMPLEMENTED`→`NOT_FOUND`; `ALREADY_EXISTS`→`CONFLICT`; `PERMISSION_DENIED`, `UNAUTHENTICATED`→`ACCESS_DENIED`; `RESOURCE_EXHAUSTED`→`OVERLOADED`; `UNAVAILABLE`→`CONNECTION`; `UNKNOWN`, `ABORTED`, `INTERNAL`, `DATA_LOSS`→`INTERNAL`. The codes and the table are the same in every ServiceBridge SDK.

The `job`, `sbhttp` and `sbtest` packages carry their own sentinels for what they refuse locally, matched the same way with `errors.Is` — each package README lists them. A refused workflow declaration comes back from `c.Workflow.Handle` as `CodeValidation`.

---

## Units of time

Everything on the wire is `int64` **unix milliseconds** for instants and `int64` **milliseconds** for durations. In Go you write a `time.Duration` (`WithTimeout`, `WithLeaseTTL`, `CallOpts.Timeout`) and the SDK converts; where a field is already a number, its name says the unit — `OccurredAtMs`, `ScheduledAtUnixMs`, `UnhealthySinceMs`.

Seconds appear in exactly one place and are always spelled out: the workflow contract's `TimeoutSec`, `DurationSec` and `WithRunTimeoutSec`. That is the runtime's unit for those fields, not a typo.

---

## Go syntax next to the Node SDK

Behaviour is the same in both SDKs — defaults, retries, error codes, wire forms, lifecycle. What differs is syntax:

- **Types are generated, not parsed.** The schema comes from the descriptor inside the generated struct; `sb.NewMethod` declares a dependency and its schema in one step.
- **Generic functions instead of methods.** Go has no generic methods, so `sb.Handle(c, …)`, `sb.Call[Req, Resp](ctx, c, …)`, `sb.PublishEvent[T](…)` take the client as an argument.
- **Streams are `iter.Seq2`.** Leaving the `range` tears the stream down.
- **Handler context is `ctx`.** `sb.CallInfoFromContext` and `sb.DeliveryFromContext` read what Node passes as the handler's second argument.
- **Logs are `slog`.**
- **gin is a separate module**, because Go has no optional dependencies.

---

## AI coding skill

The package ships a skill so an AI coding agent writes correct ServiceBridge code on the first try — the real RPC, events, jobs, workflow and HTTP API, grounded in this SDK rather than guessed. Copy it into your agent's skills directory:

```sh
cp -r $(go env GOMODCACHE)/github.com/service-bridge/sdk/go@*/skill .claude/skills/servicebridge-go
```

Or pull it straight from the repo: `npx degit service-bridge/sdk/go/skill .claude/skills/servicebridge-go`. Restart the agent to load it. Source: [`skill/`](./skill).

---

## Platform features

| Area | What you get |
|---|---|
| **Communication** | Direct RPC, server-side streaming, durable events, service discovery, full-mesh routing, a live service map |
| **Orchestration** | Workflows (DAG steps with compensation), sub-workflows, jobs (cron / interval / delayed), bidirectional replay |
| **Reliability** | At-least-once delivery, retries, DLQ, idempotency, fan-out, session resilience, multi-instance failover, circuit breakers |
| **Traffic control** | Load balancing, rate limiting, per-definition limits, filter expressions |
| **Security** | TLS by default, mTLS identity, auto-provisioned certs from a service key, granular access policy |
| **Observability** | Unified tracing with propagation, Prometheus-compatible metrics, structured logs, smart alerts |

Designed to run up to 1000 services against a single runtime.

| You'd otherwise reach for | ServiceBridge gives you |
|---|---|
| Istio / Linkerd (mesh, mTLS) | mTLS identity + routing + policy, no sidecars |
| RabbitMQ / Kafka / NATS | Durable events, fan-out, retries, DLQ |
| Temporal / Cadence | Durable workflows with compensation, signals, replay |
| A cron service / Quartz | Leased, retried scheduled jobs |
| Jaeger / Tempo + Prometheus + Loki | Tracing, metrics and logs, correlated out of the box |
| gRPC + a service registry | Typed RPC with discovery, LB and breakers |

---

## FAQ

**Do I have to use Protobuf?** For RPC and events, yes — the generated type is the contract. Workflow run state and step payloads are JSON, so those take plain Go values.

**Does ServiceBridge proxy my HTTP traffic?** No. You run your own `net/http`, chi or gin server. The integration publishes your routes for the Service Map and adds trace spans; your HTTP path is untouched.

**How do I scale horizontally?** Run as many SDK instances as you like; the runtime load-balances RPC across live instances and fails over automatically. The runtime itself is a single source of truth backed by PostgreSQL.

**What happens on a transient disconnect?** Publications wait in the in-memory queue and go out — under their original ids — when the connection returns; each one still resolves only on the runtime's acknowledgement. The client reconnects on a jittered ladder, and certificates renew in place, so live instances do not drop traffic.

**Where do I see traces, metrics and the DLQ?** In the runtime dashboard on `:14444`.

**Why does my handler registration fail?** Almost always because it ran after `Start` (`CodeState`) or on a `WithCallerOnly` client (`CodeConfig`). Declare before `Start`.

---

## Community

- **Website & docs:** [servicebridge.dev](https://servicebridge.dev) · [servicebridge.dev/docs](https://servicebridge.dev/docs)
- **API reference:** [pkg.go.dev/github.com/service-bridge/sdk/go](https://pkg.go.dev/github.com/service-bridge/sdk/go)
- **SDK umbrella repo (all languages):** [github.com/service-bridge/sdk](https://github.com/service-bridge/sdk)
- **Runtime:** [github.com/servicebridge2/runtime](https://github.com/servicebridge2/runtime)

Issues and feedback are welcome.

---

## License

Licensed under the **MIT License** — see [LICENSE](../LICENSE). Free for any use, including commercial; you only need to keep the copyright and license notice (attribution to esurkov1 <esurkovv@yandex.ru>).
