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
[![Go 1.24+](https://img.shields.io/badge/go-%E2%89%A51.24-00ADD8.svg)](https://go.dev/dl/)
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
- [Differences from the Node SDK](#differences-from-the-node-sdk)
- [Platform features](#platform-features)
- [FAQ](#faq)
- [Community](#community)
- [License](#license)

---

## Install

```sh
go get github.com/service-bridge/sdk/go
```

- **Go:** 1.24 or newer.
- **Backend:** a running ServiceBridge runtime (gRPC control plane on `:14445`) backed by PostgreSQL 18+. See [Runtime setup](#runtime-setup).
- **Cgo:** not required. The local event outbox is SQLite through a pure-Go driver.

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
sb.SubscribeEvent[T](c, name, fn)       sb.NewMethod[Req, Resp](serviceClient, method)
sb.SubscribeEventRaw(c, name, fn)
```

Everything that needs no type parameter stays a method on the domain it belongs to:

```
c.Job.Handle(...)        c.Workflow.Handle/Start/Signal/Cancel/Await/Query/Replay(...)
c.Telemetry.StartOp/Logger/Counter/Gauge/Histogram(...)
c.Identity()  c.ServiceMap()  c.PolicyEvaluation()  c.Start(ctx)  c.Stop(ctx)
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
| `WithTimeout(d)` | none — the caller's `ctx` deadline stands | Bounds one call. |
| `WithTransport(t)` | `TransportDirect` | `TransportDirect` dials the callee over mTLS, so load balancing and the breaker apply. `TransportProxy` routes through the runtime, which resolves the instance and owns the idempotency claim. |
| `WithIdempotencyKey(k)` | none | Opts into runtime-side dedup. Its presence is also what unlocks retrying failures that leave the callee's state unknown — the SDK never invents a key. |
| `WithBusinessKey(k)` | none | Labels the call in the trace view with a domain id (an order id, a customer id). |

`sb.WithCallDefaults(...)` at construction applies the same options to every call that does not override them.

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

Durable, at-least-once publish/subscribe. A publish is a local insert into an on-disk SQLite outbox, then a background drain to the runtime — an unreachable runtime never slows a publication down or fails it.

```go
placed, err := sb.DefineEvent[*orderpb.OrderPlaced](c, "order.placed")

err = sb.SubscribeEvent(c, "order.shipped",
	func(ctx context.Context, e *orderpb.OrderPlaced) error {
		return sendReceipt(ctx, e.GetOrderId())
	})

// after Start
id, err := placed.Publish(ctx,
	&orderpb.OrderPlaced{OrderId: "o-1", Total: 4200},
	sb.WithPartitionKey("o-1"),
	sb.WithEventIdempotencyKey("order-o-1-placed"),
)
```

`sb.PublishEvent[T](ctx, c, name, payload, opts...)` is the same thing without the declared handle. `sb.SubscribeEventRaw(c, name, fn)` hands the payload over undecoded, for a name whose payload type varies by publisher.

A subscription may name one event or a pattern: `*` covers exactly one segment, `#` covers zero or more, matching the runtime's AMQP routing. `order.*` sees `order.created`; `order.#` sees that and `order.eu.created` and plain `order`. The runtime routes the delivery and the SDK matches the pattern locally, so the handler is found under the concrete name the publisher used.

A publish takes a name, never a pattern. Names must match `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$`; anything else is refused at declaration with `CodeInvalidEventName`. A full outbox returns `CodeOutboxFull`.

Returning an error from a subscriber nacks the delivery, and the runtime redelivers it later — make handlers idempotent. Retries, fan-out and the dead-letter queue are the runtime's; the SDK has no DLQ API, the dashboard operates it.

| `PublishOption` | What it does |
|---|---|
| `WithEventIdempotencyKey(k)` | Dedup key at the runtime. Spelled apart from the call-side key because the two travel to different places. |
| `WithPartitionKey(k)` | Puts the event on a FIFO lane: consumers see events sharing a key in publication order. |
| `WithFireAndForget()` | Sends straight to the runtime, skipping the local buffer and every retry with it. |
| `WithHeaders(map[string]string)` | Envelope metadata. |
| `WithOccurredAt(unixMs)` | The moment the event happened. Defaults to the moment of publication. |

### Jobs

Scheduled work: cron, fixed interval, or one-shot. The runtime owns the schedule, the lease and the retries.

```go
import "github.com/service-bridge/sdk/go/job"

nightly, err := job.Cron("0 3 * * *", "UTC") // five fields, no seconds
err = c.Job.Handle("nightly-rollup",
	job.NewSpec(nightly,
		job.WithOverlap(job.OverlapSkip),
		job.WithCatchup(job.CatchupFireOnce),
		job.WithMaxAttempts(5),
		job.WithDeps(job.RPC("billing-svc.Rollup")),
	),
	func(ctx context.Context, exec job.Execution) error {
		return rollup(ctx, exec.IdempotencyKey)
	})

beat, err := job.Interval(30 * time.Second)
err = c.Job.Handle("heartbeat", job.NewSpec(beat), ping)
```

A trigger comes only from `job.Cron`, `job.Interval` or `job.At`, so a job carries exactly one by construction. The cron expression is parsed at declaration by the same parser the runtime registers with — a typo fails where you wrote it instead of never firing.

The handler receives `job.Execution`: `Name`, `ID`, `ScheduledAtUnixMs`, `LocalScheduledAtUnixMs`, `Attempt`, `IdempotencyKey`. Jobs carry no input and no output. **Be idempotent by `IdempotencyKey`, not by `Attempt`**: the key is the same across every attempt of one scheduled fire, while `Attempt` changes on each retry, so keying on it makes every retry look like new work. Wrap a failure in `job.ErrPermanent` to stop the runtime from spending the remaining attempts on it.

Options left unset are decided by the runtime — the SDK keeps no copy of the defaults to drift from. The full option list is in [`job/README.md`](./job/README.md).

### Workflows

Durable DAGs. Declare the graph once; the runtime executes it, persists state between steps, survives restarts, and compensates on failure or cancel.

```go
import wf "github.com/service-bridge/sdk/go/workflow"

err := c.Workflow.Handle("checkout", wf.Definition{
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

Callbacks run on the client's own goroutines and must not block. `Identity` is read per use, never cached: every certificate rotation mints a fresh `InstanceID` for the same `ServiceID`.

A policy violation deserves attention even though it is not an error: the runtime registers what it can and warns about the rest, so on the wire a half-wired service looks like a healthy one. `sb.WithFailOnPolicyViolation()` turns that warning into a stop.

### Testing

`sbtest` runs your handlers with no network, no runtime and no local storage.

```go
import "github.com/service-bridge/sdk/go/sbtest"

func TestCharge(t *testing.T) {
	h := sbtest.New()
	if err := sbtest.Handle(h.RPC, "Charge", chargeHandler); err != nil {
		t.Fatal(err)
	}

	res, err := sbtest.Invoke[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h.RPC, "Charge", &paymentpb.ChargeRequest{Amount: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetOk() {
		t.Fatal("expected the charge to be accepted")
	}
}
```

`sbtest.Respond` / `RespondWith` arrange answers for outbound calls, `h.RPC.Calls()` reads back what was called, and `Define` / `Subscribe` / `Publish` do the same for events.

What the double deliberately does **not** reproduce: runtime routing and wildcard event patterns, access policy, leases and fencing, retries and breakers, streaming, workflows, idempotency, partition ordering. A double that pretended to be a runtime would go green where production fails. Those belong in end-to-end tests against a live runtime. See [`sbtest/README.md`](./sbtest/README.md).

---

## Configuration

Everything is a functional option on `sb.New(url, key, opts...)`. Every wrong bound is reported there, with `CodeConfig`, before any I/O — a misconfigured limit must never look like a network condition and feed the reconnect ladder.

```go
c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"),
	sb.WithAdvertise(os.Getenv("POD_IP"), 50051),
	sb.WithCallDefaults(sb.WithTimeout(10*time.Second)),
	sb.WithCallAttempts(3),
	sb.WithDataDir("/var/lib/orders/sb"),
	sb.WithMaxOutboxRows(50_000),
	sb.WithInboundLimits(256, 256),
	sb.WithReconnectLadder(time.Second, 5*time.Second, 30*time.Second),
)
```

| Option | Default | What it does |
|---|---|---|
| `WithAdvertise(host, port)` | `127.0.0.1`, port `0` | The address peers dial for direct RPC. Port `0` asks the OS for a free one and announces what it hands back. Pass a real address in a container: the default is announced as-is. |
| `WithCallerOnly()` | off | Outbound-only instance: no inbound listener, and registering a handler is refused. Contradicts `WithAdvertise`. |
| `WithCallDefaults(opts...)` | none | `CallOption`s applied under every call that does not override them. |
| `WithCallAttempts(n)` | `3` | Total tries of one logical call, counting the first: three means one call and two retries. |
| `WithFailOnPolicyViolation()` | off | Stop the client when the runtime reports a policy violation instead of only surfacing it. |
| `WithDataDir(dir)` | `./.servicebridge` | Directory holding the local outbox database. |
| `WithMaxOutboxRows(n)` | `10000` | Cap of the local event buffer; past it `Publish` returns `CodeOutboxFull`. `0` lifts the cap — an uncapped buffer turns a long outage into a full disk. |
| `WithDrainBatchSize(n)` | `100` | Buffered events one drain iteration claims. |
| `WithMaxInFlightEvents(n)` | `32` | Concurrently processed inbound deliveries. At the cap the delivery stream stops being read, which is what the runtime feels as backpressure. |
| `WithInboundLimits(calls, streams)` | `512` / `512` | Handlers running at once across every connection, and HTTP/2 streams per connection. Past the first bound a caller gets `ResourceExhausted` — load is shed, not queued. |
| `WithReconnectAttempts(n)` | `0` — unlimited | Cap on consecutive reconnect attempts. A service that gives up mid rolling restart is a service that needs a human. |
| `WithReconnectLadder(rungs...)` | `1s, 5s, 15s, 30s, 60s` | Reconnect delays. The last rung repeats forever and every rung is jittered. |
| `WithLogger(log)` | `slog.Default()` | Where the SDK writes its own diagnostics. |

The defaults are exported as constants — `sb.DefaultDataDir`, `sb.DefaultMaxOutboxRows`, `sb.DefaultDrainBatchSize`, `sb.DefaultMaxInFlightEvents`, `sb.DefaultMaxConcurrentCalls`, `sb.DefaultMaxConcurrentStreams`, `sb.DefaultCallAttempts`, `sb.DefaultAdvertiseHost`.

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

err = c.Stop(ctx) // releases every resource, in the reverse order; idempotent
```

`Start` seals the declarations, opens the outbox, provisions the mTLS identity, binds the inbound server, registers, waits for the first registry snapshot and only then starts the subscriptions. It returns once the instance is registered and routable.

The client outlives the context passed to `Start`: that context's cancellation is dropped and its values are kept, so a request-scoped context cannot take the client down. `Stop` is the way down, and it reports the first failure without skipping the rest — a channel left open would hold its reconnect goroutines until the process dies.

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
	// the callee answered with a failure; errors.Unwrap(err) has it
}
```

| Code | Sentinel | Raised when |
|---|---|---|
| `CodeConfig` | `ErrConfig` | A configuration the SDK refuses to run with. Never reaches the reconnect ladder — no amount of retrying fixes a bad bound. |
| `CodeState` | `ErrState` | An operation in the wrong lifecycle phase: declaring after `Start`, publishing before it, using a stopped client. |
| `CodeConnection` | `ErrConnection` | Provisioning, the session, or a stream that will not open. |
| `CodeAccessDenied` | `ErrAccessDenied` | The mesh access policy refused the call, the publish or the run. |
| `CodeNotFound` | `ErrNotFound` | A name the mesh has no definition for. |
| `CodeValidation` | `ErrValidation` | A declaration or an argument the runtime would reject, caught locally where it was written. |
| `CodeTerminal` | `ErrTerminal` | A workflow run that has already finished. |
| `CodeOutboxFull` | `ErrOutboxFull` | The local event buffer is at its cap. |
| `CodeNoLiveInstance` | `ErrNoLiveInstance` | A call with nowhere to go: nothing publishes the contract, nothing advertises an address, or everything is shedding. |
| `CodeInvalidEventName` | `ErrInvalidEventName` | A name the runtime's event grammar rejects. |
| `CodeHandler` | `ErrHandler` | The callee's handler returned a failure. It is an answer, not a transport condition, and repeating it changes nothing. |
| `CodeInternal` | `ErrInternal` | Everything else. |

The `job`, `sbhttp` and `sbtest` packages carry their own sentinels for what they refuse locally, matched the same way with `errors.Is` — each package README lists them. A refused workflow declaration comes back from `c.Workflow.Handle` as `CodeValidation`.

---

## Units of time

Everything on the wire is `int64` **unix milliseconds** for instants and `int64` **milliseconds** for durations. In Go you write a `time.Duration` (`WithTimeout`, `WithLeaseTTL`, `CallOpts.Timeout`) and the SDK converts; where a field is already a number, its name says the unit — `OccurredAtMs`, `ScheduledAtUnixMs`, `UnhealthySinceMs`.

Seconds appear in exactly one place and are always spelled out: the workflow contract's `TimeoutSec`, `DurationSec` and `WithRunTimeoutSec`. That is the runtime's unit for those fields, not a typo.

---

## Differences from the Node SDK

Both SDKs speak to the same runtime and expose the same platform. Where the shape differs, it is deliberate.

**Types are generated, not parsed.** The Node SDK reads a `.proto` or `.schema.json` at runtime. Here the schema comes from the descriptor inside the generated struct, so there is no file to ship next to the binary, no parse step at boot, and a mismatch between the schema and the code you wrote is not expressible.

**No `useSchema`.** In Node, loading the schema is a separate mandatory step, and forgetting it throws on the first call in production. Here `sb.NewMethod` declares the dependency and knows the schema from its type parameters — the same mistake does not compile.

**Generic functions instead of methods.** Go has no generic methods, so `sb.Handle(c, …)`, `sb.Call[Req, Resp](ctx, c, …)`, `sb.PublishEvent[T](…)` take the client as an argument. Everything that needs no type parameter stayed a method: `c.Job.Handle`, `c.Workflow.Start`, `c.Telemetry.StartOp`.

**Streams are `iter.Seq2`.** Consuming a stream is a plain `range` over `(chunk, error)`, and leaving the loop tears the stream down by construction rather than by a `return` you have to remember inside a `for await`.

**Logs are `slog`.** The SDK exposes a `*slog.Logger` and its handler, so you plug it into the chain you already have instead of learning a second logging API.

**gin is a separate module.** Go has no optional dependencies: gin inside the main module would appear in the dependency graph of every SDK user, including everyone who has never heard of it. `sbhttp` covers `net/http` and chi in the main module; `sbgin` is its own.

**No `"auto"` transport.** Node picks direct when an endpoint is known. Here the choice is explicit and the default is `TransportDirect`, with `TransportProxy` for calls that should be resolved and claimed by the runtime.

**Different defaults where the runtime is the same.** The Go outbox caps at 10 000 rows and drains 100 per iteration; the reconnect delay is a jittered ladder rather than a fixed interval, with no attempt cap by default.

---

## AI coding skill

The package ships a skill so an AI coding agent writes correct ServiceBridge code on the first try — the real RPC, events, jobs, workflow and HTTP API, grounded in this SDK rather than guessed. Copy it into your agent's skills directory:

```sh
cp -r $(go env GOMODCACHE)/github.com/service-bridge/sdk/go@*/skill .claude/skills/servicebridge-go
```

Or pull it straight from the repo: `npx degit service-bridge/sdk/go/skill .claude/skills/servicebridge-go`. Restart the agent to load it. Source: [`skill/`](./skill).

Every example in it is compiled against this module, so what the agent copies builds.

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
| RabbitMQ / Kafka / NATS | Durable events with outbox, fan-out, retries, DLQ |
| Temporal / Cadence | Durable workflows with compensation, signals, replay |
| A cron service / Quartz | Leased, retried scheduled jobs |
| Jaeger / Tempo + Prometheus + Loki | Tracing, metrics and logs, correlated out of the box |
| gRPC + a service registry | Typed RPC with discovery, LB and breakers |

---

## FAQ

**Do I have to use Protobuf?** For RPC and events, yes — the generated type is the contract. Workflow run state and step payloads are JSON, so those take plain Go values.

**Does ServiceBridge proxy my HTTP traffic?** No. You run your own `net/http`, chi or gin server. The integration publishes your routes for the Service Map and adds trace spans; your HTTP path is untouched.

**How do I scale horizontally?** Run as many SDK instances as you like; the runtime load-balances RPC across live instances and fails over automatically. The runtime itself is a single source of truth backed by PostgreSQL.

**What happens on a transient disconnect?** Published events sit in the local SQLite outbox and drain when the connection returns. The client reconnects on a jittered ladder and rotates certificates with overlap, so live instances do not drop traffic.

**Does the outbox need cgo?** No. The driver is pure Go, so `CGO_ENABLED=0` builds work.

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
