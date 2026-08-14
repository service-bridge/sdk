---
name: servicebridge-go
description: Build backend services with the ServiceBridge Go SDK (github.com/service-bridge/sdk/go) — RPC, server-side streaming, durable events, workflows, scheduled jobs, and net/http / chi / gin integration against a self-hosted ServiceBridge runtime. Use when writing Go that calls or handles RPC, publishes or consumes events, declares workflows or jobs, or wires an HTTP framework into the service mesh.
---

# ServiceBridge Go SDK

ServiceBridge is a self-hosted runtime ("one Go binary + PostgreSQL") that replaces a service mesh, a message broker, a workflow engine and a tracing stack. A service declares its handlers and dependencies, then starts — the runtime owns transport, delivery, orchestration, policy and observability. No sidecars.

Module path is **`github.com/service-bridge/sdk/go`**; the package it declares is **`servicebridge`**. Alias it to `sb`. Everything in this skill is the real, current API — do not invent methods or options.

## The two rules that prevent most mistakes

**1. Anything needing a type parameter is a free function taking the client first.** Go has no generic methods.

```
sb.Handle(c, name, fn)                sb.Call[Req, Resp](ctx, c, service, method, req)
sb.HandleStream(c, name, fn)          sb.Stream[Req, Chunk](ctx, c, service, method, req)
sb.DefineEvent[T](c, name)            sb.PublishEvent[T](ctx, c, name, payload)
sb.SubscribeEvent[T](c, name, fn)     sb.NewMethod[Req, Resp](serviceClient, method)
sb.SubscribeEventRaw(c, name, fn)     sb.NewClient(c, service)
```

Everything that needs no type parameter stayed a method: `c.Job.Handle`, `c.Workflow.Start`, `c.Telemetry.StartOp`, `c.Identity()`, `c.ServiceMap()`, `c.Start(ctx)`, `c.Stop(ctx)`.

**2. Declare before `Start`, act after `Start`.**

1. `sb.New(url, key, opts...)` — no I/O; every bad bound fails here with `CodeConfig`.
2. Declare: `sb.Handle`, `sb.HandleStream`, `sb.NewMethod`, `sb.DefineEvent`, `sb.SubscribeEvent`, `c.Job.Handle`, `c.Workflow.Handle`, `c.Service`, HTTP route publication.
3. `c.Start(ctx)` — seals declarations, provisions mTLS, registers, waits for the first registry snapshot.
4. Act: `.Call`, `.Publish`, `c.Workflow.Start`.
5. `c.Stop(ctx)` — idempotent teardown.

Declaring after `Start` returns `CodeState`. Publishing before `Start` returns `CodeState`.

## Golden rules

- **Import as `sb "github.com/service-bridge/sdk/go"`.** The module path ends in `/go`; the package is `servicebridge`.
- **The SDK reads NO environment variables.** URL, key and every knob are arguments to `sb.New`. Read env in your own code.
- **Get the bootstrap key from the dashboard** at `http://localhost:14444` → Services → Create service. It is the `sb.…` string, second argument to `sb.New`.
- **Schemas come from generated protobuf types.** There is no `.proto` file to point the SDK at and no "register the schema" step. `sb.Handle` and `sb.NewMethod` derive the schema and the contract hash from their type parameters.
- **A service that serves RPC needs an address:** `sb.WithAdvertise(host, port)`. The default is `127.0.0.1` and it is advertised as-is, so a container without it is unreachable. A pure caller uses `sb.WithCallerOnly()`.
- **Event and job handlers must be idempotent.** Delivery is at-least-once. For jobs, dedup on `exec.IdempotencyKey`, **never** on `exec.Attempt`.
- **A state-changing call needs `sb.WithIdempotencyKey(k)` to survive a timeout.** Without it, `DeadlineExceeded` is not retried — deliberately.
- **The default transport is `sb.TransportDirect`.** There is no `auto` transport in Go.
- **Teardown is `c.Stop(ctx)`.** There is no `Close()`.
- **Every SDK error is `*sb.Error`.** Match with `errors.Is(err, sb.ErrX)` or `errors.As(err, &sbErr)` and switch on `sbErr.Code`.

## Install & connect

```sh
go get github.com/service-bridge/sdk/go
```

Requires Go 1.24+, and a running runtime (gRPC control plane on `:14445`, dashboard on `:14444`):

```sh
bash <(curl -fsSL https://servicebridge.dev/install.sh)
```

`github.com/service-bridge/sdk/go/sbgin` is a separate module (`go get` it separately) because Go has no optional dependencies.

## What each domain is for

| You want to… | Use | Reference |
|---|---|---|
| Request/response between services, plus server-side streaming | `sb.Handle` / `sb.NewMethod` | [reference/rpc.md](reference/rpc.md) |
| Durable at-least-once pub/sub | `sb.DefineEvent` / `sb.SubscribeEvent` | [reference/events.md](reference/events.md) |
| Multi-step orchestration (DAG, compensation, signals, replay) | `c.Workflow` | [reference/workflows.md](reference/workflows.md) |
| Cron / interval / one-shot scheduled work | `c.Job` | [reference/jobs.md](reference/jobs.md) |
| Put your own net/http, chi or gin server on the Service Map and in the trace | `sbhttp` / `sbgin` | [reference/http-integrations.md](reference/http-integrations.md) |
| Unit-test a handler with no runtime | `sbtest` | [reference/testing.md](reference/testing.md) |
| Constructor options, defaults, errors, telemetry | `sb.New` options | [reference/configuration.md](reference/configuration.md) |

Read the matching reference file before writing code for a domain — each has exact signatures, real defaults and a complete compilable program.

## The canonical smoke test: two services, one call

```proto
// payment.proto — only messages are needed. No `service` block,
// no protoc-gen-go-grpc: routing is the runtime's job.
syntax = "proto3";
package demo.payment;
option go_package = "example.com/orders/paymentpb";

message ChargeRequest { string user_id = 1; int64 amount = 2; string currency = 3; }
message ChargeReply   { bool ok = 1; string transaction_id = 2; }
```

```sh
protoc -I . --go_out=. --go_opt=module=example.com/orders payment.proto
```

```go
// callee/main.go — serves the method.
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

	if err := sb.Handle(c, "Charge",
		func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
			return &paymentpb.ChargeReply{
				Ok:            req.GetAmount() > 0,
				TransactionId: "tx-" + req.GetUserId(),
			}, nil
		}); err != nil {
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

```go
// caller/main.go — declares the dependency and calls it.
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
	log.Println("charged:", res.GetOk(), res.GetTransactionId())
}
```

The caller targets the callee by its **service name** from the dashboard — never a host or port. The runtime resolves routing, and it routes only to callees advertising the exact contract hash derived from the pair of Go types.

## Error handling, in one block

```go
package errorhandling

import (
	"context"
	"errors"
	"log"

	"example.com/orders/paymentpb"
	sb "github.com/service-bridge/sdk/go"
)

func Call(ctx context.Context, m *sb.Method[*paymentpb.ChargeRequest, *paymentpb.ChargeReply]) error {
	_, err := m.Call(ctx, &paymentpb.ChargeRequest{UserId: "u-1", Amount: 100})
	if err == nil {
		return nil
	}

	var sbErr *sb.Error
	if errors.As(err, &sbErr) {
		log.Printf("op=%s code=%s msg=%s", sbErr.Op, sbErr.Code, sbErr.Msg)
	}

	switch {
	case errors.Is(err, sb.ErrNoLiveInstance):
		// nothing serves this contract: wrong message shape, no advertise, or all shedding
	case errors.Is(err, sb.ErrAccessDenied):
		// the access policy refused
	case errors.Is(err, sb.ErrHandler):
		// the callee answered with a failure — not a transport fault, do not retry
	case errors.Is(err, sb.ErrState):
		// wrong lifecycle phase: declared after Start, or the client is stopped
	}
	return err
}
```

Codes: `CodeConfig`, `CodeState`, `CodeConnection`, `CodeAccessDenied`, `CodeNotFound`, `CodeValidation`, `CodeTerminal`, `CodeOutboxFull`, `CodeNoLiveInstance`, `CodeInvalidEventName`, `CodeHandler`, `CodeInternal`. Each has a sentinel: `sb.ErrConfig`, `sb.ErrState`, and so on.

## Units of time

Everything on the wire is `int64` unix-milliseconds for instants and `int64` milliseconds for durations. In Go you pass a `time.Duration` and the SDK converts; a numeric field spells its unit (`OccurredAtMs`, `ScheduledAtUnixMs`, `LeaseTTLMs`).

Seconds appear in exactly five places, always spelled out — that is the workflow contract's unit:
`wf.Control.TimeoutSec`, `wf.Definition.TimeoutSec`, `wf.Sleep.DurationSec`, `wf.StartOpts.TimeoutSec`, `sb.WithRunTimeoutSec`.
