# RPC — Go SDK reference

Request/response and server-side streaming. Load balancing, retries and the circuit breaker live on the caller side; routing and policy live in the runtime.

## Signatures

```go signature
func Handle[Req, Resp proto.Message](c *Client, name string, fn func(ctx context.Context, req Req) (Resp, error)) error
func HandleStream[Req, Chunk proto.Message](c *Client, name string, fn func(ctx context.Context, req Req, send func(Chunk) error) error) error

func NewClient(c *Client, service string) *ServiceClient
func NewMethod[Req, Resp proto.Message](sc *ServiceClient, method string) (*Method[Req, Resp], error)
func (m *Method[Req, Resp]) Call(ctx context.Context, req Req, opts ...CallOption) (Resp, error)
func (m *Method[Req, Resp]) Stream(ctx context.Context, req Req, opts ...CallOption) iter.Seq2[Resp, error]

func Call[Req, Resp proto.Message](ctx context.Context, c *Client, service, method string, req Req, opts ...CallOption) (Resp, error)
func Stream[Req, Chunk proto.Message](ctx context.Context, c *Client, service, method string, req Req, opts ...CallOption) iter.Seq2[Chunk, error]

func (c *Client) Service(name string, deps ServiceDeps) error
```

## Schemas

There is no schema file and no registration step. The request and response types **are** the contract: the SDK reads the protobuf descriptor out of the generated struct and derives the JSON Schema and the contract hash from it.

Write messages only — no `service` block, no `protoc-gen-go-grpc`:

```proto
syntax = "proto3";
package demo.payment;
option go_package = "example.com/orders/paymentpb";

message ChargeRequest { string user_id = 1; int64 amount = 2; string currency = 3; }
message ChargeReply   { bool ok = 1; string transaction_id = 2; }
```

```sh
protoc -I . --go_out=. --go_opt=module=example.com/orders payment.proto
```

The contract hash covers field numbers, types and cardinality — **not** field names. Renaming a field is wire-compatible and does not reroute traffic; changing a number, a type or cardinality does, and the runtime then routes callers only to callees advertising the matching hash. That is the version-routing mechanism, and it is also why `CodeNoLiveInstance` on a live callee almost always means the message shape drifted on one side.

## CallOption

| Option | Default | Effect |
|---|---|---|
| `sb.WithTimeout(d)` | none — the caller's `ctx` deadline stands | Bounds one call. |
| `sb.WithTransport(t)` | `sb.TransportDirect` | `TransportDirect` dials the callee over mTLS (LB + breaker apply). `TransportProxy` routes through the runtime, which resolves the instance and owns the idempotency claim. |
| `sb.WithIdempotencyKey(k)` | none | Runtime-side dedup. Its presence is also what unlocks retrying failures that leave the callee's state unknown. |
| `sb.WithBusinessKey(k)` | none | Labels the call in the trace with a domain id. |

`sb.WithCallDefaults(opts...)` at construction applies the same options under every call that does not override them.

## Retry classification

Budget is `sb.WithCallAttempts(n)`, default `3` — **total** tries counting the first. Delays: 200 ms base, ×2, capped at 5 s, ±30 % jitter.

| gRPC code | Retried |
|---|---|
| `Unavailable`, `ResourceExhausted` | Always — the request provably never executed. |
| `DeadlineExceeded`, `Internal`, `Aborted`, `Unknown` | Only with `sb.WithIdempotencyKey`. |
| anything else | Never. |
| handler error | Never — it is an answer, not a transport fault. |

`DeadlineExceeded` sits behind the idempotency gate on purpose: the deadline expires on the caller side and says nothing about the callee, which may have completed the work.

## Streaming

Handlers send through a callback; callers get `iter.Seq2` and use a plain `range`. `send` blocks while the caller is behind (that is the backpressure) and fails once the caller is gone. Leaving the loop tears the stream down by construction. Streams are never retried.

## Complete program — worker

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"example.com/orders/genpb"
	"example.com/orders/paymentpb"
	sb "github.com/service-bridge/sdk/go"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("PAYMENT_KEY"),
		sb.WithAdvertise(os.Getenv("POD_IP"), 50051),
		sb.WithInboundLimits(256, 256),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Unary. Both type parameters are inferred from the function.
	if err := sb.Handle(c, "Charge",
		func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
			if req.GetAmount() <= 0 {
				// An ordinary error is an ANSWER: the caller sees CodeHandler
				// and it is never retried.
				return nil, fmt.Errorf("amount must be positive, got %d", req.GetAmount())
			}
			return &paymentpb.ChargeReply{Ok: true, TransactionId: "tx-" + req.GetUserId()}, nil
		}); err != nil {
		log.Fatal(err)
	}

	// Server-side streaming.
	if err := sb.HandleStream(c, "Generate",
		func(ctx context.Context, req *genpb.GenRequest, send func(*genpb.Token) error) error {
			for i, word := range strings.Fields(req.GetPrompt()) {
				if err := send(&genpb.Token{Text: word, Index: int32(i)}); err != nil {
					return err // the caller is gone; stop producing
				}
			}
			return nil
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

## Complete program — caller

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"example.com/orders/genpb"
	"example.com/orders/paymentpb"
	sb "github.com/service-bridge/sdk/go"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"),
		sb.WithCallerOnly(),
		sb.WithCallDefaults(sb.WithTimeout(10*time.Second)),
		sb.WithCallAttempts(3),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Declaring the method IS declaring the dependency. There is no second
	// "load the schema" step to forget.
	payment := sb.NewClient(c, "payment-svc")
	charge, err := sb.NewMethod[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](payment, "Charge")
	if err != nil {
		log.Fatal(err)
	}

	gen := sb.NewClient(c, "gen-svc")
	generate, err := sb.NewMethod[*genpb.GenRequest, *genpb.Token](gen, "Generate")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	// A state-changing call carries a domain-derived idempotency key, so a
	// timeout is retryable instead of fatal.
	res, err := charge.Call(ctx,
		&paymentpb.ChargeRequest{UserId: "u-1", Amount: 100, Currency: "EUR"},
		sb.WithIdempotencyKey("charge:order-42"),
		sb.WithBusinessKey("order-42"),
	)
	switch {
	case errors.Is(err, sb.ErrNoLiveInstance):
		log.Fatal("nothing serves payment-svc.Charge at this contract")
	case errors.Is(err, sb.ErrAccessDenied):
		log.Fatal("access policy refused the call")
	case err != nil:
		log.Fatal(err)
	}
	log.Println("charged:", res.GetOk(), res.GetTransactionId())

	// Streaming: leaving the loop tears the stream down.
	for tok, err := range generate.Stream(ctx, &genpb.GenRequest{Prompt: "write a haiku", MaxTokens: 64}) {
		if err != nil {
			log.Println("stream failed:", err)
			break
		}
		fmt.Print(tok.GetText(), " ")
	}
	fmt.Println()
}
```

## Coarse dependency declaration

For a service map edge without a typed method, or for methods called only through the untyped form:

```go
package deps

import sb "github.com/service-bridge/sdk/go"

func Declare(c *sb.Client) error {
	return c.Service("payment-svc", sb.ServiceDeps{
		RPC:       []string{"Charge", "Refund"},
		Workflows: []string{"checkout"},
		HTTP:      []string{"POST /orders"},
	})
}
```

Duplicates collapse — the same edge declared through both `NewMethod` and `Service` lands in the frame once.

## Gotchas

- `sb.Handle` on a `sb.WithCallerOnly()` client → `CodeConfig`.
- `sb.Handle` after `Start` → `CodeState`.
- Two handlers under one name → `CodeValidation`.
- `sb.Call` cannot infer `Resp` from its arguments — write both parameters, or use `sb.NewMethod`.
- Inbound overload sheds with gRPC `ResourceExhausted`; it does not queue.
- `*sb.Method` is safe for concurrent use; build it once when wiring dependencies.
