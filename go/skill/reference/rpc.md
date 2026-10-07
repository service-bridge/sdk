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

func CallInfoFromContext(ctx context.Context) (CallInfo, bool)
type CallInfo struct {
	RequestID        string    // same across the caller's retries
	IdempotencyKey   string    // the caller's key, empty when none
	CallerServiceID  string    // SPIFFE identity of a direct caller, or the service the runtime names on the proxy path
	CallerInstanceID string    // empty on the proxy path
	Deadline         time.Time // zero when the call has none
}

type HandlerError struct {
	Code    string
	Message string
}
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
| `sb.WithTimeout(d)` | `30s` (`sb.DefaultCallTimeout`) | Bounds one call — for a stream, the whole stream. An earlier `ctx` deadline still wins. |
| `sb.WithTransport(t)` | `sb.TransportAuto` | `TransportAuto` dials the picked instance over mTLS and, when that path fails before the request was sent, sends the next attempt through the runtime proxy (no backoff; the unreachable instances are tried last). `TransportDirect` never proxies. `TransportProxy` always goes through the runtime, which picks the instance. LB and the breaker apply to direct attempts. |
| `sb.WithIdempotencyKey(k)` | none | Your dedup key; the callee reads it from `sb.CallInfoFromContext`. It does not widen what the SDK retries. The SDK never invents one. |
| `sb.WithBusinessKey(k)` | none | Labels the call in the trace with a domain id. |

`sb.WithCallDefaults(opts...)` at construction applies the same options under every call path — `sb.Call`, `sb.Stream`, declared methods and workflow `wf.Call` steps — unless a call overrides them.

## Retries

Budget is `sb.WithCallAttempts(n)`, default `3` — **total** tries counting the first. Delays: 200 ms base, ×2, capped at 5 s, ±30 % jitter.

A call is repeated **only when the SDK can prove the handler never ran**:

| Proof | Example |
|---|---|
| No instance was selectable | `CodeNoLiveInstance` at selection |
| The channel to the picked instance did not become ready before the request was written | unreachable instance, failed TLS handshake |
| The callee refused with the `x-sb-not-dispatched: 1` trailer | draining (`UNAVAILABLE "draining"`), before its first snapshot (`UNAVAILABLE "not ready"`), overloaded (`RESOURCE_EXHAUSTED`) |

Anything else returns as is: a timeout, a connection dropped mid-call, a bare `UNAVAILABLE`, a handler answer. The effect may already have happened. An idempotency key does not change this rule. Streams are never retried.

After `CodeTimeout` the outcome is unknown. Repeating is the caller's decision, and it is safe only when the callee dedups on the key it reads from `CallInfo.IdempotencyKey`.

## Handler failures

```go
return nil, &sb.HandlerError{Code: "OUT_OF_STOCK", Message: "sku 42"}
```

A handler failure is an **answer**: it travels with status `OK`, the breaker counts it as success, and the SDK never retries it. The caller gets `*sb.Error{Code: CodeHandler}` and `errors.As(err, &he)` reaches a `*sb.HandlerError` carrying the handler's own `Code` and `Message`. Any other error, and a recovered panic, reaches the caller as a `*sb.HandlerError` with `Code == "INTERNAL"`. An SDK error from a nested call returned as is also answers `INTERNAL`, not the downstream business code — wrap it in your own `*sb.HandlerError` to pass a code on.

The handler's `ctx` is cancelled when the caller cancels or its deadline passes.

## Load balancing and the breaker

Power-of-two-choices on in-flight calls over instances that publish the contract hash, advertise an endpoint, are not revoked, pass the per-instance breaker and are not flagged unhealthy by the runtime; if the health hint would exclude every candidate, it is ignored. The breaker (per instance: 10 s window, 10 calls minimum, opens at 50 % failures for 30 s) counts `CONNECTION`, `TIMEOUT`, `OVERLOADED` and internal transport statuses; a handler answer counts as success.

## Inbound refusals (callee side)

| Situation | Status | Retried elsewhere |
|---|---|---|
| Before the first registry snapshot | `UNAVAILABLE "not ready"` | yes |
| `Stop` in progress | `UNAVAILABLE "draining"` | yes |
| `sb.WithInboundLimits(calls, queued)` exhausted (default `256`/`256`) | `RESOURCE_EXHAUSTED` | yes |
| Acceptance rules refuse, or the caller is revoked | `PERMISSION_DENIED` | no |
| Method not registered | `NOT_FOUND` | no |
| Wrong kind (unary vs stream) | `FAILED_PRECONDITION` | no |
| Request does not decode | `INVALID_ARGUMENT` | no |

## Streaming

Handlers send through a callback; callers get `iter.Seq2` and use a plain `range`. `send` blocks while the caller is behind (that is the backpressure) and fails once the caller is gone. Leaving the loop tears the stream down by construction. Streams are never retried. `sb.WithTimeout` bounds the whole stream (30 s by default) — pass a longer one for a long stream.

## Complete program — worker

```go
package main

import (
	"context"
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
				// A business failure is an ANSWER: the caller sees CodeHandler
				// with this HandlerError inside, and it is never retried.
				return nil, &sb.HandlerError{Code: "INVALID_AMOUNT", Message: "amount must be positive"}
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

	// A state-changing call carries a domain-derived idempotency key. The
	// callee reads it from CallInfo and dedups on it; the SDK still never
	// retries past a timeout on its own.
	res, err := charge.Call(ctx,
		&paymentpb.ChargeRequest{UserId: "u-1", Amount: 100, Currency: "EUR"},
		sb.WithIdempotencyKey("charge:order-42"),
		sb.WithBusinessKey("order-42"),
	)
	var he *sb.HandlerError
	switch {
	case errors.As(err, &he):
		log.Fatalf("payment-svc answered %s: %s", he.Code, he.Message)
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
- Inbound overload queues up to `queued` calls behind `calls` running handlers, then sheds with `RESOURCE_EXHAUSTED` and the not-dispatched proof, so the caller retries on another instance.
- `*sb.Method` is safe for concurrent use; build it once when wiring dependencies.
