# Testing — Go SDK reference

`sbtest` runs your handlers inside a **real** `*sb.Client` whose network edges are in memory: no runtime, no listener, no TLS. Requests and responses go through the same proto encoding, handler wrapping, error mapping, publish queue and event routing as in production; only what the runtime and the peers would answer is doubled.

**Read the limits first.** A green `sbtest` run does not mean working production.

## What the harness does NOT reproduce

- access policy
- subscription filters (reproduce the runtime's decision with `WithMatchedPatterns`)
- leases, delivery retries, DLQ
- call retries, load balancing and circuit breakers
- workflows and jobs

All of the above is only verified end-to-end against a live runtime. Use `sbtest` for the domain logic inside a handler and for what the handler calls and publishes.

## Signatures

```go signature
func New(t TB, opts ...sb.Option) *Harness // the client is stopped in t.Cleanup
func (h *Harness) Start(ctx context.Context) error
func (h *Harness) Reset()

// Inbound: call a registered handler the way a peer would.
func Invoke[Req, Resp proto.Message](ctx context.Context, h *Harness, method string, req Req, opts ...InvokeOption) (Resp, error)
func InvokeStream[Req, Chunk proto.Message](ctx context.Context, h *Harness, method string, req Req, opts ...InvokeOption) ([]Chunk, error)
func WithCaller(serviceID, instanceID string) InvokeOption
func WithRequestID(id string) InvokeOption   // a fresh UUID otherwise
func WithIdempotencyKey(key string) InvokeOption

// Outbound: arrange answers, read back what was called.
func Respond[Req, Resp proto.Message](h *Harness, service, method string, fn func(ctx context.Context, req Req) (Resp, error)) error
func RespondStream[Req, Chunk proto.Message](h *Harness, service, method string, fn func(ctx context.Context, req Req) ([]Chunk, error)) error
func (h *Harness) Calls() []CallRecord
func DecodeCall[T proto.Message](rec CallRecord) (T, error)

// Events.
func (h *Harness) Published() []PublishedEvent
func DecodePublished[T proto.Message](e PublishedEvent) (T, error)
func (h *Harness) Deliver(ctx context.Context, name string, payload proto.Message, opts ...DeliverOption) (DeliveryResult, error)
func WithMatchedPatterns(patterns ...string) DeliverOption
func WithAttempt(n int32) DeliverOption // 1 by default
func WithDeliveryPartitionKey(key string) DeliverOption
func WithDeliveryHeaders(headers map[string]string) DeliverOption
func MatchPattern(pattern, name string) bool
```

```go signature
type Harness struct {
	Client *sb.Client // declare on it exactly as in production, then Start
}

type CallRecord struct {
	Service, Method string
	Payload         []byte
	IdempotencyKey  string
	BusinessKey     string
	Transport       sb.Transport
}

type PublishedEvent struct {
	ID, Name       string
	Payload        []byte
	PayloadJSON    []byte
	PartitionKey   string
	IdempotencyKey string
	Headers        map[string]string
	OccurredAtMs   int64
}

type DeliveryResult struct {
	Acked           bool
	Reason          string   // nack reason; empty when acked
	MatchedPatterns []string // the patterns the delivery carried
}
```

Sentinels: `sbtest.ErrNoResponse` (no answer arranged), `sbtest.ErrInvalidArg` (nil harness, empty name, nil function).

## Rules

| Rule | Why |
|---|---|
| Declare on `h.Client` with the ordinary API, then `h.Start(ctx)` | It is the production client; declaring after `Start` is `CodeState` here too. |
| `Invoke` errors come in the **caller's** form | A `*sb.HandlerError` the handler returned arrives as `*sb.Error{Code: CodeHandler}` with that `HandlerError` inside (`errors.As`). Any other error or a panic → `HandlerError.Code == "INTERNAL"`. An SDK error from a nested call returned as is → `INTERNAL`, not the downstream code. Unknown method → `CodeNotFound`; undecodable request → `CodeValidation`. |
| `Respond` answers `sb.Call`, a declared method's `Call` and a workflow `wf.Call` step | Request decoded into `Req`, answer encoded from `Resp` — a type mismatch fails as on the wire. A `*sb.HandlerError` from `fn` answers with that code; any other error answers `INTERNAL`. Arranging again replaces the answer. |
| An outbound call with nothing arranged fails with `ErrNoResponse` | A forgotten `Respond` is a bug in the test; a silent zero hides it. The call is still recorded in `Calls()`. |
| Publishing goes through the real publish queue | `Published()` holds what the in-memory runtime accepted, with `PayloadJSON` filled. An invalid name is `CodeInvalidEventName`. |
| `Deliver` computes the matched patterns with the runtime's rules | `*` one segment, `#` zero or more. The subscriber runs the handlers of exactly those patterns; acked only if all return `nil`; no match → nack `no handler for matched patterns`. `sb.DeliveryFromContext` works. |
| Filters are not evaluated | Pass `WithMatchedPatterns(...)` to reproduce what a filter decided. |

## Write the handler as a plain function

Keep the handler separate from its registration: production and the test register the same function, and the code under test is the code that ships.

```go
package orders

import (
	"context"

	"example.com/orders/paymentpb"
	sb "github.com/service-bridge/sdk/go"
)

type Ledger interface {
	Debit(ctx context.Context, user string, amount int64) error
}

func NewChargeHandler(ledger Ledger) func(context.Context, *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
	return func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
		if req.GetAmount() <= 0 {
			return nil, &sb.HandlerError{Code: "INVALID_AMOUNT", Message: "amount must be positive"}
		}
		if err := ledger.Debit(ctx, req.GetUserId(), req.GetAmount()); err != nil {
			return nil, err
		}
		return &paymentpb.ChargeReply{Ok: true, TransactionId: "tx-" + req.GetUserId()}, nil
	}
}

func Wire(c *sb.Client, ledger Ledger) error {
	return sb.Handle(c, "Charge", NewChargeHandler(ledger))
}
```

## Complete test file

```go
package orders_test

import (
	"context"
	"errors"
	"testing"

	"example.com/orders"
	"example.com/orders/orderpb"
	"example.com/orders/paymentpb"
	sb "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/sbtest"
)

type okLedger struct{}

func (okLedger) Debit(context.Context, string, int64) error { return nil }

func started(t *testing.T, h *sbtest.Harness) {
	t.Helper()
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestChargeAccepts(t *testing.T) {
	h := sbtest.New(t)
	if err := orders.Wire(h.Client, okLedger{}); err != nil {
		t.Fatal(err)
	}
	started(t, h)

	res, err := sbtest.Invoke[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h, "Charge",
		&paymentpb.ChargeRequest{UserId: "u-1", Amount: 100},
		sbtest.WithCaller("orders-svc-id", "inst-1"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetOk() || res.GetTransactionId() != "tx-u-1" {
		t.Fatalf("unexpected reply: %v", res)
	}
}

func TestChargeRejectsZero(t *testing.T) {
	h := sbtest.New(t)
	if err := orders.Wire(h.Client, okLedger{}); err != nil {
		t.Fatal(err)
	}
	started(t, h)

	_, err := sbtest.Invoke[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h, "Charge", &paymentpb.ChargeRequest{Amount: 0})
	var he *sb.HandlerError
	if !errors.Is(err, sb.ErrHandler) || !errors.As(err, &he) || he.Code != "INVALID_AMOUNT" {
		t.Fatalf("want INVALID_AMOUNT, got %v", err)
	}
}

func TestPlaceCallsPaymentAndPublishes(t *testing.T) {
	h := sbtest.New(t)
	payment := sb.NewClient(h.Client, "payment-svc")
	charge, err := sb.NewMethod[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](payment, "Charge")
	if err != nil {
		t.Fatal(err)
	}
	if err := sb.Handle(h.Client, "Place",
		func(ctx context.Context, req *orderpb.PlaceRequest) (*orderpb.PlaceReply, error) {
			if _, err := charge.Call(ctx, &paymentpb.ChargeRequest{UserId: req.GetUserId(), Amount: req.GetTotal()},
				sb.WithIdempotencyKey("charge:"+req.GetOrderId())); err != nil {
				return nil, err
			}
			id, err := sb.PublishEvent(ctx, h.Client, "order.placed",
				&orderpb.OrderPlaced{OrderId: req.GetOrderId(), Total: req.GetTotal(), UserId: req.GetUserId()},
				sb.WithPartitionKey(req.GetOrderId()))
			if err != nil {
				return nil, err
			}
			return &orderpb.PlaceReply{EventId: id}, nil
		}); err != nil {
		t.Fatal(err)
	}
	if err := sbtest.Respond(h, "payment-svc", "Charge",
		func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
			return &paymentpb.ChargeReply{Ok: true, TransactionId: "tx-1"}, nil
		}); err != nil {
		t.Fatal(err)
	}
	started(t, h)

	if _, err := sbtest.Invoke[*orderpb.PlaceRequest, *orderpb.PlaceReply](
		context.Background(), h, "Place",
		&orderpb.PlaceRequest{OrderId: "o-1", UserId: "u-1", Total: 4200}); err != nil {
		t.Fatal(err)
	}

	calls := h.Calls()
	if len(calls) != 1 || calls[0].Service != "payment-svc" || calls[0].IdempotencyKey != "charge:o-1" {
		t.Fatalf("calls %+v", calls)
	}
	published := h.Published()
	if len(published) != 1 || published[0].Name != "order.placed" || published[0].PartitionKey != "o-1" {
		t.Fatalf("published %+v", published)
	}
	e, err := sbtest.DecodePublished[*orderpb.OrderPlaced](published[0])
	if err != nil || e.GetTotal() != 4200 {
		t.Fatalf("decoded %v, %v", e, err)
	}
}

func TestReceiptOnOrderPlaced(t *testing.T) {
	h := sbtest.New(t)
	var seen string
	if err := sb.SubscribeEvent(h.Client, "order.*",
		func(ctx context.Context, e *orderpb.OrderPlaced) error {
			seen = e.GetOrderId()
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	started(t, h)

	res, err := h.Deliver(context.Background(), "order.placed", &orderpb.OrderPlaced{OrderId: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Acked || seen != "o-1" {
		t.Fatalf("result %+v, handler saw %q", res, seen)
	}

	res, _ = h.Deliver(context.Background(), "billing.invoice", &orderpb.OrderPlaced{})
	if res.Acked || res.Reason != "no handler for matched patterns" {
		t.Fatalf("unmatched delivery: %+v", res)
	}
}
```

## Quick reference

| Task | Code |
|---|---|
| New harness | `h := sbtest.New(t)` |
| Declare a handler, subscription, dependency | ordinary API on `h.Client` |
| Start | `h.Start(ctx)` |
| Invoke a handler | `sbtest.Invoke[Req, Resp](ctx, h, "Method", req, opts...)` |
| Invoke a streaming handler | `sbtest.InvokeStream[Req, Chunk](ctx, h, "Method", req)` |
| Arrange an outbound answer | `sbtest.Respond(h, "svc", "Method", fn)` |
| Arrange an outbound stream | `sbtest.RespondStream(h, "svc", "Method", fn)` |
| Inspect outbound calls | `h.Calls()` · `sbtest.DecodeCall[T](rec)` |
| Inspect publications | `h.Published()` · `sbtest.DecodePublished[T](e)` |
| Deliver an event | `h.Deliver(ctx, "name", payload, opts...)` |
| Check a pattern | `sbtest.MatchPattern(pattern, name)` |
| Forget answers and records | `h.Reset()` |
