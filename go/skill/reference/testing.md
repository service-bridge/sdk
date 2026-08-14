# Testing — Go SDK reference

`sbtest` runs your handlers with no network, no runtime and no local storage.

**Read the limits first.** A green `sbtest` run does not mean working production.

## What the double does NOT reproduce

- runtime routing, including event pattern matching
- access policy
- leases, epochs and fencing
- retries, backoff and circuit breakers
- streaming
- workflows
- idempotency and deduplication
- partition-key ordering

A double pretending to be a runtime is worse than no double: the test goes green where production fails. All of the above is only verified end-to-end against a live runtime. Use `sbtest` for the domain logic inside a handler.

## Signatures

```go signature
func New() *Harness
func (h *Harness) Reset()
func NewRPC() *RPC
func NewEvent() *Event

func Handle[Req, Res any](r *RPC, method string, fn Handler[Req, Res]) error
func Invoke[Req, Res any](ctx context.Context, r *RPC, method string, req Req) (Res, error)
func Respond[Req, Res any](r *RPC, service, method string, fn Responder[Req, Res]) error
func RespondWith[Res any](r *RPC, service, method string, res Res) error
func Call[Req, Res any](ctx context.Context, r *RPC, service, method string, req Req) (Res, error)
func (r *RPC) Calls() []CallRecord
func (r *RPC) Reset()

func Define[T any](e *Event, name string) error
func Subscribe[T any](e *Event, name string, fn Subscriber[T]) error
func Publish[T any](ctx context.Context, e *Event, name string, payload T) (Delivery, error)
func (e *Event) Published() []PublishRecord
func (e *Event) Deliveries() []Delivery
func (e *Event) Reset()
```

```go signature
type CallRecord struct {
	Service string
	Method  string
	Input   any
}

type Delivery struct {
	Name  string
	Acked bool
	Err   error
}
```

`h.RPC` and `h.Event` are the two doubles. Build one harness per test — registrations and recordings live on the instance, so parallel tests never see each other's.

## Behaviour that differs from a naive mock

| Rule | Why |
|---|---|
| `Handle` **refuses** a taken name (`ErrDuplicate`) | The runtime refuses a duplicate declaration too; a test that silently loses its first registration passes for the wrong reason. |
| `Respond` **replaces** a previous answer | Arranging a different answer per case is what a test does. |
| `Call` with no configured answer returns `ErrNoResponse` | A forgotten `Respond` is a bug in the test; a silent zero hides it until an assertion far below fails for an unrelated-looking reason. |
| A wrong-typed value is named (`ErrTypeMismatch`), never coerced | Coercion would run the handler against something the real decoder would never produce. |
| `Publish` of an undeclared name is refused | The runtime rejects a publish whose event was never registered. |
| A delivery nobody handled is **acked** | Routing belongs to the runtime; nacking would make it redeliver forever. |
| The first failing handler decides the whole delivery | The runtime nacks the delivery, so later handlers do not run in production either. |
| The handler's own error comes back **unwrapped** | The test asserts the business failure it wrote, not a transport classification. |

## Write the handler as a plain function

Keep the handler separate from its registration: production registers it on the client, the test registers it on the double, and the code under test is the code that ships.

```go
package orders

import (
	"context"
	"errors"

	"example.com/orders/paymentpb"
)

var ErrNonPositiveAmount = errors.New("amount must be positive")

type Ledger interface {
	Debit(ctx context.Context, user string, amount int64) error
}

func NewChargeHandler(ledger Ledger) func(context.Context, *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
	return func(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
		if req.GetAmount() <= 0 {
			return nil, ErrNonPositiveAmount
		}
		if err := ledger.Debit(ctx, req.GetUserId(), req.GetAmount()); err != nil {
			return nil, err
		}
		return &paymentpb.ChargeReply{Ok: true, TransactionId: "tx-" + req.GetUserId()}, nil
	}
}
```

Production wiring: `sb.Handle(c, "Charge", orders.NewChargeHandler(ledger))`.

## Complete test file

```go
package orders_test

import (
	"context"
	"errors"
	"testing"

	"example.com/orders/orderpb"
	"example.com/orders/paymentpb"
	"github.com/service-bridge/sdk/go/sbtest"
)

var errNonPositiveAmount = errors.New("amount must be positive")

func chargeHandler(ctx context.Context, req *paymentpb.ChargeRequest) (*paymentpb.ChargeReply, error) {
	if req.GetAmount() <= 0 {
		return nil, errNonPositiveAmount
	}
	return &paymentpb.ChargeReply{Ok: true, TransactionId: "tx-" + req.GetUserId()}, nil
}

func TestChargeAccepts(t *testing.T) {
	h := sbtest.New()
	if err := sbtest.Handle(h.RPC, "Charge", chargeHandler); err != nil {
		t.Fatal(err)
	}

	res, err := sbtest.Invoke[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h.RPC, "Charge",
		&paymentpb.ChargeRequest{UserId: "u-1", Amount: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetOk() || res.GetTransactionId() != "tx-u-1" {
		t.Fatalf("unexpected reply: %+v", res)
	}
}

func TestChargeRejectsZero(t *testing.T) {
	h := sbtest.New()
	if err := sbtest.Handle(h.RPC, "Charge", chargeHandler); err != nil {
		t.Fatal(err)
	}

	// The handler's error arrives unwrapped.
	_, err := sbtest.Invoke[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h.RPC, "Charge", &paymentpb.ChargeRequest{Amount: 0})
	if !errors.Is(err, errNonPositiveAmount) {
		t.Fatalf("want errNonPositiveAmount, got %v", err)
	}
}

func TestPlaceOrderCalls(t *testing.T) {
	h := sbtest.New()

	// Fixed answer.
	if err := sbtest.RespondWith(h.RPC, "payment-svc", "Charge",
		&paymentpb.ChargeReply{Ok: true, TransactionId: "tx-1"}); err != nil {
		t.Fatal(err)
	}
	// Or an answer computed from the request.
	if err := sbtest.Respond(h.RPC, "inventory-svc", "Reserve",
		func(ctx context.Context, req *orderpb.ShipRequest) (*orderpb.ShipReply, error) {
			return &orderpb.ShipReply{Ok: req.GetOrderId() != ""}, nil
		}); err != nil {
		t.Fatal(err)
	}

	if _, err := sbtest.Call[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h.RPC, "payment-svc", "Charge",
		&paymentpb.ChargeRequest{UserId: "u-1", Amount: 100}); err != nil {
		t.Fatal(err)
	}

	calls := h.RPC.Calls()
	if len(calls) != 1 || calls[0].Service != "payment-svc" || calls[0].Method != "Charge" {
		t.Fatalf("unexpected calls: %+v", calls)
	}

	// An unconfigured call is a refusal, not a zero value.
	if _, err := sbtest.Call[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](
		context.Background(), h.RPC, "payment-svc", "Refund",
		&paymentpb.ChargeRequest{}); !errors.Is(err, sbtest.ErrNoResponse) {
		t.Fatalf("want ErrNoResponse, got %v", err)
	}
}

func TestOrderPlacedFansOut(t *testing.T) {
	h := sbtest.New()

	// Define is mandatory before Publish.
	if err := sbtest.Define[*orderpb.OrderPlaced](h.Event, "order.placed"); err != nil {
		t.Fatal(err)
	}

	var seen string
	if err := sbtest.Subscribe(h.Event, "order.placed",
		func(ctx context.Context, e *orderpb.OrderPlaced) error {
			seen = e.GetOrderId()
			return nil
		}); err != nil {
		t.Fatal(err)
	}

	delivery, err := sbtest.Publish(context.Background(), h.Event, "order.placed",
		&orderpb.OrderPlaced{OrderId: "o-1", Total: 4200})
	if err != nil {
		t.Fatal(err)
	}
	if !delivery.Acked {
		t.Fatalf("delivery nacked: %v", delivery.Err)
	}
	if seen != "o-1" {
		t.Fatalf("handler saw %q", seen)
	}
	if got := h.Event.Published(); len(got) != 1 || got[0].Name != "order.placed" {
		t.Fatalf("unexpected publications: %+v", got)
	}
}

func TestNackedDelivery(t *testing.T) {
	h := sbtest.New()
	if err := sbtest.Define[*orderpb.OrderPlaced](h.Event, "order.placed"); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	if err := sbtest.Subscribe(h.Event, "order.placed",
		func(ctx context.Context, e *orderpb.OrderPlaced) error { return boom }); err != nil {
		t.Fatal(err)
	}

	delivery, err := sbtest.Publish(context.Background(), h.Event, "order.placed",
		&orderpb.OrderPlaced{OrderId: "o-2"})
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Acked || !errors.Is(delivery.Err, boom) {
		t.Fatalf("expected a nack carrying boom, got %+v", delivery)
	}
}
```

## Sentinels

`sbtest.ErrNoHandler`, `ErrNoResponse`, `ErrTypeMismatch`, `ErrDuplicate`, `ErrInvalidArg` — matched with `errors.Is`.

## Gotchas

- `sbtest` has no client, no `Start` and no lifecycle. Do not try to point it at a runtime.
- `Subscribe` on the double matches the **exact** name only — patterns are not reproduced.
- Nothing in `sbtest` exercises streaming, workflows or jobs; cover those end-to-end.
