package sbtest_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	sb "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/sbtest"
)

type (
	str = *wrapperspb.StringValue
	num = *wrapperspb.Int64Value
)

func started(t *testing.T, h *sbtest.Harness) {
	t.Helper()
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
}

func TestInvokeRunsTheHandlerThroughTheWireEncoding(t *testing.T) {
	h := sbtest.New(t)
	if err := sb.Handle(h.Client, "Double", func(_ context.Context, req num) (num, error) {
		return wrapperspb.Int64(req.GetValue() * 2), nil
	}); err != nil {
		t.Fatal(err)
	}
	started(t, h)

	res, err := sbtest.Invoke[num, num](context.Background(), h, "Double", wrapperspb.Int64(21))
	if err != nil || res.GetValue() != 42 {
		t.Fatalf("got %v, %v", res, err)
	}

	// The request travels as bytes: a handler bound to another type decodes
	// garbage or refuses, exactly as against a peer.
	_, err = sbtest.Invoke[str, num](context.Background(), h, "Missing", wrapperspb.String("x"))
	if !errors.Is(err, sb.ErrNotFound) {
		t.Fatalf("unknown method: %v, want NOT_FOUND", err)
	}
}

func TestHandlerFailuresArriveAsTheCallerSeesThem(t *testing.T) {
	h := sbtest.New(t)
	_ = sb.Handle(h.Client, "Business", func(context.Context, str) (str, error) {
		return nil, &sb.HandlerError{Code: "OUT_OF_STOCK", Message: "sku 42"}
	})
	_ = sb.Handle(h.Client, "Broken", func(context.Context, str) (str, error) {
		return nil, errors.New("database down")
	})
	_ = sb.Handle(h.Client, "Panics", func(context.Context, str) (str, error) {
		panic("boom")
	})
	started(t, h)

	_, err := sbtest.Invoke[str, str](context.Background(), h, "Business", wrapperspb.String("x"))
	var he *sb.HandlerError
	if !errors.Is(err, sb.ErrHandler) || !errors.As(err, &he) || he.Code != "OUT_OF_STOCK" || he.Message != "sku 42" {
		t.Fatalf("business failure: %v", err)
	}
	for _, method := range []string{"Broken", "Panics"} {
		_, err = sbtest.Invoke[str, str](context.Background(), h, method, wrapperspb.String("x"))
		if !errors.As(err, &he) || he.Code != "INTERNAL" {
			t.Fatalf("%s: %v, want error code INTERNAL", method, err)
		}
	}
}

func TestHandlerSeesCallInfoAndCancellation(t *testing.T) {
	h := sbtest.New(t)
	seen := make(chan sb.CallInfo, 1)
	_ = sb.Handle(h.Client, "Who", func(ctx context.Context, _ str) (str, error) {
		info, _ := sb.CallInfoFromContext(ctx)
		seen <- info
		<-ctx.Done()
		return nil, ctx.Err()
	})
	started(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := sbtest.Invoke[str, str](ctx, h, "Who", wrapperspb.String("x"),
		sbtest.WithCaller("svc-a", "inst-a"), sbtest.WithRequestID("req-1"), sbtest.WithIdempotencyKey("k1"))
	if err == nil {
		t.Fatal("a handler cancelled with its caller must fail")
	}
	info := <-seen
	if info.CallerServiceID != "svc-a" || info.CallerInstanceID != "inst-a" || info.RequestID != "req-1" ||
		info.IdempotencyKey != "k1" || info.Deadline.IsZero() {
		t.Fatalf("call info %+v", info)
	}
}

func TestOutboundCallsAreAnsweredAndRecorded(t *testing.T) {
	h := sbtest.New(t)
	billing := sb.NewClient(h.Client, "billing")
	charge, err := sb.NewMethod[num, str](billing, "Charge")
	if err != nil {
		t.Fatal(err)
	}
	_ = sb.Handle(h.Client, "Checkout", func(ctx context.Context, req num) (str, error) {
		return charge.Call(ctx, req, sb.WithIdempotencyKey("order-1"))
	})
	started(t, h)

	if _, err := sbtest.Invoke[num, str](context.Background(), h, "Checkout", wrapperspb.Int64(5)); !errors.Is(err, sbtest.ErrNoResponse) && !strings.Contains(err.Error(), "no response arranged") {
		t.Fatalf("a forgotten Respond must fail loudly: %v", err)
	}

	if err := sbtest.Respond(h, "billing", "Charge", func(_ context.Context, req num) (str, error) {
		if req.GetValue() > 100 {
			return nil, &sb.HandlerError{Code: "LIMIT", Message: "too much"}
		}
		return wrapperspb.String("ok"), nil
	}); err != nil {
		t.Fatal(err)
	}
	res, err := sbtest.Invoke[num, str](context.Background(), h, "Checkout", wrapperspb.Int64(5))
	if err != nil || res.GetValue() != "ok" {
		t.Fatalf("got %v, %v", res, err)
	}

	// The downstream business code is the handler's failure here, not its own
	// decision: it answers INTERNAL rather than impersonating LIMIT.
	_, err = sbtest.Invoke[num, str](context.Background(), h, "Checkout", wrapperspb.Int64(500))
	var he *sb.HandlerError
	if !errors.As(err, &he) || he.Code != "INTERNAL" || !strings.Contains(he.Message, "LIMIT") {
		t.Fatalf("propagated downstream failure: %v", err)
	}

	calls := h.Calls()
	if len(calls) != 3 || calls[1].Service != "billing" || calls[1].Method != "Charge" || calls[1].IdempotencyKey != "order-1" || calls[1].Transport != sb.TransportAuto {
		t.Fatalf("calls %+v", calls)
	}
	if req, err := sbtest.DecodeCall[num](calls[1]); err != nil || req.GetValue() != 5 {
		t.Fatalf("recorded request %v, %v", req, err)
	}
}

func TestStreamsBothWays(t *testing.T) {
	h := sbtest.New(t)
	_ = sb.HandleStream(h.Client, "Count", func(_ context.Context, req num, send func(num) error) error {
		for i := int64(1); i <= req.GetValue(); i++ {
			if err := send(wrapperspb.Int64(i)); err != nil {
				return err
			}
		}
		return nil
	})
	_ = sb.Handle(h.Client, "Sum", func(ctx context.Context, req num) (num, error) {
		var total int64
		for chunk, err := range sb.Stream[num, num](ctx, h.Client, "numbers", "Range", req) {
			if err != nil {
				return nil, err
			}
			total += chunk.GetValue()
		}
		return wrapperspb.Int64(total), nil
	})
	_ = sbtest.RespondStream(h, "numbers", "Range", func(_ context.Context, req num) ([]num, error) {
		return []num{wrapperspb.Int64(req.GetValue()), wrapperspb.Int64(req.GetValue() + 1)}, nil
	})
	started(t, h)

	chunks, err := sbtest.InvokeStream[num, num](context.Background(), h, "Count", wrapperspb.Int64(3))
	if err != nil || len(chunks) != 3 || chunks[2].GetValue() != 3 {
		t.Fatalf("stream: %v %v", chunks, err)
	}
	sum, err := sbtest.Invoke[num, num](context.Background(), h, "Sum", wrapperspb.Int64(10))
	if err != nil || sum.GetValue() != 21 {
		t.Fatalf("sum: %v %v", sum, err)
	}
}

func TestPublishGoesThroughTheRealQueue(t *testing.T) {
	h := sbtest.New(t)
	_ = sb.Handle(h.Client, "Place", func(ctx context.Context, req str) (str, error) {
		id, err := sb.PublishEvent(ctx, h.Client, "order.placed", req, sb.WithPartitionKey(req.GetValue()))
		if err != nil {
			return nil, err
		}
		return wrapperspb.String(id), nil
	})
	started(t, h)

	res, err := sbtest.Invoke[str, str](context.Background(), h, "Place", wrapperspb.String("o-1"))
	if err != nil {
		t.Fatal(err)
	}
	published := h.Published()
	if len(published) != 1 || published[0].ID != res.GetValue() || published[0].Name != "order.placed" || published[0].PartitionKey != "o-1" {
		t.Fatalf("published %+v", published)
	}
	if v, err := sbtest.DecodePublished[str](published[0]); err != nil || v.GetValue() != "o-1" {
		t.Fatalf("decoded %v %v", v, err)
	}
	if len(published[0].PayloadJSON) == 0 {
		t.Fatal("payload_json must be filled")
	}
	if _, err := sb.PublishEvent(context.Background(), h.Client, "Bad Name", wrapperspb.String("x")); !errors.Is(err, sb.ErrInvalidEventName) {
		t.Fatalf("invalid name: %v", err)
	}
}

func TestDeliveriesRouteByMatchedPatterns(t *testing.T) {
	h := sbtest.New(t)
	var ran []string
	_ = sb.SubscribeEvent(h.Client, "order.*", func(ctx context.Context, e str) error {
		info, _ := sb.DeliveryFromContext(ctx)
		ran = append(ran, "order.*:"+info.EventName+":"+e.GetValue())
		return nil
	})
	_ = sb.SubscribeEvent(h.Client, "order.created", func(context.Context, str) error {
		ran = append(ran, "order.created")
		return nil
	})
	_ = sb.SubscribeEvent(h.Client, "billing.#", func(context.Context, str) error {
		return errors.New("billing is down")
	})
	if err := sb.SubscribeEvent(h.Client, "order.*", func(context.Context, str) error { return nil }); !errors.Is(err, sb.ErrValidation) {
		t.Fatalf("second handler for a pattern: %v, want VALIDATION", err)
	}
	started(t, h)

	res, err := h.Deliver(context.Background(), "order.created", wrapperspb.String("o-1"))
	if err != nil || !res.Acked || len(ran) != 2 || len(res.MatchedPatterns) != 2 {
		t.Fatalf("result %+v ran %v err %v", res, ran, err)
	}
	if ran[0] != "order.*:order.created:o-1" && ran[1] != "order.*:order.created:o-1" {
		t.Fatalf("handler did not see the delivery: %v", ran)
	}

	res, _ = h.Deliver(context.Background(), "billing.eu.invoice", wrapperspb.String("x"))
	if res.Acked || !strings.Contains(res.Reason, "billing is down") {
		t.Fatalf("failing handler: %+v", res)
	}
	res, _ = h.Deliver(context.Background(), "unrelated.event", wrapperspb.String("x"))
	if res.Acked || res.Reason != "no handler for matched patterns" {
		t.Fatalf("no matching subscription: %+v", res)
	}
	// What a filter on the runtime decided is reproduced by naming the patterns.
	ran = nil
	res, _ = h.Deliver(context.Background(), "order.created", wrapperspb.String("o-2"), sbtest.WithMatchedPatterns("order.created"))
	if !res.Acked || len(ran) != 1 || ran[0] != "order.created" {
		t.Fatalf("explicit patterns: %+v %v", res, ran)
	}
}

func TestDeclaringAfterStartIsRefused(t *testing.T) {
	h := sbtest.New(t)
	started(t, h)
	if err := sb.Handle(h.Client, "Late", func(context.Context, str) (str, error) { return nil, nil }); !errors.Is(err, sb.ErrState) {
		t.Fatalf("late declaration: %v", err)
	}
	if err := sbtest.Respond[str, str](nil, "a", "b", nil); !errors.Is(err, sbtest.ErrInvalidArg) {
		t.Fatalf("invalid respond: %v", err)
	}
	h.Reset()
	if len(h.Calls()) != 0 || len(h.Published()) != 0 {
		t.Fatal("reset left records")
	}
}

func TestMatchPatternFollowsTheRuntimeRules(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"order.created", "order.created", true},
		{"order.*", "order.created", true},
		{"order.*", "order.eu.created", false},
		{"order.#", "order", true},
		{"order.#", "order.eu.created", true},
		{"#", "anything.at.all", true},
		{"*.created", "order.created", true},
		{"*", "a.b", false},
	}
	for _, c := range cases {
		if got := sbtest.MatchPattern(c.pattern, c.name); got != c.want {
			t.Errorf("MatchPattern(%q, %q) = %v", c.pattern, c.name, got)
		}
	}
}
