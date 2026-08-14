package sbtest

import (
	"context"
	"errors"
	"testing"
)

type orderPlaced struct {
	OrderID string
	Total   int64
}

func TestPublishReachesEverySubscriberOfTheName(t *testing.T) {
	t.Parallel()
	h := New()
	if err := Define[orderPlaced](h.Event, "order.placed"); err != nil {
		t.Fatalf("define: %v", err)
	}

	var seen []string
	for _, tag := range []string{"first", "second"} {
		if err := Subscribe(h.Event, "order.placed", func(_ context.Context, e orderPlaced) error {
			seen = append(seen, tag+":"+e.OrderID)
			return nil
		}); err != nil {
			t.Fatalf("subscribe %s: %v", tag, err)
		}
	}

	got, err := Publish(context.Background(), h.Event, "order.placed", orderPlaced{OrderID: "o-1", Total: 42})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !got.Acked {
		t.Fatalf("delivery not acked: %+v", got)
	}
	if len(seen) != 2 || seen[0] != "first:o-1" || seen[1] != "second:o-1" {
		t.Fatalf("fan-out: got %v", seen)
	}
}

// A delivery nobody handles is spent, not failed: routing belongs to the
// runtime, and nacking here would have it redeliver forever.
func TestPublishWithNoSubscriberIsAcked(t *testing.T) {
	t.Parallel()
	h := New()
	if err := Define[orderPlaced](h.Event, "order.placed"); err != nil {
		t.Fatalf("define: %v", err)
	}

	got, err := Publish(context.Background(), h.Event, "order.placed", orderPlaced{OrderID: "o-2"})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !got.Acked || got.Err != nil {
		t.Fatalf("want acked with no error, got %+v", got)
	}
}

func TestHandlerFailureNacksTheDeliveryAndStopsTheFanOut(t *testing.T) {
	t.Parallel()
	h := New()
	if err := Define[orderPlaced](h.Event, "order.placed"); err != nil {
		t.Fatalf("define: %v", err)
	}

	boom := errors.New("handler exploded")
	if err := Subscribe(h.Event, "order.placed", func(context.Context, orderPlaced) error {
		return boom
	}); err != nil {
		t.Fatalf("subscribe failing: %v", err)
	}
	reached := false
	if err := Subscribe(h.Event, "order.placed", func(context.Context, orderPlaced) error {
		reached = true
		return nil
	}); err != nil {
		t.Fatalf("subscribe second: %v", err)
	}

	got, err := Publish(context.Background(), h.Event, "order.placed", orderPlaced{OrderID: "o-3"})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got.Acked {
		t.Fatal("a failing handler must nack the delivery")
	}
	if !errors.Is(got.Err, boom) {
		t.Fatalf("want the handler error verbatim, got %v", got.Err)
	}
	// The runtime nacks the whole delivery, so the handlers after the failing
	// one never run in production either.
	if reached {
		t.Fatal("fan-out continued past a failing handler")
	}
}

// Publishing a name that was never declared is a bug in the test, and the
// runtime rejects it too — so it must refuse rather than record silently.
func TestPublishRefusesAnUndeclaredEvent(t *testing.T) {
	t.Parallel()
	h := New()

	_, err := Publish(context.Background(), h.Event, "never.declared", orderPlaced{})
	if !errors.Is(err, ErrNoHandler) {
		t.Fatalf("want ErrNoHandler, got %v", err)
	}
	if len(h.Event.Published()) != 0 {
		t.Fatal("a refused publish must not be recorded")
	}
}

func TestSubscribeRejectsAMismatchedPayload(t *testing.T) {
	t.Parallel()
	h := New()
	if err := Define[orderPlaced](h.Event, "order.placed"); err != nil {
		t.Fatalf("define: %v", err)
	}
	if err := Subscribe(h.Event, "order.placed", func(context.Context, orderPlaced) error {
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Coercing here would run the handler against something the production
	// decoder could never have produced.
	got, err := Publish(context.Background(), h.Event, "order.placed", "not an order")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got.Acked || !errors.Is(got.Err, ErrTypeMismatch) {
		t.Fatalf("want a type mismatch nack, got %+v", got)
	}
}

func TestRecordingsAndReset(t *testing.T) {
	t.Parallel()
	h := New()
	if err := Define[orderPlaced](h.Event, "order.placed"); err != nil {
		t.Fatalf("define: %v", err)
	}
	for _, id := range []string{"o-1", "o-2"} {
		if _, err := Publish(context.Background(), h.Event, "order.placed", orderPlaced{OrderID: id}); err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
	}

	published := h.Event.Published()
	if len(published) != 2 || published[0].Payload.(orderPlaced).OrderID != "o-1" {
		t.Fatalf("published, oldest first: %+v", published)
	}
	if len(h.Event.Deliveries()) != 2 {
		t.Fatalf("deliveries: %+v", h.Event.Deliveries())
	}

	h.Reset()
	if len(h.Event.Published()) != 0 || len(h.Event.Deliveries()) != 0 {
		t.Fatal("reset must clear the recordings")
	}
	// Reset drops declarations too, so a publish after it is refused again.
	if _, err := Publish(context.Background(), h.Event, "order.placed", orderPlaced{}); !errors.Is(err, ErrNoHandler) {
		t.Fatalf("want ErrNoHandler after reset, got %v", err)
	}
}

func TestDefineRefusesADuplicate(t *testing.T) {
	t.Parallel()
	h := New()
	if err := Define[orderPlaced](h.Event, "order.placed"); err != nil {
		t.Fatalf("first define: %v", err)
	}
	if err := Define[orderPlaced](h.Event, "order.placed"); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
}
