package sbtest

import (
	"context"
	"fmt"
	"sync"
)

// Subscriber is one registered event handler, with the payload already decoded.
type Subscriber[T any] func(ctx context.Context, event T) error

// PublishRecord is one published event, in the order it happened. Payload is the
// value as the publisher passed it, not a decoded copy.
type PublishRecord struct {
	Name    string
	Payload any
}

// Delivery is what one Publish did: which subscribers saw the event and how each
// answered. Acked is false when the handler returned an error — the runtime
// nacks such a delivery and redelivers it later.
type Delivery struct {
	Name  string
	Acked bool
	Err   error
}

// Event doubles both directions of durable events: what this service publishes
// and what it subscribes to.
type Event struct {
	mu       sync.Mutex
	handlers map[string][]erasedFunc
	// defined records the names declared with Define. Publishing an undeclared
	// name is refused, because the runtime rejects a publish whose event was
	// never registered and a test that skips the declaration would pass against
	// a shape production never accepts.
	defined    map[string]struct{}
	published  []PublishRecord
	deliveries []Delivery
}

// NewEvent builds an empty event double.
func NewEvent() *Event {
	return &Event{
		handlers: make(map[string][]erasedFunc),
		defined:  make(map[string]struct{}),
	}
}

// Define declares an event this service publishes.
func Define[T any](e *Event, name string) error {
	if e == nil {
		return fmt.Errorf("sbtest: define %q: nil event double: %w", name, ErrInvalidArg)
	}
	if name == "" {
		return fmt.Errorf("sbtest: define: empty event name: %w", ErrInvalidArg)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if _, taken := e.defined[name]; taken {
		return fmt.Errorf("sbtest: define %q: %w", name, ErrDuplicate)
	}
	e.defined[name] = struct{}{}
	return nil
}

// Subscribe registers a handler for an exact event name.
//
// Several handlers may share one name — the runtime fans a delivery out to all
// of them, and Publish reproduces that.
func Subscribe[T any](e *Event, name string, fn Subscriber[T]) error {
	if e == nil {
		return fmt.Errorf("sbtest: subscribe %q: nil event double: %w", name, ErrInvalidArg)
	}
	if name == "" {
		return fmt.Errorf("sbtest: subscribe: empty event name: %w", ErrInvalidArg)
	}
	if fn == nil {
		return fmt.Errorf("sbtest: subscribe %q: nil handler: %w", name, ErrInvalidArg)
	}

	wrapped := erase(func(ctx context.Context, event T) (any, error) {
		return nil, fn(ctx, event)
	})

	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[name] = append(e.handlers[name], wrapped)
	return nil
}

// Publish records the event and delivers it to every subscriber of that exact
// name.
//
// Matching is by exact name only. Wildcard routing belongs to the runtime, so a
// double that matched patterns locally would answer a question production never
// asks of the SDK.
func Publish[T any](ctx context.Context, e *Event, name string, payload T) (Delivery, error) {
	if e == nil {
		return Delivery{}, fmt.Errorf("sbtest: publish %q: nil event double: %w", name, ErrInvalidArg)
	}
	if name == "" {
		return Delivery{}, fmt.Errorf("sbtest: publish: empty event name: %w", ErrInvalidArg)
	}

	e.mu.Lock()
	if _, declared := e.defined[name]; !declared {
		e.mu.Unlock()
		return Delivery{}, fmt.Errorf("sbtest: publish %q: %w — declare it with Define first", name, ErrNoHandler)
	}
	e.published = append(e.published, PublishRecord{Name: name, Payload: payload})
	handlers := make([]erasedFunc, len(e.handlers[name]))
	copy(handlers, e.handlers[name])
	e.mu.Unlock()

	// No subscriber for the name is an ack, not a failure: routing is the
	// runtime's, so a delivery reaching a service that handles nothing under
	// that name is spent, and nacking it would have the runtime redeliver
	// forever.
	result := Delivery{Name: name, Acked: true}
	for _, h := range handlers {
		// The first failing handler decides the delivery: the runtime nacks the
		// whole delivery, so the remaining handlers of a nacked event never run
		// in production either.
		if _, err := h(ctx, any(payload)); err != nil {
			result.Acked = false
			result.Err = err
			break
		}
	}

	e.mu.Lock()
	e.deliveries = append(e.deliveries, result)
	e.mu.Unlock()
	return result, nil
}

// Published returns every event published so far, oldest first.
func (e *Event) Published() []PublishRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]PublishRecord, len(e.published))
	copy(out, e.published)
	return out
}

// Deliveries returns the outcome of every publish so far, oldest first.
func (e *Event) Deliveries() []Delivery {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Delivery, len(e.deliveries))
	copy(out, e.deliveries)
	return out
}

// Reset clears every declaration, subscription and recording.
func (e *Event) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers = make(map[string][]erasedFunc)
	e.defined = make(map[string]struct{})
	e.published = nil
	e.deliveries = nil
}
