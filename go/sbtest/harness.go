// Package sbtest is an in-memory double for the handlers a service registers
// with the ServiceBridge runtime. It runs them without a network, without a
// runtime and without local storage. See ./README.md for what it reproduces and
// what it deliberately does not.
package sbtest

import (
	"context"
	"errors"
	"fmt"
)

// Reasons the double refuses. They are sentinels rather than message text so a
// test can assert the refusal it means with errors.Is.
var (
	// ErrNoHandler marks an invocation of a name nothing was registered under.
	ErrNoHandler = errors.New("sbtest: nothing is registered under that name")
	// ErrNoResponse marks an outbound call with no configured answer. It is a
	// refusal and not a zero value: a forgotten Respond is a bug in the test, and
	// a silent zero hides it until an assertion far below fails for a reason that
	// no longer names the cause.
	ErrNoResponse = errors.New("sbtest: no response is configured for this call")
	// ErrTypeMismatch marks a value that does not match the type the handler was
	// registered with.
	ErrTypeMismatch = errors.New("sbtest: value does not match the registered type")
	// ErrDuplicate marks a name that is already taken.
	ErrDuplicate = errors.New("sbtest: name is already registered")
	// ErrInvalidArg marks an argument the double cannot work with — an empty
	// name, a nil function, a nil double.
	ErrInvalidArg = errors.New("sbtest: invalid argument")
)

// Harness bundles the two doubles a handler reaches for. Build one per test:
// registrations and recordings are held per instance, so parallel tests never
// see each other's.
type Harness struct {
	RPC   *RPC
	Event *Event
}

// New builds a fresh harness with empty doubles.
func New() *Harness {
	return &Harness{RPC: NewRPC(), Event: NewEvent()}
}

// Reset clears every registration and every recording in both doubles.
func (h *Harness) Reset() {
	h.RPC.Reset()
	h.Event.Reset()
}

// erasedFunc is the type-erased form of a typed handler or responder. The
// registries hold one shape so the delivery paths stay free of type parameters,
// exactly as internal/events does for its subscriptions.
type erasedFunc func(ctx context.Context, in any) (any, error)

// erase closes a typed function over the untyped registry. A value of the wrong
// type is named in the error instead of being coerced: coercion would run the
// handler against something the production decoder would never have produced.
func erase[In, Out any](fn func(context.Context, In) (Out, error)) erasedFunc {
	return func(ctx context.Context, in any) (any, error) {
		// A nil interface is the absence of an argument, not a wrong type: it is
		// what an untyped nil payload arrives as.
		if in == nil {
			var zero In
			return fn(ctx, zero)
		}
		typed, ok := in.(In)
		if !ok {
			var want In
			return nil, fmt.Errorf("sbtest: %w: got %T, want %T", ErrTypeMismatch, in, want)
		}
		return fn(ctx, typed)
	}
}

// cast reads the untyped result back as the type the caller asked for.
func cast[Out any](op string, out any) (Out, error) {
	var zero Out
	if out == nil {
		return zero, nil
	}
	typed, ok := out.(Out)
	if !ok {
		return zero, fmt.Errorf("sbtest: %s: %w: got %T, want %T", op, ErrTypeMismatch, out, zero)
	}
	return typed, nil
}
