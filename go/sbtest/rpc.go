package sbtest

import (
	"context"
	"fmt"
	"sync"
)

// Handler is one registered unary RPC handler: the shape a service writes, with
// the request already decoded.
type Handler[Req, Res any] func(ctx context.Context, req Req) (Res, error)

// Responder computes the answer a doubled outbound call returns.
type Responder[Req, Res any] func(ctx context.Context, req Req) (Res, error)

// CallRecord is one outbound call the double intercepted, in the order it
// happened. Input is the request as the caller passed it, not a decoded copy.
type CallRecord struct {
	Service string
	Method  string
	Input   any
}

// RPC doubles both directions of unary RPC: the handlers this service exposes
// and the calls it makes to others.
type RPC struct {
	mu         sync.Mutex
	handlers   map[string]erasedFunc
	responders map[string]erasedFunc
	calls      []CallRecord
}

// NewRPC builds an empty RPC double.
func NewRPC() *RPC {
	return &RPC{
		handlers:   make(map[string]erasedFunc),
		responders: make(map[string]erasedFunc),
	}
}

// Handle registers the handler for one method name.
//
// A name that is already taken is refused rather than overwritten: the runtime
// refuses a duplicate declaration too, and a test that quietly loses its first
// registration passes for the wrong reason.
func Handle[Req, Res any](r *RPC, method string, fn Handler[Req, Res]) error {
	if r == nil {
		return fmt.Errorf("sbtest: handle %q: nil rpc double: %w", method, ErrInvalidArg)
	}
	if method == "" {
		return fmt.Errorf("sbtest: handle: empty method name: %w", ErrInvalidArg)
	}
	if fn == nil {
		return fmt.Errorf("sbtest: handle %q: nil handler: %w", method, ErrInvalidArg)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, taken := r.handlers[method]; taken {
		return fmt.Errorf("sbtest: handle %q: %w", method, ErrDuplicate)
	}
	r.handlers[method] = erase(fn)
	return nil
}

// Invoke runs the handler registered under method against req.
//
// The handler's own error comes back unwrapped, so a test asserts the business
// failure it wrote rather than a transport classification the real server would
// have put around it.
func Invoke[Req, Res any](ctx context.Context, r *RPC, method string, req Req) (Res, error) {
	var zero Res
	if r == nil {
		return zero, fmt.Errorf("sbtest: invoke %q: nil rpc double: %w", method, ErrInvalidArg)
	}

	r.mu.Lock()
	fn, ok := r.handlers[method]
	r.mu.Unlock()
	if !ok {
		return zero, fmt.Errorf("sbtest: invoke %q: %w: register it with sbtest.Handle first",
			method, ErrNoHandler)
	}

	out, err := fn(ctx, req)
	if err != nil {
		return zero, err
	}
	return cast[Res](fmt.Sprintf("invoke %q", method), out)
}

// Call doubles one outbound call. Every call is recorded, answered response or
// not, and the answer is whatever Respond configured for that service and
// method.
func Call[Req, Res any](ctx context.Context, r *RPC, service, method string, req Req) (Res, error) {
	var zero Res
	if r == nil {
		return zero, fmt.Errorf("sbtest: call %s/%s: nil rpc double: %w", service, method, ErrInvalidArg)
	}
	if service == "" || method == "" {
		return zero, fmt.Errorf("sbtest: call %s/%s: empty service or method name: %w",
			service, method, ErrInvalidArg)
	}

	r.mu.Lock()
	r.calls = append(r.calls, CallRecord{Service: service, Method: method, Input: req})
	responder, ok := r.responders[calleeKey(service, method)]
	r.mu.Unlock()

	if !ok {
		return zero, fmt.Errorf(
			"sbtest: call %s/%s: %w: configure it with sbtest.Respond(rpc, %q, %q, …) first",
			service, method, ErrNoResponse, service, method)
	}

	out, err := responder(ctx, req)
	if err != nil {
		return zero, err
	}
	return cast[Res](fmt.Sprintf("call %s/%s", service, method), out)
}

// Respond configures the answer Call returns for one service and method.
//
// Re-configuring replaces the previous answer, unlike Handle: arranging a
// different answer per case is what a test does, while two handlers under one
// name is a declaration mistake.
func Respond[Req, Res any](r *RPC, service, method string, fn Responder[Req, Res]) error {
	if r == nil {
		return fmt.Errorf("sbtest: respond %s/%s: nil rpc double: %w", service, method, ErrInvalidArg)
	}
	if service == "" || method == "" {
		return fmt.Errorf("sbtest: respond %s/%s: empty service or method name: %w",
			service, method, ErrInvalidArg)
	}
	if fn == nil {
		return fmt.Errorf("sbtest: respond %s/%s: nil responder: %w", service, method, ErrInvalidArg)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.responders[calleeKey(service, method)] = erase(fn)
	return nil
}

// RespondWith configures a fixed answer, for the calls whose response does not
// depend on the request.
func RespondWith[Res any](r *RPC, service, method string, res Res) error {
	return Respond(r, service, method, func(context.Context, any) (Res, error) {
		return res, nil
	})
}

// Calls returns the outbound calls in the order they happened.
func (r *RPC) Calls() []CallRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]CallRecord, len(r.calls))
	copy(out, r.calls)
	return out
}

// Reset clears handlers, configured answers and recorded calls.
func (r *RPC) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	clear(r.handlers)
	clear(r.responders)
	r.calls = nil
}

func calleeKey(service, method string) string { return service + "/" + method }
