package rpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
)

// errorCodeInternal is the only error_code the dispatcher puts in a response
// body. A handler failure is an answer, not a fault of the wire, and the caller
// reads it under a gRPC OK status.
const errorCodeInternal = "INTERNAL"

// Dispatch failures. Each one picks a wire shape, so they are sentinels rather
// than free-form strings.
var (
	ErrNoHandler   = errors.New("no handler registered for the method")
	ErrWrongKind   = errors.New("method is registered under the other call kind")
	ErrSealed      = errors.New("handlers are registered before the server starts")
	ErrDuplicate   = errors.New("method already has a handler")
	ErrEmptyMethod = errors.New("method name must not be empty")
	ErrNoFunc      = errors.New("handler function is nil")
	ErrPanic       = errors.New("handler panicked")

	// ErrDecode marks a payload the handler could not decode. It is the one
	// handler failure caused by the CALLER, so it travels as a gRPC
	// InvalidArgument instead of an in-body error. The layer that owns the codec
	// wraps it around whatever the codec returned.
	ErrDecode = errors.New("payload decode failed")
)

// UnaryFunc handles one unary call: raw request payload in, raw response
// payload out. Serialization lives above this boundary; a codec failure comes
// back wrapping ErrDecode.
type UnaryFunc func(ctx context.Context, payload []byte) ([]byte, error)

// Sender writes one chunk to the caller. It blocks while the caller is behind —
// that is the backpressure — and fails once the caller is gone. A handler stops
// producing on the first error.
type Sender func(chunk []byte) error

// StreamFunc handles one server-streaming call. It must return as soon as ctx
// is done: nothing else stops a producer whose caller walked away.
type StreamFunc func(ctx context.Context, payload []byte, send Sender) error

// Outcome is the wire shape one dispatch takes.
//
// Status and ErrorCode are deliberately separate axes. A non-OK Status is a
// transport-level refusal the caller may retry elsewhere; ErrorCode under a
// gRPC OK is the callee's answer and retrying it only repeats the failure.
// Collapsing the two turns every business error into a retry storm.
type Outcome struct {
	Payload       []byte
	Status        codes.Code
	StatusMessage string
	ErrorCode     string
	ErrorMessage  string
}

// MethodInfo describes one registered handler. The registration layer declares
// the same set to the runtime.
type MethodInfo struct {
	Name      string
	Streaming bool
}

type method struct {
	name      string
	streaming bool
	unary     UnaryFunc
	stream    StreamFunc
}

// Dispatcher is the handler index behind the inbound Call server.
//
// Lookup is a single map hit on the call path. The index is replaced whole on
// every registration and read through an atomic pointer, so an incoming call
// takes no lock at all; registrations are rare and pay the copy.
type Dispatcher struct {
	log *slog.Logger

	// mu serialises registration only. Reads go through index.
	mu     sync.Mutex
	sealed bool
	index  atomic.Pointer[map[string]*method]
}

// NewDispatcher builds an empty handler index.
func NewDispatcher(log *slog.Logger) *Dispatcher {
	if log == nil {
		log = slog.Default()
	}
	d := &Dispatcher{log: log}
	empty := make(map[string]*method)
	d.index.Store(&empty)
	return d
}

// RegisterUnary adds a unary handler under name.
func (d *Dispatcher) RegisterUnary(name string, fn UnaryFunc) error {
	if fn == nil {
		return fmt.Errorf("rpc: register handler %q: %w", name, ErrNoFunc)
	}
	return d.register(&method{name: name, unary: fn})
}

// RegisterStream adds a server-streaming handler under name.
func (d *Dispatcher) RegisterStream(name string, fn StreamFunc) error {
	if fn == nil {
		return fmt.Errorf("rpc: register handler %q: %w", name, ErrNoFunc)
	}
	return d.register(&method{name: name, streaming: true, stream: fn})
}

func (d *Dispatcher) register(m *method) error {
	const op = "rpc: register handler"

	if m.name == "" {
		return fmt.Errorf("%s: %w", op, ErrEmptyMethod)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.sealed {
		return fmt.Errorf("%s %q: %w", op, m.name, ErrSealed)
	}
	cur := *d.index.Load()
	if _, exists := cur[m.name]; exists {
		return fmt.Errorf("%s %q: %w", op, m.name, ErrDuplicate)
	}

	next := make(map[string]*method, len(cur)+1)
	for k, v := range cur {
		next[k] = v
	}
	next[m.name] = m
	d.index.Store(&next)
	return nil
}

// Seal closes registration. The declared handler set travels to the runtime in
// the first RegisterRequest; a handler added after that exists locally and
// nowhere else, so the mesh would never route to it and the call would fail on
// the caller's side with no local trace of why.
func (d *Dispatcher) Seal() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sealed = true
}

// Sealed reports whether registration is closed.
func (d *Dispatcher) Sealed() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sealed
}

// Methods lists the registered handlers, ordered by name so the declaration
// sent to the runtime does not reshuffle between runs.
func (d *Dispatcher) Methods() []MethodInfo {
	cur := *d.index.Load()
	out := make([]MethodInfo, 0, len(cur))
	for _, m := range cur {
		out = append(out, MethodInfo{Name: m.name, Streaming: m.streaming})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (d *Dispatcher) lookup(name string) (*method, bool) {
	m, ok := (*d.index.Load())[name]
	return m, ok
}

// Unary runs the unary handler registered under name.
func (d *Dispatcher) Unary(ctx context.Context, name string, payload []byte) Outcome {
	m, ok := d.lookup(name)
	if !ok {
		return Outcome{
			Status:        codes.NotFound,
			StatusMessage: fmt.Sprintf("rpc: unary %s: %s", name, ErrNoHandler),
		}
	}
	if m.streaming {
		return Outcome{
			Status:        codes.FailedPrecondition,
			StatusMessage: fmt.Sprintf("rpc: unary %s: %s: it is a streaming method", name, ErrWrongKind),
		}
	}

	out, err := d.invokeUnary(ctx, m, payload)
	if err != nil {
		return d.failure(name, err)
	}
	return Outcome{Status: codes.OK, Payload: out}
}

// Stream runs the streaming handler registered under name, feeding chunks to
// send as the handler produces them.
func (d *Dispatcher) Stream(ctx context.Context, name string, payload []byte, send Sender) Outcome {
	m, ok := d.lookup(name)
	if !ok {
		return Outcome{
			Status:        codes.NotFound,
			StatusMessage: fmt.Sprintf("rpc: stream %s: %s", name, ErrNoHandler),
		}
	}
	if !m.streaming {
		return Outcome{
			Status:        codes.FailedPrecondition,
			StatusMessage: fmt.Sprintf("rpc: stream %s: %s: it is a unary method", name, ErrWrongKind),
		}
	}

	if err := d.invokeStream(ctx, m, payload, send); err != nil {
		return d.failure(name, err)
	}
	return Outcome{Status: codes.OK}
}

// failure maps a handler error onto the wire. A decode failure is the caller's
// mistake and travels as a status; everything else is the callee's answer and
// travels in the body.
func (d *Dispatcher) failure(name string, err error) Outcome {
	if errors.Is(err, ErrDecode) {
		return Outcome{
			Status:        codes.InvalidArgument,
			StatusMessage: fmt.Sprintf("rpc: %s: %s", name, err),
		}
	}
	return Outcome{
		Status:       codes.OK,
		ErrorCode:    errorCodeInternal,
		ErrorMessage: err.Error(),
	}
}

func (d *Dispatcher) invokeUnary(ctx context.Context, m *method, payload []byte) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, err = nil, d.recovered(m.name, r)
		}
	}()
	return m.unary(ctx, payload)
}

func (d *Dispatcher) invokeStream(ctx context.Context, m *method, payload []byte, send Sender) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = d.recovered(m.name, r)
		}
	}()
	return m.stream(ctx, payload, send)
}

// recovered turns a user panic into an ordinary error. A panicking handler must
// cost its own call and nothing else: the same process keeps serving every
// other caller on the same listener.
func (d *Dispatcher) recovered(name string, r any) error {
	d.log.Error("rpc: handler panicked",
		"method", name,
		"panic", fmt.Sprint(r),
		"stack", string(debug.Stack()))
	return fmt.Errorf("rpc: %s: %w: %v", name, ErrPanic, r)
}
