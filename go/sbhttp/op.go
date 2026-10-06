// Package sbhttp connects an application's own HTTP server to ServiceBridge.
//
// The runtime never proxies business HTTP (ADR-0010): the application owns its
// listener and its traffic. The integration does exactly two things — publish
// the route list into the service map and wrap every request in one
// HTTP.HANDLE span, so calls made out of a handler join the same trace tree.
package sbhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/registry"
	"github.com/service-bridge/sdk/go/internal/telemetry"
)

// Configuration failures. Wrapped as fmt.Errorf("sbhttp: <action>: %w", …) and
// matched with errors.Is.
var (
	ErrNoRuntime = errors.New("integration needs a runtime with a recorder and declarations")
	ErrNoRouter  = errors.New("router must not be nil")
	ErrPort      = errors.New("port must be in 1..65535")
)

// idempotencyKeyHeader carries the caller's business key. Absent, the key
// falls back to "<METHOD> <route>".
const idempotencyKeyHeader = "Idempotency-Key"

// UnmatchedRoute is the route template of a request no route matched.
const UnmatchedRoute = "*"

// rawJSONContract marks a payload that is already JSON rather than proto wire.
// It must equal runtime telemetry.ContractRawJSON, which is what makes the
// console render an HTTP body verbatim instead of trying to decode a schema.
const rawJSONContract = "raw/json"

// Runtime is what an HTTP integration needs from the SDK client, declared here
// because this is the consumer.
type Runtime interface {
	// Recorder emits the HTTP.HANDLE operations.
	Recorder() *telemetry.Recorder
	// Declarations accumulates the routes and the HTTP endpoint the service
	// registers.
	Declarations() *registry.Declarations
	// RestartRegistry reopens the registry stream so an endpoint published
	// after start reaches the runtime immediately. It is a no-op before start,
	// where the endpoint simply rides the first registration.
	RestartRegistry()
}

// Integration binds one HTTP server to one SDK client. See ./README.md.
type Integration struct {
	rec   *telemetry.Recorder
	decls *registry.Declarations
	rt    Runtime
	log   *slog.Logger

	hostWarn sync.Once

	trustTrace bool
	resolve    func(*http.Request) string

	mu   sync.Mutex
	seen map[string]struct{}
}

// Option tunes an Integration at construction time.
type Option func(*Integration)

// WithLogger replaces the logger used for the loopback-host warning and for
// spans that could not be started.
func WithLogger(log *slog.Logger) Option {
	return func(i *Integration) {
		if log != nil {
			i.log = log
		}
	}
}

// WithTrustTraceHeader adopts an incoming X-SB-Trace header as the parent of
// the request's span. Off by default: on a public edge a client must not be
// able to graft its requests into arbitrary traces. Turn it on only behind a
// gateway that sets the header itself.
func WithTrustTraceHeader() Option {
	return func(i *Integration) { i.trustTrace = true }
}

// WithRouteResolver names the route template of a request for routers the
// middleware cannot read on its own. It returns the template with its leading
// slash, or "" when no route matched.
func WithRouteResolver(fn func(*http.Request) string) Option {
	return func(i *Integration) { i.resolve = fn }
}

// New builds an integration over rt.
func New(rt Runtime, opts ...Option) (*Integration, error) {
	if rt == nil {
		return nil, fmt.Errorf("sbhttp: new integration: %w", ErrNoRuntime)
	}
	rec, decls := rt.Recorder(), rt.Declarations()
	if rec == nil || decls == nil {
		return nil, fmt.Errorf("sbhttp: new integration: %w", ErrNoRuntime)
	}
	i := &Integration{
		rec:   rec,
		decls: decls,
		rt:    rt,
		log:   slog.Default(),
		seen:  make(map[string]struct{}),
	}
	for _, opt := range opts {
		opt(i)
	}
	return i, nil
}

// Logger returns the integration's logger. Framework adapters living in their
// own module report a failed span through it.
func (i *Integration) Logger() *slog.Logger { return i.log }

// Begin starts the HTTP.HANDLE operation for one request and returns the
// request carrying the operation's trace context. Every framework adapter goes
// through it: the operation logic exists once, in this package.
//
// route is the route template the request matched ("/users/{id}"), never the
// raw path: a path makes every user id its own subject. Empty means no route
// matched and is recorded as "*".
//
// The returned request must replace the one handed to the handler. Without it
// the RPC calls and event publishes made inside the handler start their own
// root trace and the request falls apart into two trees.
func (i *Integration) Begin(r *http.Request, route string) (*http.Request, *Operation, error) {
	if route == "" {
		route = UnmatchedRoute
	}
	var tc telemetry.TraceContext
	var err error
	if i.trustTrace {
		tc, err = telemetry.ParseHeader(r.Header.Get(telemetry.HeaderName))
	} else {
		tc, err = telemetry.ParseHeader("")
	}
	if err != nil {
		return r, nil, fmt.Errorf("sbhttp: begin request: %w", err)
	}

	businessKey := r.Header.Get(idempotencyKeyHeader)
	if businessKey == "" {
		businessKey = r.Method + " " + route
	}
	meta, err := json.Marshal(map[string]string{"method": r.Method, "route": route})
	if err != nil {
		return r, nil, fmt.Errorf("sbhttp: begin request: encode meta: %w", err)
	}

	ctx, op, err := i.rec.Start(telemetry.WithTraceContext(r.Context(), tc), telemetry.OpSpec{
		Channel: pb.Channel_HTTP,
		Kind:    telemetry.OpKindHTTPHandle,
		// Canonical HTTP subject (ADR-0007): the route keeps its leading
		// slash, so the runtime's subject_http_route_key can turn the first
		// slash back into the space that joins it to the declared route name.
		Subject:     "http.handle:" + r.Method + "/" + route,
		BusinessKey: businessKey,
		MetaJSON:    meta,
	})
	if err != nil {
		return r, nil, fmt.Errorf("sbhttp: begin request: %w", err)
	}

	oper := &Operation{op: op, limit: int(i.rec.Policy().PayloadMaxBytes())}
	r = r.WithContext(ctx)
	if oper.Capturing() {
		r = oper.captureRequestBody(r)
	}
	return r, oper, nil
}

// routeTemplate strips the method from a Go 1.22 pattern ("GET /users/{id}").
func routeTemplate(pattern string) string {
	_, rest := splitPattern(pattern)
	if pattern == "" {
		return ""
	}
	return rest
}

// RouteOf reads the route template net/http stored on a routed request.
// Exposed for adapters that wrap a ServeMux themselves.
func RouteOf(r *http.Request) string {
	return routeTemplate(strings.TrimSpace(r.Pattern))
}

// Operation is one in-flight HTTP.HANDLE span. Its whole surface is stdlib
// typed so an adapter in another module can drive it.
type Operation struct {
	requestBody *capturedRequestBody
	op          *telemetry.Op
	limit       int
}

// Capturing reports whether building a body payload would keep anything. Ask
// before touching a body: with capture off the bytes are thrown away, and the
// read alone costs more than the span.
func (o *Operation) Capturing() bool { return o.op.Capturing() }

// PayloadLimit is how many body bytes are worth buffering per direction.
func (o *Operation) PayloadLimit() int { return o.limit }

// CaptureResponse records the response body. It is a no-op while capture is
// off.
func (o *Operation) CaptureResponse(body []byte) {
	if len(body) == 0 {
		return
	}
	o.op.CaptureOut(body, rawJSONContract)
}

// Outcome is how a request ended, as the framework adapter observed it.
type Outcome struct {
	// StatusCode is the response status. Zero counts as 200, which is what net/http
	// sends for a handler that wrote nothing.
	StatusCode int
	// Aborted marks a client that went away before the handler returned.
	Aborted bool
	// Hijacked marks a connection the handler took over (websockets). The SDK
	// cannot observe what happens on it afterwards.
	Hijacked bool
	// Panicked marks a handler that panicked.
	Panicked bool
}

// Finish closes the operation. Calling it twice does nothing.
func (o *Operation) Finish(out Outcome) {
	if o.requestBody != nil {
		o.requestBody.mu.Lock()
		body := append([]byte(nil), o.requestBody.captured...)
		o.requestBody.mu.Unlock()
		if len(body) > 0 {
			o.op.CaptureIn(body, rawJSONContract)
		}
	}
	status, message := statusOf(out)
	code := out.StatusCode
	if code == 0 {
		code = http.StatusOK
	}
	meta, err := json.Marshal(map[string]int{"status": code})
	if err != nil {
		meta = nil
	}
	o.op.EndWithMeta(status, message, meta)
}

// statusOf maps an outcome onto the operation status. The order is by
// specificity: a panic and a hijack say more about the request than the status
// code, and a client that went away outranks whatever code the handler managed
// to write before the connection died.
func statusOf(out Outcome) (pb.Status, string) {
	switch {
	case out.Panicked:
		return pb.Status_ERROR, "handler panic"
	case out.Hijacked:
		return pb.Status_SUCCESS, ""
	case out.Aborted:
		return pb.Status_TIMEOUT, "client abort"
	case out.StatusCode >= 400:
		return pb.Status_ERROR, fmt.Sprintf("HTTP %d", out.StatusCode)
	default:
		return pb.Status_SUCCESS, ""
	}
}

// captureRequestBody observes only the reads made by the application. It never
// pulls bytes or consumes a one-shot error ahead of the handler.
func (o *Operation) captureRequestBody(r *http.Request) *http.Request {
	if r.Body == nil || r.Body == http.NoBody || o.limit <= 0 {
		return r
	}
	body := &capturedRequestBody{ReadCloser: r.Body, limit: o.limit}
	o.requestBody = body
	r.Body = body
	return r
}

type capturedRequestBody struct {
	io.ReadCloser
	mu       sync.Mutex
	limit    int
	captured []byte
}

func (b *capturedRequestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.mu.Lock()
		keep := min(n, b.limit-len(b.captured))
		if keep > 0 {
			b.captured = append(b.captured, p[:keep]...)
		}
		b.mu.Unlock()
	}
	return n, err
}
