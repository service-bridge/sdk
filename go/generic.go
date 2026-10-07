package servicebridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/service-bridge/sdk/go/internal/events"
	jobi "github.com/service-bridge/sdk/go/internal/job"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/registry"
	"github.com/service-bridge/sdk/go/internal/rpc"
	"github.com/service-bridge/sdk/go/internal/serde"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	wfi "github.com/service-bridge/sdk/go/internal/workflow"
	"github.com/service-bridge/sdk/go/job"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// Handle registers a unary handler. The schema comes from the type parameters,
// so there is no separate registration step to forget: a contract the compiler
// accepts is a contract the mesh routes.
func Handle[Req, Resp proto.Message](c *Client, name string, fn func(ctx context.Context, req Req) (Resp, error)) error {
	const op = "servicebridge.Handle"
	if err := c.declarable(op); err != nil {
		return err
	}
	if fn == nil {
		return newError(CodeValidation, op, "handler must not be nil", nil)
	}
	var reqZero Req
	var respZero Resp

	if err := c.dispatch.RegisterUnary(name, func(ctx context.Context, payload []byte) ([]byte, error) {
		req := serde.New(reqZero)
		if err := serde.Decode(payload, req); err != nil {
			return nil, fmt.Errorf("%w: %w", rpc.ErrDecode, err)
		}
		resp, err := fn(ctx, req)
		if err != nil {
			return nil, handlerFailure(err)
		}
		return proto.Marshal(resp)
	}); err != nil {
		return wrap(op, err)
	}
	return wrap(op, c.declareIncoming(name, reqZero, respZero, false))
}

// HandleStream registers a server-streaming handler. Sending blocks while the
// caller is behind — that is the backpressure — and fails once the caller is
// gone, so a handler that stops on the first send error stops when it should.
func HandleStream[Req, Chunk proto.Message](c *Client, name string, fn func(ctx context.Context, req Req, send func(Chunk) error) error) error {
	const op = "servicebridge.HandleStream"
	if err := c.declarable(op); err != nil {
		return err
	}
	if fn == nil {
		return newError(CodeValidation, op, "handler must not be nil", nil)
	}
	var reqZero Req
	var chunkZero Chunk

	if err := c.dispatch.RegisterStream(name, func(ctx context.Context, payload []byte, send rpc.Sender) error {
		req := serde.New(reqZero)
		if err := serde.Decode(payload, req); err != nil {
			return fmt.Errorf("%w: %w", rpc.ErrDecode, err)
		}
		return handlerFailure(fn(ctx, req, func(chunk Chunk) error {
			raw, err := proto.Marshal(chunk)
			if err != nil {
				return err
			}
			return send(raw)
		}))
	}); err != nil {
		return wrap(op, err)
	}
	return wrap(op, c.declareIncoming(name, reqZero, chunkZero, true))
}

// handlerFailure decides what error code a handler failure answers with: the
// code of a HandlerError the handler returned itself, INTERNAL for anything
// else. An SDK error that merely carries a downstream HandlerError — a failed
// nested Call returned as is — is not this handler's business decision, so it
// answers INTERNAL rather than impersonating the downstream code.
func handlerFailure(err error) error {
	if err == nil {
		return nil
	}
	var sdk *Error
	if errors.As(err, &sdk) {
		return errors.New(err.Error())
	}
	return err
}

// CallInfo describes the inbound call a handler is serving: the request id,
// the caller's idempotency key, the calling service and instance, and the
// caller's deadline.
type CallInfo = rpc.CallInfo

// CallInfoFromContext returns the CallInfo of the inbound call ctx belongs to.
// The handler's ctx is also cancelled when the caller cancels or its deadline
// passes.
func CallInfoFromContext(ctx context.Context) (CallInfo, bool) {
	return rpc.CallInfoFromContext(ctx)
}

func (c *Client) declareIncoming(name string, req, resp proto.Message, streaming bool) error {
	in := req.ProtoReflect().Descriptor()
	out := resp.ProtoReflect().Descriptor()
	return c.decls.AddIncoming(registry.IncomingSpec{
		Type:             pb.MethodType_METHOD_TYPE_RPC,
		Name:             name,
		InputSchemaJSON:  serde.JSONSchema(in),
		OutputSchemaJSON: serde.JSONSchema(out),
		Streaming:        streaming,
		ContractHash:     serde.ContractHash(in, out),
	})
}

func (c *Client) declarable(op string) error {
	if c.isStarted() {
		return newError(CodeState, op, "handlers must be declared before Start", nil)
	}
	if c.cfg.callerOnly {
		return newError(CodeConfig, op, "a caller-only instance serves no handlers", nil)
	}
	return nil
}

// Call runs one logical call to completion. The contract hash comes from the
// type parameters and is matched exactly against what callees advertise, which
// is the version-routing mechanism: a callee deployed at a different schema
// version is not a candidate rather than a decode failure.
func Call[Req, Resp proto.Message](ctx context.Context, c *Client, service, method string, req Req, opts ...CallOption) (Resp, error) {
	const op = "servicebridge.Call"
	var zero Resp
	o := c.cfg.callOptions(opts)

	payload, err := proto.Marshal(req)
	if err != nil {
		return zero, newError(CodeValidation, op, "request does not encode", err)
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	raw, err := c.caller.Unary(ctx, rpc.Request{
		Service:        service,
		Method:         method,
		Payload:        payload,
		ContractHash:   serde.ContractHash(req.ProtoReflect().Descriptor(), zero.ProtoReflect().Descriptor()),
		IdempotencyKey: o.idempotencyKey,
		BusinessKey:    o.businessKey,
		Transport:      o.transport.internal(),
	})
	if err != nil {
		return zero, wrap(op, err)
	}
	resp := serde.New(zero)
	if err := serde.Decode(raw, resp); err != nil {
		return zero, wrap(op, err)
	}
	return resp, nil
}

// Stream opens a server-side stream and yields its chunks. Leaving the loop —
// break, return, or an error — tears the stream down, because Go runs the
// iterator's cleanup by construction. Nothing is retried: a repeat would
// re-deliver chunks the caller already consumed.
func Stream[Req, Chunk proto.Message](ctx context.Context, c *Client, service, method string, req Req, opts ...CallOption) iter.Seq2[Chunk, error] {
	const op = "servicebridge.Stream"
	var zero Chunk
	o := c.cfg.callOptions(opts)

	return func(yield func(Chunk, error) bool) {
		payload, err := proto.Marshal(req)
		if err != nil {
			yield(zero, newError(CodeValidation, op, "request does not encode", err))
			return
		}
		ctx, cancel := context.WithTimeout(ctx, o.timeout)
		defer cancel()

		st, err := c.caller.Stream(ctx, rpc.Request{
			Service:        service,
			Method:         method,
			Payload:        payload,
			ContractHash:   serde.ContractHash(req.ProtoReflect().Descriptor(), zero.ProtoReflect().Descriptor()),
			IdempotencyKey: o.idempotencyKey,
			BusinessKey:    o.businessKey,
			Transport:      o.transport.internal(),
		})
		if err != nil {
			yield(zero, wrap(op, err))
			return
		}
		drain(st, zero, yield)
	}
}

// chunkStream is the half of a stream the iterator needs. It is declared here,
// at the consumer, so the teardown-on-break rule can be proven without a mesh.
type chunkStream interface {
	Recv() ([]byte, error)
	Close() error
}

// drain pumps a stream into a range-over-func loop. The deferred Close is what
// makes leaving the loop tear the stream down: without it an abandoned range
// leaves the HTTP/2 stream and the callee's handler running until the process
// exits.
func drain[Chunk proto.Message](st chunkStream, zero Chunk, yield func(Chunk, error) bool) {
	defer func() { _ = st.Close() }()
	for {
		raw, err := st.Recv()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			yield(zero, wrap("servicebridge.Stream", err))
			return
		}
		chunk := serde.New(zero)
		if err := serde.Decode(raw, chunk); err != nil {
			yield(zero, wrap("servicebridge.Stream", err))
			return
		}
		if !yield(chunk, nil) {
			return
		}
	}
}

// NewClient names another service this one talks to. It declares nothing on its
// own: the methods do.
func NewClient(c *Client, service string) *ServiceClient {
	return &ServiceClient{c: c, service: service}
}

// ServiceClient is the handle NewMethod hangs typed methods off.
type ServiceClient struct {
	c       *Client
	service string
}

// NewMethod declares an outgoing dependency and binds its schema in one step.
// There is no separate "register the schema" call to forget: forgetting one in
// the Node SDK is a throw on the first call in production, whereas here the
// same mistake does not compile.
//
// The binding outlives this handle. A workflow call step names its target as
// text and holds only a JSON tree, so the pair of types recorded here is the
// only thing that lets such a step encode for, and route to, a typed handler.
func NewMethod[Req, Resp proto.Message](sc *ServiceClient, method string) (*Method[Req, Resp], error) {
	const op = "servicebridge.NewMethod"
	if sc == nil || sc.c == nil {
		return nil, newError(CodeConfig, op, "nil service client", nil)
	}
	if err := sc.c.decls.AddOutgoing(sc.service, method, pb.MethodType_METHOD_TYPE_RPC); err != nil {
		return nil, wrap(op, err)
	}
	var reqZero Req
	var respZero Resp
	if err := sc.c.callSchemas.Bind(sc.service, method, registry.CallSchema{
		Input:        reqZero,
		Output:       respZero,
		ContractHash: serde.ContractHash(serde.DescriptorOf(reqZero), serde.DescriptorOf(respZero)),
	}); err != nil {
		return nil, wrap(op, err)
	}
	return &Method[Req, Resp]{c: sc.c, service: sc.service, method: method}, nil
}

// Method is one declared dependency: a service, a method and the pair of types
// that decide which deployed version of it this caller routes to.
type Method[Req, Resp proto.Message] struct {
	c       *Client
	service string
	method  string
}

// Call invokes the method.
func (m *Method[Req, Resp]) Call(ctx context.Context, req Req, opts ...CallOption) (Resp, error) {
	return Call[Req, Resp](ctx, m.c, m.service, m.method, req, opts...)
}

// Stream opens the method as a server-side stream.
func (m *Method[Req, Resp]) Stream(ctx context.Context, req Req, opts ...CallOption) iter.Seq2[Resp, error] {
	return Stream[Req, Resp](ctx, m.c, m.service, m.method, req, opts...)
}

// DefineEvent declares an event this service publishes and freezes its schema.
// It is the publisher's declaration only: a subscriber never needs it.
func DefineEvent[T proto.Message](c *Client, name string) (*Event[T], error) {
	const op = "servicebridge.DefineEvent"
	if c.isStarted() {
		return nil, newError(CodeState, op, "events must be defined before Start", nil)
	}
	if !events.ValidEventName(name) {
		return nil, newError(CodeInvalidEventName, op, name, nil)
	}
	var zero T
	md := zero.ProtoReflect().Descriptor()
	if err := c.decls.PublishEvent(name, serde.JSONSchema(md), serde.EventContractHash(md)); err != nil {
		return nil, wrap(op, err)
	}
	return &Event[T]{c: c, name: name}, nil
}

// Event is a declared event name bound to its payload type.
type Event[T proto.Message] struct {
	c    *Client
	name string
}

// Name is the event name as declared.
func (e *Event[T]) Name() string { return e.name }

// Publish publishes one occurrence; see PublishEvent.
func (e *Event[T]) Publish(ctx context.Context, payload T, opts ...PublishOption) (string, error) {
	return PublishEvent[T](ctx, e.c, e.name, payload, opts...)
}

// PublishEvent publishes one event and returns its identifier once the runtime
// acknowledged it — the event is in the runtime's store. The event waits in
// an in-memory queue while the runtime is unreachable; the queue is bounded
// (CodeQueueFull) and so is the wait (CodeTimeout, which says whether the
// event was ever sent).
func PublishEvent[T proto.Message](ctx context.Context, c *Client, name string, payload T, opts ...PublishOption) (string, error) {
	const op = "servicebridge.PublishEvent"
	if !c.canPublish() {
		return "", newError(CodeState, op, "publish before Start", nil)
	}
	id, err := c.publisher.Publish(ctx, name, payload, publishOptions(opts)...)
	if err != nil {
		return "", wrap(op, err)
	}
	return id, nil
}

// SubscribeEvent registers the handler for one event name or pattern (`*` one
// segment, `#` zero or more). The payload is decoded into T, the subscriber's
// own schema: nothing has to be defined with DefineEvent. Routing is the
// runtime's — a delivery runs the handlers of the patterns it matched — and
// one pattern has one handler per process.
func SubscribeEvent[T proto.Message](c *Client, pattern string, fn func(ctx context.Context, event T) error, opts ...SubscribeOption) error {
	const op = "servicebridge.SubscribeEvent"
	if fn == nil {
		return newError(CodeValidation, op, "handler must not be nil", nil)
	}
	return c.subscribe(op, pattern, opts, func(filter string) error {
		return events.Subscribe(c.eventSub, pattern, filter, events.Handler[T](fn))
	})
}

// SubscribeEventRaw registers a handler that receives the payload undecoded.
// It is the way to serve a pattern whose payload type varies by event.
func SubscribeEventRaw(c *Client, pattern string, fn func(ctx context.Context, payload []byte) error, opts ...SubscribeOption) error {
	const op = "servicebridge.SubscribeEventRaw"
	if fn == nil {
		return newError(CodeValidation, op, "handler must not be nil", nil)
	}
	return c.subscribe(op, pattern, opts, func(filter string) error {
		return events.Subscribe(c.eventSub, pattern, filter, events.Handler[[]byte](fn))
	})
}

func (c *Client) subscribe(op, pattern string, opts []SubscribeOption, register func(filter string) error) error {
	if c.isStarted() {
		return newError(CodeState, op, "subscriptions must be declared before Start", nil)
	}
	var o subscribeOpts
	for _, opt := range opts {
		opt(&o)
	}
	filter := ""
	if len(o.filter) > 0 {
		raw, err := json.Marshal(o.filter)
		if err != nil {
			return newError(CodeValidation, op, "filter does not encode as JSON", err)
		}
		filter = string(raw)
	}
	if err := register(filter); err != nil {
		return wrap(op, err)
	}
	return wrap(op, c.decls.SubscribeEvent(pattern, filter))
}

// SubscribeOption tunes one subscription.
type SubscribeOption func(*subscribeOpts)

type subscribeOpts struct {
	filter map[string]any
}

// WithFilter narrows a subscription on the runtime: only events whose JSON
// payload has every given path equal to its literal are delivered, e.g.
// {"$.status": "paid", "$.region": "eu"}. The runtime evaluates it on the
// event's payload_json and refuses a malformed filter at registration, which
// stops the client.
func WithFilter(filter map[string]any) SubscribeOption {
	return func(o *subscribeOpts) { o.filter = filter }
}

// PublishOption tunes one publication.
type PublishOption func(*publishOpts)

type publishOpts struct {
	idempotencyKey string
	partitionKey   string
	fireAndForget  bool
	headers        map[string]string
	occurredAtMs   int64
}

// WithEventIdempotencyKey deduplicates the event at the runtime. It is spelled
// apart from the call-side key because the two travel to different places.
func WithEventIdempotencyKey(key string) PublishOption {
	return func(o *publishOpts) { o.idempotencyKey = key }
}

// WithPartitionKey puts the event on a FIFO lane: consumers see events sharing
// a key in publication order.
func WithPartitionKey(key string) PublishOption {
	return func(o *publishOpts) { o.partitionKey = key }
}

// WithFireAndForget returns the id as soon as the event is queued, without
// waiting for the runtime's acknowledgement. It accepts loss: until the runtime
// acknowledges it the event lives only in process memory and dies with the
// process, and a delivery failure is only logged. A full queue still fails.
func WithFireAndForget() PublishOption {
	return func(o *publishOpts) { o.fireAndForget = true }
}

// WithHeaders attaches envelope metadata.
func WithHeaders(h map[string]string) PublishOption {
	return func(o *publishOpts) { o.headers = h }
}

// WithOccurredAt stamps the moment the event happened, in unix milliseconds.
// The default is the moment of publication.
func WithOccurredAt(unixMs int64) PublishOption {
	return func(o *publishOpts) { o.occurredAtMs = unixMs }
}

func publishOptions(opts []PublishOption) []events.PublishOption {
	var o publishOpts
	for _, opt := range opts {
		opt(&o)
	}
	out := make([]events.PublishOption, 0, 5)
	if o.idempotencyKey != "" {
		out = append(out, events.WithIdempotencyKey(o.idempotencyKey))
	}
	if o.partitionKey != "" {
		out = append(out, events.WithPartitionKey(o.partitionKey))
	}
	if o.fireAndForget {
		out = append(out, events.WithFireAndForget())
	}
	if len(o.headers) > 0 {
		out = append(out, events.WithHeaders(o.headers))
	}
	if o.occurredAtMs != 0 {
		out = append(out, events.WithOccurredAt(o.occurredAtMs))
	}
	return out
}

// JobDomain declares scheduled work.
type JobDomain struct{ c *Client }

// Handle declares a job and the function that runs it. The specification is
// validated here — a cron expression with a typo would otherwise register as an
// ordinary string and simply never fire.
func (d *JobDomain) Handle(name string, spec job.Spec, fn job.Handler) error {
	const op = "Job.Handle"
	if d.c.isStarted() {
		return newError(CodeState, op, "jobs must be declared before Start", nil)
	}
	decl, err := d.c.jobDecls.Add(name, spec, fn)
	if err != nil {
		return wrap(op, err)
	}
	return wrap(op, d.c.decls.AddIncoming(registry.IncomingSpec{
		Type:            pb.MethodType_METHOD_TYPE_JOB,
		Name:            name,
		InputSchemaJSON: decl.SpecJSON,
		ContractHash:    decl.ContractHash,
	}))
}

// WorkflowDomain declares workflows and steers their runs.
type WorkflowDomain struct{ c *Client }

// Handle freezes a declared graph and registers it. Freezing is the only path
// onto the wire: an invalid graph cannot be registered, and the steps the
// runner executes are the same steps the registered bytes describe.
func (d *WorkflowDomain) Handle(name string, def wf.Definition) error {
	const op = "Workflow.Handle"
	if d.c.isStarted() {
		return newError(CodeState, op, "workflows must be declared before Start", nil)
	}
	frozen, err := wfi.Freeze(name, def)
	if err != nil {
		return wrap(op, err)
	}
	d.c.graphMu.Lock()
	d.c.graphs[name+":"+frozen.Fingerprint] = frozen.Steps
	d.c.graphMu.Unlock()

	return wrap(op, d.c.decls.AddIncoming(registry.IncomingSpec{
		Type:            pb.MethodType_METHOD_TYPE_WORKFLOW,
		Name:            name,
		InputSchemaJSON: frozen.JSON,
		ContractHash:    frozen.Fingerprint,
	}))
}

// StartOption tunes one run.
type StartOption func(*startOpts)

type startOpts struct {
	idempotencyKey string
	timeoutSec     int
}

// WithRunIdempotencyKey makes a repeated Start return the existing run instead
// of a second one.
func WithRunIdempotencyKey(key string) StartOption {
	return func(o *startOpts) { o.idempotencyKey = key }
}

// WithRunTimeoutSec bounds the whole run. Seconds, because that is the unit the
// workflow contract carries.
func WithRunTimeoutSec(sec int) StartOption {
	return func(o *startOpts) { o.timeoutSec = sec }
}

// Start begins a run and returns its identifier.
func (d *WorkflowDomain) Start(ctx context.Context, name string, input any, opts ...StartOption) (string, error) {
	var o startOpts
	for _, opt := range opts {
		opt(&o)
	}
	id, err := d.c.wfCall.Start(ctx, wfi.StartArgs{
		Workflow:       name,
		Input:          input,
		IdempotencyKey: o.idempotencyKey,
		TimeoutSec:     o.timeoutSec,
	})
	return id, wrap("Workflow.Start", err)
}

// Signal delivers a signal to a parked run.
func (d *WorkflowDomain) Signal(ctx context.Context, runID, signal string, payload any) error {
	return wrap("Workflow.Signal", d.c.wfCall.Signal(ctx, wfi.SignalArgs{
		RunID:   runID,
		Signal:  signal,
		Payload: payload,
	}))
}

// Cancel stops a run.
func (d *WorkflowDomain) Cancel(ctx context.Context, runID string) error {
	return wrap("Workflow.Cancel", d.c.wfCall.Cancel(ctx, runID))
}

// Await blocks until the run finishes and returns its final state.
func (d *WorkflowDomain) Await(ctx context.Context, runID string) (map[string]any, error) {
	state, err := d.c.wfCall.Await(ctx, runID)
	return state, wrap("Workflow.Await", err)
}

// Query reads a run back without waiting for it.
func (d *WorkflowDomain) Query(ctx context.Context, runID string) (RunSnapshot, error) {
	snap, err := d.c.wfCall.Query(ctx, runID)
	if err != nil {
		return RunSnapshot{}, wrap("Workflow.Query", err)
	}
	out := RunSnapshot{RunID: snap.RunID, Status: snap.Status, State: snap.State}
	for _, s := range snap.Steps {
		out.Steps = append(out.Steps, StepSnapshot{
			StepID:        s.StepID,
			Status:        s.Status,
			Output:        s.Output,
			LastError:     s.LastError,
			CompensatedBy: s.CompensatedBy,
		})
	}
	return out, nil
}

// Replay restarts a finished run from one step onward and returns the new run.
func (d *WorkflowDomain) Replay(ctx context.Context, runID, fromStepID string) (string, error) {
	id, err := d.c.wfCall.Replay(ctx, runID, fromStepID)
	return id, wrap("Workflow.Replay", err)
}

// RunSnapshot is a point-in-time view of a run.
type RunSnapshot struct {
	RunID  string
	Status string
	State  map[string]any
	Steps  []StepSnapshot
}

// StepSnapshot is what one step of a run reports.
type StepSnapshot struct {
	StepID        string
	Status        string
	Output        any
	LastError     string
	CompensatedBy string
}

// executor performs what a workflow step declares. It is a distinct type so the
// three method names — Call, Publish, StartRun — do not land on the client,
// where they would compete with the API an application actually calls.
type executor Client

// Call dispatches a call step. The step holds a JSON tree — run state is JSON
// by construction (ADR-0002) — while the callee is an ordinary typed handler,
// and the dependency declared with NewMethod is what joins the two: the tree is
// read into the request message, and the reply comes back as the JSON mirror of
// the response message, which is the same form the rest of the state is in.
//
// The declaration is also what makes the step routable. Version routing matches
// the caller's contract hash exactly, and the pair of types is the only place
// that hash can come from; an undeclared target is refused here rather than
// called at the empty hash, which matches no typed handler at all.
func (e *executor) Call(ctx context.Context, spec wfi.CallSpec) (any, error) {
	const op = "workflow.call"
	c := (*Client)(e)
	schema, bound := c.callSchemas.Lookup(spec.Service, spec.Method)
	if !bound {
		return nil, newError(CodeConfig, op, undeclaredDependency(spec.Service, spec.Method), nil)
	}
	payload, err := serde.EncodeTree(spec.Input, schema.Input)
	if err != nil {
		return nil, wrap(op, err)
	}
	// The client's call defaults apply to a workflow call step exactly as to
	// Call; the step overrides only what it states.
	o := c.cfg.callOptions(nil)
	switch spec.Transport {
	case "proxy":
		o.transport = TransportProxy
	case "direct":
		o.transport = TransportDirect
	case "auto":
		o.transport = TransportAuto
	}
	if spec.Timeout > 0 {
		o.timeout = spec.Timeout
	}
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	raw, err := c.caller.Unary(ctx, rpc.Request{
		Service:        spec.Service,
		Method:         spec.Method,
		Payload:        payload,
		ContractHash:   schema.ContractHash,
		IdempotencyKey: spec.IdempotencyKey,
		BusinessKey:    spec.RequestID,
		Transport:      o.transport.internal(),
	})
	if err != nil {
		return nil, err
	}
	out, err := serde.DecodeTree(raw, schema.Output)
	if err != nil {
		return nil, wrap(op, err)
	}
	return out, nil
}

// undeclaredDependency spells the one fix out, because the failure is a missing
// line of declaration and the reader is looking at a graph that names the target
// perfectly well.
func undeclaredDependency(service, method string) string {
	return fmt.Sprintf("%s/%s is not a declared dependency: bind it with "+
		"servicebridge.NewMethod[Req, Resp](servicebridge.NewClient(c, %q), %q) before Start",
		service, method, service, method)
}

// Publish dispatches a publish step and answers with the event identifier, so
// the run state records what was emitted.
func (e *executor) Publish(ctx context.Context, spec wfi.PublishSpec) (any, error) {
	c := (*Client)(e)
	if !c.canPublish() {
		return nil, newError(CodeState, "workflow.publish", "publish before Start", nil)
	}
	opts := []events.PublishOption{}
	if spec.IdempotencyKey != "" {
		opts = append(opts, events.WithIdempotencyKey(spec.IdempotencyKey))
	}
	if spec.PartitionKey != "" {
		opts = append(opts, events.WithPartitionKey(spec.PartitionKey))
	}
	if spec.FireAndForget {
		opts = append(opts, events.WithFireAndForget())
	}
	if len(spec.Headers) > 0 {
		opts = append(opts, events.WithHeaders(spec.Headers))
	}
	if spec.OccurredAtMs != 0 {
		opts = append(opts, events.WithOccurredAt(spec.OccurredAtMs))
	}
	id, err := c.publisher.Publish(ctx, spec.Event, spec.Payload, opts...)
	if err != nil {
		return nil, err
	}
	return map[string]any{"eventId": id}, nil
}

// StartRun starts a nested run. The runner parks afterwards rather than waiting
// in process, so this returns as soon as the child exists.
func (e *executor) StartRun(ctx context.Context, spec wfi.StartSpec) (string, error) {
	return (*Client)(e).wfCall.Start(ctx, wfi.StartArgs(spec))
}

// wrapStep opens one user sub-operation around every unit the runner executes,
// so the call and publish operations of a step hang under the step and not
// under the run root. The runtime owns the WORKFLOW.RUN operation itself.
func (c *Client) wrapStep(ctx context.Context, span wfi.StepSpan, fn func(context.Context) (any, error)) (any, error) {
	ctx, op, err := c.recorder.Start(ctx, telemetry.OpSpec{
		Channel: pb.Channel_USER,
		Kind:    telemetry.OpKindUserSubOp,
		Subject: span.Name,
	})
	if err != nil {
		// The only way an operation fails to start is a broken entropy source.
		// Losing a span is not worth losing the step.
		c.log.Warn("workflow step span did not start", "step", span.StepID, "error", err)
		return fn(ctx)
	}
	out, err := fn(ctx)
	if err != nil {
		op.End(pb.Status_ERROR, err.Error())
		return out, err
	}
	op.End(pb.Status_SUCCESS, "")
	return out, nil
}

// TelemetryDomain opens operations, keeps metrics and bridges application logs.
type TelemetryDomain struct{ c *Client }

// OpOption tunes one user operation.
type OpOption func(*telemetry.OpSpec)

// WithOpPeer names the service the operation talks to.
func WithOpPeer(serviceID string) OpOption {
	return func(s *telemetry.OpSpec) { s.PeerServiceID = serviceID }
}

// WithOpBusinessKey labels the operation with a domain identifier.
func WithOpBusinessKey(key string) OpOption {
	return func(s *telemetry.OpSpec) { s.BusinessKey = key }
}

// StartOp opens a user sub-operation. The returned context carries it as the
// parent, so every call, publication and nested operation made with that
// context lands under it in the trace view.
func (d *TelemetryDomain) StartOp(ctx context.Context, name string, opts ...OpOption) (context.Context, *Operation) {
	spec := telemetry.OpSpec{
		Channel: pb.Channel_USER,
		Kind:    telemetry.OpKindUserSubOp,
		Subject: name,
	}
	for _, opt := range opts {
		opt(&spec)
	}
	ctx, op, err := d.c.recorder.Start(ctx, spec)
	if err != nil {
		d.c.log.Warn("operation did not start", "subject", name, "error", err)
		return ctx, &Operation{}
	}
	return ctx, &Operation{op: op}
}

// Operation is one open user operation. A nil inner handle is not an error
// state: an operation that failed to start still has to be closable, or every
// call site grows a branch.
type Operation struct{ op *telemetry.Op }

// End closes the operation as successful.
func (o *Operation) End() {
	if o.op != nil {
		o.op.End(pb.Status_SUCCESS, "")
	}
}

// Fail closes the operation as failed.
func (o *Operation) Fail(err error) {
	if o.op == nil {
		return
	}
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	o.op.End(pb.Status_ERROR, msg)
}

// Logger is a slog.Logger writing into the telemetry buffer. The application
// keeps its own slog setup and adds this one to its chain rather than replacing
// it.
func (d *TelemetryDomain) Logger() *slog.Logger { return d.c.logs }

// Counter, Gauge and Histogram return handles bound to the live instance. They
// re-resolve when the identity rotates: a handle pinned at creation would keep
// reporting under an instance the runtime has already torn down, and its series
// would stop showing up on any dashboard.
func (d *TelemetryDomain) Counter(name string, labels map[string]string) *Counter {
	return &Counter{series: d.series(name, "", labels, seriesCounter)}
}

func (d *TelemetryDomain) Gauge(name, unit string, labels map[string]string) *Gauge {
	return &Gauge{series: d.series(name, unit, labels, seriesGauge)}
}

func (d *TelemetryDomain) Histogram(name, unit string, labels map[string]string, bounds []float64) *Histogram {
	s := d.series(name, unit, labels, seriesHistogram)
	s.bounds = append([]float64(nil), bounds...)
	return &Histogram{series: s}
}

type seriesKind uint8

const (
	seriesCounter seriesKind = iota
	seriesGauge
	seriesHistogram
)

// series caches one metric handle per instance identity. The handle itself is
// what makes the hot path allocation-free, so it is kept; the cache key is the
// instance it was minted for.
type series struct {
	d      *TelemetryDomain
	kind   seriesKind
	name   string
	unit   string
	labels telemetry.Labels
	bounds []float64

	mu       sync.Mutex
	instance string
	counter  *telemetry.Counter
	gauge    *telemetry.Gauge
	hist     *telemetry.Histogram
}

func (d *TelemetryDomain) series(name, unit string, labels map[string]string, kind seriesKind) *series {
	copied := make(telemetry.Labels, len(labels))
	for key, value := range labels {
		copied[key] = value
	}
	return &series{d: d, kind: kind, name: name, unit: unit, labels: copied}
}

func (s *series) resolve() error {
	id := s.d.c.instanceID()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.instance == id && (s.counter != nil || s.gauge != nil || s.hist != nil) {
		return nil
	}
	var err error
	switch s.kind {
	case seriesCounter:
		s.counter, err = s.d.c.metrics.Counter(id, s.name, s.labels)
	case seriesGauge:
		s.gauge, err = s.d.c.metrics.Gauge(id, s.name, s.unit, s.labels)
	case seriesHistogram:
		s.hist, err = s.d.c.metrics.Histogram(id, s.name, s.unit, s.labels, s.bounds)
	}
	if err != nil {
		return err
	}
	s.instance = id
	return nil
}

func (s *series) report(err error) {
	if err != nil {
		s.d.c.log.Warn("metric unavailable", "metric", s.name, "error", err)
	}
}

// Counter is a monotonically rising total.
type Counter struct{ series *series }

// Inc adds one.
func (c *Counter) Inc() { c.Add(1) }

// Add moves the counter by delta.
func (c *Counter) Add(delta float64) {
	if err := c.series.resolve(); err != nil {
		c.series.report(err)
		return
	}
	c.series.mu.Lock()
	h := c.series.counter
	c.series.mu.Unlock()
	h.Add(delta)
}

// Gauge is a value that moves in both directions.
type Gauge struct{ series *series }

// Set records the current value.
func (g *Gauge) Set(value float64) {
	if err := g.series.resolve(); err != nil {
		g.series.report(err)
		return
	}
	g.series.mu.Lock()
	h := g.series.gauge
	g.series.mu.Unlock()
	h.Set(value)
}

// Histogram distributes observations over cumulative buckets.
type Histogram struct{ series *series }

// Observe records one observation.
func (h *Histogram) Observe(value float64) {
	if err := h.series.resolve(); err != nil {
		h.series.report(err)
		return
	}
	h.series.mu.Lock()
	inner := h.series.hist
	h.series.mu.Unlock()
	inner.Observe(value)
}

// jobHandler keeps the public job.Handler type identical to the internal one so
// a declaration written against the public package needs no conversion.
var _ jobi.Handler = job.Handler(nil)
