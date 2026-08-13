package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The three ways candidate selection fails. They are separate sentinels because
// they have opposite fixes, and an operator sent to the wrong one wastes an
// outage: nothing published this contract version, the callee advertises no
// inbound address, or the fleet is shedding load. The Node SDK collapsed them
// into one message.
var (
	// ErrNoCandidates: no instance advertises this method at this contract
	// hash. Either nobody serves the method, or the callee is deployed at a
	// different schema version.
	ErrNoCandidates = errors.New("rpc: no instance advertises this method with the caller's contract hash")
	// ErrNoEndpoint: instances exist but none advertises a call endpoint. The
	// callee runs caller-only, or its advertise address is misconfigured.
	ErrNoEndpoint = errors.New("rpc: the callee advertises no inbound address")
	// ErrAllUnavailable: addressed instances exist but every one is shed by the
	// runtime's health hint or by the local circuit breaker.
	ErrAllUnavailable = errors.New("rpc: every candidate is unhealthy or circuit-open")
)

// SelectionError explains why no instance could be chosen.
//
// It reports itself as Unavailable so the retry ladder treats it as "the call
// never happened" (ADR-0001 §4): a callee redeploy at the caller's contract
// version then heals the call without the caller seeing an error.
type SelectionError struct {
	Service      string
	Method       string
	ContractHash string
	Stats        PickStats
	Reason       error
}

func (e *SelectionError) Error() string {
	return fmt.Sprintf("%s: %s/%s contract=%q (candidates=%d addressed=%d eligible=%d)",
		e.Reason.Error(), e.Service, e.Method, e.ContractHash,
		e.Stats.Total, e.Stats.Addressed, e.Stats.Eligible)
}

func (e *SelectionError) Unwrap() error { return e.Reason }

// GRPCStatus makes the selection failure classify as Unavailable.
func (e *SelectionError) GRPCStatus() *status.Status {
	return status.New(codes.Unavailable, e.Error())
}

// HandlerError is a business failure the callee returned in error_code /
// error_message. It carries no gRPC status on purpose: the handler ran and
// decided, so it is neither retryable nor a circuit-breaker fault.
type HandlerError struct {
	Code    string
	Message string
}

func (e *HandlerError) Error() string {
	if e.Message == "" {
		return "rpc: handler error " + e.Code
	}
	return "rpc: handler error " + e.Code + ": " + e.Message
}

func handlerError(code, message string) error {
	return &HandlerError{Code: code, Message: message}
}

// errStreamClosed ends a stream the caller abandoned before its last chunk.
var errStreamClosed = errors.New("rpc: stream closed by caller")

// Transport selects which path a client dispatches over.
type Transport uint8

const (
	// TransportDirect dials the callee instance over mTLS. The SDK picks the
	// instance, so load balancing and the circuit breaker apply.
	TransportDirect Transport = iota
	// TransportProxy goes through the runtime, which resolves the instance
	// itself and owns the idempotency claim. Because the SDK does not choose
	// the instance, it records neither breaker outcomes nor in-flight load
	// against one — attributing a proxied failure to a locally picked instance
	// would shed traffic from a pod that never saw the call.
	TransportProxy
)

// CandidateSource is the read side of the registry cache the call loop needs.
type CandidateSource interface {
	// Candidates returns the descriptors serving (service, method) at this
	// contract hash. Filtering by the hash IS the version routing mechanism
	// (ADR-0001 §4) — a call must never widen it.
	Candidates(service, method, contractHash string) []*pb.MethodDescriptor
	Instance(instanceID string) (*pb.ServiceInstanceInfo, bool)
}

// ClientConfig wires the outbound call path.
type ClientConfig struct {
	Registry CandidateSource
	Direct   *Direct
	Proxy    *Proxy
	Balancer *Balancer
	Breaker  *Breaker
	Recorder *telemetry.Recorder
	Retry    RetryPolicy
	// Transport is the dispatch path. Direct by default.
	Transport Transport
	// HealthHintTTLMs bounds trust in the runtime's unhealthy hint.
	HealthHintTTLMs int64
	// CallerService is this service's name, echoed to the callee on the direct
	// path so it can attribute the call without a registry lookup.
	CallerService string
	Now           func() int64
	Logger        *slog.Logger
}

// Client is the outbound RPC path: candidate selection, balancing, breaking,
// retrying and one telemetry operation per logical call.
type Client struct {
	registry  CandidateSource
	direct    *Direct
	proxy     *Proxy
	balancer  *Balancer
	breaker   *Breaker
	recorder  *telemetry.Recorder
	retry     RetryPolicy
	transport Transport
	hintTTLMs int64
	caller    string
	now       func() int64
	log       *slog.Logger
}

// NewClient builds the outbound path. The transport the config selects must be
// present; the other one is optional.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Registry == nil {
		return nil, fmt.Errorf("rpc: new client: no candidate source: %w", ErrInvalidConfig)
	}
	if cfg.Recorder == nil {
		return nil, fmt.Errorf("rpc: new client: no telemetry recorder: %w", ErrInvalidConfig)
	}
	switch cfg.Transport {
	case TransportDirect:
		if cfg.Direct == nil {
			return nil, fmt.Errorf("rpc: new client: direct transport selected but absent: %w", ErrInvalidConfig)
		}
	case TransportProxy:
		if cfg.Proxy == nil {
			return nil, fmt.Errorf("rpc: new client: proxy transport selected but absent: %w", ErrInvalidConfig)
		}
	default:
		return nil, fmt.Errorf("rpc: new client: unknown transport %d: %w", cfg.Transport, ErrInvalidConfig)
	}

	c := &Client{
		registry:  cfg.Registry,
		direct:    cfg.Direct,
		proxy:     cfg.Proxy,
		balancer:  cfg.Balancer,
		breaker:   cfg.Breaker,
		recorder:  cfg.Recorder,
		retry:     cfg.Retry.normalized(),
		transport: cfg.Transport,
		hintTTLMs: cfg.HealthHintTTLMs,
		caller:    cfg.CallerService,
		now:       cfg.Now,
		log:       cfg.Logger,
	}
	if c.balancer == nil {
		c.balancer = NewBalancer()
	}
	if c.breaker == nil {
		c.breaker = NewBreaker(DefaultBreakerConfig())
	}
	if c.hintTTLMs <= 0 {
		c.hintTTLMs = DefaultHealthHintTTLMs
	}
	if c.now == nil {
		c.now = nowUnixMs
	}
	if c.log == nil {
		c.log = slog.Default()
	}
	return c, nil
}

// Request is one logical call.
type Request struct {
	Service string
	Method  string
	Payload []byte
	// ContractHash pins the schema version. It is matched exactly against what
	// callees advertise; an empty hash matches only an empty one.
	ContractHash string
	// IdempotencyKey is caller-supplied and never generated. Its presence is
	// what unlocks retrying the ambiguous codes.
	IdempotencyKey string
	BusinessKey    string
}

func (r Request) idempotent() bool { return r.IdempotencyKey != "" }

func (r Request) subject() string { return "rpc.call:" + r.Service + "/" + r.Method }

// Unary runs one logical call to completion, retrying on the same telemetry
// operation until the ladder is exhausted.
func (c *Client) Unary(ctx context.Context, req Request) ([]byte, error) {
	cands := c.candidates(req)

	var op *telemetry.Op
	callCtx := ctx
	var lastErr error

	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		res, selErr := c.reserve(cands, req)
		if selErr != nil {
			lastErr = selErr
			// A retry re-runs selection: an instance leaving the open state or
			// a callee finishing its redeploy heals the call in place.
			if !c.wait(ctx, op, attempt, selErr, req) {
				break
			}
			continue
		}

		if op == nil {
			var err error
			// Started only now: before a candidate exists the peer and the
			// via_proxy flag would both be guesses.
			callCtx, op, err = c.startOp(ctx, req, res.target)
			if err != nil {
				res.settle(nil)
				return nil, err
			}
			op.CaptureIn(req.Payload, req.ContractHash)
		}
		op.SetAttempt(int32(attempt))

		payload, err := c.dispatch(callCtx, req, res.target, op)
		res.settle(err)
		if err == nil {
			op.End(pb.Status_SUCCESS, "")
			return payload, nil
		}

		lastErr = err
		if !c.wait(ctx, op, attempt, err, req) {
			break
		}
	}

	if op != nil {
		op.End(statusOf(lastErr), lastErr.Error())
	}
	return nil, lastErr
}

// Stream opens a server-side stream. It is never retried — not mid-stream,
// where a repeat would re-deliver chunks the caller already consumed, and not
// on the connect phase either, because a retry that only covers connect is a
// hidden mode nobody can reason about from the outside.
func (c *Client) Stream(ctx context.Context, req Request) (*Stream, error) {
	cands := c.candidates(req)

	res, selErr := c.reserve(cands, req)
	if selErr != nil {
		return nil, selErr
	}

	callCtx, op, err := c.startOp(ctx, req, res.target)
	if err != nil {
		res.settle(nil)
		return nil, err
	}
	op.CaptureIn(req.Payload, req.ContractHash)

	st, err := c.openStream(callCtx, req, res.target, op)
	if err != nil {
		res.settle(err)
		op.End(statusOf(err), err.Error())
		return nil, err
	}

	st.after = func(cause error) {
		outcome := cause
		if errors.Is(cause, io.EOF) || errors.Is(cause, errStreamClosed) {
			outcome = nil
		}
		res.settle(outcome)
		switch {
		case errors.Is(cause, io.EOF):
			op.End(pb.Status_SUCCESS, "")
		case errors.Is(cause, errStreamClosed):
			op.End(pb.Status_ABANDONED, cause.Error())
		default:
			op.End(statusOf(cause), cause.Error())
		}
	}
	return st, nil
}

// wait decides whether another attempt is worth making and pays the backoff.
func (c *Client) wait(ctx context.Context, op *telemetry.Op, attempt int, err error, req Request) bool {
	if attempt >= c.retry.MaxAttempts-1 {
		return false
	}
	if !Retryable(err, req.idempotent()) {
		return false
	}
	if serr := sleepMs(ctx, c.retry.BackoffMs(attempt)); serr != nil {
		if op != nil {
			op.SetAttempt(int32(attempt))
		}
		return false
	}
	return true
}

// reservation holds what a dispatch borrowed: the balancer's in-flight slot and
// the breaker's call slot. Both are absent on the proxy path, where the runtime
// picks the instance.
type reservation struct {
	target   Candidate
	ticket   *BreakerTicket
	balancer *Balancer
}

func (r reservation) settle(err error) {
	r.ticket.Report(err)
	if r.balancer != nil {
		r.balancer.Release(r.target)
	}
}

func (c *Client) reserve(cands []Candidate, req Request) (reservation, error) {
	now := c.now()
	healthy := func(cand Candidate) bool { return Healthy(cand, now, c.hintTTLMs) }

	if c.transport == TransportProxy {
		eligible, stats := Eligible(cands, healthy)
		if len(eligible) == 0 {
			return reservation{}, c.selectionError(req, stats)
		}
		// The target only carries the service UUID and the telemetry peer; the
		// runtime resolves the instance that actually serves the call.
		return reservation{target: eligible[0]}, nil
	}

	allows := func(cand Candidate) bool {
		return healthy(cand) && c.breaker.Allows(cand.Key())
	}
	var ticket *BreakerTicket
	acquire := func(cand Candidate) bool {
		t, ok := c.breaker.Acquire(cand.Key())
		if !ok {
			return false
		}
		ticket = t
		return true
	}

	target, stats, ok := c.balancer.Pick(cands, allows, acquire)
	if !ok {
		return reservation{}, c.selectionError(req, stats)
	}
	return reservation{target: target, ticket: ticket, balancer: c.balancer}, nil
}

func (c *Client) selectionError(req Request, stats PickStats) error {
	reason := ErrAllUnavailable
	switch {
	case stats.Total == 0:
		reason = ErrNoCandidates
	case stats.Addressed == 0:
		reason = ErrNoEndpoint
	}
	return &SelectionError{
		Service:      req.Service,
		Method:       req.Method,
		ContractHash: req.ContractHash,
		Stats:        stats,
		Reason:       reason,
	}
}

// candidates turns the registry index into dispatch candidates. The index key
// already includes the contract hash, so no filtering happens here.
func (c *Client) candidates(req Request) []Candidate {
	descs := c.registry.Candidates(req.Service, req.Method, req.ContractHash)
	if len(descs) == 0 {
		return nil
	}
	out := make([]Candidate, 0, len(descs))
	seen := make(map[string]struct{}, len(descs))
	for _, d := range descs {
		id := d.GetInstanceId()
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		cand := Candidate{
			ServiceID:   d.GetServiceId(),
			ServiceName: d.GetServiceName(),
			InstanceID:  id,
		}
		if inst, ok := c.registry.Instance(id); ok {
			cand.Endpoint = inst.GetCallEndpoint()
			cand.UnhealthySinceMs = inst.GetIsUnhealthySinceUnixMs()
		}
		out = append(out, cand)
	}
	return out
}

func (c *Client) startOp(ctx context.Context, req Request, target Candidate) (context.Context, *telemetry.Op, error) {
	meta, err := json.Marshal(callMeta{
		Method:         req.Method,
		ViaProxy:       c.transport == TransportProxy,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		return ctx, nil, fmt.Errorf("rpc: start call operation: encode meta: %w", err)
	}
	callCtx, op, err := c.recorder.Start(ctx, telemetry.OpSpec{
		Channel:       pb.Channel_RPC,
		Kind:          telemetry.OpKindRPCCall,
		Subject:       req.subject(),
		PeerServiceID: target.ServiceID,
		BusinessKey:   req.BusinessKey,
		MetaJSON:      meta,
	})
	if err != nil {
		return ctx, nil, fmt.Errorf("rpc: start call operation: %w", err)
	}
	return callCtx, op, nil
}

type callMeta struct {
	Method         string `json:"method"`
	ViaProxy       bool   `json:"via_proxy"`
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

// trace injects the header both ways: the runtime reads gRPC metadata on the
// proxy path, the callee reads the body field on the direct path.
func trace(ctx context.Context) (context.Context, string) {
	tc, ok := telemetry.FromContext(ctx)
	if !ok {
		return ctx, ""
	}
	return telemetry.InjectMetadata(ctx, tc), telemetry.FormatHeader(tc)
}

func (c *Client) dispatch(ctx context.Context, req Request, target Candidate, op *telemetry.Op) ([]byte, error) {
	mdCtx, header := trace(ctx)

	var payload []byte
	var err error
	if c.transport == TransportProxy {
		payload, err = c.proxy.Unary(mdCtx, c.invokeRequest(req, target, op, header))
	} else {
		payload, err = c.direct.Unary(mdCtx, target, c.callRequest(req, op, header))
	}
	if err != nil {
		return nil, err
	}
	op.CaptureOut(payload, req.ContractHash)
	return payload, nil
}

func (c *Client) openStream(ctx context.Context, req Request, target Candidate, op *telemetry.Op) (*Stream, error) {
	mdCtx, header := trace(ctx)

	if c.transport == TransportProxy {
		return c.proxy.Stream(mdCtx, c.invokeRequest(req, target, op, header))
	}
	return c.direct.Stream(mdCtx, target, c.callRequest(req, op, header))
}

// callRequest builds the direct-path frame. request_id is the operation id, so
// one logical call keeps one identity across retries — which is exactly what
// the callee's idempotency cache keys on.
func (c *Client) callRequest(req Request, op *telemetry.Op, header string) *pb.CallRequest {
	return &pb.CallRequest{
		Method:         req.Method,
		Payload:        req.Payload,
		CallerService:  c.caller,
		RequestId:      op.ID().String(),
		IdempotencyKey: req.IdempotencyKey,
		XSbTrace:       header,
	}
}

func (c *Client) invokeRequest(req Request, target Candidate, op *telemetry.Op, header string) *pb.InvokeRequest {
	return &pb.InvokeRequest{
		TargetServiceId: target.ServiceID,
		Method:          req.Method,
		Payload:         req.Payload,
		RequestId:       op.ID().String(),
		IdempotencyKey:  req.IdempotencyKey,
		XSbTrace:        header,
		ContractHash:    EncodeContractHash(req.ContractHash),
	}
}

func statusOf(err error) pb.Status {
	if code, ok := callCode(err); ok && code == codes.DeadlineExceeded {
		return pb.Status_TIMEOUT
	}
	return pb.Status_ERROR
}

// Stream is a server-side stream of payload chunks. It owns whatever the
// transport reserved and gives it back exactly once, on the terminal chunk or
// on Close.
type Stream struct {
	recv func() ([]byte, error)
	stop func()

	mu    sync.Mutex
	after func(error)
	done  bool
	err   error
}

func newStream(recv func() ([]byte, error), stop func()) *Stream {
	return &Stream{recv: recv, stop: stop}
}

// Recv returns the next chunk. io.EOF marks a clean end; every other error is
// terminal too and the stream stays closed.
func (s *Stream) Recv() ([]byte, error) {
	s.mu.Lock()
	if s.done {
		err := s.err
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	s.mu.Unlock()

	chunk, err := s.recv()
	if err != nil {
		s.finish(err)
		return nil, err
	}
	return chunk, nil
}

// Close ends the stream early. It is safe to call after the stream has already
// ended.
func (s *Stream) Close() error {
	s.finish(errStreamClosed)
	return nil
}

func (s *Stream) finish(cause error) {
	s.mu.Lock()
	if s.done {
		s.mu.Unlock()
		return
	}
	s.done = true
	if !errors.Is(cause, io.EOF) {
		s.err = cause
	}
	after := s.after
	s.mu.Unlock()

	s.stop()
	if after != nil {
		after(cause)
	}
}
