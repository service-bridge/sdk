package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
	"github.com/service-bridge/sdk/go/internal/telemetry"
)

// DefaultMaxInFlight caps concurrently handled deliveries.
const DefaultMaxInFlight = 32

// SubscribeStream is the client side of Events.Subscribe. The generated
// grpc.BidiStreamingClient[SubscribeClientMessage, SubscribeServerMessage]
// satisfies it.
type SubscribeStream interface {
	Send(*pb.SubscribeClientMessage) error
	Recv() (*pb.SubscribeServerMessage, error)
	CloseSend() error
}

// Handler processes one decoded event.
type Handler[T any] func(ctx context.Context, event T) error

// DeliveryInfo describes the delivery a handler is processing. LeaseToken is an
// opaque delivery generation, not a business idempotency guarantee.
type DeliveryInfo struct {
	EventID      string
	EventName    string
	Attempt      int32
	DeliveryID   string
	LeaseToken   string
	PartitionKey string
	Headers      map[string]string
	OccurredAtMs int64
}
type deliveryContextKey struct{}

// DeliveryFromContext returns metadata for the current leased event delivery.
func DeliveryFromContext(ctx context.Context) (DeliveryInfo, bool) {
	d, ok := ctx.Value(deliveryContextKey{}).(DeliveryInfo)
	return d, ok
}

// rawHandler is what the dispatch path actually holds: decoding is closed over
// by Subscribe so the delivery loop stays free of type parameters.
type rawHandler func(ctx context.Context, payload []byte) error

// ErrDuplicatePattern marks a second handler for a pattern this process
// already subscribed to.
var ErrDuplicatePattern = errors.New("events: pattern already has a handler")

// SubscriberConfig wires the delivery stream. See ./README.md.
type SubscriberConfig struct {
	// Open builds one Subscribe stream bound to ctx. Cancelling ctx must unblock
	// Recv. Evaluated on every reconnect.
	Open  func(ctx context.Context) (SubscribeStream, error)
	Codec Codec
	// Identity is read on every open, never cached: instance_id changes on
	// every certificate rotation.
	Identity func() Identity
	// MaxInFlight defaults to DefaultMaxInFlight.
	MaxInFlight int
	// Backoff pins the reconnect ladder. Zero value falls back to the shared
	// stream default.
	Backoff stream.Backoff
	OnError func(error)
	Logger  *slog.Logger
}

// Subscriber holds the delivery stream open and dispatches every inbound event
// to the handlers of the patterns the runtime says it matched.
type Subscriber struct {
	cfg SubscriberConfig
	sup *stream.Supervisor[*pb.SubscribeServerMessage, SubscribeStream]

	mu       sync.RWMutex
	handlers map[string]rawHandler
	filters  map[string]string

	// draining stops new deliveries from being taken: they stay unacked and
	// the runtime hands them out again once this stream is gone.
	draining atomic.Bool

	// sendMu serializes writes: gRPC forbids concurrent Send on one stream and
	// acks come off many handler goroutines.
	sendMu sync.Mutex

	// slots is the real concurrency limit. Taking a slot on the supervisor
	// goroutine is what stops the stream being read once the limit is reached,
	// which is the only way the limit turns into backpressure the runtime feels.
	slots chan struct{}

	chainMu sync.Mutex
	// chains holds the tail of each partition's serial queue. The runtime keeps
	// at most one delivery in flight per (consumer, key), but deliveries arrive
	// asynchronously, so without local serialization two handlers for one key
	// can overlap.
	chains map[string]chan struct{}

	wg sync.WaitGroup
}

// NewSubscriber validates the config and builds an idle subscriber.
func NewSubscriber(cfg SubscriberConfig) (*Subscriber, error) {
	if cfg.Open == nil {
		return nil, fmt.Errorf("events: new subscriber: missing Open: %w", ErrInvalidConfig)
	}
	if cfg.Codec == nil {
		return nil, fmt.Errorf("events: new subscriber: missing Codec: %w", ErrInvalidConfig)
	}
	if cfg.Identity == nil {
		return nil, fmt.Errorf("events: new subscriber: missing Identity: %w", ErrInvalidConfig)
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = DefaultMaxInFlight
	}
	if cfg.MaxInFlight > 1024 {
		return nil, fmt.Errorf("events: max in flight exceeds 1024: %w", ErrInvalidConfig)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	s := &Subscriber{
		cfg:      cfg,
		handlers: make(map[string]rawHandler),
		filters:  make(map[string]string),
		slots:    make(chan struct{}, cfg.MaxInFlight),
		chains:   make(map[string]chan struct{}),
	}
	sup, err := stream.NewSupervisor(stream.Config[*pb.SubscribeServerMessage, SubscribeStream]{
		Name:    "events.subscribe",
		Open:    s.open,
		OnData:  s.onData,
		OnError: s.reportError,
		Backoff: cfg.Backoff,
		Logger:  cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("events: new subscriber: %w", err)
	}
	s.sup = sup
	return s, nil
}

// Subscription is one declared pattern and its filter, as it goes into the
// RegisterRequest.
type Subscription struct {
	Pattern string
	// Filter is the JSON filter expression, empty for none.
	Filter string
}

// Subscribe registers the handler for one pattern. The pattern goes to the
// runtime verbatim — wildcards included — and routing is the runtime's: a
// delivery names the patterns it matched and only those handlers run. A
// second handler for the same pattern is refused.
func Subscribe[T any](s *Subscriber, pattern, filter string, fn Handler[T]) error {
	if s == nil {
		return fmt.Errorf("events: subscribe %q: nil subscriber: %w", pattern, ErrInvalidConfig)
	}
	if fn == nil {
		return fmt.Errorf("events: subscribe %q: nil handler: %w", pattern, ErrInvalidConfig)
	}
	if !ValidEventPattern(pattern) {
		return fmt.Errorf("events: subscribe %q: %w", pattern, ErrInvalidName)
	}
	codec := s.cfg.Codec
	h := func(ctx context.Context, payload []byte) error {
		var event T
		name := pattern
		if info, ok := DeliveryFromContext(ctx); ok {
			name = info.EventName
		}
		if err := codec.Decode(name, payload, &event); err != nil {
			return fmt.Errorf("decode %q: %w", name, err)
		}
		return fn(ctx, event)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, taken := s.handlers[pattern]; taken {
		return fmt.Errorf("events: subscribe %q: %w", pattern, ErrDuplicatePattern)
	}
	s.handlers[pattern] = h
	s.filters[pattern] = filter
	return nil
}

// Subscriptions returns every declared pattern with its filter, sorted, so the
// caller can declare them to the registry.
func (s *Subscriber) Subscriptions() []Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Subscription, 0, len(s.handlers))
	for pattern := range s.handlers {
		out = append(out, Subscription{Pattern: pattern, Filter: s.filters[pattern]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Pattern < out[j].Pattern })
	return out
}

// Start opens the delivery stream and keeps it open until ctx ends or Stop.
func (s *Subscriber) Start(ctx context.Context) error {
	if err := s.sup.Start(ctx); err != nil {
		return fmt.Errorf("events: start subscriber: %w", err)
	}
	return nil
}

// Drain stops taking new deliveries. Handlers already running finish; what
// arrives meanwhile is left unacked for the runtime to hand out again.
func (s *Subscriber) Drain() { s.draining.Store(true) }

// Wait blocks until every running handler returned or ctx ends.
func (s *Subscriber) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("events: wait for handlers: %w", ctx.Err())
	}
}

// Stop closes the stream and waits for every in-flight handler. Terminal.
// Handlers see a cancelled context, so one that ignores cancellation holds Stop
// for as long as it runs.
func (s *Subscriber) Stop() {
	s.sup.Stop()
	s.wg.Wait()
}

// handlersFor collects the handlers of every matched pattern this process
// subscribed to, each once, in the order the runtime listed them.
func (s *Subscriber) handlersFor(matched []string) []rawHandler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]rawHandler, 0, len(matched))
	seen := make(map[string]struct{}, len(matched))
	for _, pattern := range matched {
		if _, dup := seen[pattern]; dup {
			continue
		}
		seen[pattern] = struct{}{}
		if h, ok := s.handlers[pattern]; ok {
			out = append(out, h)
		}
	}
	return out
}

// open builds one stream generation and sends the init frame on it. Identity is
// read here, per open.
func (s *Subscriber) open(ctx context.Context) (SubscribeStream, error) {
	st, err := s.cfg.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("events: subscribe: open: %w", err)
	}
	ident := s.cfg.Identity()
	init := &pb.SubscribeClientMessage{
		Kind: &pb.SubscribeClientMessage_Init{
			Init: &pb.SubscribeInit{
				SubscriberServiceId:  ident.ServiceID,
				SubscriberInstanceId: ident.InstanceID,
				MaxInFlight:          int32(s.cfg.MaxInFlight),
			},
		},
	}
	if err := s.send(st, init); err != nil {
		return nil, fmt.Errorf("events: subscribe: init: %w", err)
	}
	return st, nil
}

// onData runs on the supervisor goroutine. It blocks on a free slot, which is
// what stops the stream being drained past the limit.
func (s *Subscriber) onData(ctx context.Context, msg *pb.SubscribeServerMessage, st SubscribeStream) {
	delivery := msg.GetDelivery()
	if delivery == nil || s.draining.Load() {
		return
	}
	if !s.acquire(ctx) {
		if ctx.Err() == nil {
			s.nack(st, delivery.GetDeliveryId(), delivery.GetEnvelope().GetId(), "local delivery concurrency exhausted", delivery.GetLeaseToken())
		}
		return
	}

	key := delivery.GetEnvelope().GetPartitionKey()
	if key == "" {
		// No key, no ordering requirement: handled in parallel.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.release()
			s.process(ctx, delivery, st)
		}()
		return
	}

	s.chainMu.Lock()
	prev := s.chains[key]
	done := make(chan struct{})
	s.chains[key] = done
	s.chainMu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.release()
		// Closing done unblocks whoever queued behind this delivery, so it must
		// happen on every exit path.
		defer s.retireChain(key, done)

		if prev != nil {
			select {
			case <-prev:
			case <-ctx.Done():
				return
			}
		}
		s.process(ctx, delivery, st)
	}()
}

func (s *Subscriber) retireChain(key string, done chan struct{}) {
	close(done)
	s.chainMu.Lock()
	if s.chains[key] == done {
		delete(s.chains, key)
	}
	s.chainMu.Unlock()
}

func (s *Subscriber) acquire(ctx context.Context) bool {
	select {
	case s.slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

func (s *Subscriber) release() { <-s.slots }

func (s *Subscriber) process(ctx context.Context, d *pb.EventDelivery, st SubscribeStream) {
	env := d.GetEnvelope()
	if env == nil {
		s.nack(st, d.GetDeliveryId(), "", "missing envelope", d.GetLeaseToken())
		return
	}
	acked, reason := s.Handle(ctx, d)
	if ctx.Err() != nil {
		return
	}
	if acked {
		s.ack(st, d.GetDeliveryId(), env.GetId(), d.GetLeaseToken())
		return
	}
	s.nack(st, d.GetDeliveryId(), env.GetId(), reason, d.GetLeaseToken())
}

// NoHandlerReason is the nack reason of a delivery none of whose matched
// patterns this process handles.
const NoHandlerReason = "no handler for matched patterns"

// Handle runs one delivery through the handlers of its matched patterns and
// reports the verdict: ack only if every handler succeeded, otherwise the
// reason of the first failure. It is the whole delivery contract minus the
// stream, which is what the test harness drives.
func (s *Subscriber) Handle(ctx context.Context, d *pb.EventDelivery) (acked bool, reason string) {
	env := d.GetEnvelope()
	handlers := s.handlersFor(d.GetMatchedPatterns())
	if len(handlers) == 0 {
		return false, NoHandlerReason
	}
	handlerCtx := s.traceContext(ctx, env.GetXSbTrace())
	handlerCtx = context.WithValue(handlerCtx, deliveryContextKey{}, DeliveryInfo{
		EventID:      env.GetId(),
		EventName:    env.GetName(),
		Attempt:      d.GetAttempt(),
		DeliveryID:   d.GetDeliveryId(),
		LeaseToken:   d.GetLeaseToken(),
		PartitionKey: env.GetPartitionKey(),
		Headers:      env.GetHeaders(),
		OccurredAtMs: env.GetOccurredAtUnixMs(),
	})
	for _, h := range handlers {
		if err := invokeHandler(handlerCtx, h, env.GetPayload()); err != nil {
			return false, err.Error()
		}
	}
	return true, ""
}

// traceContext puts the publisher's trace into the handler context so nested
// calls hang under the same tree. A broken header yields a fresh root rather
// than an error.
func (s *Subscriber) traceContext(ctx context.Context, header string) context.Context {
	tc, err := telemetry.ParseHeader(header)
	if err != nil {
		s.cfg.Logger.Warn("events: subscriber: trace context", "err", err)
		return ctx
	}
	return telemetry.WithTraceContext(ctx, tc)
}

// invokeHandler turns a panicking handler into a rejection: a panic on the
// delivery goroutine would otherwise take the whole process down and leave the
// delivery unanswered.
func invokeHandler(ctx context.Context, h rawHandler, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return h(ctx, payload)
}

func (s *Subscriber) ack(st SubscribeStream, deliveryID, eventID string, token ...string) {
	msg := &pb.SubscribeClientMessage{
		Kind: &pb.SubscribeClientMessage_Ack{
			Ack: &pb.Ack{LeaseToken: firstToken(token), DeliveryId: deliveryID, EventId: []byte(eventID)},
		},
	}
	if err := s.send(st, msg); err != nil {
		s.cfg.Logger.Warn("events: subscriber: ack", "delivery_id", deliveryID, "err", err)
	}
}

func (s *Subscriber) nack(st SubscribeStream, deliveryID, eventID, reason string, token ...string) {
	msg := &pb.SubscribeClientMessage{
		Kind: &pb.SubscribeClientMessage_Nack{
			Nack: &pb.Nack{LeaseToken: firstToken(token), DeliveryId: deliveryID, EventId: []byte(eventID), ErrorMessage: reason},
		},
	}
	if err := s.send(st, msg); err != nil {
		s.cfg.Logger.Warn("events: subscriber: nack", "delivery_id", deliveryID, "err", err)
	}
}

func (s *Subscriber) send(st SubscribeStream, msg *pb.SubscribeClientMessage) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := st.Send(msg); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	return nil
}

func (s *Subscriber) reportError(err error) {
	if s.cfg.OnError != nil {
		s.cfg.OnError(err)
	}
}

func firstToken(token []string) string {
	if len(token) > 0 {
		return token[0]
	}
	return ""
}
