package events

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

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

// rawHandler is what the dispatch path actually holds: decoding is closed over
// by Subscribe so the delivery loop stays free of type parameters.
type rawHandler func(ctx context.Context, payload []byte) error

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
// to the handlers registered for its exact name.
type Subscriber struct {
	cfg SubscriberConfig
	sup *stream.Supervisor[*pb.SubscribeServerMessage, SubscribeStream]

	mu       sync.RWMutex
	handlers map[string][]rawHandler

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
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	s := &Subscriber{
		cfg:      cfg,
		handlers: make(map[string][]rawHandler),
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

// Subscribe registers a typed handler for one exact event name. Several
// handlers may share a name; they run in registration order and one failure
// rejects the delivery.
//
// Routing is server-side (ADR-0002): there is no local pattern matching, so the
// name must be the one the runtime delivers.
func Subscribe[T any](s *Subscriber, name string, fn Handler[T]) error {
	if s == nil {
		return fmt.Errorf("events: subscribe %q: nil subscriber: %w", name, ErrInvalidConfig)
	}
	if fn == nil {
		return fmt.Errorf("events: subscribe %q: nil handler: %w", name, ErrInvalidConfig)
	}
	// A subscription may carry a wildcard where a publish may not: the runtime
	// routes on the pattern, and refusing it here would drop a capability the
	// wire contract offers.
	if !ValidEventPattern(name) {
		return fmt.Errorf("events: subscribe %q: %w", name, ErrInvalidName)
	}
	codec := s.cfg.Codec
	s.addHandler(name, func(ctx context.Context, payload []byte) error {
		var event T
		if err := codec.Decode(name, payload, &event); err != nil {
			return fmt.Errorf("decode %q: %w", name, err)
		}
		return fn(ctx, event)
	})
	return nil
}

// Names returns the event names with at least one handler, so the caller can
// declare them to the registry.
func (s *Subscriber) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.handlers))
	for name := range s.handlers {
		names = append(names, name)
	}
	return names
}

// Start opens the delivery stream and keeps it open until ctx ends or Stop.
func (s *Subscriber) Start(ctx context.Context) error {
	if err := s.sup.Start(ctx); err != nil {
		return fmt.Errorf("events: start subscriber: %w", err)
	}
	return nil
}

// Stop closes the stream and waits for every in-flight handler. Terminal.
// Handlers see a cancelled context, so one that ignores cancellation holds Stop
// for as long as it runs.
func (s *Subscriber) Stop() {
	s.sup.Stop()
	s.wg.Wait()
}

func (s *Subscriber) addHandler(name string, h rawHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[name] = append(s.handlers[name], h)
}

// handlersFor collects every handler whose subscription covers the delivered
// name. Exact names hit the map directly; a wildcard subscription is stored
// under its pattern and only a match finds it.
func (s *Subscriber) handlersFor(name string) []rawHandler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := s.handlers[name]
	for pattern, hs := range s.handlers {
		if pattern != name && MatchEventPattern(pattern, name) {
			out = append(out, hs...)
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
	if delivery == nil {
		return
	}
	if !s.acquire(ctx) {
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
	}
}

func (s *Subscriber) release() { <-s.slots }

func (s *Subscriber) process(ctx context.Context, d *pb.EventDelivery, st SubscribeStream) {
	env := d.GetEnvelope()
	if env == nil {
		s.nack(st, d.GetDeliveryId(), "", "missing envelope")
		return
	}
	handlers := s.handlersFor(env.GetName())
	if len(handlers) == 0 {
		// Routing is the runtime's job, so an unmatched name is not a local
		// mistake to retry: acknowledge and move on.
		s.ack(st, d.GetDeliveryId(), env.GetId())
		return
	}

	handlerCtx := s.traceContext(ctx, env.GetXSbTrace())
	for _, h := range handlers {
		if err := invokeHandler(handlerCtx, h, env.GetPayload()); err != nil {
			s.nack(st, d.GetDeliveryId(), env.GetId(), err.Error())
			return
		}
	}
	s.ack(st, d.GetDeliveryId(), env.GetId())
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

func (s *Subscriber) ack(st SubscribeStream, deliveryID, eventID string) {
	msg := &pb.SubscribeClientMessage{
		Kind: &pb.SubscribeClientMessage_Ack{
			Ack: &pb.Ack{DeliveryId: deliveryID, EventId: []byte(eventID)},
		},
	}
	if err := s.send(st, msg); err != nil {
		s.cfg.Logger.Warn("events: subscriber: ack", "delivery_id", deliveryID, "err", err)
	}
}

func (s *Subscriber) nack(st SubscribeStream, deliveryID, eventID, reason string) {
	msg := &pb.SubscribeClientMessage{
		Kind: &pb.SubscribeClientMessage_Nack{
			Nack: &pb.Nack{DeliveryId: deliveryID, EventId: []byte(eventID), ErrorMessage: reason},
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
