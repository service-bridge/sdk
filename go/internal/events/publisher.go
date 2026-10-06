// Package events owns event publication and subscription: the in-memory
// publish queue and its sender, and the inbound delivery stream.
package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
)

// Publisher defaults. See ./README.md.
const (
	// DefaultMaxPending caps the publish queue. Past it a publish fails at once
	// with ErrQueueFull instead of growing memory without bound.
	DefaultMaxPending = 10_000
	// DefaultPublishTimeout bounds one publication, from enqueue to the
	// runtime's acknowledgement.
	DefaultPublishTimeout = 30 * time.Second
	// DefaultBatchSize caps one Publish request.
	DefaultBatchSize = 100
	// requestTimeout bounds one Publish RPC. A batch the runtime does not
	// answer within it is retried with the same ids.
	requestTimeout = 10 * time.Second
)

// defaultRetryLadder spaces the retries of a batch the runtime did not settle.
// The last rung repeats; a settled batch resets the ladder.
func defaultRetryLadder() []time.Duration {
	return []time.Duration{
		100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond,
		time.Second, 2 * time.Second, 5 * time.Second,
	}
}

// eventNameRE accepts dot-separated segments of lowercase alphanumerics,
// underscores and hyphens. It mirrors runtime/internal/events/event_name.go: a
// name the runtime would reject must fail where it was written, not after a
// round trip.
var eventNameRE = regexp.MustCompile(`^[a-z0-9_-]+(\.[a-z0-9_-]+)*$`)

// Sentinels for errors.Is.
var (
	// ErrInvalidName marks a name the runtime rejects.
	ErrInvalidName = errors.New("events: invalid event name")
	// ErrInvalidConfig marks a missing required dependency.
	ErrInvalidConfig = errors.New("events: invalid config")
	// ErrAlreadyStarted marks a second Start on a single-use component.
	ErrAlreadyStarted = errors.New("events: already started")
	// ErrQueueFull marks a publish refused because the queue is at its cap.
	ErrQueueFull = errors.New("events: publish queue is full")
	// ErrNotSent marks a publish that timed out before it was ever sent: the
	// event is not in the runtime.
	ErrNotSent = errors.New("events: publish timed out, event not sent")
	// ErrOutcomeUnknown marks a publish that timed out after it was sent: the
	// runtime may or may not hold the event.
	ErrOutcomeUnknown = errors.New("events: publish timed out, outcome unknown")
	// ErrConflict marks an id the runtime already holds with other content.
	ErrConflict = errors.New("events: event id already accepted with different content")
	// ErrForbidden marks a publish the access policy denies.
	ErrForbidden = errors.New("events: publish denied by policy")
	// ErrStopped marks a publish still queued when the client stopped.
	ErrStopped = errors.New("events: client stopped")
)

// ValidEventName reports whether the runtime would accept name.
func ValidEventName(name string) bool { return eventNameRE.MatchString(name) }

// eventPatternRE accepts a subscription pattern: the same segments as a name,
// plus `*` for exactly one segment and `#` for zero or more. The runtime routes
// on these, so a subscription may carry them where a publish may not.
var eventPatternRE = regexp.MustCompile(`^([a-z0-9_-]+|\*|#)(\.([a-z0-9_-]+|\*|#))*$`)

// ValidEventPattern reports whether the runtime would accept pattern as a
// subscription.
func ValidEventPattern(pattern string) bool { return eventPatternRE.MatchString(pattern) }

// Identity is the live session identity stamped on outgoing frames.
type Identity struct {
	ServiceID  string
	InstanceID string
}

// Encoded is the pair of wire forms one payload takes plus its contract hash.
type Encoded struct {
	// Proto is the canonical payload; the runtime treats it as opaque bytes.
	Proto []byte
	// JSON mirrors Proto so the runtime can evaluate JSON-path filters without
	// decoding protobuf.
	JSON         []byte
	ContractHash string
}

// Codec turns application payloads into the wire forms and back. Declared here
// because publication and delivery are its only consumers.
type Codec interface {
	Encode(name string, payload any) (Encoded, error)
	Decode(name string, payload []byte, out any) error
}

// PublishFunc is the one unary call this package makes. Narrower than the
// generated client so the transport can be swapped and faked.
type PublishFunc func(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error)

// PublishOptions carries the per-event knobs.
type PublishOptions struct {
	IdempotencyKey string
	PartitionKey   string
	// FireAndForget returns the id right after enqueue instead of waiting for
	// the runtime's acknowledgement. The event lives only in process memory
	// until it is acknowledged: it is lost if the process dies first, and a
	// delivery failure is only logged.
	FireAndForget bool
	Headers       map[string]string
	// OccurredAtMs is unix-ms; zero means now.
	OccurredAtMs int64
}

// PublishOption mutates PublishOptions.
type PublishOption func(*PublishOptions)

// WithIdempotencyKey deduplicates the event on the runtime side.
func WithIdempotencyKey(key string) PublishOption {
	return func(o *PublishOptions) { o.IdempotencyKey = key }
}

// WithPartitionKey pins the event to a FIFO lane.
func WithPartitionKey(key string) PublishOption {
	return func(o *PublishOptions) { o.PartitionKey = key }
}

// WithFireAndForget returns without waiting for the acknowledgement.
func WithFireAndForget() PublishOption {
	return func(o *PublishOptions) { o.FireAndForget = true }
}

// WithHeaders attaches string metadata to the envelope.
func WithHeaders(h map[string]string) PublishOption {
	return func(o *PublishOptions) { o.Headers = h }
}

// WithOccurredAt overrides the event time, in unix-ms.
func WithOccurredAt(unixMs int64) PublishOption {
	return func(o *PublishOptions) { o.OccurredAtMs = unixMs }
}

// PolicyViolation reports an event the runtime refused on policy grounds.
type PolicyViolation struct {
	EventID   string
	EventName string
	Reason    string
}

// PublisherConfig wires the publish path. See ./README.md.
type PublisherConfig struct {
	Codec   Codec
	Publish PublishFunc
	// MaxPending defaults to DefaultMaxPending.
	MaxPending int
	// Timeout defaults to DefaultPublishTimeout.
	Timeout time.Duration
	// BatchSize defaults to DefaultBatchSize.
	BatchSize int
	// RetryLadder defaults to 100, 250, 500, 1000, 2000, 5000 ms.
	RetryLadder []time.Duration
	// OnPolicyViolation surfaces a publish the policy denied.
	OnPolicyViolation func(PolicyViolation)
	// Now returns unix-ms; defaults to the wall clock.
	Now func() int64
	// NewID mints the event identifier; defaults to UUIDv7, monotonic.
	NewID  func() (string, error)
	Logger *slog.Logger
}

// entry is one queued publication. Every field after env is guarded by the
// publisher's mutex.
type entry struct {
	env           *pb.EventEnvelope
	fireAndForget bool
	done          chan struct{}

	inflight  bool
	sent      bool
	abandoned bool
	resolved  bool
	id        string
	err       error
}

// Publisher queues events in memory and sends them to the runtime, one batch
// at a time, until each is acknowledged.
type Publisher struct {
	cfg PublisherConfig

	mu      sync.Mutex
	queue   []*entry
	closed  bool
	started bool
	idle    chan struct{} // closed and replaced whenever the queue empties

	wake   chan struct{} // a new event: an idle sender goes
	kick   chan struct{} // a reconnect: a sender in backoff retries now
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPublisher validates the config and fills its defaults.
func NewPublisher(cfg PublisherConfig) (*Publisher, error) {
	if cfg.Codec == nil {
		return nil, fmt.Errorf("events: new publisher: missing Codec: %w", ErrInvalidConfig)
	}
	if cfg.Publish == nil {
		return nil, fmt.Errorf("events: new publisher: missing Publish: %w", ErrInvalidConfig)
	}
	if cfg.MaxPending < 0 || cfg.BatchSize < 0 || cfg.Timeout < 0 {
		return nil, fmt.Errorf("events: new publisher: negative bound: %w", ErrInvalidConfig)
	}
	if cfg.MaxPending == 0 {
		cfg.MaxPending = DefaultMaxPending
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultPublishTimeout
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if len(cfg.RetryLadder) == 0 {
		cfg.RetryLadder = defaultRetryLadder()
	}
	if cfg.Now == nil {
		cfg.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.NewID == nil {
		cfg.NewID = newEventID
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Publisher{
		cfg:  cfg,
		idle: make(chan struct{}),
		wake: make(chan struct{}, 1),
		kick: make(chan struct{}, 1),
	}, nil
}

// Start runs the sender until Close.
func (p *Publisher) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return fmt.Errorf("events: publisher: start: %w", ErrAlreadyStarted)
	}
	p.started = true
	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.wg.Add(1)
	go p.run(runCtx)
	return nil
}

// Kick makes a sender waiting out a retry rung try again now. The client calls
// it on every reconnect: the failure the rung was waiting out is gone.
func (p *Publisher) Kick() { signal(p.kick) }

// Pending reports how many events wait for an acknowledgement.
func (p *Publisher) Pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queue)
}

// Publish enqueues one event and waits for the runtime's acknowledgement,
// returning the event id the runtime holds. A full queue fails at once; a
// timeout says whether the event was ever sent.
func (p *Publisher) Publish(ctx context.Context, name string, payload any, opts ...PublishOption) (string, error) {
	if !ValidEventName(name) {
		return "", fmt.Errorf("events: publish %q: %w", name, ErrInvalidName)
	}
	var o PublishOptions
	for _, opt := range opts {
		opt(&o)
	}
	enc, err := p.cfg.Codec.Encode(name, payload)
	if err != nil {
		return "", fmt.Errorf("events: publish %q: encode: %w", name, err)
	}
	occurredAt := o.OccurredAtMs
	if occurredAt == 0 {
		occurredAt = p.cfg.Now()
	}
	e := &entry{
		env: &pb.EventEnvelope{
			Name:             name,
			Payload:          enc.Proto,
			PayloadJson:      enc.JSON,
			ContractHash:     enc.ContractHash,
			PartitionKey:     o.PartitionKey,
			IdempotencyKey:   o.IdempotencyKey,
			Headers:          o.Headers,
			OccurredAtUnixMs: occurredAt,
			XSbTrace:         traceHeader(ctx),
		},
		fireAndForget: o.FireAndForget,
		done:          make(chan struct{}),
	}

	p.mu.Lock()
	switch {
	case p.closed:
		p.mu.Unlock()
		return "", fmt.Errorf("events: publish %q: %w", name, ErrStopped)
	case len(p.queue) >= p.cfg.MaxPending:
		p.mu.Unlock()
		return "", fmt.Errorf("events: publish %q: %d events pending: %w", name, p.cfg.MaxPending, ErrQueueFull)
	}
	// Minted under the queue lock, so ids rise in queue order.
	id, err := p.cfg.NewID()
	if err != nil {
		p.mu.Unlock()
		return "", fmt.Errorf("events: publish %q: mint id: %w", name, err)
	}
	e.env.Id = id
	p.queue = append(p.queue, e)
	p.mu.Unlock()
	signal(p.wake)

	if e.fireAndForget {
		return id, nil
	}

	timer := time.NewTimer(p.cfg.Timeout)
	defer timer.Stop()
	select {
	case <-e.done:
	case <-timer.C:
		if sent, settled := p.abandon(e); !settled {
			if sent {
				return "", fmt.Errorf("events: publish %q (%s): %w", name, id, ErrOutcomeUnknown)
			}
			return "", fmt.Errorf("events: publish %q (%s): %w", name, id, ErrNotSent)
		}
	case <-ctx.Done():
		if _, settled := p.abandon(e); !settled {
			return "", fmt.Errorf("events: publish %q (%s): %w", name, id, ctx.Err())
		}
	}
	return e.id, e.err
}

// abandon takes an entry the caller stopped waiting for out of the queue. It
// reports whether the entry was ever sent and whether it settled first, in
// which case its result stands.
func (p *Publisher) abandon(e *entry) (sent, settled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e.resolved {
		return e.sent, true
	}
	e.abandoned = true
	if !e.inflight {
		p.removeLocked(e)
	}
	return e.sent, false
}

// Close stops accepting events, keeps sending until the queue is empty or ctx
// ends, and fails whatever is left with ErrStopped.
func (p *Publisher) Close(ctx context.Context) {
	p.mu.Lock()
	p.closed = true
	empty := len(p.queue) == 0
	idle := p.idle
	p.mu.Unlock()

	if !empty {
		p.Kick()
		select {
		case <-idle:
		case <-ctx.Done():
		}
	}
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()

	p.mu.Lock()
	left := p.queue
	p.queue = nil
	for _, e := range left {
		p.resolveLocked(e, "", fmt.Errorf("events: publish %q (%s): %w", e.env.GetName(), e.env.GetId(), ErrStopped))
	}
	p.mu.Unlock()
}

func (p *Publisher) run(ctx context.Context) {
	defer p.wg.Done()
	rung := 0
	for {
		batch := p.take()
		if len(batch) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-p.wake:
			case <-p.kick:
			}
			continue
		}

		envs := make([]*pb.EventEnvelope, len(batch))
		for i, e := range batch {
			envs[i] = e.env
		}
		reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		resp, err := p.cfg.Publish(reqCtx, &pb.PublishRequest{Events: envs})
		cancel()
		if ctx.Err() != nil {
			p.release(batch)
			return
		}

		if !p.settle(batch, resp, err) {
			rung = 0
			continue
		}
		delay := p.cfg.RetryLadder[min(rung, len(p.cfg.RetryLadder)-1)]
		rung++
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-p.kick:
			rung = 0
		case <-timer.C:
		}
		timer.Stop()
	}
}

// take marks the next batch in flight: queue order, at most BatchSize events,
// at most one event per non-empty partition key. A later event of a key that
// is already in the batch waits for the next one, which is what keeps every
// key in publish order across retries.
func (p *Publisher) take() []*entry {
	p.mu.Lock()
	defer p.mu.Unlock()
	var batch []*entry
	keys := make(map[string]struct{})
	for _, e := range p.queue {
		if len(batch) >= p.cfg.BatchSize {
			break
		}
		if key := e.env.GetPartitionKey(); key != "" {
			if _, busy := keys[key]; busy {
				continue
			}
			keys[key] = struct{}{}
		}
		e.inflight = true
		e.sent = true
		batch = append(batch, e)
	}
	return batch
}

// release puts a batch back without a verdict.
func (p *Publisher) release(batch []*entry) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range batch {
		p.unflightLocked(e)
	}
}

// settle applies the runtime's verdicts and reports whether anything is left
// to retry. A transport failure or a missing or UNSPECIFIED verdict keeps the
// event queued under the same id, so the retry is deduplicated by the runtime.
func (p *Publisher) settle(batch []*entry, resp *pb.PublishResponse, err error) bool {
	if err != nil {
		p.cfg.Logger.Warn("events: publish batch failed, retrying", "events", len(batch), "error", err)
	}
	results := resp.GetResults()
	var violations []PolicyViolation
	retry := false

	p.mu.Lock()
	for i, e := range batch {
		if err != nil || i >= len(results) {
			retry = true
			p.unflightLocked(e)
			continue
		}
		r := results[i]
		name, id := e.env.GetName(), e.env.GetId()
		switch r.GetStatus() {
		case pb.PublishStatus_PUBLISH_STATUS_ACCEPTED:
			p.resolveLocked(e, id, nil)
		case pb.PublishStatus_PUBLISH_STATUS_REJECTED_DUPLICATE:
			// The runtime already holds this publication; its id is the answer.
			original := r.GetEventId()
			if original == "" {
				original = id
			}
			p.resolveLocked(e, original, nil)
		case pb.PublishStatus_PUBLISH_STATUS_REJECTED_CONFLICT:
			p.resolveLocked(e, "", fmt.Errorf("events: publish %q (%s): %s: %w", name, id, r.GetMessage(), ErrConflict))
		case pb.PublishStatus_PUBLISH_STATUS_REJECTED_INVALID_NAME:
			p.resolveLocked(e, "", fmt.Errorf("events: publish %q (%s): %s: %w", name, id, r.GetMessage(), ErrInvalidName))
		case pb.PublishStatus_PUBLISH_STATUS_REJECTED_FORBIDDEN:
			p.resolveLocked(e, "", fmt.Errorf("events: publish %q (%s): %s: %w", name, id, r.GetMessage(), ErrForbidden))
			violations = append(violations, PolicyViolation{EventID: id, EventName: name, Reason: r.GetMessage()})
		default:
			retry = true
			p.cfg.Logger.Warn("events: publish not settled, retrying",
				"event", name, "event_id", id, "message", r.GetMessage())
			p.unflightLocked(e)
		}
	}
	p.mu.Unlock()

	if p.cfg.OnPolicyViolation != nil {
		for _, v := range violations {
			p.cfg.OnPolicyViolation(v)
		}
	}
	return retry
}

// unflightLocked returns an entry to the queue for the next batch, or drops it
// if its caller has already given up on it.
func (p *Publisher) unflightLocked(e *entry) {
	e.inflight = false
	if e.abandoned {
		p.removeLocked(e)
	}
}

func (p *Publisher) resolveLocked(e *entry, id string, err error) {
	if e.resolved {
		return
	}
	e.resolved, e.inflight = true, false
	e.id, e.err = id, err
	p.removeLocked(e)
	close(e.done)
	if err != nil && e.fireAndForget {
		p.cfg.Logger.Warn("events: fire-and-forget publish failed",
			"event", e.env.GetName(), "event_id", e.env.GetId(), "error", err)
	}
}

func (p *Publisher) removeLocked(e *entry) {
	for i, q := range p.queue {
		if q == e {
			p.queue = append(p.queue[:i], p.queue[i+1:]...)
			break
		}
	}
	if len(p.queue) == 0 {
		close(p.idle)
		p.idle = make(chan struct{})
	}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// traceHeader renders the trace ctx carries. Empty when ctx has none, in which
// case the runtime mints a fresh root trace on ingest.
func traceHeader(ctx context.Context) string {
	tc, ok := telemetry.FromContext(ctx)
	if !ok {
		return ""
	}
	return telemetry.FormatHeader(tc)
}

func newEventID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("mint uuidv7: %w", err)
	}
	return id.String(), nil
}
