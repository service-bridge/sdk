// Package events owns durable event publication and subscription: the local
// outbox write, the drain that hands batches to the runtime, and the inbound
// delivery stream.
package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/service-bridge/sdk/go/internal/outbox"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
)

// DefaultMaxOutboxRows caps the local buffer. Past it a publish fails loudly
// instead of growing the file without bound.
const DefaultMaxOutboxRows = 10_000

// eventNameRE accepts dot-separated segments of lowercase alphanumerics,
// underscores and hyphens. It mirrors runtime/internal/events/event_name.go: a
// name the runtime would reject must fail where it was written, not after a
// round trip.
var eventNameRE = regexp.MustCompile(`^[a-z0-9_-]+(\.[a-z0-9_-]+)*$`)

// Sentinels for errors.Is.
var (
	// ErrInvalidName marks a name the runtime would reject.
	ErrInvalidName = errors.New("events: invalid event name")
	// ErrOutboxFull marks a publish refused because the local buffer is at its
	// cap. It is outbox.ErrFull so callers match either spelling.
	ErrOutboxFull = outbox.ErrFull
	// ErrInvalidConfig marks a missing required dependency.
	ErrInvalidConfig = errors.New("events: invalid config")
	// ErrAlreadyStarted marks a second Start on a single-use component.
	ErrAlreadyStarted = errors.New("events: already started")
)

// ValidEventName reports whether the runtime would accept name.
func ValidEventName(name string) bool { return eventNameRE.MatchString(name) }

// Identity is the live session identity stamped on outgoing frames. Read per
// use: instance_id changes on every certificate rotation.
type Identity struct {
	ServiceID  string
	InstanceID string
}

// Encoded is the pair of wire forms one payload takes plus its contract hash.
type Encoded struct {
	// Proto is the canonical payload; the runtime treats it as opaque bytes.
	Proto []byte
	// JSON mirrors Proto so the runtime can evaluate JSON-path wait_event
	// filters without decoding protobuf. Empty when the payload has no JSON
	// form.
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
	// FireAndForget sends straight to the runtime instead of buffering. The
	// call fails with the transport error; nothing is retried.
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

// WithPartitionKey pins the event to a FIFO lane: consumers process events
// sharing a key strictly in order.
func WithPartitionKey(key string) PublishOption {
	return func(o *PublishOptions) { o.PartitionKey = key }
}

// WithFireAndForget skips the outbox. The event is lost if the runtime is
// unreachable — durability is exactly what the outbox provides.
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

// PublisherConfig wires the publish path. See ./README.md.
type PublisherConfig struct {
	Storage  *outbox.Storage
	Codec    Codec
	Publish  PublishFunc
	Identity func() Identity
	// Kick wakes the drain after an enqueue. Optional.
	Kick func()
	// MaxOutboxRows caps the local buffer; zero takes DefaultMaxOutboxRows. An
	// uncapped buffer is not on offer: it turns a long outage into a full disk.
	MaxOutboxRows int
	// Now returns unix-ms; defaults to the wall clock.
	Now func() int64
	// NewID mints the event identifier; defaults to UUIDv7.
	NewID  func() (string, error)
	Logger *slog.Logger
}

// Publisher writes events into the local outbox, or sends them straight to the
// runtime on the no-wait path.
type Publisher struct {
	cfg PublisherConfig
}

// NewPublisher validates the config and fills its defaults.
func NewPublisher(cfg PublisherConfig) (*Publisher, error) {
	if cfg.Storage == nil {
		return nil, fmt.Errorf("events: new publisher: missing Storage: %w", ErrInvalidConfig)
	}
	if cfg.Codec == nil {
		return nil, fmt.Errorf("events: new publisher: missing Codec: %w", ErrInvalidConfig)
	}
	if cfg.Publish == nil {
		return nil, fmt.Errorf("events: new publisher: missing Publish: %w", ErrInvalidConfig)
	}
	if cfg.Identity == nil {
		return nil, fmt.Errorf("events: new publisher: missing Identity: %w", ErrInvalidConfig)
	}
	if cfg.MaxOutboxRows < 0 {
		return nil, fmt.Errorf("events: new publisher: negative MaxOutboxRows: %w", ErrInvalidConfig)
	}
	if cfg.MaxOutboxRows == 0 {
		cfg.MaxOutboxRows = DefaultMaxOutboxRows
	}
	if cfg.Kick == nil {
		cfg.Kick = func() {}
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
	return &Publisher{cfg: cfg}, nil
}

// Publish buffers one event and returns its identifier. It is a local insert,
// not a network call: the runtime being down must not slow a publish down or
// fail it.
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
	id, err := p.cfg.NewID()
	if err != nil {
		return "", fmt.Errorf("events: publish %q: mint id: %w", name, err)
	}
	occurredAt := o.OccurredAtMs
	if occurredAt == 0 {
		occurredAt = p.cfg.Now()
	}
	trace := traceHeader(ctx)

	if o.FireAndForget {
		if err := p.sendNow(ctx, id, name, enc, o, occurredAt, trace); err != nil {
			return "", err
		}
		return id, nil
	}

	rec := outbox.Record{
		ID:             id,
		Name:           name,
		Payload:        enc.Proto,
		PayloadJSON:    enc.JSON,
		ContractHash:   enc.ContractHash,
		PartitionKey:   o.PartitionKey,
		IdempotencyKey: o.IdempotencyKey,
		Headers:        o.Headers,
		OccurredAtMs:   occurredAt,
		EnqueuedAtMs:   p.cfg.Now(),
		Trace:          trace,
	}
	if err := p.cfg.Storage.Enqueue(ctx, rec, p.cfg.MaxOutboxRows); err != nil {
		return "", fmt.Errorf("events: publish %q: %w", name, err)
	}
	p.cfg.Kick()
	return id, nil
}

// sendNow is the no-wait path: one envelope straight to the runtime, no buffer,
// no retry.
func (p *Publisher) sendNow(ctx context.Context, id, name string, enc Encoded, o PublishOptions, occurredAt int64, trace string) error {
	ident := p.cfg.Identity()
	env := &pb.EventEnvelope{
		Id:               id,
		Name:             name,
		Payload:          enc.Proto,
		PayloadJson:      enc.JSON,
		ContractHash:     enc.ContractHash,
		PartitionKey:     o.PartitionKey,
		IdempotencyKey:   o.IdempotencyKey,
		FireAndForget:    true,
		Headers:          o.Headers,
		OccurredAtUnixMs: occurredAt,
		XSbTrace:         trace,
	}
	resp, err := p.cfg.Publish(ctx, &pb.PublishRequest{
		PublisherServiceId:  ident.ServiceID,
		PublisherInstanceId: ident.InstanceID,
		Events:              []*pb.EventEnvelope{env},
	})
	if err != nil {
		return fmt.Errorf("events: publish %q: send: %w", name, err)
	}
	for _, r := range resp.GetResults() {
		if r.GetEventId() != id && r.GetEventId() != "" {
			continue
		}
		if terminalStatus(r.GetStatus()) {
			return fmt.Errorf("events: publish %q: rejected %s: %s: %w",
				name, r.GetStatus(), r.GetMessage(), ErrRejected)
		}
	}
	return nil
}

// ErrRejected marks an envelope the runtime refused for a reason no retry
// fixes: a name it will never accept, or a policy that denies the publish.
var ErrRejected = errors.New("events: rejected by runtime")

// terminalStatus reports whether a per-envelope status is worth no retry.
func terminalStatus(s pb.PublishStatus) bool {
	switch s {
	case pb.PublishStatus_PUBLISH_STATUS_REJECTED_INVALID_NAME,
		pb.PublishStatus_PUBLISH_STATUS_REJECTED_FORBIDDEN:
		return true
	default:
		return false
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
