package sbtest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/serde"
)

// PublishedEvent is one event the code under test published, as the runtime
// received it.
type PublishedEvent struct {
	ID             string
	Name           string
	Payload        []byte
	PayloadJSON    []byte
	PartitionKey   string
	IdempotencyKey string
	Headers        map[string]string
	OccurredAtMs   int64
}

// DecodePublished reads a published payload back as T.
func DecodePublished[T proto.Message](e PublishedEvent) (T, error) {
	var zero T
	out := serde.New(zero)
	if err := serde.Decode(e.Payload, out); err != nil {
		return zero, fmt.Errorf("sbtest: decode published %s: %w", e.Name, err)
	}
	return out, nil
}

// Published returns the events the runtime acknowledged, in order. Every one
// went through the client's real publish queue; the in-memory runtime accepts
// each envelope.
func (h *Harness) Published() []PublishedEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]PublishedEvent(nil), h.published...)
}

func (h *Harness) publish(_ context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	resp := &pb.PublishResponse{}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, env := range req.GetEvents() {
		h.published = append(h.published, PublishedEvent{
			ID:             env.GetId(),
			Name:           env.GetName(),
			Payload:        env.GetPayload(),
			PayloadJSON:    env.GetPayloadJson(),
			PartitionKey:   env.GetPartitionKey(),
			IdempotencyKey: env.GetIdempotencyKey(),
			Headers:        env.GetHeaders(),
			OccurredAtMs:   env.GetOccurredAtUnixMs(),
		})
		resp.Results = append(resp.Results, &pb.PublishStatusEntry{
			EventId: env.GetId(),
			Status:  pb.PublishStatus_PUBLISH_STATUS_ACCEPTED,
		})
	}
	return resp, nil
}

// DeliveryResult is what the subscriber answered.
type DeliveryResult struct {
	Acked bool
	// Reason is the nack reason; empty when acked.
	Reason string
	// MatchedPatterns are the patterns the delivery carried.
	MatchedPatterns []string
}

// DeliverOption tunes one simulated delivery.
type DeliverOption func(*pb.EventDelivery)

// WithMatchedPatterns sets the patterns the delivery names instead of
// computing them. Use it to reproduce what a filter on the runtime decided.
func WithMatchedPatterns(patterns ...string) DeliverOption {
	return func(d *pb.EventDelivery) { d.MatchedPatterns = patterns }
}

// WithAttempt sets the delivery attempt (1 by default).
func WithAttempt(n int32) DeliverOption {
	return func(d *pb.EventDelivery) { d.Attempt = n }
}

// WithDeliveryPartitionKey sets the envelope's partition key.
func WithDeliveryPartitionKey(key string) DeliverOption {
	return func(d *pb.EventDelivery) { d.Envelope.PartitionKey = key }
}

// WithDeliveryHeaders sets the envelope's headers.
func WithDeliveryHeaders(headers map[string]string) DeliverOption {
	return func(d *pb.EventDelivery) { d.Envelope.Headers = headers }
}

// Deliver hands one event to the client's subscriber the way the runtime
// does. The delivery names the subscription patterns the event name matches —
// computed here with the runtime's rules (`*` one segment, `#` zero or more),
// filters are not evaluated — and the subscriber runs the handlers of exactly
// those patterns. The payload goes through the real encoding; the handler gets
// it decoded into its own type, and DeliveryFromContext works.
func (h *Harness) Deliver(ctx context.Context, name string, payload proto.Message, opts ...DeliverOption) (DeliveryResult, error) {
	enc, err := serde.Encode(payload)
	if err != nil {
		return DeliveryResult{}, fmt.Errorf("sbtest: deliver %s: encode: %w", name, err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return DeliveryResult{}, fmt.Errorf("sbtest: deliver %s: mint id: %w", name, err)
	}
	d := &pb.EventDelivery{
		DeliveryId: uuid.NewString(),
		LeaseToken: uuid.NewString(),
		Attempt:    1,
		Envelope: &pb.EventEnvelope{
			Id:               id.String(),
			Name:             name,
			Payload:          enc.Proto,
			PayloadJson:      enc.JSON,
			ContractHash:     enc.ContractHash,
			OccurredAtUnixMs: time.Now().UnixMilli(),
		},
	}
	for _, sub := range h.mem.Subscriptions() {
		if MatchPattern(sub.Pattern, name) {
			d.MatchedPatterns = append(d.MatchedPatterns, sub.Pattern)
		}
	}
	for _, opt := range opts {
		opt(d)
	}
	acked, reason := h.mem.Deliver(ctx, d)
	return DeliveryResult{Acked: acked, Reason: reason, MatchedPatterns: d.MatchedPatterns}, nil
}

// MatchPattern reports whether the runtime routes an event name to a
// subscription pattern: segments separated by dots, `*` exactly one segment,
// `#` zero or more. It stands in for the runtime here; the client itself never
// matches patterns.
func MatchPattern(pattern, name string) bool {
	return matchSegments(strings.Split(pattern, "."), strings.Split(name, "."))
}

func matchSegments(pat, seg []string) bool {
	if len(pat) == 0 {
		return len(seg) == 0
	}
	switch pat[0] {
	case "#":
		for i := 0; i <= len(seg); i++ {
			if matchSegments(pat[1:], seg[i:]) {
				return true
			}
		}
		return false
	case "*":
		return len(seg) > 0 && matchSegments(pat[1:], seg[1:])
	default:
		return len(seg) > 0 && pat[0] == seg[0] && matchSegments(pat[1:], seg[1:])
	}
}
