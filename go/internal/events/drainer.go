package events

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/service-bridge/sdk/go/internal/outbox"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
)

// DefaultBatchSize is how many rows one drain iteration claims.
const DefaultBatchSize = 100

// DefaultPublishTimeout bounds one Events.Publish call.
const DefaultPublishTimeout = 30 * time.Second

// RetryLadder is the delay between drain attempts for one row. It is a rate
// limiter, not a budget: the last rung repeats forever. A ladder that expired
// would mark every buffered event failed after a few minutes of downtime — and
// the publish that buffered them already returned success and promised
// durability.
func RetryLadder() stream.Backoff {
	return stream.NewBackoff(
		stream.WithLadder(
			1*time.Second,
			5*time.Second,
			30*time.Second,
			2*time.Minute,
			10*time.Minute,
		),
		stream.WithJitterRatio(0.25),
	)
}

// PolicyViolation reports an event the runtime refused on policy grounds. The
// publish that buffered it returned long ago, so this is the only way the
// denial reaches the owner.
type PolicyViolation struct {
	EventID   string
	EventName string
	Reason    string
}

// DrainerConfig wires the drain loop. See ./README.md.
type DrainerConfig struct {
	Storage  *outbox.Storage
	Publish  PublishFunc
	Identity func() Identity
	// BatchSize defaults to DefaultBatchSize.
	BatchSize int
	// Backoff pins the retry ladder. Zero value falls back to RetryLadder().
	Backoff stream.Backoff
	// PublishTimeout defaults to DefaultPublishTimeout.
	PublishTimeout time.Duration
	// OnPolicyViolation surfaces a terminal policy denial to the owner. It runs
	// on the drain goroutine after the batch is committed.
	OnPolicyViolation func(PolicyViolation)
	// OnError reports storage failures. The loop keeps running.
	OnError func(error)
	// Now returns unix-ms; defaults to the wall clock.
	Now func() int64
	// Sleep waits out d or returns early when ctx ends. Defaults to a timer.
	Sleep  func(ctx context.Context, d time.Duration)
	Logger *slog.Logger
}

// Drainer moves buffered events to the runtime. One goroutine owns the loop;
// Stop cancels it and waits.
type Drainer struct {
	cfg DrainerConfig

	// kick is edge-triggered and coalescing. Buffered so a wake raised while an
	// iteration runs survives until the next wait instead of being lost.
	kick chan struct{}

	wg sync.WaitGroup

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
}

// NewDrainer validates the config and fills its defaults.
func NewDrainer(cfg DrainerConfig) (*Drainer, error) {
	if cfg.Storage == nil {
		return nil, fmt.Errorf("events: new drainer: missing Storage: %w", ErrInvalidConfig)
	}
	if cfg.Publish == nil {
		return nil, fmt.Errorf("events: new drainer: missing Publish: %w", ErrInvalidConfig)
	}
	if cfg.Identity == nil {
		return nil, fmt.Errorf("events: new drainer: missing Identity: %w", ErrInvalidConfig)
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if len(cfg.Backoff.Rungs()) == 0 {
		cfg.Backoff = RetryLadder()
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = DefaultPublishTimeout
	}
	if cfg.Now == nil {
		cfg.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Drainer{cfg: cfg, kick: make(chan struct{}, 1)}, nil
}

// Start launches the drain goroutine. Single-use: Stop is terminal.
func (d *Drainer) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return fmt.Errorf("events: start drainer: %w", ErrAlreadyStarted)
	}
	runCtx, cancel := context.WithCancel(ctx)
	d.started = true
	d.cancel = cancel
	d.mu.Unlock()

	d.wg.Add(1)
	go d.run(runCtx)
	return nil
}

// Stop cancels the loop and waits for it. Idempotent.
func (d *Drainer) Stop() {
	d.mu.Lock()
	cancel := d.cancel
	d.cancel = nil
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	d.wg.Wait()
}

// Kick wakes an idle drain. Non-blocking and coalescing.
func (d *Drainer) Kick() {
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

func (d *Drainer) run(ctx context.Context) {
	defer d.wg.Done()

	errAttempt := 0
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := d.drainOnce(ctx)
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			d.reportError(err)
			// A storage failure must not spin the loop: back off like any other
			// failure and try again.
			d.cfg.Sleep(ctx, d.cfg.Backoff.Delay(errAttempt))
			errAttempt++
		case n > 0:
			errAttempt = 0
		default:
			errAttempt = 0
			d.waitForWork(ctx)
		}
	}
}

// drainOnce claims one batch, hands it to the runtime and applies the outcome.
// It returns how many rows it claimed.
func (d *Drainer) drainOnce(ctx context.Context) (int, error) {
	recs, err := d.cfg.Storage.ClaimDue(ctx, d.cfg.Now(), d.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("events: drain: claim: %w", err)
	}
	if len(recs) == 0 {
		return 0, nil
	}

	resp, sendErr := d.send(ctx, recs)
	result, violations := d.classify(recs, resp, sendErr)

	if err := d.cfg.Storage.Complete(ctx, result); err != nil {
		// The batch stays inflight until the next Open resets it. Nothing is
		// lost, but nothing moves either, so this is loud.
		return 0, fmt.Errorf("events: drain: complete: %w", err)
	}

	for _, v := range violations {
		d.cfg.Logger.Warn("events: drain: event rejected by policy",
			"event_id", v.EventID, "event_name", v.EventName, "reason", v.Reason)
		if d.cfg.OnPolicyViolation != nil {
			d.cfg.OnPolicyViolation(v)
		}
	}
	return len(recs), nil
}

func (d *Drainer) send(ctx context.Context, recs []outbox.Record) (*pb.PublishResponse, error) {
	ident := d.cfg.Identity()
	events := make([]*pb.EventEnvelope, len(recs))
	for i, r := range recs {
		events[i] = &pb.EventEnvelope{
			Id:               r.ID,
			Name:             r.Name,
			Payload:          r.Payload,
			PayloadJson:      r.PayloadJSON,
			ContractHash:     r.ContractHash,
			PartitionKey:     r.PartitionKey,
			IdempotencyKey:   r.IdempotencyKey,
			FireAndForget:    r.FireAndForget,
			Headers:          r.Headers,
			OccurredAtUnixMs: r.OccurredAtMs,
			XSbTrace:         r.Trace,
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, d.cfg.PublishTimeout)
	defer cancel()

	resp, err := d.cfg.Publish(callCtx, &pb.PublishRequest{
		PublisherServiceId:  ident.ServiceID,
		PublisherInstanceId: ident.InstanceID,
		Events:              events,
	})
	if err != nil {
		return nil, fmt.Errorf("events: drain: publish %d event(s): %w", len(recs), err)
	}
	return resp, nil
}

// classify turns one runtime answer into the row updates it implies.
//
// A transport failure says nothing about any single event, so it never parks a
// row: every row is re-armed on the saturating ladder and retried for as long
// as the outbox lives. Only a per-envelope verdict the runtime will repeat —
// a name it rejects, a policy that denies the publish — is terminal.
func (d *Drainer) classify(recs []outbox.Record, resp *pb.PublishResponse, sendErr error) (outbox.Result, []PolicyViolation) {
	var (
		result     outbox.Result
		violations []PolicyViolation
	)

	if sendErr != nil {
		d.cfg.Logger.Warn("events: drain: batch failed, retrying",
			"events", len(recs), "err", sendErr)
		for _, r := range recs {
			result.Retry = append(result.Retry, d.rearm(r, sendErr.Error()))
		}
		return result, nil
	}

	byID := make(map[string]*pb.PublishStatusEntry, len(resp.GetResults()))
	for _, entry := range resp.GetResults() {
		byID[entry.GetEventId()] = entry
	}

	transient := 0
	for _, r := range recs {
		entry := byID[r.ID]
		switch entry.GetStatus() {
		case pb.PublishStatus_PUBLISH_STATUS_ACCEPTED,
			pb.PublishStatus_PUBLISH_STATUS_REJECTED_DUPLICATE:
			result.Done = append(result.Done, r.ID)

		case pb.PublishStatus_PUBLISH_STATUS_REJECTED_INVALID_NAME:
			result.Failed = append(result.Failed, outbox.Failure{
				ID:        r.ID,
				Attempts:  r.Attempts + 1,
				LastError: "invalid name: " + entry.GetMessage(),
			})

		case pb.PublishStatus_PUBLISH_STATUS_REJECTED_FORBIDDEN:
			reason := entry.GetMessage()
			if reason == "" {
				reason = "event.publish denied by policy"
			}
			result.Failed = append(result.Failed, outbox.Failure{
				ID:        r.ID,
				Attempts:  r.Attempts + 1,
				LastError: "forbidden: " + reason,
			})
			violations = append(violations, PolicyViolation{
				EventID:   r.ID,
				EventName: r.Name,
				Reason:    reason,
			})

		default:
			// Unspecified, or no verdict at all for this id: the runtime said
			// nothing about the event, which is the transport case again.
			transient++
			result.Retry = append(result.Retry, d.rearm(r, "unspecified status"))
		}
	}
	if transient > 0 {
		d.cfg.Logger.Warn("events: drain: no verdict, retrying", "events", transient)
	}
	return result, violations
}

// rearm schedules one row for a later attempt. attempts only picks the ladder
// rung — it is never compared against a limit.
func (d *Drainer) rearm(r outbox.Record, lastError string) outbox.Retry {
	attempts := r.Attempts + 1
	delay := d.cfg.Backoff.Delay(int(r.Attempts))
	return outbox.Retry{
		ID:              r.ID,
		Attempts:        attempts,
		LastError:       lastError,
		NextAttemptAtMs: d.cfg.Now() + delay.Milliseconds(),
	}
}

// waitForWork parks until a publish kicks the loop or the earliest deferred row
// comes due. With nothing pending it waits on the kick alone — an idle SDK arms
// no timer.
func (d *Drainer) waitForWork(ctx context.Context) {
	select {
	case <-d.kick:
		// A publish landed while the iteration ran. Re-query at once.
		return
	default:
	}

	dueAt, ok, err := d.cfg.Storage.NextDueAt(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		d.reportError(fmt.Errorf("events: drain: next due: %w", err))
		d.waitKick(ctx, d.cfg.Backoff.Delay(0))
		return
	}
	if !ok {
		d.waitKick(ctx, 0)
		return
	}
	wait := dueAt - d.cfg.Now()
	if wait <= 0 {
		return
	}
	d.waitKick(ctx, time.Duration(wait)*time.Millisecond)
}

// waitKick blocks until the kick fires, ctx ends, or d elapses. A non-positive
// d means no timer at all.
func (d *Drainer) waitKick(ctx context.Context, wait time.Duration) {
	if wait <= 0 {
		select {
		case <-d.kick:
		case <-ctx.Done():
		}
		return
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-d.kick:
	case <-t.C:
	case <-ctx.Done():
	}
}

func (d *Drainer) reportError(err error) {
	d.cfg.Logger.Error("events: drain", "err", err)
	if d.cfg.OnError != nil {
		d.cfg.OnError(err)
	}
}

func sleepCtx(ctx context.Context, wait time.Duration) {
	if wait <= 0 {
		return
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
