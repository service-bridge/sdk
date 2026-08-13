package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// ErrAlreadyStarted is returned by Start on a supervisor that is already
// running. A supervisor is single-use: Stop is terminal.
var ErrAlreadyStarted = errors.New("supervisor already started")

// ErrInvalidConfig is returned by NewSupervisor when a required hook is missing.
var ErrInvalidConfig = errors.New("invalid supervisor config")

// Receiver is the receive side of one long-lived stream. Every gRPC
// server-streaming and bidi client stub satisfies it.
type Receiver[M any] interface {
	Recv() (M, error)
}

// Config carries everything the domain must supply. See ./README.md.
type Config[M any, S Receiver[M]] struct {
	// Name identifies the stream in logs.
	Name string
	// Open builds a fresh stream bound to ctx. Cancelling ctx must unblock Recv.
	// An error — no identity yet, runtime unreachable, channel torn down
	// mid-rotation — is a normal break: the supervisor goes on the ladder.
	Open func(ctx context.Context) (S, error)
	// OnData receives every frame of the current stream together with the stream
	// itself, so a bidi handler can write back on the same call. It runs on the
	// supervisor goroutine: a blocking handler blocks the whole lifecycle.
	OnData func(ctx context.Context, msg M, stream S)
	// OnError reports stream-level failures. Notification only — the supervisor
	// owns the reconnect decision. A clean close is not an error and is not
	// reported here.
	OnError func(err error)
	// OnBackoff reports the delay chosen before the next reopen.
	OnBackoff func(attempt int, delay time.Duration)
	// Backoff pins the reconnect ladder. Zero value falls back to NewBackoff().
	Backoff Backoff
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// Supervisor drives one stream through open → receive → break → wait on the
// ladder → reopen, until its context is cancelled. See ./README.md for the
// three invariants it exists to enforce.
type Supervisor[M any, S Receiver[M]] struct {
	cfg Config[M, S]

	restart chan struct{}
	events  chan streamEvent[M]

	wg sync.WaitGroup

	mu      sync.Mutex
	started bool
	stopped bool
	cancel  context.CancelFunc
	cur     S
	hasCur  bool
}

// streamEvent carries one frame or one termination, tagged with the generation
// of the stream that produced it.
type streamEvent[M any] struct {
	gen uint64
	msg M
	err error
}

type outcome int

const (
	outcomeStopped outcome = iota
	outcomeBroken
	outcomeRestarted
)

// NewSupervisor validates the config and builds an idle supervisor.
func NewSupervisor[M any, S Receiver[M]](cfg Config[M, S]) (*Supervisor[M, S], error) {
	if cfg.Open == nil {
		return nil, fmt.Errorf("stream: new supervisor %q: missing Open: %w", cfg.Name, ErrInvalidConfig)
	}
	if cfg.OnData == nil {
		return nil, fmt.Errorf("stream: new supervisor %q: missing OnData: %w", cfg.Name, ErrInvalidConfig)
	}
	if cfg.Backoff.isZero() {
		cfg.Backoff = NewBackoff()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Supervisor[M, S]{
		cfg:     cfg,
		restart: make(chan struct{}, 1),
		events:  make(chan streamEvent[M], 1),
	}, nil
}

// Start launches the lifecycle goroutine. Cancelling ctx stops the supervisor
// exactly like Stop, minus the wait for the goroutines to finish.
func (s *Supervisor[M, S]) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("stream: start %q: %w", s.cfg.Name, ErrAlreadyStarted)
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.started = true
	s.cancel = cancel
	s.mu.Unlock()

	s.wg.Add(1)
	go s.run(runCtx)
	return nil
}

// Stop cancels the lifecycle and waits for every goroutine it owns. Terminal:
// a stopped supervisor cannot be started again.
func (s *Supervisor[M, S]) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.stopped = true
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
	s.clearCurrent()
}

// Restart drops the live stream and reopens immediately on a fresh ladder. Used
// when the stream's own parameters went stale — a rotated identity, a changed
// declaration — so waiting out the current rung would serve a dead stream.
// Non-blocking and coalescing: concurrent calls collapse into one restart.
func (s *Supervisor[M, S]) Restart() {
	select {
	case s.restart <- struct{}{}:
	default:
	}
}

// Current exposes the live stream for writes. The second result is false
// between a break and the next successful open.
func (s *Supervisor[M, S]) Current() (S, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur, s.hasCur
}

func (s *Supervisor[M, S]) run(ctx context.Context) {
	defer s.wg.Done()

	attempt := 0
	gen := uint64(0)
	for {
		if ctx.Err() != nil {
			return
		}
		gen++

		streamCtx, cancelStream := context.WithCancel(ctx)
		st, err := s.cfg.Open(streamCtx)
		if err != nil {
			cancelStream()
			s.reportError(fmt.Errorf("stream: %s: open: %w", s.cfg.Name, err))
			if !s.wait(ctx, &attempt) {
				return
			}
			continue
		}

		s.setCurrent(st)
		s.wg.Add(1)
		go s.pump(ctx, gen, st)

		res, recvErr := s.serve(ctx, streamCtx, gen, st, &attempt)
		cancelStream()
		s.clearCurrent()

		switch res {
		case outcomeStopped:
			return
		case outcomeRestarted:
			// A restart means the parameters changed, not that the runtime is
			// unhealthy — the ladder must not carry over.
			attempt = 0
		case outcomeBroken:
			if recvErr != nil && !errors.Is(recvErr, io.EOF) {
				s.reportError(fmt.Errorf("stream: %s: recv: %w", s.cfg.Name, recvErr))
			}
			if !s.wait(ctx, &attempt) {
				return
			}
		}
	}
}

// serve consumes one stream generation until it breaks, a restart is requested
// or the supervisor is cancelled.
func (s *Supervisor[M, S]) serve(ctx, streamCtx context.Context, gen uint64, st S, attempt *int) (outcome, error) {
	for {
		select {
		case <-ctx.Done():
			return outcomeStopped, nil
		case <-s.restart:
			return outcomeRestarted, nil
		case ev := <-s.events:
			// Identity guard. A replaced stream keeps draining: its Recv unblocks
			// only after the cancel propagates, and it still delivers whatever the
			// transport had buffered. Acting on those frames would let a dead
			// stream tear down the live one — the live stream becomes
			// uncancellable and the runtime rejects the next bind with
			// ALREADY_EXISTS, looping forever.
			if ev.gen != gen {
				continue
			}
			if ev.err != nil {
				return outcomeBroken, ev.err
			}
			// Progress — and only progress — proves the stream is usable. A clean
			// close never resets the ladder: a runtime that keeps closing streams
			// gracefully would otherwise be reopened once per second forever.
			*attempt = 0
			s.cfg.OnData(streamCtx, ev.msg, st)
		}
	}
}

// pump moves frames off one stream generation onto the shared event channel.
// Owned by run: it exits when Recv fails or when the supervisor context is
// cancelled, and Stop waits for it through the WaitGroup.
func (s *Supervisor[M, S]) pump(ctx context.Context, gen uint64, st S) {
	defer s.wg.Done()
	for {
		msg, err := st.Recv()
		select {
		case s.events <- streamEvent[M]{gen: gen, msg: msg, err: err}:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// wait sleeps out the ladder rung for the current attempt. Returns false when
// the supervisor was cancelled while waiting. A restart during the wait resets
// the ladder and reopens immediately.
func (s *Supervisor[M, S]) wait(ctx context.Context, attempt *int) bool {
	d := s.cfg.Backoff.Delay(*attempt)
	if s.cfg.OnBackoff != nil {
		s.cfg.OnBackoff(*attempt, d)
	}
	s.cfg.Logger.Debug("stream: reconnect scheduled",
		"stream", s.cfg.Name, "attempt", *attempt, "delay_ms", d.Milliseconds())
	*attempt++

	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-s.restart:
		*attempt = 0
		return true
	case <-t.C:
		return true
	}
}

func (s *Supervisor[M, S]) reportError(err error) {
	s.cfg.Logger.Warn("stream: broken", "stream", s.cfg.Name, "err", err)
	if s.cfg.OnError != nil {
		s.cfg.OnError(err)
	}
}

func (s *Supervisor[M, S]) setCurrent(st S) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur = st
	s.hasCur = true
}

func (s *Supervisor[M, S]) clearCurrent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	var zero S
	s.cur = zero
	s.hasCur = false
}
