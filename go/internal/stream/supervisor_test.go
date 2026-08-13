package stream_test

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/stream"
)

// frame is one Recv result the test hands to a fake stream.
type frame struct {
	msg string
	err error
}

// fakeStream replays scripted Recv results. ignoreCancel models the real
// hazard the identity guard exists for: a cancelled transport keeps delivering
// whatever it had buffered instead of stopping on the spot.
type fakeStream struct {
	ctx          context.Context
	frames       chan frame
	halt         chan struct{}
	ignoreCancel bool
}

func newFake(ctx context.Context, capacity int) *fakeStream {
	return &fakeStream{
		ctx:    ctx,
		frames: make(chan frame, capacity),
		halt:   make(chan struct{}),
	}
}

func (f *fakeStream) Recv() (string, error) {
	if f.ignoreCancel {
		select {
		case fr := <-f.frames:
			return fr.msg, fr.err
		case <-f.halt:
			return "", io.EOF
		}
	}
	select {
	case fr := <-f.frames:
		return fr.msg, fr.err
	case <-f.ctx.Done():
		return "", f.ctx.Err()
	case <-f.halt:
		return "", io.EOF
	}
}

func testLadder() stream.Backoff {
	return stream.NewBackoff(
		stream.WithLadder(
			1*time.Millisecond,
			2*time.Millisecond,
			3*time.Millisecond,
			4*time.Millisecond,
			5*time.Millisecond,
		),
		stream.WithJitterRatio(0),
	)
}

func recvWithin[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// TestSupervisorCleanCloseClimbsLadder is the regression guard for the bug that
// bit the Node SDK twice: a stream the runtime closes cleanly must still walk up
// the ladder. Resetting on a clean close turns an orderly runtime shutdown into
// a once-per-second reconnect storm that never backs off.
func TestSupervisorCleanCloseClimbsLadder(t *testing.T) {
	delays := make(chan time.Duration, 16)
	open := func(ctx context.Context) (*fakeStream, error) {
		fs := newFake(ctx, 1)
		fs.frames <- frame{err: io.EOF}
		return fs, nil
	}

	sup, err := stream.NewSupervisor(stream.Config[string, *fakeStream]{
		Name:      "test",
		Open:      open,
		OnData:    func(context.Context, string, *fakeStream) {},
		OnBackoff: func(_ int, d time.Duration) { delays <- d },
		Backoff:   testLadder(),
	})
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	if err := sup.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sup.Stop()

	want := []time.Duration{1, 2, 3, 4, 5, 5}
	for i, ms := range want {
		got := recvWithin(t, delays, "backoff notification")
		if got != time.Duration(ms)*time.Millisecond {
			t.Fatalf("clean close %d: delay %s, want %dms", i, got, ms)
		}
	}
}

// TestSupervisorDataFrameResetsLadder pins the other half of the same
// invariant: progress, and only progress, drops the supervisor back to the
// first rung.
func TestSupervisorDataFrameResetsLadder(t *testing.T) {
	delays := make(chan time.Duration, 16)
	data := make(chan string, 4)
	var opened atomic.Int64

	open := func(ctx context.Context) (*fakeStream, error) {
		n := opened.Add(1)
		fs := newFake(ctx, 2)
		if n == 3 {
			fs.frames <- frame{msg: "progress"}
		}
		fs.frames <- frame{err: io.EOF}
		return fs, nil
	}

	sup, err := stream.NewSupervisor(stream.Config[string, *fakeStream]{
		Name:      "test",
		Open:      open,
		OnData:    func(_ context.Context, msg string, _ *fakeStream) { data <- msg },
		OnBackoff: func(_ int, d time.Duration) { delays <- d },
		Backoff:   testLadder(),
	})
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	if err := sup.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sup.Stop()

	// Two clean closes climb to the second rung; the third stream delivers a
	// frame before closing, so the fourth wait starts from the first rung again.
	want := []time.Duration{1, 2, 1, 2}
	for i, ms := range want {
		got := recvWithin(t, delays, "backoff notification")
		if got != time.Duration(ms)*time.Millisecond {
			t.Fatalf("break %d: delay %s, want %dms", i, got, ms)
		}
	}
	if msg := recvWithin(t, data, "data frame"); msg != "progress" {
		t.Fatalf("got frame %q, want %q", msg, "progress")
	}
}

// TestSupervisorIgnoresReplacedStream covers the second Node bug: a frame — or a
// termination — arriving from a stream that was already replaced must not touch
// the live one. Without the guard the stale termination nulls out the live
// stream, which then can neither be cancelled nor rebound (the runtime answers
// ALREADY_EXISTS), and the supervisor loops forever.
func TestSupervisorIgnoresReplacedStream(t *testing.T) {
	opens := make(chan *fakeStream, 4)
	data := make(chan string, 8)
	backoffs := make(chan time.Duration, 8)

	open := func(ctx context.Context) (*fakeStream, error) {
		// Unbuffered frames: a completed send proves Recv consumed the previous
		// one, which is what makes the ordering below deterministic.
		fs := newFake(ctx, 0)
		fs.ignoreCancel = true
		opens <- fs
		return fs, nil
	}

	sup, err := stream.NewSupervisor(stream.Config[string, *fakeStream]{
		Name:      "test",
		Open:      open,
		OnData:    func(_ context.Context, msg string, _ *fakeStream) { data <- msg },
		OnBackoff: func(_ int, d time.Duration) { backoffs <- d },
		Backoff:   testLadder(),
	})
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	if err := sup.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}

	first := recvWithin(t, opens, "first open")
	sup.Restart()
	second := recvWithin(t, opens, "second open")
	defer func() {
		close(first.halt)
		close(second.halt)
		sup.Stop()
	}()

	// The replaced stream delivers a data frame and then terminates. Both events
	// carry the stale generation and must be dropped.
	first.frames <- frame{msg: "stale"}
	// This send unblocks only once the pump consumed "stale" and finished
	// forwarding it, so the stale frame is already queued ahead of anything the
	// live stream produces next.
	first.frames <- frame{err: io.EOF}

	second.frames <- frame{msg: "live"}
	if msg := recvWithin(t, data, "live frame"); msg != "live" {
		t.Fatalf("delivered %q, want %q — stale stream leaked into the handler", msg, "live")
	}

	select {
	case msg := <-data:
		t.Fatalf("stale stream delivered %q after being replaced", msg)
	default:
	}
	select {
	case d := <-backoffs:
		t.Fatalf("stale termination pushed the live stream onto the ladder (%s)", d)
	default:
	}
	if got := len(opens); got != 0 {
		t.Fatalf("stale termination triggered %d extra open(s)", got)
	}
	if cur, ok := sup.Current(); !ok || cur != second {
		t.Fatal("stale termination replaced the live stream reference")
	}
}

func TestSupervisorOpenErrorGoesOnLadder(t *testing.T) {
	openErr := errors.New("no identity yet")
	delays := make(chan time.Duration, 8)
	errs := make(chan error, 8)
	data := make(chan string, 4)
	var opened atomic.Int64

	open := func(ctx context.Context) (*fakeStream, error) {
		if opened.Add(1) <= 3 {
			return nil, openErr
		}
		fs := newFake(ctx, 1)
		fs.frames <- frame{msg: "connected"}
		return fs, nil
	}

	sup, err := stream.NewSupervisor(stream.Config[string, *fakeStream]{
		Name:      "test",
		Open:      open,
		OnData:    func(_ context.Context, msg string, _ *fakeStream) { data <- msg },
		OnError:   func(err error) { errs <- err },
		OnBackoff: func(_ int, d time.Duration) { delays <- d },
		Backoff:   testLadder(),
	})
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	if err := sup.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sup.Stop()

	for i, ms := range []time.Duration{1, 2, 3} {
		got := recvWithin(t, delays, "backoff notification")
		if got != ms*time.Millisecond {
			t.Fatalf("open failure %d: delay %s, want %dms", i, got, ms)
		}
		reported := recvWithin(t, errs, "open error")
		if !errors.Is(reported, openErr) {
			t.Fatalf("open failure %d: reported %v, want %v", i, reported, openErr)
		}
	}
	if msg := recvWithin(t, data, "data frame"); msg != "connected" {
		t.Fatalf("got frame %q, want %q", msg, "connected")
	}
}

func TestSupervisorRestartResetsLadder(t *testing.T) {
	delays := make(chan time.Duration, 16)
	opens := make(chan *fakeStream, 8)

	open := func(ctx context.Context) (*fakeStream, error) {
		fs := newFake(ctx, 1)
		opens <- fs
		return fs, nil
	}

	sup, err := stream.NewSupervisor(stream.Config[string, *fakeStream]{
		Name:      "test",
		Open:      open,
		OnData:    func(context.Context, string, *fakeStream) {},
		OnBackoff: func(_ int, d time.Duration) { delays <- d },
		Backoff:   testLadder(),
	})
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	if err := sup.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer sup.Stop()

	first := recvWithin(t, opens, "first open")
	first.frames <- frame{err: io.EOF}
	if got := recvWithin(t, delays, "first backoff"); got != time.Millisecond {
		t.Fatalf("first break: delay %s, want 1ms", got)
	}

	second := recvWithin(t, opens, "second open")
	second.frames <- frame{err: io.EOF}
	if got := recvWithin(t, delays, "second backoff"); got != 2*time.Millisecond {
		t.Fatalf("second break: delay %s, want 2ms", got)
	}

	third := recvWithin(t, opens, "third open")
	sup.Restart()
	fourth := recvWithin(t, opens, "fourth open after restart")
	if third == fourth {
		t.Fatal("restart reused the previous stream")
	}
	// A restart is a parameter change, not a sick runtime: the next break starts
	// from the first rung again.
	fourth.frames <- frame{err: io.EOF}
	if got := recvWithin(t, delays, "backoff after restart"); got != time.Millisecond {
		t.Fatalf("after restart: delay %s, want 1ms", got)
	}
}

func TestSupervisorStopReleasesGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	opens := make(chan *fakeStream, 4)
	open := func(ctx context.Context) (*fakeStream, error) {
		fs := newFake(ctx, 1)
		opens <- fs
		return fs, nil
	}

	sup, err := stream.NewSupervisor(stream.Config[string, *fakeStream]{
		Name:    "test",
		Open:    open,
		OnData:  func(context.Context, string, *fakeStream) {},
		Backoff: testLadder(),
	})
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	if err := sup.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}

	live := recvWithin(t, opens, "first open")
	if cur, ok := sup.Current(); !ok || cur != live {
		t.Fatal("Current did not expose the live stream")
	}

	sup.Stop()

	if _, ok := sup.Current(); ok {
		t.Fatal("Current still reports a stream after Stop")
	}
	if err := sup.Start(t.Context()); !errors.Is(err, stream.ErrAlreadyStarted) {
		t.Fatalf("restarting a stopped supervisor: got %v, want ErrAlreadyStarted", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if runtime.NumGoroutine() <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines leaked: %d before, %d after Stop", before, runtime.NumGoroutine())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSupervisorContextCancelStops(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	opens := make(chan *fakeStream, 4)
	open := func(streamCtx context.Context) (*fakeStream, error) {
		fs := newFake(streamCtx, 1)
		opens <- fs
		return fs, nil
	}

	sup, err := stream.NewSupervisor(stream.Config[string, *fakeStream]{
		Name:    "test",
		Open:    open,
		OnData:  func(context.Context, string, *fakeStream) {},
		Backoff: testLadder(),
	})
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	if err := sup.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	recvWithin(t, opens, "first open")
	cancel()
	// Stop after an external cancel must still return, and must be idempotent.
	sup.Stop()
	sup.Stop()

	select {
	case fs := <-opens:
		t.Fatalf("supervisor opened stream %p after cancellation", fs)
	default:
	}
}

func TestNewSupervisorRejectsIncompleteConfig(t *testing.T) {
	if _, err := stream.NewSupervisor(stream.Config[string, *fakeStream]{
		OnData: func(context.Context, string, *fakeStream) {},
	}); !errors.Is(err, stream.ErrInvalidConfig) {
		t.Fatalf("missing Open: got %v, want ErrInvalidConfig", err)
	}
	if _, err := stream.NewSupervisor(stream.Config[string, *fakeStream]{
		Open: func(context.Context) (*fakeStream, error) { return nil, nil },
	}); !errors.Is(err, stream.ErrInvalidConfig) {
		t.Fatalf("missing OnData: got %v, want ErrInvalidConfig", err)
	}
}
