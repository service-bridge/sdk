package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

type logSink struct {
	mu      sync.Mutex
	records []*pb.Log
}

func (l *logSink) PushLog(rec *pb.Log) {
	l.mu.Lock()
	l.records = append(l.records, rec)
	l.mu.Unlock()
}

func (l *logSink) only(t *testing.T) *pb.Log {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.records) != 1 {
		t.Fatalf("records = %d, want 1", len(l.records))
	}
	return l.records[0]
}

func (l *logSink) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.records)
}

func newTestLogger(sink LogSink, opts HandlerOptions) *slog.Logger {
	if opts.Level == nil {
		opts.Level = slog.LevelDebug
	}
	return slog.New(NewHandler(sink, opts))
}

func decodeFields(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("fields_json is not valid JSON: %v (%s)", err, raw)
	}
	return out
}

// The console filters logs by trace. A handler that left these fields empty
// makes "the logs of this trace" permanently empty for every service.
func TestLogInsideATraceCarriesTraceAndOp(t *testing.T) {
	sink := &logSink{}
	logger := newTestLogger(sink, HandlerOptions{InstanceID: func() string { return "inst-1" }})

	traceID := uuid.New()
	opID := uuid.New()
	ctx := WithTraceContext(context.Background(), TraceContext{TraceID: traceID, ParentOpID: opID})

	logger.InfoContext(ctx, "handled")

	rec := sink.only(t)
	if rec.GetTraceId() != traceID.String() {
		t.Fatalf("trace_id = %q, want %q", rec.GetTraceId(), traceID)
	}
	if rec.GetOpId() != opID.String() {
		t.Fatalf("op_id = %q, want %q", rec.GetOpId(), opID)
	}
	if rec.GetInstanceId() != "inst-1" {
		t.Fatalf("instance_id = %q", rec.GetInstanceId())
	}
	if rec.GetSource() != DefaultLogSource {
		t.Fatalf("source = %q", rec.GetSource())
	}
}

func TestLogOutsideATraceCarriesNoIdentifiers(t *testing.T) {
	sink := &logSink{}
	logger := newTestLogger(sink, HandlerOptions{})

	logger.Info("no trace here")

	rec := sink.only(t)
	if rec.GetTraceId() != "" || rec.GetOpId() != "" {
		t.Fatalf("trace_id = %q, op_id = %q, want both empty", rec.GetTraceId(), rec.GetOpId())
	}
}

// A root operation has no parent, and a zero uuid is not an operation that
// exists — the runtime must store NULL, not a dangling identifier.
func TestLogInARootTraceLeavesOpEmpty(t *testing.T) {
	sink := &logSink{}
	logger := newTestLogger(sink, HandlerOptions{})

	traceID := uuid.New()
	ctx := WithTraceContext(context.Background(), TraceContext{TraceID: traceID})

	logger.InfoContext(ctx, "root")
	rec := sink.only(t)
	if rec.GetTraceId() != traceID.String() {
		t.Fatalf("trace_id = %q", rec.GetTraceId())
	}
	if rec.GetOpId() != "" {
		t.Fatalf("op_id = %q, want empty", rec.GetOpId())
	}
}

// An operation started through the recorder puts itself in the context as the
// parent of what nests under it, which is the operation its logs belong to.
func TestLogInsideAnOperationCarriesThatOperation(t *testing.T) {
	sink := &logSink{}
	ring := NewRing(Budgets{})
	recorder := NewRecorder(ring, NewPolicy())
	logger := newTestLogger(sink, HandlerOptions{})

	ctx, op, err := recorder.Start(context.Background(), OpSpec{Channel: pb.Channel_RPC, Subject: "svc.method"})
	if err != nil {
		t.Fatalf("start op: %v", err)
	}
	logger.InfoContext(ctx, "inside")
	op.End(pb.Status_SUCCESS, "")

	rec := sink.only(t)
	if rec.GetOpId() != op.ID().String() {
		t.Fatalf("op_id = %q, want %q", rec.GetOpId(), op.ID())
	}
	if rec.GetTraceId() != op.TraceID().String() {
		t.Fatalf("trace_id = %q, want %q", rec.GetTraceId(), op.TraceID())
	}
}

func TestLogLevelsMapOntoTheWire(t *testing.T) {
	cases := []struct {
		level slog.Level
		want  pb.LogLevel
	}{
		{slog.LevelDebug - 4, pb.LogLevel_LOG_LEVEL_DEBUG},
		{slog.LevelDebug, pb.LogLevel_LOG_LEVEL_DEBUG},
		{slog.LevelInfo, pb.LogLevel_LOG_LEVEL_INFO},
		{slog.LevelWarn, pb.LogLevel_LOG_LEVEL_WARN},
		{slog.LevelError, pb.LogLevel_LOG_LEVEL_ERROR},
		{slog.LevelError + 4, pb.LogLevel_LOG_LEVEL_ERROR},
	}
	for _, c := range cases {
		sink := &logSink{}
		logger := newTestLogger(sink, HandlerOptions{Level: slog.LevelDebug - 8})
		logger.Log(context.Background(), c.level, "msg")
		if got := sink.only(t).GetLevel(); got != c.want {
			t.Fatalf("level %v mapped to %v, want %v", c.level, got, c.want)
		}
	}
}

func TestHandlerHonoursItsLevel(t *testing.T) {
	sink := &logSink{}
	logger := newTestLogger(sink, HandlerOptions{Level: slog.LevelWarn})

	logger.Info("dropped")
	logger.Warn("kept")

	if sink.count() != 1 {
		t.Fatalf("records = %d, want only the warning", sink.count())
	}
}

func TestAttributesLandInFieldsJSON(t *testing.T) {
	sink := &logSink{}
	logger := newTestLogger(sink, HandlerOptions{}).
		With("service", "billing").
		WithGroup("http").
		With("method", "GET")

	logger.Info("done", "status", 500, "took", 1500*time.Millisecond, "err", errors.New("boom"))

	fields := decodeFields(t, sink.only(t).GetFieldsJson())
	if fields["service"] != "billing" {
		t.Fatalf("service = %v", fields["service"])
	}
	group, ok := fields["http"].(map[string]any)
	if !ok {
		t.Fatalf("http group missing: %v", fields)
	}
	if group["method"] != "GET" {
		t.Fatalf("method = %v", group["method"])
	}
	if group["status"] != float64(500) {
		t.Fatalf("status = %v", group["status"])
	}
	// ADR-0006: a duration crosses the wire as milliseconds, never as a string
	// no query can order by.
	if group["took"] != float64(1500) {
		t.Fatalf("took = %v, want 1500 ms", group["took"])
	}
	if group["err"] != "boom" {
		t.Fatalf("err = %v", group["err"])
	}
}

func TestDerivingWithNothingReusesTheHandler(t *testing.T) {
	handler := NewHandler(&logSink{}, HandlerOptions{})

	if got := handler.WithAttrs(nil); got != slog.Handler(handler) {
		t.Fatal("WithAttrs(nil) built a new handler")
	}
	if got := handler.WithGroup(""); got != slog.Handler(handler) {
		t.Fatal(`WithGroup("") built a new handler`)
	}
}

func TestAttributeShapesAreFlattenedAsSlogDefines(t *testing.T) {
	sink := &logSink{}
	handler := NewHandler(sink, HandlerOptions{Level: slog.LevelDebug})

	at := time.UnixMilli(1_700_000_000_123)
	rec := slog.NewRecord(at, slog.LevelInfo, "shapes", 0)
	rec.AddAttrs(
		slog.Group("db", slog.String("table", "orders"), slog.Int("rows", 3)),
		slog.Group("", slog.String("inlined", "yes")),
		slog.Group("hollow"),
		slog.Attr{},
		slog.Any("id", uuid.Nil),
		slog.Time("seen", at),
		slog.Bool("ok", true),
	)
	if err := handler.Handle(context.Background(), rec); err != nil {
		t.Fatalf("handle: %v", err)
	}

	fields := decodeFields(t, sink.only(t).GetFieldsJson())
	db, ok := fields["db"].(map[string]any)
	if !ok || db["table"] != "orders" || db["rows"] != float64(3) {
		t.Fatalf("db group = %v", fields["db"])
	}
	if fields["inlined"] != "yes" {
		t.Fatalf("an unnamed group must be inlined, got %v", fields)
	}
	if _, present := fields["hollow"]; present {
		t.Fatalf("an empty group must be elided, got %v", fields)
	}
	if fields["id"] != uuid.Nil.String() {
		t.Fatalf("id = %v, want the rendered uuid", fields["id"])
	}
	if fields["seen"] != float64(1_700_000_000_123) {
		t.Fatalf("seen = %v, want unix milliseconds", fields["seen"])
	}
	if fields["ok"] != true {
		t.Fatalf("ok = %v", fields["ok"])
	}
}

func TestRecordWithoutAttributesCarriesNoFields(t *testing.T) {
	sink := &logSink{}
	newTestLogger(sink, HandlerOptions{}).Info("bare")

	if raw := sink.only(t).GetFieldsJson(); len(raw) != 0 {
		t.Fatalf("fields_json = %s, want empty", raw)
	}
}

// A group nobody wrote into is elided, matching every other slog handler.
func TestEmptyGroupIsElided(t *testing.T) {
	sink := &logSink{}
	newTestLogger(sink, HandlerOptions{}).WithGroup("empty").Info("bare")

	if raw := sink.only(t).GetFieldsJson(); len(raw) != 0 {
		t.Fatalf("fields_json = %s, want empty", raw)
	}
}

// Two loggers derived from one parent must not overwrite each other's tail.
func TestDerivedHandlersDoNotShareState(t *testing.T) {
	sink := &logSink{}
	base := newTestLogger(sink, HandlerOptions{}).With("common", "yes")

	base.With("branch", "a").Info("first")
	base.With("branch", "b").Info("second")

	sink.mu.Lock()
	defer sink.mu.Unlock()
	first := decodeFields(t, sink.records[0].GetFieldsJson())
	second := decodeFields(t, sink.records[1].GetFieldsJson())
	if first["branch"] != "a" || second["branch"] != "b" {
		t.Fatalf("branches = %v / %v", first["branch"], second["branch"])
	}
	if first["common"] != "yes" || second["common"] != "yes" {
		t.Fatalf("common attribute lost: %v / %v", first, second)
	}
}

// A value JSON cannot carry costs its record the fields, not the operator's log
// line, and the failure is reported rather than swallowed.
func TestUnencodableFieldStillBuffersTheMessage(t *testing.T) {
	sink := &logSink{}
	handler := NewHandler(sink, HandlerOptions{Level: slog.LevelDebug})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "kept", 0)
	rec.AddAttrs(slog.Any("chan", make(chan int)))

	if err := handler.Handle(context.Background(), rec); err == nil {
		t.Fatal("err = nil, want the encoding failure reported")
	}
	entry := sink.only(t)
	if entry.GetMessage() != "kept" {
		t.Fatalf("message = %q", entry.GetMessage())
	}
	if len(entry.GetFieldsJson()) != 0 {
		t.Fatalf("fields_json = %s, want empty", entry.GetFieldsJson())
	}
}

func TestRecordTimestampIsUnixMilliseconds(t *testing.T) {
	sink := &logSink{}
	handler := NewHandler(sink, HandlerOptions{Level: slog.LevelDebug})

	at := time.UnixMilli(1_700_000_000_123)
	rec := slog.NewRecord(at, slog.LevelInfo, "stamped", 0)
	if err := handler.Handle(context.Background(), rec); err != nil {
		t.Fatalf("handle: %v", err)
	}

	if got := sink.only(t).GetAtUnixMs(); got != 1_700_000_000_123 {
		t.Fatalf("at_unix_ms = %d", got)
	}
}

func TestHandlerIsSafeUnderConcurrentUse(t *testing.T) {
	sink := &logSink{}
	logger := newTestLogger(sink, HandlerOptions{InstanceID: func() string { return "inst-1" }})

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				logger.With("worker", w).Info("tick", "i", i)
			}
		}(w)
	}
	wg.Wait()

	if got := sink.count(); got != 200 {
		t.Fatalf("records = %d, want 200", got)
	}
}
