package sbhttp_test

import (
	"log/slog"
	"testing"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/registry"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	"github.com/service-bridge/sdk/go/sbhttp"
)

// testRuntime is the client seen from the integration's side: a recorder, a
// declaration set and a countable registry restart.
type testRuntime struct {
	rec      *telemetry.Recorder
	decls    *registry.Declarations
	restarts int
}

func (r *testRuntime) Recorder() *telemetry.Recorder        { return r.rec }
func (r *testRuntime) Declarations() *registry.Declarations { return r.decls }
func (r *testRuntime) RestartRegistry()                     { r.restarts++ }

func newIntegration(t *testing.T, httpMode telemetry.Mode) (*sbhttp.Integration, *testRuntime) {
	t.Helper()
	return newIntegrationWithLimit(t, httpMode, int32(telemetry.DefaultPayloadMaxBytes))
}

func newIntegrationWithLimit(t *testing.T, httpMode telemetry.Mode, payloadMaxBytes int32) (*sbhttp.Integration, *testRuntime) {
	t.Helper()
	policy := telemetry.NewPolicy()
	modes := telemetry.DefaultModes()
	modes.HTTP = httpMode
	modes.PayloadMaxBytes = payloadMaxBytes
	policy.Set(modes)

	rt := &testRuntime{
		rec:   telemetry.NewRecorder(telemetry.NewRing(telemetry.DefaultBudgets()), policy),
		decls: registry.NewDeclarations(),
	}
	integ, err := sbhttp.New(rt, sbhttp.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatalf("new integration: %v", err)
	}
	return integ, rt
}

// ops returns the buffered operation frames without draining the ring.
func ops(rt *testRuntime) []*pb.OpReport {
	batch := rt.rec.Ring().Peek(1024)
	out := make([]*pb.OpReport, 0, len(batch.Ops))
	for _, item := range batch.Ops {
		out = append(out, item.Msg)
	}
	return out
}

func payloads(rt *testRuntime) []*pb.PayloadAttachment {
	batch := rt.rec.Ring().Peek(1024)
	out := make([]*pb.PayloadAttachment, 0, len(batch.Payloads))
	for _, item := range batch.Payloads {
		out = append(out, item.Msg)
	}
	return out
}

// startEnd asserts the ring holds exactly one operation and returns its two
// frames.
func startEnd(t *testing.T, rt *testRuntime) (start, end *pb.OpReport) {
	t.Helper()
	frames := ops(rt)
	if len(frames) != 2 {
		t.Fatalf("op frames: got %d, want 2 (one START + one END)", len(frames))
	}
	start, end = frames[0], frames[1]
	if start.GetOpId() != end.GetOpId() {
		t.Fatalf("END frame closes a different operation: %q vs %q", end.GetOpId(), start.GetOpId())
	}
	return start, end
}

func TestNewRejectsMissingRuntime(t *testing.T) {
	if _, err := sbhttp.New(nil); err == nil {
		t.Fatal("nil runtime must be rejected")
	}
	if _, err := sbhttp.New(&testRuntime{}); err == nil {
		t.Fatal("runtime without a recorder must be rejected")
	}
}
