package telemetry

import (
	"bytes"
	"testing"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

func TestDefaultModesFailSafe(t *testing.T) {
	m := DefaultModes()

	for _, ch := range []pb.Channel{pb.Channel_RPC, pb.Channel_HTTP, pb.Channel_EVENT, pb.Channel_WORKFLOW} {
		if got := m.ForChannel(ch); got != ModeNone {
			t.Fatalf("%s mode = %s, want none before the first snapshot", ch, got)
		}
	}
	if !m.Enabled {
		t.Fatal("telemetry is off before the first snapshot")
	}
	if m.PayloadMaxBytes != 65536 {
		t.Fatalf("payload cap = %d, want 65536", m.PayloadMaxBytes)
	}
}

func TestModesForChannelWithoutPolicy(t *testing.T) {
	m := DefaultModes()
	m.RPC = ModeAll

	for _, ch := range []pb.Channel{pb.Channel_JOB, pb.Channel_USER, pb.Channel_CHANNEL_UNSPECIFIED} {
		if got := m.ForChannel(ch); got != ModeNone {
			t.Fatalf("%s mode = %s, want none", ch, got)
		}
	}
}

func TestModesFromProto(t *testing.T) {
	m := ModesFromProto(&pb.CaptureModes{
		Rpc:              pb.CaptureMode_CAPTURE_MODE_ALL,
		Http:             pb.CaptureMode_CAPTURE_MODE_ERRORS,
		Event:            pb.CaptureMode_CAPTURE_MODE_NONE,
		Workflow:         pb.CaptureMode(99),
		TelemetryEnabled: false,
		PayloadMaxBytes:  4096,
	})

	if m.RPC != ModeAll || m.HTTP != ModeErrors || m.Event != ModeNone {
		t.Fatalf("modes = %+v", m)
	}
	if m.Workflow != ModeNone {
		t.Fatalf("unknown wire mode = %s, want none", m.Workflow)
	}
	if m.Enabled {
		t.Fatal("telemetry_enabled=false was ignored")
	}
	if m.PayloadMaxBytes != 4096 {
		t.Fatalf("payload cap = %d, want 4096", m.PayloadMaxBytes)
	}
}

// payload_max_bytes = 0 is proto3's "unset", so the message carries no
// telemetry globals and the fail-safe values stand.
func TestModesFromProtoWithoutGlobals(t *testing.T) {
	m := ModesFromProto(&pb.CaptureModes{Rpc: pb.CaptureMode_CAPTURE_MODE_ALL})

	if m.RPC != ModeAll {
		t.Fatalf("rpc mode = %s, want all", m.RPC)
	}
	if !m.Enabled {
		t.Fatal("telemetry was switched off by an unset field")
	}
	if m.PayloadMaxBytes != DefaultPayloadMaxBytes {
		t.Fatalf("payload cap = %d, want %d", m.PayloadMaxBytes, DefaultPayloadMaxBytes)
	}
}

func TestModesFromProtoNilMessage(t *testing.T) {
	if got := ModesFromProto(nil); got != DefaultModes() {
		t.Fatalf("ModesFromProto(nil) = %+v, want the fail-safe defaults", got)
	}
}

func TestOverrideNarrowsOnly(t *testing.T) {
	cases := []struct {
		name     string
		pushed   Mode
		override Mode
		want     Mode
	}{
		{"inherit keeps the pushed mode", ModeAll, ModeInherit, ModeAll},
		{"all narrowed to errors", ModeAll, ModeErrors, ModeErrors},
		{"all narrowed to none", ModeAll, ModeNone, ModeNone},
		{"errors narrowed to none", ModeErrors, ModeNone, ModeNone},
		{"errors cannot widen to all", ModeErrors, ModeAll, ModeErrors},
		{"none cannot widen to errors", ModeNone, ModeErrors, ModeNone},
		{"none cannot widen to all", ModeNone, ModeAll, ModeNone},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.pushed.Narrow(c.override); got != c.want {
				t.Fatalf("%s narrowed by %s = %s, want %s", c.pushed, c.override, got, c.want)
			}
		})
	}
}

func TestModeString(t *testing.T) {
	names := map[Mode]string{
		ModeInherit: "inherit",
		ModeNone:    "none",
		ModeErrors:  "errors",
		ModeAll:     "all",
		Mode(200):   "unknown",
	}
	for m, want := range names {
		if got := m.String(); got != want {
			t.Fatalf("Mode(%d) = %q, want %q", m, got, want)
		}
	}
}

func TestPolicyResolve(t *testing.T) {
	p := NewPolicy()

	if p.Resolve(pb.Channel_RPC, ModeInherit) != ModeNone {
		t.Fatal("fresh policy captures before the first snapshot")
	}
	if p.Capturing(pb.Channel_RPC, ModeInherit) {
		t.Fatal("fresh policy reports itself as capturing")
	}

	modes := DefaultModes()
	modes.RPC = ModeAll
	p.Set(modes)

	if got := p.Resolve(pb.Channel_RPC, ModeInherit); got != ModeAll {
		t.Fatalf("resolved = %s, want all", got)
	}
	if got := p.Resolve(pb.Channel_RPC, ModeErrors); got != ModeErrors {
		t.Fatalf("narrowed = %s, want errors", got)
	}
	if got := p.Resolve(pb.Channel_HTTP, ModeAll); got != ModeNone {
		t.Fatalf("http resolved = %s, want none — an override cannot widen", got)
	}
	if !p.Capturing(pb.Channel_RPC, ModeInherit) {
		t.Fatal("policy does not report capturing after a push")
	}
}

func TestPolicyDisabledTelemetryCapturesNothing(t *testing.T) {
	p := NewPolicy()
	modes := DefaultModes()
	modes.RPC = ModeAll
	modes.Enabled = false
	p.Set(modes)

	if got := p.Resolve(pb.Channel_RPC, ModeInherit); got != ModeNone {
		t.Fatalf("resolved = %s, want none while telemetry is off", got)
	}
	if p.Enabled() {
		t.Fatal("Enabled reported true")
	}
}

func TestPolicyPayloadMaxBytes(t *testing.T) {
	p := NewPolicy()
	if got := p.PayloadMaxBytes(); got != DefaultPayloadMaxBytes {
		t.Fatalf("cap = %d, want %d", got, DefaultPayloadMaxBytes)
	}

	modes := DefaultModes()
	modes.PayloadMaxBytes = 128
	p.Set(modes)

	if got := p.PayloadMaxBytes(); got != 128 {
		t.Fatalf("cap = %d, want 128", got)
	}
}

func TestCapPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 100)

	capped, original := CapPayload(payload, 10)
	if len(capped) != 10 {
		t.Fatalf("capped length = %d, want 10", len(capped))
	}
	if original != 100 {
		t.Fatalf("original size = %d, want 100", original)
	}

	capped, original = CapPayload(payload, 1000)
	if len(capped) != 100 || original != 100 {
		t.Fatalf("under-cap payload was altered: len=%d original=%d", len(capped), original)
	}

	capped, _ = CapPayload(payload, 0)
	if len(capped) != 100 {
		t.Fatalf("non-positive cap did not fall back to the default: len=%d", len(capped))
	}
}
