package telemetry

import (
	"math"
	"sync/atomic"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

// Mode is a payload capture policy. The zero value is ModeInherit so a handler
// that sets nothing takes what the runtime pushed.
type Mode uint8

const (
	// ModeInherit is "no per-handler opinion".
	ModeInherit Mode = iota
	// ModeNone captures nothing.
	ModeNone
	// ModeErrors captures the last payload per direction and emits it only when
	// the operation fails.
	ModeErrors
	// ModeAll captures every payload immediately.
	ModeAll
)

func (m Mode) String() string {
	switch m {
	case ModeInherit:
		return "inherit"
	case ModeNone:
		return "none"
	case ModeErrors:
		return "errors"
	case ModeAll:
		return "all"
	default:
		return "unknown"
	}
}

// rank orders the modes by how much they capture: none < errors < all.
// Narrowing picks the lower rank. ModeInherit carries no rank of its own.
func (m Mode) rank() int {
	switch m {
	case ModeErrors:
		return 1
	case ModeAll:
		return 2
	default:
		return 0
	}
}

// Narrow applies a per-handler override to the runtime-pushed mode. An override
// may only lower the rank: the runtime is the privacy authority, and a handler
// that could widen capture would let application code publish payloads the
// operator disabled.
func (m Mode) Narrow(override Mode) Mode {
	if override == ModeInherit {
		return m
	}
	if override.rank() < m.rank() {
		return override
	}
	return m
}

// ModeFromProto maps a wire mode. Anything unset or unrecognised fails safe to
// ModeNone.
func ModeFromProto(m pb.CaptureMode) Mode {
	switch m {
	case pb.CaptureMode_CAPTURE_MODE_ALL:
		return ModeAll
	case pb.CaptureMode_CAPTURE_MODE_ERRORS:
		return ModeErrors
	default:
		return ModeNone
	}
}

// DefaultPayloadMaxBytes caps a captured payload per direction until the
// runtime pushes its own limit.
const DefaultPayloadMaxBytes int32 = 65536

// Modes is one snapshot of the runtime-pushed telemetry authority. Job and user
// operations carry no payload, so those channels have no mode.
type Modes struct {
	RPC             Mode
	HTTP            Mode
	Event           Mode
	Workflow        Mode
	Enabled         bool
	PayloadMaxBytes int32
}

// DefaultModes is what applies before the first registry snapshot lands:
// nothing is captured, the cap is the default and the transport runs. The SDK
// never captures a payload the runtime did not authorise, but it does keep
// reporting operations, which is what the service map is built from.
func DefaultModes() Modes {
	return Modes{
		RPC:             ModeNone,
		HTTP:            ModeNone,
		Event:           ModeNone,
		Workflow:        ModeNone,
		Enabled:         true,
		PayloadMaxBytes: DefaultPayloadMaxBytes,
	}
}

// ModesFromProto reads a pushed CaptureModes message. Modes always come whole,
// so a missing message means "capture nothing". The telemetry globals are
// different: payload_max_bytes = 0 is proto3's "unset", and honouring it
// literally would truncate every payload to nothing — a message without it is
// treated as not carrying the globals at all.
func ModesFromProto(m *pb.CaptureModes) Modes {
	if m == nil {
		return DefaultModes()
	}
	next := Modes{
		RPC:             ModeFromProto(m.GetRpc()),
		HTTP:            ModeFromProto(m.GetHttp()),
		Event:           ModeFromProto(m.GetEvent()),
		Workflow:        ModeFromProto(m.GetWorkflow()),
		Enabled:         true,
		PayloadMaxBytes: DefaultPayloadMaxBytes,
	}
	if m.GetPayloadMaxBytes() > 0 {
		next.PayloadMaxBytes = m.GetPayloadMaxBytes()
		next.Enabled = m.GetTelemetryEnabled()
	}
	return next
}

// ForChannel returns the mode of one operation channel. Channels the runtime
// carries no mode for never capture.
func (m Modes) ForChannel(ch pb.Channel) Mode {
	switch ch {
	case pb.Channel_RPC:
		return m.RPC
	case pb.Channel_HTTP:
		return m.HTTP
	case pb.Channel_EVENT:
		return m.Event
	case pb.Channel_WORKFLOW:
		return m.Workflow
	default:
		return ModeNone
	}
}

// Policy holds the current Modes snapshot. The registry stream replaces it
// wholesale; operations read it on every start, so the swap is atomic rather
// than locked.
type Policy struct {
	modes atomic.Pointer[Modes]
}

// NewPolicy returns a policy seeded with DefaultModes.
func NewPolicy() *Policy {
	p := &Policy{}
	p.Set(DefaultModes())
	return p
}

// Set replaces the snapshot with what the runtime pushed.
func (p *Policy) Set(m Modes) { p.modes.Store(&m) }

// Modes returns the current snapshot.
func (p *Policy) Modes() Modes { return *p.modes.Load() }

// Resolve computes the effective mode for one operation: the pushed mode for
// its channel, narrowed by the handler's override. Telemetry switched off
// captures nothing at all.
func (p *Policy) Resolve(ch pb.Channel, override Mode) Mode {
	m := p.Modes()
	if !m.Enabled {
		return ModeNone
	}
	return m.ForChannel(ch).Narrow(override)
}

// Capturing answers "is preparing a payload worth anything at all" without
// touching the payload. Serialising first and checking the mode afterwards
// makes every request on the default configuration pay a full serialisation
// that is thrown away.
func (p *Policy) Capturing(ch pb.Channel, override Mode) bool {
	return p.Resolve(ch, override) != ModeNone
}

// Enabled reports whether the runtime wants telemetry reported at all.
func (p *Policy) Enabled() bool { return p.Modes().Enabled }

// PayloadMaxBytes returns the current per-direction cap.
func (p *Policy) PayloadMaxBytes() int32 { return p.Modes().PayloadMaxBytes }

// CapPayload truncates a payload to maxBytes and reports its original length so
// the console can badge the truncation. A non-positive cap falls back to the
// default. The result aliases the input; the caller copies what it retains.
func CapPayload(payload []byte, maxBytes int32) (capped []byte, originalSize int32) {
	limit := maxBytes
	if limit <= 0 {
		limit = DefaultPayloadMaxBytes
	}
	originalSize = math.MaxInt32
	if len(payload) < math.MaxInt32 {
		originalSize = int32(len(payload))
	}
	if int64(len(payload)) <= int64(limit) {
		return payload, originalSize
	}
	return payload[:limit], originalSize
}
