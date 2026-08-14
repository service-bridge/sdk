package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

// LogSink receives the records the handler builds. *Ring satisfies it.
type LogSink interface {
	PushLog(*pb.Log)
}

// DefaultLogSource is what the runtime files a record under when the caller
// pins no source of its own.
const DefaultLogSource = "sdk"

// HandlerOptions configures the slog handler. See ./README.md.
type HandlerOptions struct {
	// Level is the minimum level reported. nil means slog.LevelInfo.
	Level slog.Leveler
	// InstanceID is resolved per record: the handler can be installed before the
	// runtime hands out an instance identity, and every record after that point
	// must carry it.
	InstanceID func() string
	// Source labels where the record came from. Empty means DefaultLogSource.
	Source string
	// Now supplies the timestamp for records that carry none. Empty means the
	// wall clock.
	Now func() int64
}

// Handler is a slog.Handler that buffers records for the telemetry transport.
// Being a slog.Handler rather than a logger of its own is the point: the
// application keeps its own slog setup and adds this one to the chain.
type Handler struct {
	sink       LogSink
	level      slog.Leveler
	instanceID func() string
	source     string
	now        func() int64

	// goas holds the WithAttrs / WithGroup calls in the order they were made,
	// which is what decides how deep each attribute nests.
	goas []groupOrAttrs
}

// groupOrAttrs is one WithGroup or one WithAttrs call.
type groupOrAttrs struct {
	group string
	attrs []slog.Attr
}

// NewHandler builds a handler writing into sink.
func NewHandler(sink LogSink, opts HandlerOptions) *Handler {
	h := &Handler{
		sink:       sink,
		level:      opts.Level,
		instanceID: opts.InstanceID,
		source:     opts.Source,
		now:        opts.Now,
	}
	if h.level == nil {
		h.level = slog.LevelInfo
	}
	if h.instanceID == nil {
		h.instanceID = func() string { return "" }
	}
	if h.source == "" {
		h.source = DefaultLogSource
	}
	if h.now == nil {
		h.now = nowUnixMs
	}
	return h
}

// Enabled reports whether records at this level are buffered.
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

// WithAttrs returns a handler that stamps attrs onto every record.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	return h.with(groupOrAttrs{attrs: attrs})
}

// WithGroup returns a handler that nests everything that follows under name.
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h.with(groupOrAttrs{group: name})
}

func (h *Handler) with(goa groupOrAttrs) *Handler {
	next := *h
	// The slice is copied rather than appended in place: two handlers derived
	// from the same parent would otherwise write over each other's tail.
	next.goas = make([]groupOrAttrs, len(h.goas), len(h.goas)+1)
	copy(next.goas, h.goas)
	next.goas = append(next.goas, goa)
	return &next
}

// Handle buffers one record.
//
// trace_id and op_id come from ctx, which is exactly why slog.Handler is the
// right shape for this: the runtime indexes logs by trace, and a handler that
// left those fields empty would make "the logs of this trace" permanently empty
// for every service on this SDK. The op is the one the record was emitted
// inside — the operation the trace context names as parent for anything nested.
func (h *Handler) Handle(ctx context.Context, rec slog.Record) error {
	fields, fieldsErr := h.fields(rec)

	var traceID, opID string
	if tc, ok := FromContext(ctx); ok {
		if tc.TraceID != uuid.Nil {
			traceID = tc.TraceID.String()
		}
		if tc.ParentOpID != uuid.Nil {
			opID = tc.ParentOpID.String()
		}
	}

	atUnixMs := h.now()
	if !rec.Time.IsZero() {
		atUnixMs = rec.Time.UnixMilli()
	}

	h.sink.PushLog(&pb.Log{
		AtUnixMs:   atUnixMs,
		Level:      logLevel(rec.Level),
		Message:    rec.Message,
		FieldsJson: fields,
		TraceId:    traceID,
		OpId:       opID,
		InstanceId: h.instanceID(),
		Source:     h.source,
	})
	return fieldsErr
}

// fields renders the attributes as JSON. A value that cannot be encoded — a
// channel, a function — costs its own record its fields, not the operator's log
// line: the message is buffered either way and the encoding failure is
// returned.
func (h *Handler) fields(rec slog.Record) ([]byte, error) {
	root := map[string]any{}
	cur := root
	for _, goa := range h.goas {
		if goa.group != "" {
			next := map[string]any{}
			cur[goa.group] = next
			cur = next
			continue
		}
		for _, a := range goa.attrs {
			putAttr(cur, a)
		}
	}
	rec.Attrs(func(a slog.Attr) bool {
		putAttr(cur, a)
		return true
	})

	pruneEmpty(root)
	if len(root) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("telemetry: log: encode fields: %w", err)
	}
	return encoded, nil
}

func putAttr(m map[string]any, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		members := a.Value.Group()
		if len(members) == 0 {
			return
		}
		// An unnamed group is inlined into its parent, per the slog contract.
		if a.Key == "" {
			for _, member := range members {
				putAttr(m, member)
			}
			return
		}
		nested := map[string]any{}
		for _, member := range members {
			putAttr(nested, member)
		}
		if len(nested) == 0 {
			return
		}
		m[a.Key] = nested
		return
	}
	if a.Key == "" {
		return
	}
	m[a.Key] = attrValue(a.Value)
}

// attrValue maps a slog value onto something JSON carries. Times and durations
// follow ADR-0006: unix-ms and ms, never a formatted string a query cannot
// order by.
func attrValue(v slog.Value) any {
	switch v.Kind() {
	case slog.KindTime:
		return v.Time().UnixMilli()
	case slog.KindDuration:
		return v.Duration().Milliseconds()
	case slog.KindAny:
		switch concrete := v.Any().(type) {
		case error:
			return concrete.Error()
		case fmt.Stringer:
			return concrete.String()
		default:
			return concrete
		}
	default:
		return v.Any()
	}
}

// pruneEmpty drops groups that ended up with no attributes, which slog elides.
func pruneEmpty(m map[string]any) {
	for k, v := range m {
		nested, ok := v.(map[string]any)
		if !ok {
			continue
		}
		pruneEmpty(nested)
		if len(nested) == 0 {
			delete(m, k)
		}
	}
}

func logLevel(level slog.Level) pb.LogLevel {
	switch {
	case level <= slog.LevelDebug:
		return pb.LogLevel_LOG_LEVEL_DEBUG
	case level < slog.LevelWarn:
		return pb.LogLevel_LOG_LEVEL_INFO
	case level < slog.LevelError:
		return pb.LogLevel_LOG_LEVEL_WARN
	default:
		return pb.LogLevel_LOG_LEVEL_ERROR
	}
}

// compile-time proof the handler is usable wherever slog is.
var _ slog.Handler = (*Handler)(nil)
