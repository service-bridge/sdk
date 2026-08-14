package telemetry

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

// ErrMetricConflict reports a metric resolved twice under one series key with a
// different shape — another unit, another bucket layout. One series has one
// shape; merging two shapes would make the stored points unreadable.
var ErrMetricConflict = errors.New("metric already registered with a different shape")

// ErrInvalidBounds reports histogram bounds that cannot describe buckets.
var ErrInvalidBounds = errors.New("invalid histogram bounds")

// Labels are the dimensions one metric series is keyed by.
type Labels map[string]string

// MetricSink receives the points one drain produced. *Ring satisfies it.
type MetricSink interface {
	PushMetric(*pb.MetricPoint)
}

// Units carried on the wire, in UCUM as the runtime stores them.
const (
	unitDimensionless = "1"
	unitSeconds       = "s"
	unitPercent       = "%"
	unitBytes         = "By"
)

// seriesSep joins the segments of a series key. It is NUL because a printable
// separator can be forged by a label value: under "|", {a: "1|b"} and
// {a: "1", b: ""} produce the same key and two unrelated series silently
// collapse into one.
const seriesSep = "\x00"

// DefaultHistogramBounds returns the latency ladder in seconds, as a fresh
// slice so a caller mutating it cannot corrupt other histograms.
func DefaultHistogramBounds() []float64 {
	return []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
}

// Metrics accumulates state per (kind, name, instance, labels) series and emits
// exactly one MetricPoint per changed series per drain.
//
// Aggregation is the whole point: one ring item per increment puts a service at
// 1000 rps far past what the metrics ring holds, and everything but the last
// few points is evicted before the transport ever sees it. The hot path
// (Inc/Set/Observe) touches only the series the handle closes over — the map is
// consulted once, when the handle is created, and no call allocates.
type Metrics struct {
	sink MetricSink

	mu     sync.Mutex
	series map[string]*series
	// order preserves creation order so a drain emits points deterministically;
	// Go map iteration would reshuffle them on every flush.
	order []*series
}

// NewMetrics binds an aggregator to the buffer its points are pushed into.
func NewMetrics(sink MetricSink) *Metrics {
	return &Metrics{sink: sink, series: make(map[string]*series)}
}

type series struct {
	kind       pb.MetricKind
	name       string
	instanceID string
	labels     map[string]string
	unit       string
	// bounds is immutable after creation, so Observe reads it without the lock.
	bounds []float64

	mu     sync.Mutex
	dirty  bool
	sum    float64
	value  float64
	counts []uint64
}

// Counter is a monotonically rising total.
type Counter struct{ s *series }

// Inc adds one.
func (c *Counter) Inc() { c.Add(1) }

// Add raises the counter by delta.
func (c *Counter) Add(delta float64) {
	c.s.mu.Lock()
	c.s.sum += delta
	c.s.dirty = true
	c.s.mu.Unlock()
}

// Gauge is a value that moves in both directions.
type Gauge struct{ s *series }

// Set replaces the current value.
func (g *Gauge) Set(value float64) {
	g.s.mu.Lock()
	g.s.value = value
	g.s.dirty = true
	g.s.mu.Unlock()
}

// Histogram distributes observations over cumulative buckets.
type Histogram struct{ s *series }

// Observe records one value.
func (h *Histogram) Observe(value float64) {
	s := h.s
	idx := len(s.bounds)
	for i, b := range s.bounds {
		if value <= b {
			idx = i
			break
		}
	}
	s.mu.Lock()
	s.counts[idx]++
	s.sum += value
	s.dirty = true
	s.mu.Unlock()
}

// Counter resolves the counter series and returns a handle on it. Two calls
// with the same instance, name and labels address the same series.
func (m *Metrics) Counter(instanceID, name string, labels Labels) (*Counter, error) {
	s, err := m.resolve(pb.MetricKind_METRIC_KIND_COUNTER, instanceID, name, unitDimensionless, labels, nil)
	if err != nil {
		return nil, fmt.Errorf("telemetry: counter %q: %w", name, err)
	}
	return &Counter{s: s}, nil
}

// Gauge resolves the gauge series and returns a handle on it.
func (m *Metrics) Gauge(instanceID, name, unit string, labels Labels) (*Gauge, error) {
	if unit == "" {
		unit = unitDimensionless
	}
	s, err := m.resolve(pb.MetricKind_METRIC_KIND_GAUGE, instanceID, name, unit, labels, nil)
	if err != nil {
		return nil, fmt.Errorf("telemetry: gauge %q: %w", name, err)
	}
	return &Gauge{s: s}, nil
}

// Histogram resolves the histogram series and returns a handle on it. An empty
// unit means seconds and nil bounds mean DefaultHistogramBounds. Bounds must be
// finite and strictly ascending, and must match what the series was created
// with.
func (m *Metrics) Histogram(instanceID, name, unit string, labels Labels, bounds []float64) (*Histogram, error) {
	if unit == "" {
		unit = unitSeconds
	}
	if bounds == nil {
		bounds = DefaultHistogramBounds()
	}
	if err := validateBounds(bounds); err != nil {
		return nil, fmt.Errorf("telemetry: histogram %q: %w", name, err)
	}
	s, err := m.resolve(pb.MetricKind_METRIC_KIND_HISTOGRAM, instanceID, name, unit, labels, bounds)
	if err != nil {
		return nil, fmt.Errorf("telemetry: histogram %q: %w", name, err)
	}
	return &Histogram{s: s}, nil
}

// Drain returns one point per series that changed since the previous drain and
// resets the accumulators. atUnixMs stamps every point.
func (m *Metrics) Drain(atUnixMs int64) []*pb.MetricPoint {
	m.mu.Lock()
	tracked := m.order
	m.mu.Unlock()

	out := make([]*pb.MetricPoint, 0, len(tracked))
	for _, s := range tracked {
		if p := s.point(atUnixMs); p != nil {
			out = append(out, p)
		}
	}
	return out
}

// Flush drains and pushes every produced point into the sink, returning how
// many points it emitted.
func (m *Metrics) Flush(atUnixMs int64) int {
	points := m.Drain(atUnixMs)
	for _, p := range points {
		m.sink.PushMetric(p)
	}
	return len(points)
}

// SeriesCount reports how many distinct series are tracked.
// @internal — см. ./README.md
func (m *Metrics) SeriesCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.series)
}

// point renders one series and resets what the emission consumed. A counter and
// a histogram start the next window empty; a gauge keeps its value but goes
// clean, so it is re-emitted only once it is set again. Re-sending an unchanged
// gauge on every flush tick would put a stream of points carrying no new
// information on the wire and re-create the ring overflow aggregation exists to
// prevent — the runtime stores raw points, so the last emitted value stays
// readable for as long as retention keeps it.
func (s *series) point(atUnixMs int64) *pb.MetricPoint {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return nil
	}
	s.dirty = false

	var (
		value   float64
		buckets []byte
	)
	switch s.kind {
	case pb.MetricKind_METRIC_KIND_COUNTER:
		value = s.sum
		s.sum = 0
	case pb.MetricKind_METRIC_KIND_GAUGE:
		value = s.value
	case pb.MetricKind_METRIC_KIND_HISTOGRAM:
		buckets = encodeBuckets(s.bounds, s.counts)
		value = s.sum
		s.sum = 0
		for i := range s.counts {
			s.counts[i] = 0
		}
	default:
		s.mu.Unlock()
		panic("telemetry: metric series with no kind")
	}
	s.mu.Unlock()

	return &pb.MetricPoint{
		AtUnixMs:    atUnixMs,
		Name:        s.name,
		Kind:        s.kind,
		Labels:      s.labels,
		InstanceId:  s.instanceID,
		Value:       value,
		Unit:        s.unit,
		BucketsJson: buckets,
	}
}

func (m *Metrics) resolve(
	kind pb.MetricKind,
	instanceID, name, unit string,
	labels Labels,
	bounds []float64,
) (*series, error) {
	keys, copied := normalizeLabels(labels)
	key := seriesKey(kind, name, instanceID, keys, copied)

	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.series[key]; ok {
		if existing.unit != unit {
			return nil, fmt.Errorf("unit %q, got %q: %w", existing.unit, unit, ErrMetricConflict)
		}
		if !sameBounds(existing.bounds, bounds) {
			return nil, fmt.Errorf("different bucket layout: %w", ErrMetricConflict)
		}
		return existing, nil
	}

	s := &series{
		kind:       kind,
		name:       name,
		instanceID: instanceID,
		labels:     copied,
		unit:       unit,
		bounds:     bounds,
	}
	if kind == pb.MetricKind_METRIC_KIND_HISTOGRAM {
		s.counts = make([]uint64, len(bounds)+1)
	}
	m.series[key] = s
	m.order = append(m.order, s)
	return s, nil
}

// normalizeLabels sorts the keys and copies the map, so the caller may reuse
// its own map and so {a,b} and {b,a} address one series.
func normalizeLabels(labels Labels) (keys []string, copied map[string]string) {
	if len(labels) == 0 {
		return nil, nil
	}
	keys = make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	copied = make(map[string]string, len(labels))
	for _, k := range keys {
		copied[k] = labels[k]
	}
	return keys, copied
}

func seriesKey(kind pb.MetricKind, name, instanceID string, keys []string, labels map[string]string) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(int(kind)))
	b.WriteString(seriesSep)
	b.WriteString(name)
	b.WriteString(seriesSep)
	b.WriteString(instanceID)
	for _, k := range keys {
		b.WriteString(seriesSep)
		b.WriteString(k)
		b.WriteString(seriesSep)
		b.WriteString(labels[k])
	}
	return b.String()
}

func validateBounds(bounds []float64) error {
	if len(bounds) == 0 {
		return fmt.Errorf("no bounds: %w", ErrInvalidBounds)
	}
	for i, b := range bounds {
		if math.IsNaN(b) || math.IsInf(b, 0) {
			return fmt.Errorf("bound %d is not finite: %w", i, ErrInvalidBounds)
		}
		if i > 0 && b <= bounds[i-1] {
			return fmt.Errorf("bounds must ascend strictly, got %v then %v: %w", bounds[i-1], b, ErrInvalidBounds)
		}
	}
	return nil
}

func sameBounds(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// encodeBuckets renders the cumulative bucket table the runtime stores verbatim
// in JSONB: [{"le":<bound>,"count":<observations <= bound>}, ...] closed by an
// "+Inf" entry carrying the total. Without it a histogram is indistinguishable
// from a stream of gauges and no percentile can be computed from it. The bytes
// are built directly rather than marshalled: bounds are validated finite and
// counts are integers, so the output cannot fail to encode.
func encodeBuckets(bounds []float64, counts []uint64) []byte {
	buf := make([]byte, 0, len(bounds)*24+24)
	buf = append(buf, '[')
	var cumulative uint64
	for i, b := range bounds {
		cumulative += counts[i]
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, `{"le":`...)
		buf = strconv.AppendFloat(buf, b, 'g', -1, 64)
		buf = append(buf, `,"count":`...)
		buf = strconv.AppendUint(buf, cumulative, 10)
		buf = append(buf, '}')
	}
	cumulative += counts[len(bounds)]
	buf = append(buf, `,{"le":"+Inf","count":`...)
	buf = strconv.AppendUint(buf, cumulative, 10)
	buf = append(buf, '}', ']')
	return buf
}
