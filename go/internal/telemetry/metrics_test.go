package telemetry

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

// collectingSink keeps the drained points so a test can read them without a ring.
type collectingSink struct {
	mu     sync.Mutex
	points []*pb.MetricPoint
}

func (c *collectingSink) PushMetric(p *pb.MetricPoint) {
	c.mu.Lock()
	c.points = append(c.points, p)
	c.mu.Unlock()
}

func (c *collectingSink) drained() []*pb.MetricPoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*pb.MetricPoint(nil), c.points...)
}

// One ring item per increment is what made a service at 1000 rps lose almost
// every metric: the buffer evicted them faster than the transport drained it.
func TestCounterAggregatesToOnePointPerWindow(t *testing.T) {
	sink := &collectingSink{}
	m := NewMetrics(sink)

	counter, err := m.Counter("inst-1", "requests_total", Labels{"route": "/health"})
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	for i := 0; i < 1000; i++ {
		counter.Inc()
	}

	if n := m.Flush(1_700_000_000_000); n != 1 {
		t.Fatalf("points emitted = %d, want 1", n)
	}
	points := sink.drained()
	if len(points) != 1 {
		t.Fatalf("points in the sink = %d, want 1", len(points))
	}
	p := points[0]
	if p.GetValue() != 1000 {
		t.Fatalf("value = %v, want 1000", p.GetValue())
	}
	if p.GetKind() != pb.MetricKind_METRIC_KIND_COUNTER {
		t.Fatalf("kind = %v", p.GetKind())
	}
	if p.GetAtUnixMs() != 1_700_000_000_000 {
		t.Fatalf("at_unix_ms = %d", p.GetAtUnixMs())
	}
	if p.GetInstanceId() != "inst-1" || p.GetLabels()["route"] != "/health" {
		t.Fatalf("identity lost: %v %v", p.GetInstanceId(), p.GetLabels())
	}
}

func TestCounterIsSafeUnderConcurrentIncrements(t *testing.T) {
	m := NewMetrics(&collectingSink{})
	counter, err := m.Counter("inst-1", "requests_total", nil)
	if err != nil {
		t.Fatalf("counter: %v", err)
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 125; i++ {
				counter.Inc()
			}
		}()
	}
	wg.Wait()

	points := m.Drain(1)
	if len(points) != 1 || points[0].GetValue() != 1000 {
		t.Fatalf("points = %v", points)
	}
}

func TestCounterResetsAfterDrain(t *testing.T) {
	m := NewMetrics(&collectingSink{})
	counter, _ := m.Counter("inst-1", "requests_total", nil)

	counter.Add(5)
	if points := m.Drain(1); len(points) != 1 || points[0].GetValue() != 5 {
		t.Fatalf("first drain = %v", points)
	}
	// Nothing changed since, so nothing is re-emitted.
	if points := m.Drain(2); len(points) != 0 {
		t.Fatalf("second drain = %v, want nothing", points)
	}
	counter.Add(2)
	if points := m.Drain(3); len(points) != 1 || points[0].GetValue() != 2 {
		t.Fatalf("third drain = %v, want the delta only", points)
	}
}

func TestGaugeKeepsItsValueButIsNotReEmitted(t *testing.T) {
	m := NewMetrics(&collectingSink{})
	gauge, err := m.Gauge("inst-1", "queue_depth", "", nil)
	if err != nil {
		t.Fatalf("gauge: %v", err)
	}

	gauge.Set(7)
	if points := m.Drain(1); len(points) != 1 || points[0].GetValue() != 7 {
		t.Fatalf("first drain = %v", points)
	}
	if points := m.Drain(2); len(points) != 0 {
		t.Fatalf("an unchanged gauge must not be re-emitted, got %v", points)
	}
	gauge.Set(7)
	if points := m.Drain(3); len(points) != 1 || points[0].GetValue() != 7 {
		t.Fatalf("a re-set gauge must be emitted again, got %v", points)
	}
}

func TestLabelOrderDoesNotSplitASeries(t *testing.T) {
	m := NewMetrics(&collectingSink{})

	first, err := m.Counter("inst-1", "hits", Labels{"a": "1", "b": "2"})
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	second, err := m.Counter("inst-1", "hits", Labels{"b": "2", "a": "1"})
	if err != nil {
		t.Fatalf("counter: %v", err)
	}

	first.Inc()
	second.Inc()

	points := m.Drain(1)
	if len(points) != 1 {
		t.Fatalf("series = %d, want 1", len(points))
	}
	if points[0].GetValue() != 2 {
		t.Fatalf("value = %v, want 2 — both handles address one series", points[0].GetValue())
	}
}

// Under a printable separator such as "|", these two label sets render the very
// same key and two unrelated series silently become one.
func TestLabelValueCannotForgeASeriesKey(t *testing.T) {
	m := NewMetrics(&collectingSink{})

	forged, err := m.Counter("inst-1", "hits", Labels{"a": "1|b|2"})
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	plain, err := m.Counter("inst-1", "hits", Labels{"a": "1", "b": "2"})
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	forged.Inc()
	plain.Inc()

	if got := m.SeriesCount(); got != 2 {
		t.Fatalf("series = %d, want 2 — a label value must not be able to spell the separator", got)
	}
	for _, p := range m.Drain(1) {
		if p.GetValue() != 1 {
			t.Fatalf("series %v merged: value = %v, want 1", p.GetLabels(), p.GetValue())
		}
	}
}

func TestInstanceIDSplitsSeries(t *testing.T) {
	m := NewMetrics(&collectingSink{})
	a, _ := m.Counter("inst-1", "hits", nil)
	b, _ := m.Counter("inst-2", "hits", nil)
	a.Inc()
	b.Inc()

	if got := m.SeriesCount(); got != 2 {
		t.Fatalf("series = %d, want 2", got)
	}
}

func TestHistogramEmitsCumulativeBuckets(t *testing.T) {
	m := NewMetrics(&collectingSink{})
	h, err := m.Histogram("inst-1", "latency", "", nil, []float64{0.1, 0.5, 1})
	if err != nil {
		t.Fatalf("histogram: %v", err)
	}

	for _, v := range []float64{0.05, 0.2, 0.2, 0.7, 5} {
		h.Observe(v)
	}

	points := m.Drain(1)
	if len(points) != 1 {
		t.Fatalf("points = %d, want 1", len(points))
	}
	p := points[0]
	if p.GetKind() != pb.MetricKind_METRIC_KIND_HISTOGRAM {
		t.Fatalf("kind = %v", p.GetKind())
	}
	if p.GetUnit() != unitSeconds {
		t.Fatalf("unit = %q, want seconds", p.GetUnit())
	}
	if got, want := p.GetValue(), 0.05+0.2+0.2+0.7+5; got != want {
		t.Fatalf("value = %v, want the observation sum %v", got, want)
	}

	var buckets []struct {
		Le    any     `json:"le"`
		Count float64 `json:"count"`
	}
	if err := json.Unmarshal(p.GetBucketsJson(), &buckets); err != nil {
		t.Fatalf("buckets_json is not valid JSON: %v (%s)", err, p.GetBucketsJson())
	}
	if len(buckets) != 4 {
		t.Fatalf("buckets = %d, want 3 bounds plus +Inf", len(buckets))
	}
	want := []float64{1, 3, 4, 5}
	for i, b := range buckets {
		if b.Count != want[i] {
			t.Fatalf("bucket %d count = %v, want %v (counts must be cumulative)", i, b.Count, want[i])
		}
	}
	if buckets[3].Le != "+Inf" {
		t.Fatalf("last bound = %v, want the +Inf overflow entry", buckets[3].Le)
	}
	if le, ok := buckets[0].Le.(float64); !ok || le != 0.1 {
		t.Fatalf("first bound = %v, want 0.1", buckets[0].Le)
	}
}

func TestHistogramResetsAfterDrain(t *testing.T) {
	m := NewMetrics(&collectingSink{})
	h, _ := m.Histogram("inst-1", "latency", "", nil, []float64{1})

	h.Observe(0.5)
	if points := m.Drain(1); len(points) != 1 {
		t.Fatalf("first drain = %v", points)
	}
	h.Observe(0.5)
	points := m.Drain(2)
	if len(points) != 1 {
		t.Fatalf("second drain = %v", points)
	}
	if got := string(points[0].GetBucketsJson()); got != `[{"le":1,"count":1},{"le":"+Inf","count":1}]` {
		t.Fatalf("buckets after reset = %s", got)
	}
}

func TestHistogramFallsBackToTheLatencyLadder(t *testing.T) {
	m := NewMetrics(&collectingSink{})
	h, err := m.Histogram("inst-1", "latency", "", nil, nil)
	if err != nil {
		t.Fatalf("histogram: %v", err)
	}
	h.Observe(0.3)

	points := m.Drain(1)
	if len(points) != 1 {
		t.Fatalf("points = %d", len(points))
	}
	var buckets []struct {
		Le    any     `json:"le"`
		Count float64 `json:"count"`
	}
	if err := json.Unmarshal(points[0].GetBucketsJson(), &buckets); err != nil {
		t.Fatalf("buckets_json: %v", err)
	}
	if want := len(DefaultHistogramBounds()) + 1; len(buckets) != want {
		t.Fatalf("buckets = %d, want %d", len(buckets), want)
	}
	// 0.3 falls in the 0.5 bucket, so everything from there on counts it.
	if buckets[len(buckets)-1].Count != 1 {
		t.Fatalf("+Inf count = %v, want 1", buckets[len(buckets)-1].Count)
	}
}

func TestHistogramRejectsBadBounds(t *testing.T) {
	m := NewMetrics(&collectingSink{})
	cases := map[string][]float64{
		"empty":      {},
		"descending": {1, 0.5},
		"repeated":   {1, 1},
	}
	for name, bounds := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := m.Histogram("inst-1", "latency", "", nil, bounds); !errors.Is(err, ErrInvalidBounds) {
				t.Fatalf("err = %v, want ErrInvalidBounds", err)
			}
		})
	}
}

func TestSeriesRejectsAConflictingShape(t *testing.T) {
	m := NewMetrics(&collectingSink{})

	if _, err := m.Histogram("inst-1", "latency", "", nil, []float64{1, 2}); err != nil {
		t.Fatalf("histogram: %v", err)
	}
	if _, err := m.Histogram("inst-1", "latency", "", nil, []float64{1, 3}); !errors.Is(err, ErrMetricConflict) {
		t.Fatalf("err = %v, want ErrMetricConflict for a changed bucket layout", err)
	}

	if _, err := m.Gauge("inst-1", "temperature", "C", nil); err != nil {
		t.Fatalf("gauge: %v", err)
	}
	if _, err := m.Gauge("inst-1", "temperature", "K", nil); !errors.Is(err, ErrMetricConflict) {
		t.Fatalf("err = %v, want ErrMetricConflict for a changed unit", err)
	}
}

func TestFlushPushesIntoTheRing(t *testing.T) {
	ring := NewRing(Budgets{})
	m := NewMetrics(ring)

	counter, _ := m.Counter("inst-1", "hits", nil)
	counter.Inc()
	m.Flush(1)

	if got := ring.Len(RingMetrics); got != 1 {
		t.Fatalf("buffered metric points = %d, want 1", got)
	}
}

func TestMutatingTheCallersLabelsDoesNotMoveASeries(t *testing.T) {
	m := NewMetrics(&collectingSink{})
	labels := Labels{"route": "/a"}

	counter, _ := m.Counter("inst-1", "hits", labels)
	labels["route"] = "/b"
	counter.Inc()

	points := m.Drain(1)
	if len(points) != 1 || points[0].GetLabels()["route"] != "/a" {
		t.Fatalf("labels = %v, want the value captured at creation", points)
	}
}
