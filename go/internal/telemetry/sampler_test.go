package telemetry

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

// fakeClock advances only when a test says so, so a sampling interval is
// exercised without waiting one out.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func pointsByName(points []*pb.MetricPoint) map[string]*pb.MetricPoint {
	out := make(map[string]*pb.MetricPoint, len(points))
	for _, p := range points {
		out[p.GetName()] = p
	}
	return out
}

func TestSamplerRecordsCPUShareAndResidentMemory(t *testing.T) {
	clock := &fakeClock{now: time.UnixMilli(1_700_000_000_000)}
	var cpu atomic.Int64
	metrics := NewMetrics(&collectingSink{})

	sampler, err := NewSampler(SamplerConfig{
		Metrics:       metrics,
		InstanceID:    func() string { return "inst-1" },
		CPUTime:       func() (time.Duration, error) { return time.Duration(cpu.Load()), nil },
		ResidentBytes: func() uint64 { return 4096 },
		Now:           clock.Now,
	})
	if err != nil {
		t.Fatalf("new sampler: %v", err)
	}

	// Half a core over the interval.
	clock.advance(10 * time.Second)
	cpu.Store(int64(5 * time.Second))
	if err := sampler.Sample(); err != nil {
		t.Fatalf("sample: %v", err)
	}

	points := pointsByName(metrics.Drain(clock.Now().UnixMilli()))
	cpuPoint, ok := points[metricCPUPercent]
	if !ok {
		t.Fatalf("no cpu point in %v", points)
	}
	if cpuPoint.GetValue() != 50 {
		t.Fatalf("cpu percent = %v, want 50", cpuPoint.GetValue())
	}
	if cpuPoint.GetUnit() != unitPercent || cpuPoint.GetKind() != pb.MetricKind_METRIC_KIND_GAUGE {
		t.Fatalf("cpu point shape = %v %v", cpuPoint.GetUnit(), cpuPoint.GetKind())
	}
	rssPoint, ok := points[metricRSSBytes]
	if !ok {
		t.Fatalf("no rss point in %v", points)
	}
	if rssPoint.GetValue() != 4096 || rssPoint.GetUnit() != unitBytes {
		t.Fatalf("rss point = %v %v", rssPoint.GetValue(), rssPoint.GetUnit())
	}
	if rssPoint.GetInstanceId() != "inst-1" {
		t.Fatalf("instance_id = %q", rssPoint.GetInstanceId())
	}
}

// The identity arrives after the sampler is built, and every sample taken from
// then on must carry it.
func TestSamplerResolvesInstanceIDPerSample(t *testing.T) {
	clock := &fakeClock{now: time.UnixMilli(1)}
	metrics := NewMetrics(&collectingSink{})
	var instanceID atomic.Value
	instanceID.Store("")

	sampler, err := NewSampler(SamplerConfig{
		Metrics:       metrics,
		InstanceID:    func() string { return instanceID.Load().(string) },
		CPUTime:       func() (time.Duration, error) { return 0, nil },
		ResidentBytes: func() uint64 { return 1 },
		Now:           clock.Now,
	})
	if err != nil {
		t.Fatalf("new sampler: %v", err)
	}

	if err := sampler.Sample(); err != nil {
		t.Fatalf("sample: %v", err)
	}
	metrics.Drain(1)

	instanceID.Store("inst-late")
	clock.advance(time.Second)
	if err := sampler.Sample(); err != nil {
		t.Fatalf("sample: %v", err)
	}

	points := metrics.Drain(2)
	if len(points) != 2 {
		t.Fatalf("points = %d, want both gauges", len(points))
	}
	for _, p := range points {
		if p.GetInstanceId() != "inst-late" {
			t.Fatalf("%s carried instance %q", p.GetName(), p.GetInstanceId())
		}
	}
}

func TestSamplerReportsAFailedCPURead(t *testing.T) {
	metrics := NewMetrics(&collectingSink{})
	boom := errors.New("boom")

	sampler, err := NewSampler(SamplerConfig{
		Metrics:       metrics,
		InstanceID:    func() string { return "inst-1" },
		CPUTime:       func() (time.Duration, error) { return 0, boom },
		ResidentBytes: func() uint64 { return 1 },
	})
	if err != nil {
		t.Fatalf("new sampler: %v", err)
	}

	if err := sampler.Sample(); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the read failure", err)
	}
	if got := metrics.SeriesCount(); got != 0 {
		t.Fatalf("series = %d, want none on a failed read", got)
	}
}

// The application may already hold a metric under one of the sampler's names.
// That collision is reported, not papered over with a mismatched series.
func TestSamplerReportsAConflictingMetricName(t *testing.T) {
	metrics := NewMetrics(&collectingSink{})
	if _, err := metrics.Gauge("inst-1", metricRSSBytes, "MB", nil); err != nil {
		t.Fatalf("gauge: %v", err)
	}

	sampler, err := NewSampler(SamplerConfig{
		Metrics:       metrics,
		InstanceID:    func() string { return "inst-1" },
		CPUTime:       func() (time.Duration, error) { return 0, nil },
		ResidentBytes: func() uint64 { return 1 },
	})
	if err != nil {
		t.Fatalf("new sampler: %v", err)
	}

	if err := sampler.Sample(); !errors.Is(err, ErrMetricConflict) {
		t.Fatalf("err = %v, want ErrMetricConflict", err)
	}
}

func TestSamplerSamplesImmediatelyAndOnTheInterval(t *testing.T) {
	metrics := NewMetrics(&collectingSink{})
	var samples atomic.Int64

	sampler, err := NewSampler(SamplerConfig{
		Metrics:    metrics,
		InstanceID: func() string { return "inst-1" },
		Interval:   2 * time.Millisecond,
		CPUTime: func() (time.Duration, error) {
			samples.Add(1)
			return 0, nil
		},
		ResidentBytes: func() uint64 { return 1 },
	})
	if err != nil {
		t.Fatalf("new sampler: %v", err)
	}
	if err := sampler.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(sampler.Stop)

	waitFor(t, "the immediate sample", func() bool { return samples.Load() >= 1 })
	waitFor(t, "a sample on the interval", func() bool { return samples.Load() >= 3 })
}

func TestSamplerStopsWithItsContext(t *testing.T) {
	before := runtime.NumGoroutine()
	metrics := NewMetrics(&collectingSink{})
	var samples atomic.Int64

	sampler, err := NewSampler(SamplerConfig{
		Metrics:    metrics,
		InstanceID: func() string { return "inst-1" },
		Interval:   time.Millisecond,
		CPUTime: func() (time.Duration, error) {
			samples.Add(1)
			return 0, nil
		},
		ResidentBytes: func() uint64 { return 1 },
	})
	if err != nil {
		t.Fatalf("new sampler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := sampler.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, "sampling to begin", func() bool { return samples.Load() >= 2 })

	cancel()
	waitFor(t, "the goroutine to exit", func() bool { return runtime.NumGoroutine() <= before })

	settled := samples.Load()
	time.Sleep(10 * time.Millisecond)
	if got := samples.Load(); got != settled {
		t.Fatalf("samples kept coming after cancel: %d then %d", settled, got)
	}
	sampler.Stop()
}

// A failing sample must not stop the loop: the next interval may well succeed.
func TestSamplerKeepsSamplingAfterAFailure(t *testing.T) {
	metrics := NewMetrics(&collectingSink{})
	failures := make(chan error, 4)

	sampler, err := NewSampler(SamplerConfig{
		Metrics:       metrics,
		InstanceID:    func() string { return "inst-1" },
		Interval:      time.Millisecond,
		CPUTime:       func() (time.Duration, error) { return 0, errors.New("boom") },
		ResidentBytes: func() uint64 { return 1 },
		OnError:       func(err error) { failures <- err },
	})
	if err != nil {
		t.Fatalf("new sampler: %v", err)
	}
	if err := sampler.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(sampler.Stop)

	for i := 0; i < 2; i++ {
		select {
		case <-failures:
		case <-time.After(2 * time.Second):
			t.Fatal("the sampler stopped after a failed sample")
		}
	}
}

func TestSamplerStartsOnlyOnce(t *testing.T) {
	metrics := NewMetrics(&collectingSink{})
	sampler, err := NewSampler(SamplerConfig{
		Metrics:       metrics,
		InstanceID:    func() string { return "inst-1" },
		Interval:      time.Hour,
		CPUTime:       func() (time.Duration, error) { return 0, nil },
		ResidentBytes: func() uint64 { return 1 },
	})
	if err != nil {
		t.Fatalf("new sampler: %v", err)
	}
	if err := sampler.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(sampler.Stop)

	if err := sampler.Start(context.Background()); !errors.Is(err, ErrSamplerStarted) {
		t.Fatalf("err = %v, want ErrSamplerStarted", err)
	}
}

func TestNewSamplerRejectsMissingDependencies(t *testing.T) {
	cases := map[string]SamplerConfig{
		"metrics":  {InstanceID: func() string { return "" }},
		"instance": {Metrics: NewMetrics(&collectingSink{})},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewSampler(cfg); !errors.Is(err, ErrSamplerConfig) {
				t.Fatalf("err = %v, want ErrSamplerConfig", err)
			}
		})
	}
}

func TestCPUPercent(t *testing.T) {
	cases := []struct {
		name      string
		prev, cur time.Duration
		elapsed   time.Duration
		want      float64
	}{
		{"half a core", 0, time.Second, 2 * time.Second, 50},
		{"two cores", 0, 4 * time.Second, 2 * time.Second, 200},
		{"no interval to average over", 0, time.Second, 0, 0},
		{"counter did not move", time.Second, time.Second, time.Second, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := cpuPercent(c.prev, c.cur, c.elapsed); got != c.want {
				t.Fatalf("cpuPercent = %v, want %v", got, c.want)
			}
		})
	}
}

func TestProcessReadersReturnSomething(t *testing.T) {
	cpu, err := processCPUTime()
	if err != nil {
		t.Fatalf("process cpu time: %v", err)
	}
	if cpu <= 0 {
		t.Fatalf("cpu time = %v, want the accounting this process already accrued", cpu)
	}
	if residentBytes() == 0 {
		t.Fatal("resident bytes = 0")
	}
}
