package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// ErrSamplerConfig is returned by NewSampler when a required dependency is
// missing.
var ErrSamplerConfig = errors.New("invalid process sampler config")

// ErrSamplerStarted is returned by Start on a sampler that already runs.
var ErrSamplerStarted = errors.New("process sampler already started")

// DefaultSampleInterval is how often process resource use is measured.
const DefaultSampleInterval = 30 * time.Second

// Metric names the console reads the per-instance resource charts from.
const (
	metricCPUPercent = "process.cpu_percent"
	metricRSSBytes   = "process.rss_bytes"
)

// SamplerConfig wires the process sampler. See ./README.md.
type SamplerConfig struct {
	// Metrics is where the two gauges are kept.
	Metrics *Metrics
	// InstanceID is resolved per sample: the sampler starts before the runtime
	// hands out an instance identity, and the samples taken after that point
	// must carry it.
	InstanceID func() string
	// Interval defaults to DefaultSampleInterval.
	Interval time.Duration
	// CPUTime reads the process CPU time consumed so far. Empty means the
	// operating system's own accounting.
	CPUTime func() (time.Duration, error)
	// ResidentBytes reads the memory the process holds. Empty means the Go
	// runtime's own accounting.
	ResidentBytes func() uint64
	// Now supplies the clock. Empty means the wall clock.
	Now func() time.Time
	// OnError reports a failed sample. Sampling continues regardless.
	OnError func(err error)
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// Sampler measures process CPU share and resident memory on an interval and
// records them as gauges.
type Sampler struct {
	cfg SamplerConfig

	wg sync.WaitGroup

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	prevCPU time.Duration
	prevAt  time.Time
}

// NewSampler validates the config and builds an idle sampler. The baseline for
// the first CPU share is taken here, so construction belongs next to the point
// the process starts doing work.
func NewSampler(cfg SamplerConfig) (*Sampler, error) {
	if cfg.Metrics == nil {
		return nil, fmt.Errorf("telemetry: new sampler: missing Metrics: %w", ErrSamplerConfig)
	}
	if cfg.InstanceID == nil {
		return nil, fmt.Errorf("telemetry: new sampler: missing InstanceID: %w", ErrSamplerConfig)
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultSampleInterval
	}
	if cfg.CPUTime == nil {
		cfg.CPUTime = processCPUTime
	}
	if cfg.ResidentBytes == nil {
		cfg.ResidentBytes = residentBytes
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	s := &Sampler{cfg: cfg, prevAt: cfg.Now()}
	if cpu, err := cfg.CPUTime(); err == nil {
		s.prevCPU = cpu
	}
	return s, nil
}

// Start takes the first sample and keeps sampling until ctx is cancelled or
// Stop runs.
func (s *Sampler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("telemetry: sampler: start: %w", ErrSamplerStarted)
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.started = true
	s.cancel = cancel
	s.mu.Unlock()

	s.wg.Add(1)
	go s.run(runCtx)
	return nil
}

// Stop cancels sampling and waits for the goroutine it owns. Terminal.
func (s *Sampler) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.wg.Wait()
}

func (s *Sampler) run(ctx context.Context) {
	defer s.wg.Done()

	// The first sample is taken on the spot: a service that just connected must
	// show its footprint from the moment it started, not one interval later.
	s.sampleOnce()

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sampleOnce()
		}
	}
}

func (s *Sampler) sampleOnce() {
	if err := s.Sample(); err != nil {
		s.cfg.Logger.Warn("telemetry: process sample failed", "err", err)
		if s.cfg.OnError != nil {
			s.cfg.OnError(err)
		}
	}
}

// Sample measures once and records both gauges.
func (s *Sampler) Sample() error {
	cpu, err := s.cfg.CPUTime()
	if err != nil {
		return fmt.Errorf("telemetry: sampler: read cpu time: %w", err)
	}
	now := s.cfg.Now()

	s.mu.Lock()
	share := cpuPercent(s.prevCPU, cpu, now.Sub(s.prevAt))
	s.prevCPU = cpu
	s.prevAt = now
	s.mu.Unlock()

	instanceID := s.cfg.InstanceID()

	cpuGauge, err := s.cfg.Metrics.Gauge(instanceID, metricCPUPercent, unitPercent, nil)
	if err != nil {
		return fmt.Errorf("telemetry: sampler: %w", err)
	}
	rssGauge, err := s.cfg.Metrics.Gauge(instanceID, metricRSSBytes, unitBytes, nil)
	if err != nil {
		return fmt.Errorf("telemetry: sampler: %w", err)
	}

	cpuGauge.Set(share)
	rssGauge.Set(float64(s.cfg.ResidentBytes()))
	return nil
}

// cpuPercent is the share of one core the process consumed over the interval: a
// process saturating two cores reports ~200. An interval of zero or less has
// nothing to average over — that is the first sample of a freshly built
// sampler, not a process using no CPU.
func cpuPercent(prev, cur, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	delta := cur - prev
	if delta <= 0 {
		return 0
	}
	return float64(delta) / float64(elapsed) * 100
}

// processCPUTime reads user plus system time from the kernel's own accounting
// for this process.
func processCPUTime() (time.Duration, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, fmt.Errorf("getrusage: %w", err)
	}
	return timevalToDuration(usage.Utime) + timevalToDuration(usage.Stime), nil
}

func timevalToDuration(tv syscall.Timeval) time.Duration {
	return time.Duration(tv.Sec)*time.Second + time.Duration(tv.Usec)*time.Microsecond
}

// residentBytes reports the memory the Go runtime holds from the operating
// system. It is not the kernel's RSS: the standard library exposes no portable
// reading of that, and getrusage only carries the high-water mark, which never
// falls and so cannot show a leak being released. Sys minus what has been given
// back is the closest honest measure of what this process currently occupies.
func residentBytes() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Sys - stats.HeapReleased
}
