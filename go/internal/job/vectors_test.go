package job_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
	_ "time/tzdata" // the vectors name IANA zones; a minimal image has no zoneinfo

	job "github.com/service-bridge/sdk/go/internal/job"
)

// vectorInput is the Node SDK's JobOpts shape, so one file drives both SDKs.
type vectorInput struct {
	Version string `json:"version"`
	Trigger struct {
		Cron     *string `json:"cron"`
		TZ       string  `json:"tz"`
		Interval *int64  `json:"interval"`
		Delayed  *struct {
			At int64 `json:"at"`
		} `json:"delayed"`
	} `json:"trigger"`
	Catchup string `json:"catchup"`
	Overlap string `json:"overlap"`
	Deps    []struct {
		RPC      *string `json:"rpc"`
		Event    *string `json:"event"`
		Workflow *string `json:"workflow"`
	} `json:"deps"`
	MaxAttempts   *int   `json:"maxAttempts"`
	LeaseTTLMs    *int64 `json:"leaseTtlMs"`
	MaxConcurrent *int   `json:"maxConcurrent"`
	Retry         *struct {
		InitialMs  int64   `json:"initialMs"`
		MaxMs      int64   `json:"maxMs"`
		Multiplier float64 `json:"multiplier"`
		Jitter     float64 `json:"jitter"`
	} `json:"retry"`
}

func (in vectorInput) spec(t *testing.T) job.Spec {
	t.Helper()
	s := job.Spec{
		Version:       in.Version,
		Catchup:       job.CatchupPolicy(in.Catchup),
		Overlap:       job.OverlapPolicy(in.Overlap),
		MaxAttempts:   in.MaxAttempts,
		LeaseTTLMs:    in.LeaseTTLMs,
		MaxConcurrent: in.MaxConcurrent,
	}
	var err error
	switch {
	case in.Trigger.Cron != nil:
		s.Trigger, err = job.NewCronTrigger(*in.Trigger.Cron, in.Trigger.TZ)
	case in.Trigger.Interval != nil:
		s.Trigger, err = job.NewIntervalTrigger(time.Duration(*in.Trigger.Interval) * time.Millisecond)
	case in.Trigger.Delayed != nil:
		s.Trigger, err = job.NewAtTrigger(time.UnixMilli(in.Trigger.Delayed.At))
	}
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	for _, d := range in.Deps {
		switch {
		case d.RPC != nil:
			s.Deps = append(s.Deps, job.RPCDep(*d.RPC))
		case d.Event != nil:
			s.Deps = append(s.Deps, job.EventDep(*d.Event))
		case d.Workflow != nil:
			s.Deps = append(s.Deps, job.WorkflowDep(*d.Workflow))
		}
	}
	if in.Retry != nil {
		s.Retry = &job.RetryPolicy{InitialMs: in.Retry.InitialMs, MaxMs: in.Retry.MaxMs, Multiplier: in.Retry.Multiplier, Jitter: in.Retry.Jitter}
	}
	return s
}

// TestCanonicalSpecMatchesTheSharedVectors pins the canonical bytes to the ones
// the Node SDK produces: sdk/job-canonical-vectors.json is generated with
// JSON.stringify and read by the Node tests too. A spec that encodes
// differently in the two SDKs registers under two contract hashes.
func TestCanonicalSpecMatchesTheSharedVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "job-canonical-vectors.json"))
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var file struct {
		Vectors []struct {
			Name      string      `json:"name"`
			Input     vectorInput `json:"input"`
			Canonical string      `json:"canonical"`
			SHA256    string      `json:"sha256"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	if len(file.Vectors) < 12 {
		t.Fatalf("only %d vectors", len(file.Vectors))
	}
	for _, v := range file.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			got, err := v.Input.spec(t).CanonicalJSON()
			if err != nil {
				t.Fatalf("CanonicalJSON: %v", err)
			}
			if string(got) != v.Canonical {
				t.Fatalf("canonical bytes differ\n got: %s\nwant: %s", got, v.Canonical)
			}
			if hash := job.ContractHash(got); hash != v.SHA256 {
				t.Fatalf("hash %s, want %s", hash, v.SHA256)
			}
		})
	}
}
