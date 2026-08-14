//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/job"
)

// The canonical job spec (go/internal/job/canonical.go ↔ node/src/job/domain.ts)
// is hashed byte-for-byte by each SDK and never round-trips through the
// runtime: RegisterJobs (runtime/internal/jobs/canonical.go) parses
// input_schema_json straight into a jobs.Definition and persists the parsed
// fields — it stores no contract_hash for a job at all (unlike a workflow's
// fingerprint column). So the only way to prove the two SDKs agree is to
// register an equivalent spec from both and compare what the runtime actually
// parsed out of each — a field-name or unit mismatch on one side changes what
// lands in job_definitions, or gets silently dropped, without ever producing a
// visible error.

// TestJobCanonicalSpecMatchesAcrossLanguages registers the same job semantics
// from a Go client and a Node agent and asserts the runtime parsed both
// registrations into identical job_definitions rows (bar id/service/name/
// timestamps). No firing involved: the interval is set far in the future so
// this only exercises registration + parsing, the layer where a canonical
// encoding mismatch would actually surface.
func TestJobCanonicalSpecMatchesAcrossLanguages(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	goName := uniqueName("xlang.jobspec.go")
	nodeName := uniqueName("xlang.jobspec.node")
	const intervalMs = 3_600_000 // 1h: never fires within the test.

	trigger, err := job.Interval(intervalMs * time.Millisecond)
	if err != nil {
		t.Fatalf("build interval trigger: %v", err)
	}
	spec := job.NewSpec(trigger,
		job.WithCatchup(job.CatchupFireOnce),
		job.WithOverlap(job.OverlapAllow),
		job.WithDeps(job.RPC("some-service.SomeMethod")),
		job.WithMaxAttempts(7),
		job.WithLeaseTTL(12_345*time.Millisecond),
		job.WithMaxConcurrent(1),
		job.WithRetry(job.RetryPolicy{InitialMs: 1000, MaxMs: 60_000, Multiplier: 2.0, Jitter: 0.25}),
	)

	goClient := newClient(t, domainXLang, 1)
	if err := goClient.Job.Handle(goName, spec, func(context.Context, job.Execution) error { return nil }); err != nil {
		t.Fatalf("declare Go job: %v", err)
	}
	start(ctx, t, goClient)

	nodeAgentCfg := newAgentConfig(t)
	nodeAgentCfg.JobName = nodeName
	nodeAgentCfg.JobOpts = map[string]any{
		"trigger":       map[string]any{"interval": intervalMs},
		"catchup":       "fire_once",
		"overlap":       "allow",
		"deps":          []map[string]any{{"rpc": "some-service.SomeMethod"}},
		"maxAttempts":   7,
		"leaseTtlMs":    12345,
		"maxConcurrent": 1,
		"retry": map[string]any{
			"initialMs":  1000,
			"maxMs":      60000,
			"multiplier": 2.0,
			"jitter":     0.25,
		},
	}
	startNodeAgent(ctx, t, nodeAgentCfg)

	goRows := waitRows(ctx, t, rowTimeout, "the Go job definition", fmt.Sprintf(
		`SELECT trigger_kind, interval_ms, catchup_policy, overlap_policy, declared_deps,
		        max_attempts, lease_ttl_ms, max_concurrent, retry_policy
		   FROM job_definitions WHERE name = %s`, lit(t, goName)), 1)
	nodeRows := waitRows(ctx, t, rowTimeout, "the Node job definition", fmt.Sprintf(
		`SELECT trigger_kind, interval_ms, catchup_policy, overlap_policy, declared_deps,
		        max_attempts, lease_ttl_ms, max_concurrent, retry_policy
		   FROM job_definitions WHERE name = %s`, lit(t, nodeName)), 1)

	goRow, nodeRow := goRows[0], nodeRows[0]
	for _, col := range []string{
		"trigger_kind", "interval_ms", "catchup_policy", "overlap_policy",
		"declared_deps", "max_attempts", "lease_ttl_ms", "max_concurrent", "retry_policy",
	} {
		if fmt.Sprint(goRow[col]) != fmt.Sprint(nodeRow[col]) {
			t.Errorf("column %s differs between languages: go=%v node=%v", col, goRow[col], nodeRow[col])
		}
	}
}

// TestJobRegisteredInNodeExecutesOnGoInstance proves a job the Node SDK
// registered fires correctly on a Go instance of the same service: the
// dispatcher (runtime/internal/jobs/dispatcher.go) picks a live instance by
// service id alone, with no notion of which language registered or will run
// it, so a real cross-language worker handoff needs the Node registrant gone
// before fire time and a Go replica of the same service holding an identical
// declaration in its place — the same shape as the runtime's own
// jobs-lifecycle "fresh replica" tests, done across languages instead of
// across restarts.
func TestJobRegisteredInNodeExecutesOnGoInstance(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	jobName := uniqueName("xlang.job.node2go")
	fireAtMs := time.Now().Add(6 * time.Second).UnixMilli()

	cfg := newAgentConfig(t)
	cfg.Key = bootstrapKey(t, domainXLang, 3)
	cfg.JobName = jobName
	cfg.JobOpts = map[string]any{
		"trigger":     map[string]any{"delayed": map[string]any{"at": fireAtMs}},
		"maxAttempts": 3,
	}
	agent := startNodeAgent(ctx, t, cfg)

	waitRows(ctx, t, rowTimeout, "the Node-registered job definition", fmt.Sprintf(
		`SELECT 1 FROM job_definitions WHERE name = %s`, lit(t, jobName)), 1)

	agent.stop()

	trigger, err := job.At(time.UnixMilli(fireAtMs))
	if err != nil {
		t.Fatalf("build delayed trigger: %v", err)
	}
	fired := make(chan job.Execution, 4)
	goClient := newClient(t, domainXLang, 3)
	err = goClient.Job.Handle(jobName, job.NewSpec(trigger, job.WithMaxAttempts(3)),
		func(_ context.Context, exec job.Execution) error {
			select {
			case fired <- exec:
			default:
			}
			return nil
		})
	if err != nil {
		t.Fatalf("declare Go job: %v", err)
	}
	start(ctx, t, goClient)

	var exec job.Execution
	select {
	case exec = <-fired:
	case <-time.After(deliveryTimeout):
		t.Fatalf("job %s never fired on the Go instance within %s", jobName, deliveryTimeout)
	}
	if exec.Name != jobName {
		t.Errorf("execution names job %q, want %q", exec.Name, jobName)
	}

	rows := waitRows(ctx, t, rowTimeout, "the job execution to reach a terminal state", fmt.Sprintf(
		`SELECT e.status AS status FROM job_executions e
		   JOIN job_definitions d ON d.id = e.job_definition_id
		  WHERE d.name = %s AND e.completed_at IS NOT NULL`, lit(t, jobName)), 1)
	if got := str(rows[0], "status"); got != "success" {
		t.Errorf("execution status is %q, want %q", got, "success")
	}
}

// TestJobRegisteredInGoExecutesOnNodeInstance is the reverse direction: a job
// the Go SDK registered, then vacates before fire time, fires on a Node
// instance of the same service.
func TestJobRegisteredInGoExecutesOnNodeInstance(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	jobName := uniqueName("xlang.job.go2node")
	fireAtMs := time.Now().Add(6 * time.Second).UnixMilli()

	trigger, err := job.At(time.UnixMilli(fireAtMs))
	if err != nil {
		t.Fatalf("build delayed trigger: %v", err)
	}
	goClient := newClient(t, domainXLang, 3)
	err = goClient.Job.Handle(jobName, job.NewSpec(trigger, job.WithMaxAttempts(3)),
		func(context.Context, job.Execution) error {
			t.Error("the Go instance's handler fired: it should have disconnected before fire time")
			return nil
		})
	if err != nil {
		t.Fatalf("declare Go job: %v", err)
	}
	start(ctx, t, goClient)

	waitRows(ctx, t, rowTimeout, "the Go-registered job definition", fmt.Sprintf(
		`SELECT 1 FROM job_definitions WHERE name = %s`, lit(t, jobName)), 1)

	stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	if err := goClient.Stop(stopCtx); err != nil {
		t.Fatalf("stop Go client: %v", err)
	}
	cancel()

	cfg := newAgentConfig(t)
	cfg.Key = bootstrapKey(t, domainXLang, 3)
	cfg.JobName = jobName
	cfg.JobOpts = map[string]any{
		"trigger":     map[string]any{"delayed": map[string]any{"at": fireAtMs}},
		"maxAttempts": 3,
	}
	agent := startNodeAgent(ctx, t, cfg)

	got := agent.waitJob(t, deliveryTimeout)
	if got.Name != jobName {
		t.Errorf("the Node agent's handler fired for job %q, want %q", got.Name, jobName)
	}

	rows := waitRows(ctx, t, rowTimeout, "the job execution to reach a terminal state", fmt.Sprintf(
		`SELECT e.status AS status FROM job_executions e
		   JOIN job_definitions d ON d.id = e.job_definition_id
		  WHERE d.name = %s AND e.completed_at IS NOT NULL`, lit(t, jobName)), 1)
	if got := str(rows[0], "status"); got != "success" {
		t.Errorf("execution status is %q, want %q", got, "success")
	}
}
