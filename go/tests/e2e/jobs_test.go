//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/job"
)

// TestDelayedJobFiresAndReports proves both halves of the job contract: the
// runtime schedules the declared trigger and dispatches it to this instance,
// and the handler's outcome travels back — the execution row reaches a terminal
// state on its own, without the SDK being asked for it.
func TestDelayedJobFiresAndReports(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)

	name := uniqueName("go.job.delayed")
	fired := make(chan job.Execution, 4)

	c := newClient(t, domainJobs, 1)
	trigger, err := job.At(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatalf("build delayed trigger: %v", err)
	}
	err = c.Job.Handle(name, job.NewSpec(trigger, job.WithMaxAttempts(1)),
		func(_ context.Context, exec job.Execution) error {
			select {
			case fired <- exec:
			default:
			}
			return nil
		})
	if err != nil {
		t.Fatalf("declare job: %v", err)
	}
	start(ctx, t, c)

	var exec job.Execution
	select {
	case exec = <-fired:
	case <-time.After(deliveryTimeout):
		t.Fatalf("job %s never fired within %s", name, deliveryTimeout)
	}

	if exec.Name != name {
		t.Errorf("execution names job %q, want %q", exec.Name, name)
	}
	if exec.ID == "" {
		t.Error("execution carries no identifier")
	}
	if exec.IdempotencyKey == "" {
		t.Error("execution carries no idempotency key: retries of one fire cannot be deduplicated")
	}
	if exec.ScheduledAtUnixMs == 0 {
		t.Error("execution carries no scheduled time")
	}

	rows := waitRows(ctx, t, rowTimeout, "the job execution to reach a terminal state", fmt.Sprintf(
		`SELECT e.status AS status, e.attempt AS attempt, e.last_error AS last_error
		   FROM job_executions e
		   JOIN job_definitions d ON d.id = e.job_definition_id
		  WHERE d.name = %s AND e.completed_at IS NOT NULL`, lit(t, name)), 1)

	if len(rows) != 1 {
		t.Fatalf("job %s produced %d completed executions, want 1: %v", name, len(rows), rows)
	}
	if got := str(rows[0], "status"); got != "success" {
		t.Errorf("execution status is %q, want %q (last error: %q)", got, "success", str(rows[0], "last_error"))
	}

	ops := waitRows(ctx, t, rowTimeout, "the job's execution operation", fmt.Sprintf(
		`SELECT channel, kind, status FROM operations WHERE subject = %s`,
		lit(t, "job.exec:"+name)), 1)
	if got := num(t, ops[0], "channel"); got != 5 {
		t.Errorf("job operation is on channel %v, want 5 (JOB)", got)
	}
	if got := num(t, ops[0], "kind"); got != 1 {
		t.Errorf("job operation kind is %v, want 1 (EXEC)", got)
	}
}
