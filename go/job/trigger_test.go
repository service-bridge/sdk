package job_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/job"
)

func mustCron(t *testing.T, expr, tz string) job.Trigger {
	t.Helper()
	trg, err := job.Cron(expr, tz)
	if err != nil {
		t.Fatalf("job.Cron(%q, %q): %v", expr, tz, err)
	}
	return trg
}

// TestInvalidCronFailsAtDeclaration is the whole point of validating in the
// constructor: a typo that the runtime would accept as a well-formed string
// turns a job into one that simply never fires, and nothing in the logs says so.
func TestInvalidCronFailsAtDeclaration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		expr string
		tz   string
		want error
	}{
		{name: "seconds field", expr: "0 */5 * * * *", want: job.ErrCronFieldCount},
		{name: "too few fields", expr: "* * *", want: job.ErrCronFieldCount},
		{name: "descriptor", expr: "@hourly", want: job.ErrCronFieldCount},
		{name: "empty", expr: "", want: job.ErrCronFieldCount},
		{name: "hour out of range", expr: "0 33 * * *", want: job.ErrCronExpr},
		{name: "unparseable field", expr: "* * * * xyz", want: job.ErrCronExpr},
		{name: "unknown timezone", expr: "0 3 * * *", tz: "Middle/Earth", want: job.ErrCronTZ},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := job.Cron(tc.expr, tc.tz)
			if !errors.Is(err, tc.want) {
				t.Fatalf("job.Cron(%q, %q) = %v, want %v", tc.expr, tc.tz, err, tc.want)
			}
		})
	}
}

func TestValidTriggersAreAccepted(t *testing.T) {
	t.Parallel()

	if _, err := job.Cron("*/10 * * * 1-5", "Europe/Moscow"); err != nil {
		t.Fatalf("valid cron rejected: %v", err)
	}
	if _, err := job.Interval(30 * time.Second); err != nil {
		t.Fatalf("valid interval rejected: %v", err)
	}
	if _, err := job.At(time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("valid delayed trigger rejected: %v", err)
	}
}

func TestIntervalAndAtRejectUnusableValues(t *testing.T) {
	t.Parallel()

	if _, err := job.Interval(0); !errors.Is(err, job.ErrInterval) {
		t.Fatalf("job.Interval(0) = %v, want %v", err, job.ErrInterval)
	}
	if _, err := job.At(time.Time{}); !errors.Is(err, job.ErrRunAt) {
		t.Fatalf("job.At(zero) = %v, want %v", err, job.ErrRunAt)
	}
}

// TestOneTriggerByConstruction: the API offers no way to build a spec with two
// triggers or with none — the constructors are the only source of a Trigger, and
// NewSpec takes exactly one.
func TestOneTriggerByConstruction(t *testing.T) {
	t.Parallel()

	interval, err := job.Interval(time.Minute)
	if err != nil {
		t.Fatalf("interval: %v", err)
	}
	at, err := job.At(time.UnixMilli(1767225600000))
	if err != nil {
		t.Fatalf("at: %v", err)
	}

	for want, trg := range map[string]job.Trigger{
		"cron":     mustCron(t, "0 3 * * *", "UTC"),
		"interval": interval,
		"delayed":  at,
	} {
		raw, err := job.NewSpec(trg).CanonicalJSON()
		if err != nil {
			t.Fatalf("%s: canonical json: %v", want, err)
		}
		var probe struct {
			Trigger map[string]json.RawMessage `json:"trigger"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil {
			t.Fatalf("%s: decode: %v", want, err)
		}
		if len(probe.Trigger) != 1 {
			t.Fatalf("%s: %d triggers encoded: %v", want, len(probe.Trigger), probe.Trigger)
		}
		if _, ok := probe.Trigger[want]; !ok {
			t.Fatalf("%s: wrong trigger kind encoded: %v", want, probe.Trigger)
		}
	}

	if _, err := job.NewSpec(job.Trigger{}).CanonicalJSON(); !errors.Is(err, job.ErrNoTrigger) {
		t.Fatalf("spec with no trigger = %v, want %v", err, job.ErrNoTrigger)
	}
}

func TestOptionsBuildTheSpecTheRuntimeReads(t *testing.T) {
	t.Parallel()

	spec := job.NewSpec(mustCron(t, "*/5 * * * *", "Europe/Moscow"),
		job.WithCatchup(job.CatchupFireOnce),
		job.WithOverlap(job.OverlapAllow),
		job.WithDeps(job.RPC("billing.Charge")),
		job.WithDeps(job.Event("orders.created"), job.Workflow("nightly-close")),
		job.WithMaxAttempts(3),
		job.WithLeaseTTL(45*time.Second),
		job.WithMaxConcurrent(2),
		job.WithRetry(job.RetryPolicy{InitialMs: 1000, MaxMs: 600000, Multiplier: 2, Jitter: 0.25}),
	)

	raw, err := spec.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}
	want := `{"trigger":{"cron":{"expr":"*/5 * * * *","tz":"Europe/Moscow"}},` +
		`"catchup":"fire_once","overlap":"allow",` +
		`"deps":[{"kind":"rpc","target":"billing.Charge"},` +
		`{"kind":"event","target":"orders.created"},` +
		`{"kind":"workflow","target":"nightly-close"}],` +
		`"maxAttempts":3,"leaseTtlMs":45000,"maxConcurrent":2,` +
		`"retry":{"initial_ms":1000,"max_ms":600000,"multiplier":2,"jitter":0.25}}`
	if string(raw) != want {
		t.Fatalf("options built another document\n got: %s\nwant: %s", raw, want)
	}
}

// TestUnsetOptionsLeaveTheDefaultsToTheRuntime keeps the SDK from becoming a
// second source of truth: an operator changing a jobs setting must not have to
// change the SDK too.
func TestUnsetOptionsLeaveTheDefaultsToTheRuntime(t *testing.T) {
	t.Parallel()

	raw, err := job.NewSpec(mustCron(t, "0 * * * *", "")).CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}
	if got, want := string(raw), `{"trigger":{"cron":{"expr":"0 * * * *"}}}`; got != want {
		t.Fatalf("bare spec carries more than its trigger\n got: %s\nwant: %s", got, want)
	}
}

func TestBadOptionsAreRefused(t *testing.T) {
	t.Parallel()

	trg := mustCron(t, "0 * * * *", "UTC")
	cases := []struct {
		name string
		opt  job.Option
		want error
	}{
		{"catchup", job.WithCatchup("eventually"), job.ErrCatchupPolicy},
		{"overlap", job.WithOverlap("queue"), job.ErrOverlapPolicy},
		{"dep target", job.WithDeps(job.RPC("")), job.ErrDepTarget},
		{"max attempts", job.WithMaxAttempts(-1), job.ErrNegativeLimit},
		{"retry", job.WithRetry(job.RetryPolicy{MaxMs: 1000}), job.ErrRetryInitial},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := job.NewSpec(trg, tc.opt).CanonicalJSON(); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}
