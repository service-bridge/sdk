package job_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/job"
)

// ---------------------------------------------------------------------------
// canonical spec: the exact bytes the runtime parses
// ---------------------------------------------------------------------------

// runtimeCanonicalJobSpec replicates runtime/internal/jobs/canonical.go verbatim
// — key names, field order and omission rules included. Decoding the SDK's
// output into it proves the two declarations still describe the same document;
// the golden strings below pin the bytes themselves.
type runtimeCanonicalJobSpec struct {
	Trigger       runtimeCanonicalTrigger `json:"trigger"`
	Catchup       string                  `json:"catchup,omitempty"`
	Overlap       string                  `json:"overlap,omitempty"`
	Deps          []runtimeCanonicalDep   `json:"deps,omitempty"`
	MaxAttempts   int                     `json:"maxAttempts,omitempty"`
	LeaseTTLMs    int                     `json:"leaseTtlMs,omitempty"`
	MaxConcurrent int                     `json:"maxConcurrent,omitempty"`
	Retry         *runtimeRetryPolicy     `json:"retry,omitempty"`
}

type runtimeCanonicalTrigger struct {
	Cron     *runtimeCanonicalCron     `json:"cron,omitempty"`
	Delayed  *runtimeCanonicalDelayed  `json:"delayed,omitempty"`
	Interval *runtimeCanonicalInterval `json:"interval,omitempty"`
}

type runtimeCanonicalCron struct {
	Expr string `json:"expr"`
	TZ   string `json:"tz,omitempty"`
}

type runtimeCanonicalDelayed struct {
	RunAtUnixMs int64 `json:"runAtUnixMs"`
}

type runtimeCanonicalInterval struct {
	EveryMs int64 `json:"everyMs"`
}

type runtimeCanonicalDep struct {
	Kind   string `json:"kind"`
	Target string `json:"target"`
}

type runtimeRetryPolicy struct {
	InitialMs  int64   `json:"initial_ms"`
	MaxMs      int64   `json:"max_ms"`
	Multiplier float64 `json:"multiplier"`
	Jitter     float64 `json:"jitter"`
}

func mustCron(t *testing.T, expr, tz string) job.Trigger {
	t.Helper()
	trg, err := job.NewCronTrigger(expr, tz)
	if err != nil {
		t.Fatalf("cron trigger %q: %v", expr, err)
	}
	return trg
}

func mustJSON(t *testing.T, spec job.Spec) string {
	t.Helper()
	out, err := spec.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}
	return string(out)
}

// TestCanonicalSpecMatchesTheRuntimeByteForByte is the test that keeps jobs
// registrable at all: the spec travels inside input_schema_json and the runtime
// decodes it by key name, so a renamed or reordered field turns into a
// registration failure with no useful message anywhere near the cause.
func TestCanonicalSpecMatchesTheRuntimeByteForByte(t *testing.T) {
	t.Parallel()

	delayed, err := job.NewAtTrigger(time.UnixMilli(1767225600000).UTC())
	if err != nil {
		t.Fatalf("delayed trigger: %v", err)
	}
	interval, err := job.NewIntervalTrigger(90 * time.Second)
	if err != nil {
		t.Fatalf("interval trigger: %v", err)
	}

	cases := []struct {
		name string
		spec job.Spec
		want string
	}{
		{
			name: "cron with every option set",
			spec: job.Spec{
				Trigger: mustCron(t, "*/5 * * * *", "Europe/Moscow"),
				Catchup: job.CatchupFireOnce,
				Overlap: job.OverlapAllow,
				Deps: []job.Dep{
					job.RPCDep("billing.Charge"),
					job.EventDep("orders.created"),
					job.WorkflowDep("nightly-close"),
				},
				MaxAttempts:   3,
				LeaseTTLMs:    45000,
				MaxConcurrent: 2,
				Retry:         &job.RetryPolicy{InitialMs: 1000, MaxMs: 600000, Multiplier: 2, Jitter: 0.25},
			},
			want: `{"trigger":{"cron":{"expr":"*/5 * * * *","tz":"Europe/Moscow"}},` +
				`"catchup":"fire_once","overlap":"allow",` +
				`"deps":[{"kind":"rpc","target":"billing.Charge"},` +
				`{"kind":"event","target":"orders.created"},` +
				`{"kind":"workflow","target":"nightly-close"}],` +
				`"maxAttempts":3,"leaseTtlMs":45000,"maxConcurrent":2,` +
				`"retry":{"initial_ms":1000,"max_ms":600000,"multiplier":2,"jitter":0.25}}`,
		},
		{
			name: "cron without a timezone omits tz",
			spec: job.Spec{Trigger: mustCron(t, "0 3 * * *", "")},
			want: `{"trigger":{"cron":{"expr":"0 3 * * *"}}}`,
		},
		{
			name: "delayed carries unix milliseconds",
			spec: job.Spec{Trigger: delayed},
			want: `{"trigger":{"delayed":{"runAtUnixMs":1767225600000}}}`,
		},
		{
			name: "interval carries milliseconds",
			spec: job.Spec{Trigger: interval},
			want: `{"trigger":{"interval":{"everyMs":90000}}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := mustJSON(t, tc.spec); got != tc.want {
				t.Fatalf("canonical spec drifted from the runtime shape\n got: %s\nwant: %s", got, tc.want)
			}
		})
	}
}

// TestCanonicalSpecDecodesIntoTheRuntimeStruct checks the same bytes from the
// other side: every key lands in a field the runtime declares, and no key the
// runtime does not know rides along.
func TestCanonicalSpecDecodesIntoTheRuntimeStruct(t *testing.T) {
	t.Parallel()

	spec := job.Spec{
		Trigger:       mustCron(t, "*/5 * * * *", "Europe/Moscow"),
		Catchup:       job.CatchupFireAll,
		Overlap:       job.OverlapBufferOne,
		Deps:          []job.Dep{job.RPCDep("billing.Charge")},
		MaxAttempts:   7,
		LeaseTTLMs:    30000,
		MaxConcurrent: 4,
		Retry:         &job.RetryPolicy{InitialMs: 250, MaxMs: 5000, Multiplier: 1.5, Jitter: 0.1},
	}
	raw, err := spec.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}

	var decoded runtimeCanonicalJobSpec
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&decoded); err != nil {
		t.Fatalf("runtime cannot decode the canonical spec: %v", err)
	}

	if decoded.Trigger.Cron == nil || decoded.Trigger.Cron.Expr != "*/5 * * * *" || decoded.Trigger.Cron.TZ != "Europe/Moscow" {
		t.Fatalf("trigger lost in decode: %+v", decoded.Trigger)
	}
	if decoded.Catchup != "fire_all" || decoded.Overlap != "buffer_one" {
		t.Fatalf("policies lost in decode: catchup=%q overlap=%q", decoded.Catchup, decoded.Overlap)
	}
	if len(decoded.Deps) != 1 || decoded.Deps[0] != (runtimeCanonicalDep{Kind: "rpc", Target: "billing.Charge"}) {
		t.Fatalf("deps lost in decode: %+v", decoded.Deps)
	}
	if decoded.MaxAttempts != 7 || decoded.LeaseTTLMs != 30000 || decoded.MaxConcurrent != 4 {
		t.Fatalf("limits lost in decode: %+v", decoded)
	}
	want := runtimeRetryPolicy{InitialMs: 250, MaxMs: 5000, Multiplier: 1.5, Jitter: 0.1}
	if decoded.Retry == nil || *decoded.Retry != want {
		// A camelCase retry block decodes into zeroes here, and the runtime then
		// replaces the whole policy with its own default without a word.
		t.Fatalf("retry policy lost in decode: %+v", decoded.Retry)
	}
}

// TestUnsetOptionsStayOutOfTheSpec keeps the runtime the only owner of the
// defaults: a spec that writes them down would drift the moment an operator
// changes a setting.
func TestUnsetOptionsStayOutOfTheSpec(t *testing.T) {
	t.Parallel()

	got := mustJSON(t, job.Spec{Trigger: mustCron(t, "* * * * *", "")})
	if got != `{"trigger":{"cron":{"expr":"* * * * *"}}}` {
		t.Fatalf("a bare spec carries more than its trigger: %s", got)
	}
}

// ---------------------------------------------------------------------------
// contract hash
// ---------------------------------------------------------------------------

func TestContractHashIsStableAndSpecific(t *testing.T) {
	t.Parallel()

	spec := job.Spec{Trigger: mustCron(t, "*/5 * * * *", "UTC"), MaxAttempts: 3}
	first, err := spec.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}
	second, err := spec.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}
	if job.ContractHash(first) != job.ContractHash(second) {
		t.Fatal("the same spec hashed differently across runs")
	}
	if got := job.ContractHash(first); len(got) != 64 {
		t.Fatalf("contract hash is not a sha-256 hex digest: %q", got)
	}

	changed := spec
	changed.MaxAttempts = 4
	other, err := changed.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}
	if job.ContractHash(first) == job.ContractHash(other) {
		t.Fatal("a changed spec kept the same contract hash")
	}
}

// ---------------------------------------------------------------------------
// triggers
// ---------------------------------------------------------------------------

// TestCronIsRejectedAtDeclaration pins the moment a bad expression fails. Left
// to the runtime, the same typo would register fine and the job would simply
// never fire.
func TestCronIsRejectedAtDeclaration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		expr string
		tz   string
		want error
	}{
		{name: "six fields look like seconds", expr: "*/5 * * * * *", want: job.ErrCronFieldCount},
		{name: "four fields", expr: "* * * *", want: job.ErrCronFieldCount},
		{name: "empty", expr: "", want: job.ErrCronFieldCount},
		{name: "descriptor", expr: "@daily", want: job.ErrCronFieldCount},
		{name: "minute out of range", expr: "99 * * * *", want: job.ErrCronExpr},
		{name: "garbage field", expr: "* * * * nope", want: job.ErrCronExpr},
		{name: "unknown timezone", expr: "* * * * *", tz: "Mars/Olympus", want: job.ErrCronTZ},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := job.NewCronTrigger(tc.expr, tc.tz)
			if !errors.Is(err, tc.want) {
				t.Fatalf("cron %q tz %q: got %v, want %v", tc.expr, tc.tz, err, tc.want)
			}
		})
	}
}

func TestValidCronIsAccepted(t *testing.T) {
	t.Parallel()

	for _, expr := range []string{"* * * * *", "*/5 * * * *", "0 3 * * 1-5", "15,45 */2 1 1 *"} {
		if _, err := job.NewCronTrigger(expr, "UTC"); err != nil {
			t.Fatalf("cron %q rejected: %v", expr, err)
		}
	}
}

func TestIntervalAndDelayedRejectUnusableValues(t *testing.T) {
	t.Parallel()

	if _, err := job.NewIntervalTrigger(0); !errors.Is(err, job.ErrInterval) {
		t.Fatalf("zero interval: got %v, want %v", err, job.ErrInterval)
	}
	if _, err := job.NewIntervalTrigger(-time.Second); !errors.Is(err, job.ErrInterval) {
		t.Fatalf("negative interval: got %v, want %v", err, job.ErrInterval)
	}
	// Sub-millisecond truncates to zero on the wire, which the runtime would read
	// as "no interval".
	if _, err := job.NewIntervalTrigger(500 * time.Microsecond); !errors.Is(err, job.ErrInterval) {
		t.Fatalf("sub-millisecond interval: got %v, want %v", err, job.ErrInterval)
	}
	if _, err := job.NewAtTrigger(time.Time{}); !errors.Is(err, job.ErrRunAt) {
		t.Fatalf("zero run time: got %v, want %v", err, job.ErrRunAt)
	}
}

// TestExactlyOneTriggerByConstruction walks every constructor and checks the
// encoded trigger object holds exactly one key. Nothing in the API can produce
// a second one.
func TestExactlyOneTriggerByConstruction(t *testing.T) {
	t.Parallel()

	interval, err := job.NewIntervalTrigger(time.Minute)
	if err != nil {
		t.Fatalf("interval trigger: %v", err)
	}
	delayed, err := job.NewAtTrigger(time.UnixMilli(1767225600000))
	if err != nil {
		t.Fatalf("delayed trigger: %v", err)
	}

	cases := map[string]job.Trigger{
		"cron":     mustCron(t, "* * * * *", "UTC"),
		"interval": interval,
		"delayed":  delayed,
	}
	for want, trg := range cases {
		raw, err := job.Spec{Trigger: trg}.CanonicalJSON()
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
			t.Fatalf("%s: trigger holds %d kinds: %v", want, len(probe.Trigger), probe.Trigger)
		}
		if _, ok := probe.Trigger[want]; !ok {
			t.Fatalf("%s: trigger holds the wrong kind: %v", want, probe.Trigger)
		}
	}

	// The zero value is the only shape with no trigger, and it never encodes.
	if _, err := (job.Spec{}).CanonicalJSON(); !errors.Is(err, job.ErrNoTrigger) {
		t.Fatalf("zero spec: got %v, want %v", err, job.ErrNoTrigger)
	}
}

// ---------------------------------------------------------------------------
// spec validation
// ---------------------------------------------------------------------------

func TestSpecRejectsWhatTheRuntimeWouldRefuseOrRewrite(t *testing.T) {
	t.Parallel()

	base := func() job.Spec { return job.Spec{Trigger: mustCron(t, "* * * * *", "UTC")} }

	cases := []struct {
		name  string
		build func() job.Spec
		want  error
	}{
		{
			name:  "unknown catchup policy",
			build: func() job.Spec { s := base(); s.Catchup = "later"; return s },
			want:  job.ErrCatchupPolicy,
		},
		{
			name:  "unknown overlap policy",
			build: func() job.Spec { s := base(); s.Overlap = "queue"; return s },
			want:  job.ErrOverlapPolicy,
		},
		{
			name:  "unknown dep kind",
			build: func() job.Spec { s := base(); s.Deps = []job.Dep{{Kind: "http", Target: "x"}}; return s },
			want:  job.ErrDepKind,
		},
		{
			name:  "dep without a target",
			build: func() job.Spec { s := base(); s.Deps = []job.Dep{job.RPCDep("")}; return s },
			want:  job.ErrDepTarget,
		},
		{
			name:  "negative limit",
			build: func() job.Spec { s := base(); s.MaxAttempts = -1; return s },
			want:  job.ErrNegativeLimit,
		},
		{
			name: "retry policy the runtime would silently replace",
			build: func() job.Spec {
				s := base()
				s.Retry = &job.RetryPolicy{MaxMs: 5000, Multiplier: 2, Jitter: 0.1}
				return s
			},
			want: job.ErrRetryInitial,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := tc.build().CanonicalJSON(); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// declarations
// ---------------------------------------------------------------------------

func TestDeclarationsFreezeTheCanonicalFormAndItsHash(t *testing.T) {
	t.Parallel()

	decls := job.NewDeclarations()
	spec := job.Spec{Trigger: mustCron(t, "0 * * * *", "UTC")}
	noop := func(context.Context, job.Execution) error { return nil }

	decl, err := decls.Add("hourly", spec, noop)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	raw, err := spec.CanonicalJSON()
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}
	if string(decl.SpecJSON) != string(raw) {
		t.Fatalf("declaration froze other bytes: %s", decl.SpecJSON)
	}
	if decl.ContractHash != job.ContractHash(raw) {
		t.Fatalf("declaration hash %q does not match its own bytes", decl.ContractHash)
	}
	if got, ok := decls.Lookup("hourly"); !ok || got.Name != "hourly" {
		t.Fatalf("lookup: %+v ok=%v", got, ok)
	}
	if _, ok := decls.Lookup("nope"); ok {
		t.Fatal("lookup found a job that was never declared")
	}
	if decls.Len() != 1 {
		t.Fatalf("len = %d, want 1", decls.Len())
	}
}

func TestDeclarationsRejectBrokenDeclarations(t *testing.T) {
	t.Parallel()

	decls := job.NewDeclarations()
	spec := job.Spec{Trigger: mustCron(t, "0 * * * *", "UTC")}
	noop := func(context.Context, job.Execution) error { return nil }

	if _, err := decls.Add("", spec, noop); !errors.Is(err, job.ErrEmptyName) {
		t.Fatalf("empty name: got %v, want %v", err, job.ErrEmptyName)
	}
	if _, err := decls.Add("nohandler", spec, nil); !errors.Is(err, job.ErrNoHandler) {
		t.Fatalf("missing handler: got %v, want %v", err, job.ErrNoHandler)
	}
	if _, err := decls.Add("notrigger", job.Spec{}, noop); !errors.Is(err, job.ErrNoTrigger) {
		t.Fatalf("missing trigger: got %v, want %v", err, job.ErrNoTrigger)
	}
	if _, err := decls.Add("hourly", spec, noop); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if _, err := decls.Add("hourly", spec, noop); !errors.Is(err, job.ErrDuplicateName) {
		t.Fatalf("duplicate name: got %v, want %v", err, job.ErrDuplicateName)
	}
}

func TestEachWalksInDeclarationOrder(t *testing.T) {
	t.Parallel()

	decls := job.NewDeclarations()
	spec := job.Spec{Trigger: mustCron(t, "0 * * * *", "UTC")}
	noop := func(context.Context, job.Execution) error { return nil }
	for _, name := range []string{"c", "a", "b"} {
		if _, err := decls.Add(name, spec, noop); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
	}

	var seen []string
	decls.Each(func(d job.Declaration) bool {
		seen = append(seen, d.Name)
		return true
	})
	if len(seen) != 3 || seen[0] != "c" || seen[1] != "a" || seen[2] != "b" {
		t.Fatalf("walk order: %v", seen)
	}

	seen = nil
	decls.Each(func(d job.Declaration) bool {
		seen = append(seen, d.Name)
		return false
	})
	if len(seen) != 1 {
		t.Fatalf("returning false did not stop the walk: %v", seen)
	}
}
