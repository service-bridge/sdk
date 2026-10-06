package workflow_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	iwf "github.com/service-bridge/sdk/go/internal/workflow"
	wf "github.com/service-bridge/sdk/go/workflow"
)

func lit(t *testing.T, e *pb.Expr) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(e.GetLiteral(), &v); err != nil {
		t.Fatalf("literal %v: %v", e, err)
	}
	return v
}

func TestEncodeEveryKind(t *testing.T) {
	fn := func(context.Context, map[string]any) (any, error) { return 1, nil }
	def := wf.Definition{
		Version:        "v2",
		Input:          map[string]any{"type": "object"},
		Retry:          &wf.RetryPolicy{MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, Factor: 2},
		MaxParallelism: 4,
		Timeout:        time.Minute,
		Steps: []wf.Step{
			wf.Call{
				Control: wf.Control{ID: "charge", Timeout: time.Second, Compensate: &wf.Compensation{
					Method: wf.Name("refund"), Input: wf.Path("$.charge"),
				}},
				Service: wf.Name("billing"), Method: wf.Name("charge"),
				Input: map[string]any{"amount": wf.Path("$.input.amount"), "list": []any{1, wf.Path("$.x")}},
				Opts:  &wf.CallOpts{Timeout: 500 * time.Millisecond, Transport: wf.TransportProxy, IdempotencyKey: wf.Path("$.input.id")},
			},
			wf.Publish{
				Control: wf.Control{ID: "note", WaitFor: []string{"charge"},
					When:       wf.And(wf.Equals(wf.Path("$.input.notify"), true), wf.Not(wf.In("x", wf.Path("$.input.tags")))),
					Compensate: &wf.Compensation{Kind: wf.CompensateCall, Service: wf.Name("billing"), Method: wf.Name("void")},
				},
				Event: wf.Name("order.charged"),
				Opts:  &wf.PublishOpts{PartitionKey: "pk", Headers: map[string]any{"h": wf.Path("$.input.h")}},
			},
			wf.Local{Control: wf.Control{ID: "calc", When: wf.Or(wf.Truthy("$.input.on"))}, Fn: fn},
			wf.Sleep{Control: wf.Control{ID: "nap"}, Duration: 2 * time.Second},
			wf.WaitEvent{Control: wf.Control{ID: "evt"}, Event: "paid", Filter: map[string]any{"$.id": wf.Path("$.input.id")}},
			wf.WaitSignal{Control: wf.Control{ID: "sig"}, Signal: "approve"},
			wf.SubWorkflow{Control: wf.Control{ID: "kid"}, Service: wf.Name("other"), Workflow: wf.Path("$.input.wf"), IdempotencyKey: "k", Timeout: time.Second},
			wf.Parallel{Control: wf.Control{ID: "each"}, ForEach: &wf.ForEach{From: "$.input.items", As: "it"},
				Steps: []wf.Step{wf.Local{Control: wf.Control{ID: "inner"}, Fn: fn}}},
			wf.Sequence{Control: wf.Control{ID: "seq"}},
		},
	}
	enc, err := iwf.Encode("order", def)
	if err != nil {
		t.Fatal(err)
	}
	d := enc.Definition
	if d.GetName() != "order" || d.GetVersion() != "v2" || d.GetMaxParallelism() != 4 || d.GetTimeoutMs() != 60000 ||
		d.GetRetry().GetBaseDelayMs() != 100 || string(d.GetInputSchemaJson()) != `{"type":"object"}` {
		t.Fatalf("definition header %+v", d)
	}
	s := d.GetSteps()
	if lit(t, s[0].GetCall().GetService()) != "billing" || s[0].GetTimeoutMs() != 1000 ||
		s[0].GetCall().GetInput().GetObject().GetFields()["amount"].GetPath() != "$.input.amount" ||
		s[0].GetCall().GetInput().GetObject().GetFields()["list"].GetList().GetItems()[1].GetPath() != "$.x" ||
		s[0].GetCall().GetOpts().GetTransport() != "proxy" || s[0].GetCall().GetOpts().GetIdempotencyKey().GetPath() != "$.input.id" ||
		lit(t, s[0].GetCall().GetCompensate().GetCall().GetMethod()) != "refund" ||
		s[0].GetCall().GetCompensate().GetInput().GetPath() != "$.charge" {
		t.Fatalf("call %+v", s[0])
	}
	and := s[1].GetWhen().GetAnd().GetItems()
	if and[0].GetEquals().GetLeft().GetPath() != "$.input.notify" || and[1].GetNot().GetIn().GetRight().GetPath() != "$.input.tags" ||
		s[1].GetPublish().GetCompensate().GetCall() == nil || lit(t, s[1].GetPublish().GetOpts().GetPartitionKey()) != "pk" ||
		s[1].GetPublish().GetOpts().GetHeaders()["h"].GetPath() != "$.input.h" {
		t.Fatalf("publish %+v", s[1])
	}
	if s[2].GetLocal() == nil || s[2].GetWhen().GetOr().GetItems()[0].GetTruthy().GetPath() != "$.input.on" {
		t.Fatalf("local %+v", s[2])
	}
	if s[3].GetSleep().GetDurationMs() != 2000 || s[4].GetWaitEvent().GetFilter()["$.id"].GetPath() != "$.input.id" ||
		s[5].GetWaitSignal().GetSignal() != "approve" {
		t.Fatalf("parks %+v %+v %+v", s[3], s[4], s[5])
	}
	kid := s[6].GetWorkflow()
	if lit(t, kid.GetService()) != "other" || kid.GetWorkflow().GetPath() != "$.input.wf" || lit(t, kid.GetIdempotencyKey()) != "k" || kid.GetTimeoutMs() != 1000 {
		t.Fatalf("workflow %+v", kid)
	}
	if s[7].GetParallel().GetForEach().GetAs() != "it" || s[8].GetSequence() == nil {
		t.Fatalf("groups %+v %+v", s[7], s[8])
	}
	if enc.Locals["calc"] == nil || enc.Locals["inner"] == nil {
		t.Fatal("local functions not kept")
	}
}

func TestEncodeRefusesWhatCannotTravel(t *testing.T) {
	cases := map[string]wf.Definition{
		"nil step":     {Steps: []wf.Step{nil}},
		"local w/o fn": {Steps: []wf.Step{wf.Local{Control: wf.Control{ID: "a"}}}},
		"bad literal":  {Steps: []wf.Step{wf.Call{Control: wf.Control{ID: "a"}, Input: func() {}}}},
		"bad schema":   {Input: map[string]any{"x": func() {}}},
		"bad comp":     {Steps: []wf.Step{wf.Call{Control: wf.Control{ID: "a", Compensate: &wf.Compensation{Kind: "undo"}}}}},
	}
	for name, def := range cases {
		if _, err := iwf.Encode("w", def); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
