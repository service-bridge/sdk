//go:build e2e

package e2e

import (
	"fmt"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
)

// TestUserOperationAndLogShareOneTrace proves the two halves of the SDK's
// observability contract meet in the database. An operation opened by the
// application reaches `operations`, and a log written inside it reaches
// `telemetry_logs` carrying the same trace and the operation as its parent —
// without that link a log line cannot be placed on the trace it belongs to.
func TestUserOperationAndLogShareOneTrace(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	subject := uniqueName("go.telemetry.subop")
	message := uniqueID("go-log-message")

	c := newClient(t, domainMisc, 1)
	start(ctx, t, c)

	opCtx, op := c.Telemetry.StartOp(ctx, subject, servicebridge.WithOpBusinessKey("order-42"))
	c.Telemetry.Logger().InfoContext(opCtx, message, "stage", "e2e")
	op.End()

	rows := waitRows(ctx, t, rowTimeout, "the user operation row", fmt.Sprintf(
		`SELECT trace_id::text AS trace_id, op_id::text AS op_id, channel, kind, status, business_key
		   FROM operations WHERE subject = %s`, lit(t, subject)), 1)
	if len(rows) != 1 {
		t.Fatalf("the operation produced %d rows, want 1: %v", len(rows), rows)
	}
	row := rows[0]
	if got := num(t, row, "channel"); got != 6 {
		t.Errorf("channel is %v, want 6 (USER)", got)
	}
	if got := num(t, row, "kind"); got != 1 {
		t.Errorf("kind is %v, want 1 (SUBOP)", got)
	}
	if got := num(t, row, "status"); got != 2 {
		t.Errorf("status is %v, want 2 (SUCCESS)", got)
	}
	if got := str(row, "business_key"); got != "order-42" {
		t.Errorf("business key is %q, want %q", got, "order-42")
	}

	logs := waitRows(ctx, t, rowTimeout, "the log record", fmt.Sprintf(
		`SELECT trace_id::text AS trace_id, op_id::text AS op_id, level::text AS level, fields::text AS fields
		   FROM telemetry_logs WHERE message = %s`, lit(t, message)), 1)
	if len(logs) != 1 {
		t.Fatalf("the log line produced %d rows, want 1: %v", len(logs), logs)
	}
	if got, want := str(logs[0], "trace_id"), str(row, "trace_id"); got != want {
		t.Errorf("the log carries trace %s but the operation carries %s: the log cannot be placed on the trace", got, want)
	}
	if got, want := str(logs[0], "op_id"), str(row, "op_id"); got != want {
		t.Errorf("the log hangs under operation %s, want %s", got, want)
	}
	if got := str(logs[0], "level"); got != "info" {
		t.Errorf("log level is %q, want %q", got, "info")
	}
}

// TestCallTelemetryNestsUnderUserOperation proves trace context travels through
// the call path in-process: a call issued inside a user operation is recorded as
// its child, which is what makes a trace a tree rather than a pile of roots.
func TestCallTelemetryNestsUnderUserOperation(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	method := uniqueName("rpc.nested")
	callee := serviceName(domainRPC, 1)
	subject := uniqueName("go.telemetry.parent")

	provider := newClient(t, domainRPC, 1, servicebridge.WithAdvertise("127.0.0.1", 0))
	if err := servicebridge.Handle(provider, method, echoHandler("nested")); err != nil {
		t.Fatalf("declare handler: %v", err)
	}
	start(ctx, t, provider)

	consumer := newClient(t, domainRPC, 2)
	if err := consumer.Service(callee, servicebridge.ServiceDeps{RPC: []string{method}}); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	start(ctx, t, consumer)
	waitForMethod(ctx, t, consumer, callee, method)

	opCtx, op := consumer.Telemetry.StartOp(ctx, subject)
	_, err := servicebridge.Call[*e2epb.Echo, *e2epb.EchoReply](opCtx, consumer, callee, method,
		&e2epb.Echo{Text: "nested"}, servicebridge.WithTimeout(20*time.Second))
	op.End()
	if err != nil {
		t.Fatalf("call: %v", err)
	}

	parents := waitRows(ctx, t, rowTimeout, "the enclosing user operation", fmt.Sprintf(
		`SELECT trace_id::text AS trace_id, op_id::text AS op_id FROM operations WHERE subject = %s`,
		lit(t, subject)), 1)
	calls := waitRows(ctx, t, rowTimeout, "the nested call operation", fmt.Sprintf(
		`SELECT trace_id::text AS trace_id, parent_op_id::text AS parent_op_id FROM operations WHERE subject = %s`,
		lit(t, "rpc.call:"+callee+"/"+method)), 1)

	if got, want := str(calls[0], "trace_id"), str(parents[0], "trace_id"); got != want {
		t.Errorf("the call is on trace %s, the operation that made it on %s", got, want)
	}
	if got, want := str(calls[0], "parent_op_id"), str(parents[0], "op_id"); got != want {
		t.Errorf("the call hangs under operation %s, want %s", got, want)
	}
}
