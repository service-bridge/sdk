//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/tests/e2e/e2epb"
)

// TestUnaryCallOverBothTransports runs the same logical call down the direct
// mTLS path and through the runtime proxy. Both have to answer identically:
// the transport is an implementation choice, not part of the contract.
func TestUnaryCallOverBothTransports(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	method := uniqueName("rpc.unary")
	callee := serviceName(domainRPC, 1)

	provider := newClient(t, domainRPC, 1, servicebridge.WithAdvertise("127.0.0.1", 0))
	if err := servicebridge.Handle(provider, method, echoHandler("unary")); err != nil {
		t.Fatalf("declare handler: %v", err)
	}
	start(ctx, t, provider)

	consumer := newClient(t, domainRPC, 2)
	if err := consumer.Service(callee, servicebridge.ServiceDeps{RPC: []string{method}}); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	start(ctx, t, consumer)
	waitForMethod(ctx, t, consumer, callee, method)

	for _, tc := range []struct {
		name      string
		transport servicebridge.Transport
	}{
		{"direct", servicebridge.TransportDirect},
		{"proxy", servicebridge.TransportProxy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := servicebridge.Call[*e2epb.Echo, *e2epb.EchoReply](ctx, consumer, callee, method,
				&e2epb.Echo{Text: "ping-" + tc.name, N: 7},
				servicebridge.WithTransport(tc.transport),
				servicebridge.WithTimeout(20*time.Second))
			if err != nil {
				t.Fatalf("call over %s: %v", tc.name, err)
			}
			if resp.GetText() != "ping-"+tc.name {
				t.Errorf("text came back as %q, want %q", resp.GetText(), "ping-"+tc.name)
			}
			if resp.GetN() != 7 {
				t.Errorf("n came back as %d, want 7", resp.GetN())
			}
			if resp.GetHandledBy() != "unary" {
				t.Errorf("call was served by %q, want %q", resp.GetHandledBy(), "unary")
			}
		})
	}
}

// TestServerStream proves the streaming path carries every chunk in order and
// terminates on its own, without the caller having to break out of the loop.
func TestServerStream(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	method := uniqueName("rpc.stream")
	callee := serviceName(domainRPC, 1)
	const chunks = 5

	provider := newClient(t, domainRPC, 1, servicebridge.WithAdvertise("127.0.0.1", 0))
	err := servicebridge.HandleStream(provider, method,
		func(_ context.Context, req *e2epb.Echo, send func(*e2epb.EchoReply) error) error {
			for i := range int64(chunks) {
				if err := send(&e2epb.EchoReply{Text: req.GetText(), N: i, HandledBy: "stream"}); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("declare stream handler: %v", err)
	}
	start(ctx, t, provider)

	consumer := newClient(t, domainRPC, 2)
	if err := consumer.Service(callee, servicebridge.ServiceDeps{RPC: []string{method}}); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	start(ctx, t, consumer)
	waitForMethod(ctx, t, consumer, callee, method)

	var got []int64
	for chunk, err := range servicebridge.Stream[*e2epb.Echo, *e2epb.EchoReply](ctx, consumer, callee, method,
		&e2epb.Echo{Text: "stream-me"}) {
		if err != nil {
			t.Fatalf("stream chunk %d: %v", len(got), err)
		}
		if chunk.GetText() != "stream-me" {
			t.Errorf("chunk %d carries text %q, want %q", len(got), chunk.GetText(), "stream-me")
		}
		got = append(got, chunk.GetN())
	}

	want := []int64{0, 1, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("received %d chunks, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunks arrived out of order: %v, want %v", got, want)
		}
	}
}

// TestContractHashRouting is the version-routing proof. Two instances of one
// service publish the same method name under different schemas; the caller's
// type parameters decide which of them is even a candidate. A caller reaching
// the wrong one would not fail to decode — it would silently read a different
// contract's fields.
func TestContractHashRouting(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	method := uniqueName("rpc.versioned")
	callee := serviceName(domainRPC, 1)

	v1 := newClient(t, domainRPC, 1, servicebridge.WithAdvertise("127.0.0.1", 0))
	if err := servicebridge.Handle(v1, method, echoHandler("v1")); err != nil {
		t.Fatalf("declare v1 handler: %v", err)
	}
	start(ctx, t, v1)

	v2 := newClient(t, domainRPC, 1, servicebridge.WithAdvertise("127.0.0.1", 0))
	err := servicebridge.Handle(v2, method, func(_ context.Context, req *e2epb.EchoV2) (*e2epb.EchoReplyV2, error) {
		return &e2epb.EchoReplyV2{Text: req.GetText(), N: req.GetN(), HandledBy: "v2", Region: req.GetRegion()}, nil
	})
	if err != nil {
		t.Fatalf("declare v2 handler: %v", err)
	}
	start(ctx, t, v2)

	consumer := newClient(t, domainRPC, 2)
	if err := consumer.Service(callee, servicebridge.ServiceDeps{RPC: []string{method}}); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	start(ctx, t, consumer)

	waitFor(ctx, t, discoveryTimeout, "both schema versions of "+method+" to be advertised",
		func(context.Context) (bool, error) {
			hashes := map[string]struct{}{}
			for _, m := range consumer.ServiceMap().Methods {
				if m.ServiceName == callee && m.Name == method {
					hashes[m.ContractHash] = struct{}{}
				}
			}
			return len(hashes) >= 2, nil
		})

	// Ten calls, because a single one could land on the right instance by
	// chance if the hash were ignored and the balancer picked one of two.
	for i := range 10 {
		resp, err := servicebridge.Call[*e2epb.Echo, *e2epb.EchoReply](ctx, consumer, callee, method,
			&e2epb.Echo{Text: fmt.Sprintf("v1-%d", i), N: int64(i)},
			servicebridge.WithTimeout(20*time.Second))
		if err != nil {
			t.Fatalf("v1 call %d: %v", i, err)
		}
		if resp.GetHandledBy() != "v1" {
			t.Fatalf("v1 call %d was served by the %q handler: the contract hash did not narrow the candidates",
				i, resp.GetHandledBy())
		}
	}

	for i := range 10 {
		resp, err := servicebridge.Call[*e2epb.EchoV2, *e2epb.EchoReplyV2](ctx, consumer, callee, method,
			&e2epb.EchoV2{Text: fmt.Sprintf("v2-%d", i), N: int64(i), Region: "eu"},
			servicebridge.WithTimeout(20*time.Second))
		if err != nil {
			t.Fatalf("v2 call %d: %v", i, err)
		}
		if resp.GetHandledBy() != "v2" {
			t.Fatalf("v2 call %d was served by the %q handler", i, resp.GetHandledBy())
		}
		if resp.GetRegion() != "eu" {
			t.Fatalf("v2 call %d lost the region field: %q", i, resp.GetRegion())
		}
	}
}

// TestContractHashMismatchHasNoCandidate proves the hash narrows candidates
// rather than being carried along for information: a schema nobody serves has
// no instance to reach, and the call fails before any payload is sent.
func TestContractHashMismatchHasNoCandidate(t *testing.T) {
	ctx := testContext(t, time.Minute)

	method := uniqueName("rpc.mismatch")
	callee := serviceName(domainRPC, 1)

	provider := newClient(t, domainRPC, 1, servicebridge.WithAdvertise("127.0.0.1", 0))
	if err := servicebridge.Handle(provider, method, echoHandler("only-v1")); err != nil {
		t.Fatalf("declare handler: %v", err)
	}
	start(ctx, t, provider)

	consumer := newClient(t, domainRPC, 2)
	if err := consumer.Service(callee, servicebridge.ServiceDeps{RPC: []string{method}}); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	start(ctx, t, consumer)
	waitForMethod(ctx, t, consumer, callee, method)

	_, err := servicebridge.Call[*e2epb.EchoV2, *e2epb.EchoReplyV2](ctx, consumer, callee, method,
		&e2epb.EchoV2{Text: "nobody-serves-this"}, servicebridge.WithTimeout(10*time.Second))
	if err == nil {
		t.Fatal("a call under an unserved contract succeeded")
	}
	var sbErr *servicebridge.Error
	if !errors.As(err, &sbErr) {
		t.Fatalf("error is %T, not an SDK error: %v", err, err)
	}
	if sbErr.Code != servicebridge.CodeNoLiveInstance {
		t.Errorf("error code is %q, want %q: %v", sbErr.Code, servicebridge.CodeNoLiveInstance, err)
	}
}

// TestOneOperationRowPerCall is the ADR-0001 invariant: a logical RPC call is
// exactly one row. The callee emits nothing and the runtime emits no forward
// hop, so a trace view built from these rows shows one span per call rather
// than three stacked ones that all describe the same request.
func TestOneOperationRowPerCall(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)

	method := uniqueName("rpc.oneop")
	callee := serviceName(domainRPC, 1)
	subject := "rpc.call:" + callee + "/" + method

	provider := newClient(t, domainRPC, 1, servicebridge.WithAdvertise("127.0.0.1", 0))
	if err := servicebridge.Handle(provider, method, echoHandler("oneop")); err != nil {
		t.Fatalf("declare handler: %v", err)
	}
	start(ctx, t, provider)

	consumer := newClient(t, domainRPC, 2)
	if err := consumer.Service(callee, servicebridge.ServiceDeps{RPC: []string{method}}); err != nil {
		t.Fatalf("declare dependency: %v", err)
	}
	start(ctx, t, consumer)
	waitForMethod(ctx, t, consumer, callee, method)

	if _, err := servicebridge.Call[*e2epb.Echo, *e2epb.EchoReply](ctx, consumer, callee, method,
		&e2epb.Echo{Text: "one-op", N: 1}, servicebridge.WithTimeout(20*time.Second)); err != nil {
		t.Fatalf("call: %v", err)
	}

	rows := waitRows(ctx, t, rowTimeout, "the call's operation row", fmt.Sprintf(
		`SELECT trace_id::text AS trace_id, op_id::text AS op_id, channel, kind, status,
		        actor_service_id::text AS actor_service_id, peer_service_id::text AS peer_service_id
		   FROM operations WHERE subject = %s`, lit(t, subject)), 1)

	if len(rows) != 1 {
		t.Fatalf("the call produced %d operation rows, want exactly 1: %v", len(rows), rows)
	}
	row := rows[0]
	if got := num(t, row, "channel"); got != 2 {
		t.Errorf("channel is %v, want 2 (RPC)", got)
	}
	if got := num(t, row, "kind"); got != 1 {
		t.Errorf("kind is %v, want 1 (CALL)", got)
	}
	if got := num(t, row, "status"); got != 2 {
		t.Errorf("status is %v, want 2 (SUCCESS)", got)
	}
	if got, want := str(row, "actor_service_id"), serviceIDOf(ctx, t, serviceName(domainRPC, 2)); got != want {
		t.Errorf("actor is service %s, want the caller %s", got, want)
	}
	if got, want := str(row, "peer_service_id"), serviceIDOf(ctx, t, callee); got != want {
		t.Errorf("peer is service %s, want the callee %s", got, want)
	}

	trace := str(row, "trace_id")
	extra := mustQuery(ctx, t, fmt.Sprintf(
		`SELECT kind, subject FROM operations WHERE trace_id = %s::uuid AND channel = 2 AND kind <> 1`,
		lit(t, trace)))
	if len(extra) != 0 {
		t.Errorf("the trace carries %d FORWARD/HANDLE rows, want none (ADR-0001): %v", len(extra), extra)
	}
}
