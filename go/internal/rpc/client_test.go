package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testService = "billing"
	testMethod  = "Charge"
	testHash    = "v2:abc123"
)

// stubRegistry is the registry index the call loop reads. It keys candidates by
// (service, method, contractHash) exactly as the real cache does, so a call that
// widens the hash finds nothing here either.
type stubRegistry struct {
	methods   map[string][]*pb.MethodDescriptor
	instances map[string]*pb.ServiceInstanceInfo
}

func (r *stubRegistry) Candidates(service, method, contractHash string) []*pb.MethodDescriptor {
	return r.methods[service+"|"+method+"|"+contractHash]
}

func (r *stubRegistry) Instance(instanceID string) (*pb.ServiceInstanceInfo, bool) {
	inst, ok := r.instances[instanceID]
	return inst, ok
}

// registryWith indexes one descriptor per instance under the test triple.
func registryWith(instances ...*pb.ServiceInstanceInfo) *stubRegistry {
	r := &stubRegistry{
		methods:   map[string][]*pb.MethodDescriptor{},
		instances: map[string]*pb.ServiceInstanceInfo{},
	}
	key := testService + "|" + testMethod + "|" + testHash
	for _, inst := range instances {
		r.methods[key] = append(r.methods[key], &pb.MethodDescriptor{
			ServiceName:  testService,
			ServiceId:    inst.GetServiceId(),
			InstanceId:   inst.GetInstanceId(),
			Name:         testMethod,
			ContractHash: testHash,
			Type:         pb.MethodType_METHOD_TYPE_RPC,
		})
		r.instances[inst.GetInstanceId()] = inst
	}
	return r
}

func instanceInfo(instanceID, endpoint string) *pb.ServiceInstanceInfo {
	return &pb.ServiceInstanceInfo{
		InstanceId:   instanceID,
		ServiceId:    "svc-uuid",
		ServiceName:  testService,
		CallEndpoint: endpoint,
		Status:       "connected",
	}
}

func recorderUnderTest() (*telemetry.Recorder, *telemetry.Ring) {
	ring := telemetry.NewRing(telemetry.DefaultBudgets())
	return telemetry.NewRecorder(ring, telemetry.NewPolicy()), ring
}

func opReports(ring *telemetry.Ring) []*pb.OpReport {
	batch := ring.Peek(1000)
	out := make([]*pb.OpReport, 0, len(batch.Ops))
	for _, item := range batch.Ops {
		out = append(out, item.Msg)
	}
	return out
}

// fastRetry keeps the ladder's shape but drops the wall-clock cost.
func fastRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseMs: 1, MaxMs: 4, Multiplier: 2, JitterRatio: 0}
}

func proxyClient(t *testing.T, reg CandidateSource, stub pb.InvokeClient, ring **telemetry.Ring) *Client {
	t.Helper()

	rec, r := recorderUnderTest()
	if ring != nil {
		*ring = r
	}
	c, err := NewClient(ClientConfig{
		Registry:      reg,
		Proxy:         proxyOver(t, stub),
		Recorder:      rec,
		Retry:         fastRetry(),
		Transport:     TransportProxy,
		CallerService: "checkout",
		Logger:        outboundTestLogger(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return c
}

func testRequest() Request {
	return Request{
		Service:      testService,
		Method:       testMethod,
		Payload:      []byte("body"),
		ContractHash: testHash,
	}
}

// TestSelectionFailuresAreDistinguishable is the operator-facing invariant: the
// three ways a call finds nobody to talk to have three different fixes, so they
// must not collapse into one message the way they did in the Node SDK.
func TestSelectionFailuresAreDistinguishable(t *testing.T) {
	cases := []struct {
		name     string
		registry *stubRegistry
		want     error
		mentions string
	}{
		{
			name:     "nothing serves this contract hash",
			registry: registryWith(),
			want:     ErrNoCandidates,
			mentions: "candidates=0",
		},
		{
			name:     "the callee advertises no address",
			registry: registryWith(instanceInfo("inst-1", ""), instanceInfo("inst-2", "")),
			want:     ErrNoEndpoint,
			mentions: "addressed=0",
		},
		{
			name: "everything is shed",
			registry: func() *stubRegistry {
				a := instanceInfo("inst-1", "10.0.0.1:14446")
				a.IsUnhealthySinceUnixMs = time.Now().UnixMilli()
				b := instanceInfo("inst-2", "10.0.0.2:14446")
				b.IsUnhealthySinceUnixMs = time.Now().UnixMilli()
				return registryWith(a, b)
			}(),
			want:     ErrAllUnavailable,
			mentions: "eligible=0",
		},
	}

	seen := map[string]string{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := proxyClient(t, tc.registry, &stubInvoke{}, nil)

			_, err := c.Unary(context.Background(), testRequest())
			if err == nil {
				t.Fatal("expected the call to fail")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want it to unwrap to %v", err, tc.want)
			}

			var sel *SelectionError
			if !errors.As(err, &sel) {
				t.Fatalf("error must be a SelectionError, got %T", err)
			}
			if !strings.Contains(err.Error(), tc.mentions) {
				t.Fatalf("message %q must carry the evidence %q", err.Error(), tc.mentions)
			}
			// A selection failure is Unavailable so a callee redeploy heals it.
			if code, ok := callCode(err); !ok || code != codes.Unavailable {
				t.Fatalf("selection failure must report Unavailable, got %v (ok=%v)", code, ok)
			}

			for other, msg := range seen {
				if msg == err.Error() {
					t.Fatalf("%q and %q produce the identical message %q", other, tc.name, msg)
				}
			}
			seen[tc.name] = err.Error()
		})
	}

	if len(seen) != 3 {
		t.Fatalf("expected three distinct failure messages, got %d", len(seen))
	}
}

func TestSelectionFailureEmitsNoOperation(t *testing.T) {
	var ring *telemetry.Ring
	c := proxyClient(t, registryWith(), &stubInvoke{}, &ring)

	if _, err := c.Unary(context.Background(), testRequest()); err == nil {
		t.Fatal("expected the call to fail")
	}
	if got := len(opReports(ring)); got != 0 {
		t.Fatalf("emitted %d operation frames before a candidate existed; the peer and via_proxy fields would be guesses", got)
	}
}

// TestOneOperationPerLogicalCall pins ADR-0001: retries mutate the same
// operation, they do not mint a row each.
func TestOneOperationPerLogicalCall(t *testing.T) {
	stub := &stubInvoke{
		err:      status.Error(codes.Unavailable, "conn refused"),
		errUntil: 2,
		resp:     &pb.InvokeResponse{Payload: []byte("ok")},
	}
	var ring *telemetry.Ring
	c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, &ring)

	got, err := c.Unary(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("payload = %q, want ok", got)
	}
	if stub.calls != 3 {
		t.Fatalf("dispatched %d times, want 3 (two Unavailable retries then success)", stub.calls)
	}

	frames := opReports(ring)
	if len(frames) != 2 {
		t.Fatalf("emitted %d operation frames, want exactly 2 (START + END) for one logical call", len(frames))
	}
	if frames[0].GetOpId() != frames[1].GetOpId() {
		t.Fatalf("START and END carry different op ids (%s vs %s); a retry must not mint a new row",
			frames[0].GetOpId(), frames[1].GetOpId())
	}
	if frames[0].GetStatus() != pb.Status_PENDING || frames[1].GetStatus() != pb.Status_SUCCESS {
		t.Fatalf("frame statuses = %v, %v, want PENDING then SUCCESS", frames[0].GetStatus(), frames[1].GetStatus())
	}
	if got := frames[1].GetAttempt(); got != 2 {
		t.Fatalf("END attempt = %d, want 2 (zero-indexed third try)", got)
	}
	if frames[0].GetChannel() != pb.Channel_RPC || frames[0].GetKind() != uint32(telemetry.OpKindRPCCall) {
		t.Fatalf("operation is not an RPC.CALL: channel=%v kind=%d", frames[0].GetChannel(), frames[0].GetKind())
	}
	if want := "rpc.call:" + testService + "/" + testMethod; frames[0].GetSubject() != want {
		t.Fatalf("subject = %q, want %q", frames[0].GetSubject(), want)
	}
	if frames[0].GetPeerServiceId() != "svc-uuid" {
		t.Fatalf("peer service = %q, want the callee's UUID", frames[0].GetPeerServiceId())
	}

	var meta callMeta
	if err := json.Unmarshal(frames[0].GetMetaJson(), &meta); err != nil {
		t.Fatalf("meta_json must be a JSON object: %v", err)
	}
	if !meta.ViaProxy || meta.Method != testMethod {
		t.Fatalf("meta = %#v, want via_proxy true and the method name", meta)
	}
}

// TestDeadlineExceededIsNotRetriedWithoutAKey is the ADR-0001 §2 rule end to
// end: a Charge that times out must be attempted once, not three times.
func TestDeadlineExceededIsNotRetriedWithoutAKey(t *testing.T) {
	reg := registryWith(instanceInfo("inst-1", "10.0.0.1:14446"))

	t.Run("without an idempotency key", func(t *testing.T) {
		stub := &stubInvoke{err: status.Error(codes.DeadlineExceeded, "too slow")}
		c := proxyClient(t, reg, stub, nil)

		if _, err := c.Unary(context.Background(), testRequest()); err == nil {
			t.Fatal("expected the call to fail")
		}
		if stub.calls != 1 {
			t.Fatalf("dispatched %d times; a deadline without an idempotency key must never be repeated", stub.calls)
		}
	})

	t.Run("with an idempotency key", func(t *testing.T) {
		stub := &stubInvoke{err: status.Error(codes.DeadlineExceeded, "too slow")}
		c := proxyClient(t, reg, stub, nil)

		req := testRequest()
		req.IdempotencyKey = "charge-42"
		if _, err := c.Unary(context.Background(), req); err == nil {
			t.Fatal("expected the call to fail")
		}
		if stub.calls != 3 {
			t.Fatalf("dispatched %d times, want the full ladder once the caller opted in", stub.calls)
		}
		if stub.requests[0].GetIdempotencyKey() != "charge-42" {
			t.Fatal("the idempotency key must travel on the wire")
		}
	})
}

func TestUnavailableRetriesWithoutAKey(t *testing.T) {
	stub := &stubInvoke{err: status.Error(codes.Unavailable, "conn refused")}
	c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, nil)

	if _, err := c.Unary(context.Background(), testRequest()); err == nil {
		t.Fatal("expected the call to fail")
	}
	if stub.calls != 3 {
		t.Fatalf("dispatched %d times, want 3: Unavailable proves the request never ran", stub.calls)
	}
}

func TestBusinessErrorStopsTheLadderImmediately(t *testing.T) {
	stub := &stubInvoke{resp: &pb.InvokeResponse{ErrorCode: "VALIDATION", ErrorMessage: "amount must be positive"}}
	var ring *telemetry.Ring
	c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, &ring)

	req := testRequest()
	req.IdempotencyKey = "charge-42"
	_, err := c.Unary(context.Background(), req)

	var he *HandlerError
	if !errors.As(err, &he) {
		t.Fatalf("expected a HandlerError, got %v", err)
	}
	if stub.calls != 1 {
		t.Fatalf("dispatched %d times; the handler already decided", stub.calls)
	}
	frames := opReports(ring)
	if frames[len(frames)-1].GetStatus() != pb.Status_ERROR {
		t.Fatalf("END status = %v, want ERROR", frames[len(frames)-1].GetStatus())
	}
}

func TestDeadlineEndsTheOperationAsTimeout(t *testing.T) {
	stub := &stubInvoke{err: status.Error(codes.DeadlineExceeded, "too slow")}
	var ring *telemetry.Ring
	c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, &ring)

	if _, err := c.Unary(context.Background(), testRequest()); err == nil {
		t.Fatal("expected the call to fail")
	}
	frames := opReports(ring)
	if got := frames[len(frames)-1].GetStatus(); got != pb.Status_TIMEOUT {
		t.Fatalf("END status = %v, want TIMEOUT", got)
	}
}

// TestStreamIsNeverRetried covers both halves of the rule: not mid-stream,
// where a repeat re-delivers consumed chunks, and not on connect either.
func TestStreamIsNeverRetried(t *testing.T) {
	t.Run("connect failure", func(t *testing.T) {
		stub := &stubInvoke{err: status.Error(codes.Unavailable, "conn refused")}
		c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, nil)

		if _, err := c.Stream(context.Background(), testRequest()); err == nil {
			t.Fatal("expected the stream to fail")
		}
		if stub.calls != 1 {
			t.Fatalf("opened %d streams; retrying only the connect phase is a hidden mode", stub.calls)
		}
	})

	t.Run("mid-stream failure", func(t *testing.T) {
		stub := &stubInvoke{chunks: []*pb.InvokeChunk{
			{Payload: []byte("a")},
			{ErrorCode: "INTERNAL", ErrorMessage: "blew up"},
		}}
		var ring *telemetry.Ring
		c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, &ring)

		st, err := c.Stream(context.Background(), testRequest())
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}
		if _, err := st.Recv(); err != nil {
			t.Fatalf("first chunk: %v", err)
		}
		if _, err := st.Recv(); err == nil {
			t.Fatal("expected the second chunk to fail")
		}
		if stub.calls != 1 {
			t.Fatalf("opened %d streams; a mid-stream failure must never be replayed", stub.calls)
		}

		frames := opReports(ring)
		if len(frames) != 2 {
			t.Fatalf("emitted %d frames, want START + END for one stream", len(frames))
		}
		if frames[1].GetStatus() != pb.Status_ERROR {
			t.Fatalf("END status = %v, want ERROR", frames[1].GetStatus())
		}
	})
}

func TestStreamClosedEarlyIsAbandoned(t *testing.T) {
	stub := &stubInvoke{chunks: []*pb.InvokeChunk{{Payload: []byte("a")}, {Payload: []byte("b")}}}
	var ring *telemetry.Ring
	c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, &ring)

	st, err := c.Stream(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := st.Recv(); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Closing twice must stay harmless.
	if err := st.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	frames := opReports(ring)
	if got := frames[len(frames)-1].GetStatus(); got != pb.Status_ABANDONED {
		t.Fatalf("END status = %v, want ABANDONED", got)
	}
}

func TestStreamRunToEndSucceeds(t *testing.T) {
	stub := &stubInvoke{chunks: []*pb.InvokeChunk{{Payload: []byte("a")}, {Payload: []byte("b")}}}
	var ring *telemetry.Ring
	c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, &ring)

	st, err := c.Stream(context.Background(), testRequest())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	for {
		if _, rerr := st.Recv(); rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				t.Fatalf("stream ended with %v", rerr)
			}
			break
		}
	}
	frames := opReports(ring)
	if got := frames[len(frames)-1].GetStatus(); got != pb.Status_SUCCESS {
		t.Fatalf("END status = %v, want SUCCESS", got)
	}
}

// TestTraceHeaderTravelsInBothMetadataAndBody: the runtime reads gRPC metadata
// on the proxy path, the callee reads the body field on the direct path, and
// one call may take either — so both are always set.
func TestTraceHeaderTravelsInBothMetadataAndBody(t *testing.T) {
	stub := &stubInvoke{}
	var ring *telemetry.Ring
	c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, &ring)

	if _, err := c.Unary(context.Background(), testRequest()); err != nil {
		t.Fatalf("call: %v", err)
	}

	body := stub.requests[0].GetXSbTrace()
	if body == "" {
		t.Fatal("x_sb_trace must be set in the request body")
	}
	md := stub.metas[0]
	header := md.Get(telemetry.MetadataKey)
	if len(header) == 0 {
		t.Fatalf("metadata %q must carry the trace header", telemetry.MetadataKey)
	}
	if header[0] != body {
		t.Fatalf("metadata header %q and body field %q must agree", header[0], body)
	}

	tc, err := telemetry.ParseHeader(body)
	if err != nil {
		t.Fatalf("the header must parse with the SDK's only parser: %v", err)
	}
	frames := opReports(ring)
	if tc.TraceID.String() != frames[0].GetTraceId() {
		t.Fatalf("propagated trace %s differs from the operation's %s", tc.TraceID, frames[0].GetTraceId())
	}
	if tc.ParentOpID.String() != frames[0].GetOpId() {
		t.Fatal("the callee must see this call's operation as its parent")
	}
}

func TestRequestIdIsStableAcrossRetries(t *testing.T) {
	stub := &stubInvoke{
		err:      status.Error(codes.Unavailable, "conn refused"),
		errUntil: 2,
		resp:     &pb.InvokeResponse{Payload: []byte("ok")},
	}
	c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, nil)

	req := testRequest()
	req.IdempotencyKey = "charge-42"
	if _, err := c.Unary(context.Background(), req); err != nil {
		t.Fatalf("call: %v", err)
	}
	if len(stub.requests) != 3 {
		t.Fatalf("want 3 attempts, got %d", len(stub.requests))
	}
	first := stub.requests[0].GetRequestId()
	if first == "" {
		t.Fatal("request_id must be set")
	}
	for i, r := range stub.requests {
		if r.GetRequestId() != first {
			t.Fatalf("attempt %d carries request_id %q, want the same %q: one logical call, one identity", i, r.GetRequestId(), first)
		}
	}
}

func TestVersionRoutingIsNotWidened(t *testing.T) {
	reg := registryWith(instanceInfo("inst-1", "10.0.0.1:14446"))
	stub := &stubInvoke{}
	c := proxyClient(t, reg, stub, nil)

	req := testRequest()
	req.ContractHash = "v2:a-different-version"

	_, err := c.Unary(context.Background(), req)
	if !errors.Is(err, ErrNoCandidates) {
		t.Fatalf("a call at another contract hash must find nobody, got %v", err)
	}
	if stub.calls != 0 {
		t.Fatal("nothing may be dispatched when no instance advertises the caller's contract hash")
	}
}

func TestNewClientRequiresItsDependencies(t *testing.T) {
	rec, _ := recorderUnderTest()
	reg := registryWith()

	cases := []struct {
		name string
		cfg  ClientConfig
	}{
		{"no registry", ClientConfig{Recorder: rec, Direct: NewDirect(DirectConfig{})}},
		{"no recorder", ClientConfig{Registry: reg, Direct: NewDirect(DirectConfig{})}},
		{"direct selected but absent", ClientConfig{Registry: reg, Recorder: rec, Transport: TransportDirect}},
		{"proxy selected but absent", ClientConfig{Registry: reg, Recorder: rec, Transport: TransportProxy}},
		{"unknown transport", ClientConfig{Registry: reg, Recorder: rec, Transport: Transport(9)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewClient(tc.cfg); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("expected ErrInvalidConfig, got %v", err)
			}
		})
	}
}

// TestDirectPathCallsThePinnedPeer exercises the whole outbound path against a
// real mTLS callee: selection, balancing, breaking, dialling and telemetry.
func TestDirectPathCallsThePinnedPeer(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(time.Now().UnixMilli())
	peer := &echoPeer{}
	addr := startPeer(t, ca, ca.serviceLeaf(t, "svc-uuid", "inst-1"), peer)

	direct := directUnderTest(t, ca, clk, GRPCPeerDialer{})
	rec, ring := recorderUnderTest()
	breaker := breakerUnderTest(clk)
	balancer := NewBalancer()

	c, err := NewClient(ClientConfig{
		Registry:      registryWith(instanceInfo("inst-1", addr)),
		Direct:        direct,
		Balancer:      balancer,
		Breaker:       breaker,
		Recorder:      rec,
		Retry:         fastRetry(),
		Transport:     TransportDirect,
		CallerService: "checkout",
		Now:           clk.now,
		Logger:        outboundTestLogger(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := c.Unary(ctx, testRequest())
	if err != nil {
		t.Fatalf("direct call: %v", err)
	}
	if string(got) != "body" {
		t.Fatalf("payload = %q, want body", got)
	}
	if peer.lastReq.GetCallerService() != "checkout" {
		t.Fatalf("caller_service = %q, want checkout", peer.lastReq.GetCallerService())
	}
	if peer.lastReq.GetXSbTrace() == "" {
		t.Fatal("the direct path must carry the trace in the body: that is where the callee reads it")
	}

	frames := opReports(ring)
	if len(frames) != 2 {
		t.Fatalf("emitted %d frames, want START + END", len(frames))
	}
	var meta callMeta
	if err := json.Unmarshal(frames[0].GetMetaJson(), &meta); err != nil {
		t.Fatalf("meta_json: %v", err)
	}
	if meta.ViaProxy {
		t.Fatal("a direct call must not report itself as proxied")
	}

	// Every reservation is handed back once the call settles.
	if got := balancer.Tracked(); got != 0 {
		t.Fatalf("balancer still holds %d reservations after the call", got)
	}
}

func TestDirectPathReleasesItsReservationsOnFailure(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(time.Now().UnixMilli())
	// Nothing listens here, so every attempt fails at the transport.
	direct := directUnderTest(t, ca, clk, GRPCPeerDialer{})
	rec, _ := recorderUnderTest()
	balancer := NewBalancer()

	c, err := NewClient(ClientConfig{
		Registry:  registryWith(instanceInfo("inst-1", "127.0.0.1:1")),
		Direct:    direct,
		Balancer:  balancer,
		Breaker:   breakerUnderTest(clk),
		Recorder:  rec,
		Retry:     RetryPolicy{MaxAttempts: 2, BaseMs: 1, MaxMs: 1, Multiplier: 1, JitterRatio: 0},
		Transport: TransportDirect,
		Now:       clk.now,
		Logger:    outboundTestLogger(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Unary(ctx, testRequest()); err == nil {
		t.Fatal("expected the call to fail against a dead endpoint")
	}
	if got := balancer.Tracked(); got != 0 {
		t.Fatalf("balancer leaked %d reservations on the failure path", got)
	}
}

// TestOutboundCallLeavesNoGoroutinesBehind: the outbound path owns no
// goroutines of its own, and a finished stream must not leave gRPC ones either.
func TestOutboundCallLeavesNoGoroutinesBehind(t *testing.T) {
	stub := &stubInvoke{chunks: []*pb.InvokeChunk{{Payload: []byte("a")}}}
	c := proxyClient(t, registryWith(instanceInfo("inst-1", "10.0.0.1:14446")), stub, nil)

	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		if _, err := c.Unary(context.Background(), testRequest()); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		st, err := c.Stream(context.Background(), testRequest())
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		for {
			if _, rerr := st.Recv(); rerr != nil {
				break
			}
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}

	for i := 0; i < 50; i++ {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines grew from %d to %d over 20 call/stream cycles", before, runtime.NumGoroutine())
}

func TestConcurrentCallsAreRaceFree(t *testing.T) {
	stub := &stubInvoke{}
	c := proxyClient(t, registryWith(
		instanceInfo("inst-1", "10.0.0.1:14446"),
		instanceInfo("inst-2", "10.0.0.2:14446"),
	), stub, nil)

	done := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func() {
			_, err := c.Unary(context.Background(), testRequest())
			done <- err
		}()
	}
	for i := 0; i < 16; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent call failed: %v", err)
		}
	}
}

// TestBackoffStopsWhenTheCallerContextExpires: a retry ladder must not outlive
// the deadline its caller set.
func TestBackoffStopsWhenTheCallerContextExpires(t *testing.T) {
	stub := &stubInvoke{err: status.Error(codes.Unavailable, "conn refused")}
	rec, _ := recorderUnderTest()
	c, err := NewClient(ClientConfig{
		Registry:  registryWith(instanceInfo("inst-1", "10.0.0.1:14446")),
		Proxy:     proxyOver(t, stub),
		Recorder:  rec,
		Retry:     RetryPolicy{MaxAttempts: 3, BaseMs: 30_000, MaxMs: 30_000, Multiplier: 1, JitterRatio: 0},
		Transport: TransportProxy,
		Logger:    outboundTestLogger(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := c.Unary(ctx, testRequest()); err == nil {
		t.Fatal("expected the call to fail")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the ladder waited %v past the caller's deadline", elapsed)
	}
	if stub.calls != 1 {
		t.Fatalf("dispatched %d times; the backoff must abort once the caller's context is done", stub.calls)
	}
}
