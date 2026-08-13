package rpc

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	gcreds "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	inboundCalleeID = "svc-callee"
	inboundCallerID = "svc-caller"
)

// stubAcceptance stands in for the registry cache.
type stubAcceptance struct{ eval *pb.PolicyEvaluation }

func (s stubAcceptance) Policy() *pb.PolicyEvaluation { return s.eval }

type inboundFixture struct {
	ca       *inboundCA
	srv      *Server
	endpoint string
}

func newInboundFixture(t *testing.T, d *Dispatcher, limits ServerLimits, policies AcceptancePolicySource) *inboundFixture {
	t.Helper()

	ca := newInboundCA(t)
	srv, err := NewServer(ServerConfig{
		Host:       "127.0.0.1",
		Limits:     limits,
		Dispatcher: d,
		Policies:   policies,
		Logger:     inboundTestLogger(),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	leaf := ca.serviceLeaf(t, inboundCalleeID, "inst-callee")
	if err := srv.UseCredentials(context.Background(), ca.credentials(t, leaf, inboundCalleeID, "inst-callee")); err != nil {
		t.Fatalf("UseCredentials: %v", err)
	}

	endpoint, err := srv.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	return &inboundFixture{ca: ca, srv: srv, endpoint: endpoint}
}

func (f *inboundFixture) client(t *testing.T, leaf tls.Certificate) pb.CallClient {
	t.Helper()

	conn, err := grpc.NewClient(f.endpoint,
		grpc.WithTransportCredentials(gcreds.NewTLS(connection.MutualTLSConfig(f.ca.cert, leaf))))
	if err != nil {
		t.Fatalf("dial %s: %v", f.endpoint, err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close client conn: %v", err)
		}
	})
	return pb.NewCallClient(conn)
}

func (f *inboundFixture) serviceClient(t *testing.T, serviceID string) pb.CallClient {
	t.Helper()
	return f.client(t, f.ca.serviceLeaf(t, serviceID, serviceID+"-inst"))
}

func inboundCallCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestNewServerRejectsInvalidConfig(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())

	tests := []struct {
		name string
		cfg  ServerConfig
	}{
		{"no host", ServerConfig{Limits: DefaultServerLimits(), Dispatcher: d}},
		{"negative port", ServerConfig{Host: "127.0.0.1", Port: -1, Limits: DefaultServerLimits(), Dispatcher: d}},
		{"port above range", ServerConfig{Host: "127.0.0.1", Port: 70000, Limits: DefaultServerLimits(), Dispatcher: d}},
		{"no dispatcher", ServerConfig{Host: "127.0.0.1", Limits: DefaultServerLimits()}},
		{"zero call limit", ServerConfig{Host: "127.0.0.1", Dispatcher: d, Limits: ServerLimits{MaxConcurrentCalls: 0, MaxConcurrentStreams: 8}}},
		{"negative call limit", ServerConfig{Host: "127.0.0.1", Dispatcher: d, Limits: ServerLimits{MaxConcurrentCalls: -1, MaxConcurrentStreams: 8}}},
		{"zero stream limit", ServerConfig{Host: "127.0.0.1", Dispatcher: d, Limits: ServerLimits{MaxConcurrentCalls: 8, MaxConcurrentStreams: 0}}},
		{"negative stream limit", ServerConfig{Host: "127.0.0.1", Dispatcher: d, Limits: ServerLimits{MaxConcurrentCalls: 8, MaxConcurrentStreams: -3}}},
		{"stream limit beyond uint32", ServerConfig{Host: "127.0.0.1", Dispatcher: d, Limits: ServerLimits{MaxConcurrentCalls: 8, MaxConcurrentStreams: math.MaxUint32 + 1}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := NewServer(tc.cfg)
			if !errors.Is(err, ErrServerConfig) {
				t.Fatalf("err = %v, want ErrServerConfig", err)
			}
			if srv != nil {
				t.Fatal("a rejected config must not yield a server")
			}
		})
	}
}

func TestStartAdvertisesTheBoundPort(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "echo", echoUnaryHandler)

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)

	host, portStr, err := net.SplitHostPort(f.endpoint)
	if err != nil {
		t.Fatalf("split endpoint %q: %v", f.endpoint, err)
	}
	if host != "127.0.0.1" {
		t.Fatalf("host = %q, want the configured advertise host", host)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port %q: %v", portStr, err)
	}
	if port == 0 {
		t.Fatal("port 0 was advertised verbatim; the OS-assigned port must be published instead")
	}

	got, err := f.srv.Endpoint()
	if err != nil || got != f.endpoint {
		t.Fatalf("Endpoint() = %q, %v; want %q", got, err, f.endpoint)
	}

	// The lifecycle calls Start on every connect attempt; the mesh already holds
	// the first address.
	again, err := f.srv.Start(context.Background())
	if err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if again != f.endpoint {
		t.Fatalf("second Start returned %q, want %q", again, f.endpoint)
	}
}

func TestEndpointBeforeStart(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	srv, err := NewServer(ServerConfig{Host: "127.0.0.1", Limits: DefaultServerLimits(), Dispatcher: d, Logger: inboundTestLogger()})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if _, err := srv.Endpoint(); !errors.Is(err, ErrServerNotStarted) {
		t.Fatalf("err = %v, want ErrServerNotStarted", err)
	}
}

func TestStartAfterCloseIsRefused(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	f := newInboundFixture(t, d, DefaultServerLimits(), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.srv.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := f.srv.Start(context.Background()); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("err = %v, want ErrServerClosed", err)
	}
}

func TestHandshakeBeforeCredentialsIsRefused(t *testing.T) {
	ca := newInboundCA(t)
	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "echo", echoUnaryHandler)

	srv, err := NewServer(ServerConfig{Host: "127.0.0.1", Limits: DefaultServerLimits(), Dispatcher: d, Logger: inboundTestLogger()})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	endpoint, err := srv.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(
		gcreds.NewTLS(connection.MutualTLSConfig(ca.cert, ca.serviceLeaf(t, inboundCallerID, "inst-1")))))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pb.NewCallClient(conn).Unary(ctx, &pb.CallRequest{Method: "echo"}); err == nil {
		t.Fatal("a listener with no certificate must refuse the handshake")
	}
}

func TestHandlerErrorArrivesInTheBodyUnderStatusOK(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "charge", func(context.Context, []byte) ([]byte, error) {
		return nil, errors.New("insufficient funds")
	})

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)
	client := f.serviceClient(t, inboundCallerID)

	resp, err := client.Unary(inboundCallCtx(t), &pb.CallRequest{Method: "charge"})
	if err != nil {
		t.Fatalf("a handler error must not surface as a gRPC status: %v", err)
	}
	if resp.GetErrorCode() != errorCodeInternal {
		t.Fatalf("error_code = %q, want %q", resp.GetErrorCode(), errorCodeInternal)
	}
	if resp.GetErrorMessage() != "insufficient funds" {
		t.Fatalf("error_message = %q", resp.GetErrorMessage())
	}
}

func TestTransportFailuresArriveAsStatuses(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "echo", echoUnaryHandler)
	mustAddStream(t, d, "feed", func(context.Context, []byte, Sender) error { return nil })
	mustAddUnary(t, d, "strict", func(context.Context, []byte) ([]byte, error) {
		return nil, errors.Join(errors.New("unmarshal request"), ErrDecode)
	})

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)
	client := f.serviceClient(t, inboundCallerID)

	tests := []struct {
		name   string
		method string
		want   codes.Code
	}{
		{"unknown method", "missing", codes.NotFound},
		{"streaming method called as unary", "feed", codes.FailedPrecondition},
		{"decode failure", "strict", codes.InvalidArgument},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.Unary(inboundCallCtx(t), &pb.CallRequest{Method: tc.method})
			if got := status.Code(err); got != tc.want {
				t.Fatalf("code = %v, want %v (err %v)", got, tc.want, err)
			}
		})
	}

	t.Run("unary method called as stream", func(t *testing.T) {
		stream, err := client.Stream(inboundCallCtx(t), &pb.CallRequest{Method: "echo"})
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}
		_, err = stream.Recv()
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("code = %v, want FailedPrecondition (err %v)", got, err)
		}
	})
}

func TestServerEmitsNoTelemetryOperation(t *testing.T) {
	caller := telemetry.TraceContext{TraceID: newTestUUID(t), ParentOpID: newTestUUID(t)}
	ring := telemetry.NewRing(telemetry.DefaultBudgets())
	recorder := telemetry.NewRecorder(ring, telemetry.NewPolicy())

	var seen telemetry.TraceContext
	var seenOK bool

	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "work", func(ctx context.Context, payload []byte) ([]byte, error) {
		seen, seenOK = telemetry.FromContext(ctx)
		if _, _, err := recorder.Start(ctx, telemetry.OpSpec{
			Channel: pb.Channel_USER,
			Kind:    telemetry.OpKindUserSubOp,
			Subject: "nested",
		}); err != nil {
			return nil, err
		}
		return payload, nil
	})

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)
	client := f.serviceClient(t, inboundCallerID)

	ctx := metadata.AppendToOutgoingContext(inboundCallCtx(t), telemetry.MetadataKey, telemetry.FormatHeader(caller))
	if _, err := client.Unary(ctx, &pb.CallRequest{Method: "work"}); err != nil {
		t.Fatalf("Unary: %v", err)
	}

	if !seenOK {
		t.Fatal("the handler ran without a trace context")
	}
	if seen != caller {
		t.Fatalf("handler trace = %+v, want the caller's %+v", seen, caller)
	}

	batch := ring.Peek(16)
	if len(batch.Ops) != 1 {
		t.Fatalf("buffered %d op frames, want exactly the handler's own: the server must add no row (ADR-0001)", len(batch.Ops))
	}
	if got := batch.Ops[0].Msg.GetParentOpId(); got != caller.ParentOpID.String() {
		t.Fatalf("the handler's sub-op parents to %q, want the caller's op %q: a server-side row would sit in between",
			got, caller.ParentOpID)
	}
}

func TestInboundTraceSources(t *testing.T) {
	valid := telemetry.TraceContext{TraceID: newTestUUID(t), ParentOpID: newTestUUID(t)}
	fromBody := telemetry.TraceContext{TraceID: newTestUUID(t), ParentOpID: newTestUUID(t)}

	var seen telemetry.TraceContext
	var seenOK bool

	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "trace", func(ctx context.Context, payload []byte) ([]byte, error) {
		seen, seenOK = telemetry.FromContext(ctx)
		return payload, nil
	})

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)
	client := f.serviceClient(t, inboundCallerID)

	call := func(t *testing.T, md string, req *pb.CallRequest) telemetry.TraceContext {
		t.Helper()
		seen, seenOK = telemetry.TraceContext{}, false
		ctx := inboundCallCtx(t)
		if md != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, telemetry.MetadataKey, md)
		}
		if _, err := client.Unary(ctx, req); err != nil {
			t.Fatalf("Unary: %v", err)
		}
		if !seenOK {
			t.Fatal("the handler ran without a trace context")
		}
		return seen
	}

	t.Run("metadata carries the proxy path", func(t *testing.T) {
		got := call(t, telemetry.FormatHeader(valid), &pb.CallRequest{Method: "trace"})
		if got != valid {
			t.Fatalf("got %+v, want %+v", got, valid)
		}
	})

	t.Run("request field carries the direct path", func(t *testing.T) {
		got := call(t, "", &pb.CallRequest{Method: "trace", XSbTrace: telemetry.FormatHeader(fromBody)})
		if got != fromBody {
			t.Fatalf("got %+v, want %+v", got, fromBody)
		}
	})

	t.Run("metadata wins over the request field", func(t *testing.T) {
		got := call(t, telemetry.FormatHeader(valid), &pb.CallRequest{Method: "trace", XSbTrace: telemetry.FormatHeader(fromBody)})
		if got != valid {
			t.Fatalf("got %+v, want %+v", got, valid)
		}
	})

	t.Run("malformed value starts a new root trace", func(t *testing.T) {
		got := call(t, "", &pb.CallRequest{Method: "trace", XSbTrace: "definitely-not-a-trace"})
		if got.TraceID == uuid.Nil {
			t.Fatal("a malformed trace must still yield a usable trace id")
		}
		if !got.Root() {
			t.Fatalf("a malformed trace must start a new tree, got parent %s", got.ParentOpID)
		}
		if got.TraceID == valid.TraceID || got.TraceID == fromBody.TraceID {
			t.Fatal("the replacement trace reused a caller's trace id")
		}
	})

	t.Run("absent value starts a new root trace", func(t *testing.T) {
		got := call(t, "", &pb.CallRequest{Method: "trace"})
		if got.TraceID == uuid.Nil || !got.Root() {
			t.Fatalf("got %+v, want a fresh root", got)
		}
	})
}

func TestOverloadShedsWithResourceExhausted(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})

	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "block", func(ctx context.Context, _ []byte) ([]byte, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	})

	// One handler slot, plenty of HTTP/2 streams: the shed must come from the
	// server's own bound, not from transport-level flow control.
	f := newInboundFixture(t, d, ServerLimits{MaxConcurrentCalls: 1, MaxConcurrentStreams: 64}, nil)
	client := f.serviceClient(t, inboundCallerID)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := client.Unary(inboundCallCtx(t), &pb.CallRequest{Method: "block"}); err != nil {
			t.Errorf("the admitted call failed: %v", err)
		}
	}()
	defer func() {
		close(release)
		wg.Wait()
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first call never reached its handler")
	}

	// A short deadline is the discriminator: shedding answers now,
	// queueing answers with DeadlineExceeded once the slot frees.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := client.Unary(ctx, &pb.CallRequest{Method: "block"})
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted (err %v)", got, err)
	}
}

func TestAcceptanceRunsBeforeTheSlotIsTaken(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})

	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "block", func(ctx context.Context, _ []byte) ([]byte, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	})

	policy := stubAcceptance{eval: &pb.PolicyEvaluation{
		Acceptance: []*pb.PolicyRule{{Action: actionRPCHandle, PeerServiceId: inboundCallerID, TargetName: wildcardTarget}},
	}}
	f := newInboundFixture(t, d, ServerLimits{MaxConcurrentCalls: 1, MaxConcurrentStreams: 64}, policy)

	admitted := f.serviceClient(t, inboundCallerID)
	denied := f.serviceClient(t, "svc-intruder")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := admitted.Unary(inboundCallCtx(t), &pb.CallRequest{Method: "block"}); err != nil {
			t.Errorf("the admitted call failed: %v", err)
		}
	}()
	defer func() {
		close(release)
		wg.Wait()
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the admitted call never reached its handler")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := denied.Unary(ctx, &pb.CallRequest{Method: "block"})
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied: the check must run before the slot, "+
			"or a refused caller competes for capacity (err %v)", got, err)
	}
}

func TestUnidentifiedPeerIsRejected(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "echo", echoUnaryHandler)
	mustAddStream(t, d, "feed", func(context.Context, []byte, Sender) error { return nil })

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)

	tests := []struct {
		name string
		leaf tls.Certificate
	}{
		{"no SAN and a foreign CN", f.ca.sanlessLeaf(t, "servicebridge-leaf")},
		{"URI SAN that is not SPIFFE", f.ca.foreignURILeaf(t, "https://example.test/whoami")},
		{"SPIFFE URI in another trust domain", f.ca.foreignURILeaf(t, "spiffe://elsewhere/service/a/instance/b")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := f.client(t, tc.leaf)

			_, err := client.Unary(inboundCallCtx(t), &pb.CallRequest{Method: "echo"})
			if got := status.Code(err); got != codes.Unauthenticated {
				t.Fatalf("unary code = %v, want Unauthenticated (err %v)", got, err)
			}

			stream, err := client.Stream(inboundCallCtx(t), &pb.CallRequest{Method: "feed"})
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			if _, err := stream.Recv(); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("stream code = %v, want Unauthenticated (err %v)", status.Code(err), err)
			}
		})
	}
}

func TestRuntimeProxyIsAdmitted(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "echo", echoUnaryHandler)

	// A policy that admits nobody by service id: the proxy must still pass,
	// because the caller-side gate has already run against the originating
	// service and the proxy carries no service identity of its own.
	policy := stubAcceptance{eval: &pb.PolicyEvaluation{
		Acceptance: []*pb.PolicyRule{{Action: actionRPCHandle, PeerServiceId: "svc-nobody", TargetName: "nothing"}},
	}}
	f := newInboundFixture(t, d, DefaultServerLimits(), policy)
	client := f.client(t, f.ca.runtimeLeaf(t))

	resp, err := client.Unary(inboundCallCtx(t), &pb.CallRequest{Method: "echo", Payload: []byte("hi")})
	if err != nil {
		t.Fatalf("the runtime proxy was rejected: %v", err)
	}
	if string(resp.GetPayload()) != "hi" {
		t.Fatalf("payload = %q", resp.GetPayload())
	}
}

func TestPanicInHandlerDoesNotKillTheProcess(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "boom", func(context.Context, []byte) ([]byte, error) {
		panic("handler exploded")
	})
	mustAddUnary(t, d, "echo", echoUnaryHandler)

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)
	client := f.serviceClient(t, inboundCallerID)

	resp, err := client.Unary(inboundCallCtx(t), &pb.CallRequest{Method: "boom"})
	if err != nil {
		t.Fatalf("a panic must come back as an answer, not a status: %v", err)
	}
	if resp.GetErrorCode() != errorCodeInternal {
		t.Fatalf("error_code = %q, want %q", resp.GetErrorCode(), errorCodeInternal)
	}

	// The same process still serves the next caller.
	echo, err := client.Unary(inboundCallCtx(t), &pb.CallRequest{Method: "echo", Payload: []byte("still here")})
	if err != nil {
		t.Fatalf("the server died with the handler: %v", err)
	}
	if string(echo.GetPayload()) != "still here" {
		t.Fatalf("payload = %q", echo.GetPayload())
	}
}

func TestStreamDeliversChunksAndThenTheHandlerError(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	mustAddStream(t, d, "half", func(_ context.Context, _ []byte, send Sender) error {
		if err := send([]byte("first")); err != nil {
			return err
		}
		return errors.New("ran dry")
	})

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)
	client := f.serviceClient(t, inboundCallerID)

	stream, err := client.Stream(inboundCallCtx(t), &pb.CallRequest{Method: "half"})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if string(first.GetPayload()) != "first" {
		t.Fatalf("first chunk payload = %q", first.GetPayload())
	}

	failure, err := stream.Recv()
	if err != nil {
		t.Fatalf("error chunk: %v", err)
	}
	if failure.GetErrorCode() != errorCodeInternal || failure.GetErrorMessage() != "ran dry" {
		t.Fatalf("error chunk = %+v", failure)
	}

	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("a handler failure must close the stream with OK, got %v", err)
	}
}

func TestCallerCancellationStopsStreamProduction(t *testing.T) {
	var produced atomic.Int64
	stopped := make(chan struct{})

	d := NewDispatcher(inboundTestLogger())
	mustAddStream(t, d, "firehose", func(ctx context.Context, _ []byte, send Sender) error {
		defer close(stopped)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := send([]byte("x")); err != nil {
				return err
			}
			produced.Add(1)
			time.Sleep(time.Millisecond)
		}
	})

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)
	client := f.serviceClient(t, inboundCallerID)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Stream(ctx, &pb.CallRequest{Method: "firehose"})
	if err != nil {
		cancel()
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		cancel()
		t.Fatalf("first chunk: %v", err)
	}
	cancel()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler kept producing after the caller cancelled")
	}

	settled := produced.Load()
	time.Sleep(200 * time.Millisecond)
	if got := produced.Load(); got != settled {
		t.Fatalf("production continued after the handler returned: %d then %d", settled, got)
	}
}

func TestStreamBackpressureDeliversEveryChunkInOrder(t *testing.T) {
	const chunks = 256

	d := NewDispatcher(inboundTestLogger())
	mustAddStream(t, d, "ordered", func(_ context.Context, _ []byte, send Sender) error {
		for i := range chunks {
			if err := send([]byte(strconv.Itoa(i))); err != nil {
				return err
			}
		}
		return nil
	})

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)
	client := f.serviceClient(t, inboundCallerID)

	stream, err := client.Stream(inboundCallCtx(t), &pb.CallRequest{Method: "ordered"})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	for i := range chunks {
		chunk, err := stream.Recv()
		if err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
		if string(chunk.GetPayload()) != strconv.Itoa(i) {
			t.Fatalf("chunk %d payload = %q", i, chunk.GetPayload())
		}
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF after the last chunk, got %v", err)
	}
}

func TestCredentialsRotateUnderALiveListener(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "echo", echoUnaryHandler)

	f := newInboundFixture(t, d, DefaultServerLimits(), nil)
	before := f.endpoint

	next := f.ca.serviceLeaf(t, inboundCalleeID, "inst-rotated")
	if err := f.srv.UseCredentials(context.Background(), f.ca.credentials(t, next, inboundCalleeID, "inst-rotated")); err != nil {
		t.Fatalf("UseCredentials: %v", err)
	}
	if got, err := f.srv.Endpoint(); err != nil || got != before {
		t.Fatalf("rotation moved the advertised endpoint: %q -> %q (%v)", before, got, err)
	}

	client := f.serviceClient(t, inboundCallerID)
	if _, err := client.Unary(inboundCallCtx(t), &pb.CallRequest{Method: "echo"}); err != nil {
		t.Fatalf("call after rotation: %v", err)
	}
}

func TestUseCredentialsRejectsAnEmptyLease(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	srv, err := NewServer(ServerConfig{Host: "127.0.0.1", Limits: DefaultServerLimits(), Dispatcher: d, Logger: inboundTestLogger()})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := srv.UseCredentials(context.Background(), connection.Credentials{}); !errors.Is(err, ErrServerConfig) {
		t.Fatalf("err = %v, want ErrServerConfig", err)
	}
}

func TestStartSealsTheDispatcher(t *testing.T) {
	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "echo", echoUnaryHandler)

	newInboundFixture(t, d, DefaultServerLimits(), nil)

	if err := d.RegisterUnary("late", echoUnaryHandler); !errors.Is(err, ErrSealed) {
		t.Fatalf("err = %v, want ErrSealed", err)
	}
}

func TestServerLeavesNoGoroutinesBehind(t *testing.T) {
	cycle := func() {
		ca := newInboundCA(t)
		d := NewDispatcher(inboundTestLogger())
		mustAddUnary(t, d, "echo", echoUnaryHandler)

		srv, err := NewServer(ServerConfig{Host: "127.0.0.1", Limits: DefaultServerLimits(), Dispatcher: d, Logger: inboundTestLogger()})
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		leaf := ca.serviceLeaf(t, inboundCalleeID, "inst-callee")
		if err := srv.UseCredentials(context.Background(), ca.credentials(t, leaf, inboundCalleeID, "inst-callee")); err != nil {
			t.Fatalf("UseCredentials: %v", err)
		}
		endpoint, err := srv.Start(context.Background())
		if err != nil {
			t.Fatalf("Start: %v", err)
		}

		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(
			gcreds.NewTLS(connection.MutualTLSConfig(ca.cert, ca.serviceLeaf(t, inboundCallerID, "inst-1")))))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if _, err := pb.NewCallClient(conn).Unary(ctx, &pb.CallRequest{Method: "echo", Payload: []byte("hi")}); err != nil {
			cancel()
			t.Fatalf("Unary: %v", err)
		}
		cancel()

		if err := conn.Close(); err != nil {
			t.Fatalf("close conn: %v", err)
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := srv.Close(stopCtx); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// The first cycle pays for whatever gRPC starts once per process.
	cycle()
	time.Sleep(500 * time.Millisecond)
	before := runtime.NumGoroutine()

	cycle()
	waitGoroutines(t, before)
}

// waitGoroutines waits until the live goroutine count drops back to want.
func waitGoroutines(t *testing.T, want int) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		got := runtime.NumGoroutine()
		if got <= want {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<18)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutines did not waitGoroutines: %d live, want <= %d\n%s", got, want, buf[:n])
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func newTestUUID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid: %v", err)
	}
	return id
}
