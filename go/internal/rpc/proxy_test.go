package rpc

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// stubInvoke stands in for the runtime's Invoke service.
type stubInvoke struct {
	mu       sync.Mutex
	calls    int
	requests []*pb.InvokeRequest
	metas    []metadata.MD

	resp   *pb.InvokeResponse
	err    error
	chunks []*pb.InvokeChunk

	// errUntil fails the first N calls with err, then answers with resp.
	errUntil int
}

func (s *stubInvoke) record(ctx context.Context, in *pb.InvokeRequest) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	s.requests = append(s.requests, in)
	md, _ := metadata.FromOutgoingContext(ctx)
	s.metas = append(s.metas, md)
	return s.calls
}

func (s *stubInvoke) Unary(ctx context.Context, in *pb.InvokeRequest, _ ...grpc.CallOption) (*pb.InvokeResponse, error) {
	n := s.record(ctx, in)
	if s.err != nil && (s.errUntil == 0 || n <= s.errUntil) {
		return nil, s.err
	}
	if s.resp != nil {
		return s.resp, nil
	}
	return &pb.InvokeResponse{Payload: in.GetPayload()}, nil
}

func (s *stubInvoke) Stream(ctx context.Context, in *pb.InvokeRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[pb.InvokeChunk], error) {
	n := s.record(ctx, in)
	if s.err != nil && (s.errUntil == 0 || n <= s.errUntil) {
		return nil, s.err
	}
	return &stubChunkStream{chunks: s.chunks}, nil
}

// stubChunkStream embeds the interface so only Recv needs a body; nothing in
// the outbound path calls the rest.
type stubChunkStream struct {
	grpc.ClientStream
	chunks []*pb.InvokeChunk
	i      int
}

func (s *stubChunkStream) Recv() (*pb.InvokeChunk, error) {
	if s.i >= len(s.chunks) {
		return nil, io.EOF
	}
	c := s.chunks[s.i]
	s.i++
	return c, nil
}

// stubInvokeSource hands out the same stub on every call.
type stubInvokeSource struct {
	client pb.InvokeClient
	err    error
}

func (s stubInvokeSource) InvokeClient(context.Context) (pb.InvokeClient, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.client, nil
}

func proxyOver(t *testing.T, client pb.InvokeClient) *Proxy {
	t.Helper()
	p, err := NewProxy(stubInvokeSource{client: client})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	return p
}

// TestEncodeContractHashIsAUTF8RoundTrip pins the wire mismatch: the field is
// bytes, the registry carries a string, and the runtime compares
// string(contract_hash) against a text column — so the bytes must be the
// characters of the hash, not its hex decoding.
func TestEncodeContractHashIsAUTF8RoundTrip(t *testing.T) {
	const hash = "v2:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

	got := EncodeContractHash(hash)
	if string(got) != hash {
		t.Fatalf("EncodeContractHash(%q) = %q, want the same characters back", hash, got)
	}
	if len(got) != len(hash) {
		t.Fatalf("encoded length %d, want %d — a hex decode would halve it", len(got), len(hash))
	}
	if EncodeContractHash("") != nil {
		t.Fatal("an empty hash must stay empty on the wire: the runtime matches it against an empty hash")
	}
}

func TestNewProxyRequiresAClientSource(t *testing.T) {
	if _, err := NewProxy(nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a proxy without a stub source must be refused, got %v", err)
	}
}

func TestProxyUnaryForwardsTheRequestAndReturnsThePayload(t *testing.T) {
	stub := &stubInvoke{resp: &pb.InvokeResponse{Payload: []byte("pong")}}
	p := proxyOver(t, stub)

	req := &pb.InvokeRequest{
		TargetServiceId: "svc-uuid",
		Method:          "Ping",
		Payload:         []byte("ping"),
		ContractHash:    EncodeContractHash("v2:abc"),
		XSbTrace:        "trace-header",
	}
	got, err := p.Unary(context.Background(), req)
	if err != nil {
		t.Fatalf("proxy unary: %v", err)
	}
	if string(got) != "pong" {
		t.Fatalf("payload = %q, want pong", got)
	}
	if stub.calls != 1 {
		t.Fatalf("invoked %d times, want 1", stub.calls)
	}
	if string(stub.requests[0].GetContractHash()) != "v2:abc" {
		t.Fatalf("contract hash on the wire = %q, want v2:abc", stub.requests[0].GetContractHash())
	}
}

func TestProxyUnaryMapsABodyErrorToAHandlerError(t *testing.T) {
	stub := &stubInvoke{resp: &pb.InvokeResponse{ErrorCode: "NOT_FOUND", ErrorMessage: "no such account"}}
	p := proxyOver(t, stub)

	_, err := p.Unary(context.Background(), &pb.InvokeRequest{Method: "Get"})
	var he *HandlerError
	if !errors.As(err, &he) {
		t.Fatalf("a body error must surface as a HandlerError, got %v", err)
	}
	if he.Code != "NOT_FOUND" || he.Message != "no such account" {
		t.Fatalf("handler error = %#v, want the code and message from the body", he)
	}
	if Retryable(err, true) {
		t.Fatal("a body error must not be retryable even with an idempotency key")
	}
}

func TestProxyUnaryPreservesTheTransportStatus(t *testing.T) {
	stub := &stubInvoke{err: status.Error(codes.Unavailable, "runtime down")}
	p := proxyOver(t, stub)

	_, err := p.Unary(context.Background(), &pb.InvokeRequest{Method: "Ping"})
	if err == nil {
		t.Fatal("a transport failure must surface")
	}
	if got := Classify(err); got != RetryAlways {
		t.Fatalf("wrapping must preserve the status: Classify = %v, want %v", got, RetryAlways)
	}
	if !strings.Contains(err.Error(), "Ping") {
		t.Fatalf("the error must name the method, got %q", err.Error())
	}
}

func TestProxyStreamDeliversChunksThenEOF(t *testing.T) {
	stub := &stubInvoke{chunks: []*pb.InvokeChunk{
		{Payload: []byte("a")},
		{Payload: []byte("b")},
	}}
	p := proxyOver(t, stub)

	st, err := p.Stream(context.Background(), &pb.InvokeRequest{Method: "Tokens"})
	if err != nil {
		t.Fatalf("proxy stream: %v", err)
	}

	var got []string
	for {
		chunk, rerr := st.Recv()
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				t.Fatalf("stream ended with %v, want io.EOF", rerr)
			}
			break
		}
		got = append(got, string(chunk))
	}
	if strings.Join(got, "") != "ab" {
		t.Fatalf("chunks = %v, want a, b", got)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestProxyStreamMapsAChunkErrorToAHandlerError(t *testing.T) {
	stub := &stubInvoke{chunks: []*pb.InvokeChunk{
		{Payload: []byte("a")},
		{ErrorCode: "INTERNAL", ErrorMessage: "handler blew up"},
	}}
	p := proxyOver(t, stub)

	st, err := p.Stream(context.Background(), &pb.InvokeRequest{Method: "Tokens"})
	if err != nil {
		t.Fatalf("proxy stream: %v", err)
	}
	if _, err := st.Recv(); err != nil {
		t.Fatalf("first chunk: %v", err)
	}

	_, err = st.Recv()
	var he *HandlerError
	if !errors.As(err, &he) || he.Code != "INTERNAL" {
		t.Fatalf("a chunk error must surface as a HandlerError, got %v", err)
	}
	// The stream is terminal: a later Recv must keep reporting the same cause.
	if _, again := st.Recv(); !errors.As(again, &he) {
		t.Fatalf("a closed stream must keep reporting its cause, got %v", again)
	}
}

func TestProxyReportsAMissingStub(t *testing.T) {
	p, err := NewProxy(stubInvokeSource{err: errors.New("no session")})
	if err != nil {
		t.Fatalf("new proxy: %v", err)
	}
	if _, err := p.Unary(context.Background(), &pb.InvokeRequest{Method: "Ping"}); err == nil {
		t.Fatal("a missing stub must fail the call")
	}
	if _, err := p.Stream(context.Background(), &pb.InvokeRequest{Method: "Ping"}); err == nil {
		t.Fatal("a missing stub must fail the stream")
	}
}
