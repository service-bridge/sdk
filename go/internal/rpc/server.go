// Package rpc hosts the inbound Call server of an SDK instance: the mTLS
// listener that peers and the runtime dial, the admission check in front of it
// and the handler index behind it.
package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	gcreds "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Defaults of the inbound Call server. See ./README.md.
const (
	DefaultMaxConcurrentCalls   = 512
	DefaultMaxConcurrentStreams = 512

	// maxPort is the highest bindable TCP port.
	maxPort = 65535
)

// Server failures.
var (
	// ErrServerConfig marks a configuration the server refuses to be built with. It is
	// deliberately its own sentinel: a misconfigured bound must be
	// distinguishable from a network failure, or the reconnect ladder retries a
	// mistake that no retry can fix.
	ErrServerConfig = errors.New("invalid inbound Call server config")

	// ErrServerNotStarted is returned by Endpoint before the listener is bound.
	ErrServerNotStarted = errors.New("inbound Call server is not listening")

	// ErrServerClosed marks use of a server that has already been closed.
	ErrServerClosed = errors.New("inbound Call server is closed")

	// ErrNoCredentials fails a handshake that arrives before the first
	// certificate is published. The listener binds before the control plane is
	// up, so this window is real and must refuse rather than serve untrusted.
	ErrNoCredentials = errors.New("inbound Call server has no certificate yet")

	// ErrOverloaded means every concurrency slot is taken.
	ErrOverloaded = errors.New("inbound Call server is at its concurrency limit")
)

// ServerLimits bounds the inbound work one instance admits at a time.
type ServerLimits struct {
	// MaxConcurrentCalls caps handlers running at once across every connection.
	MaxConcurrentCalls int
	// MaxConcurrentStreams caps HTTP/2 streams per connection.
	MaxConcurrentStreams int
}

// DefaultServerLimits is the bound to pass when the operator states no preference.
func DefaultServerLimits() ServerLimits {
	return ServerLimits{
		MaxConcurrentCalls:   DefaultMaxConcurrentCalls,
		MaxConcurrentStreams: DefaultMaxConcurrentStreams,
	}
}

// validate rejects a bound that cannot mean anything. Go's type system already
// removes the fractional case; what is left is non-positive, which would either
// wedge every call or, taken as "unset", silently apply a limit nobody chose.
func (l ServerLimits) validate() error {
	const op = "rpc: limits"

	if l.MaxConcurrentCalls <= 0 {
		return fmt.Errorf("%s: MaxConcurrentCalls must be > 0, got %d (use DefaultServerLimits for the default): %w",
			op, l.MaxConcurrentCalls, ErrServerConfig)
	}
	if l.MaxConcurrentStreams <= 0 {
		return fmt.Errorf("%s: MaxConcurrentStreams must be > 0, got %d (use DefaultServerLimits for the default): %w",
			op, l.MaxConcurrentStreams, ErrServerConfig)
	}
	if l.MaxConcurrentStreams > math.MaxUint32 {
		return fmt.Errorf("%s: MaxConcurrentStreams must fit in uint32, got %d: %w",
			op, l.MaxConcurrentStreams, ErrServerConfig)
	}
	return nil
}

// AcceptancePolicySource yields the acceptance rules the runtime last pushed. The registry
// cache satisfies it. A nil evaluation means nothing has been pushed yet.
type AcceptancePolicySource interface {
	Policy() *pb.PolicyEvaluation
}

// ServerConfig wires the inbound Call server. See ./README.md.
type ServerConfig struct {
	// Host is the address peers dial. It is mandatory: container schedulers give
	// a pod several addresses and only the operator knows which one is routable,
	// so there is no auto-detection to fall back on.
	Host string
	// Port is the port to bind. Zero asks the OS for a free one, and the port it
	// hands back — not the zero — is what gets advertised.
	Port int
	// Limits bounds concurrent inbound work.
	Limits ServerLimits
	// Dispatcher owns the handler index.
	Dispatcher *Dispatcher
	// Policies yields the acceptance rules. Nil means default-allow.
	Policies AcceptancePolicySource
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// material is the mTLS identity the listener currently serves with. It is
// replaced on every certificate rotation without rebinding the port: the
// advertised endpoint is already in every peer's registry cache, and rebinding
// would point the whole mesh at a socket nobody listens on.
type material struct {
	leaf  tls.Certificate
	roots *x509.CertPool
}

// Server is the inbound Call listener of one SDK instance.
//
// It satisfies connection.InboundServer and connection.CredentialConsumer, so
// the lifecycle binds it before the first RegisterAndWatch — its address has to
// be in that first RegisterRequest — and hands it a fresh certificate on every
// rotation.
type Server struct {
	cfg   ServerConfig
	log   *slog.Logger
	slots *semaphore.Weighted
	certs atomic.Pointer[material]

	mu        sync.Mutex
	closed    bool
	lis       net.Listener
	grpc      *grpc.Server
	endpoint  string
	serveDone chan struct{}
}

// NewServer validates the config and builds the server. Nothing binds yet.
func NewServer(cfg ServerConfig) (*Server, error) {
	const op = "rpc: new inbound server"

	if cfg.Host == "" {
		return nil, fmt.Errorf("%s: Host is required, there is no auto-detect: %w", op, ErrServerConfig)
	}
	if cfg.Port < 0 || cfg.Port > maxPort {
		return nil, fmt.Errorf("%s: Port %d is outside [0, %d]: %w", op, cfg.Port, maxPort, ErrServerConfig)
	}
	if cfg.Dispatcher == nil {
		return nil, fmt.Errorf("%s: Dispatcher is required: %w", op, ErrServerConfig)
	}
	if err := cfg.Limits.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Server{
		cfg:   cfg,
		log:   cfg.Logger,
		slots: semaphore.NewWeighted(int64(cfg.Limits.MaxConcurrentCalls)),
	}, nil
}

// UseCredentials switches the listener onto the certificate of the current
// lease. It is called before the first Start and again on every rotation.
func (s *Server) UseCredentials(_ context.Context, creds connection.Credentials) error {
	const op = "rpc: use credentials"

	if len(creds.Lease.CertDER) == 0 {
		return fmt.Errorf("%s: lease carries no leaf certificate: %w", op, ErrServerConfig)
	}
	chain, err := x509.ParseCertificates(creds.Lease.CAChainDER)
	if err != nil {
		return fmt.Errorf("%s: parse CA chain: %w", op, err)
	}
	if len(chain) == 0 {
		return fmt.Errorf("%s: lease carries no CA chain: %w", op, ErrServerConfig)
	}

	roots := x509.NewCertPool()
	for _, c := range chain {
		roots.AddCert(c)
	}
	s.certs.Store(&material{leaf: creds.Lease.TLSCert, roots: roots})
	s.log.Debug("rpc: inbound server switched to a new certificate",
		"service_id", creds.Lease.Identity.ServiceID,
		"instance_id", creds.Lease.Identity.InstanceID)
	return nil
}

// Start binds the listener and serves it. It is idempotent: the lifecycle calls
// it on every connect attempt, and every attempt after the first must reuse the
// address already advertised to the mesh.
func (s *Server) Start(_ context.Context) (string, error) {
	const op = "rpc: start inbound server"

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return "", fmt.Errorf("%s: %w", op, ErrServerClosed)
	}
	if s.lis != nil {
		return s.endpoint, nil
	}

	lis, err := net.Listen("tcp", net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port)))
	if err != nil {
		return "", fmt.Errorf("%s: listen on %s: %w", op, s.cfg.Host, err)
	}

	port, err := boundPort(lis.Addr())
	if err != nil {
		if cerr := lis.Close(); cerr != nil {
			s.log.Warn("rpc: closing the listener after a failed bind", "error", cerr)
		}
		return "", fmt.Errorf("%s: %w", op, err)
	}

	srv := grpc.NewServer(
		grpc.Creds(gcreds.NewTLS(s.serverTLSConfig())),
		grpc.MaxConcurrentStreams(uint32(s.cfg.Limits.MaxConcurrentStreams)), //nolint:gosec // bounded by ServerLimits.validate
	)
	pb.RegisterCallServer(srv, s)

	// Handlers are declared to the runtime from the same index the dispatcher
	// serves; one registered after this point would exist only here.
	s.cfg.Dispatcher.Seal()

	s.lis = lis
	s.grpc = srv
	s.endpoint = net.JoinHostPort(s.cfg.Host, strconv.Itoa(port))
	s.serveDone = make(chan struct{})

	done := s.serveDone
	go func() {
		defer close(done)
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.log.Error("rpc: inbound Call server stopped", "error", err)
		}
	}()

	s.log.Info("rpc: inbound Call server listening",
		"endpoint", s.endpoint,
		"max_concurrent_calls", s.cfg.Limits.MaxConcurrentCalls,
		"max_concurrent_streams", s.cfg.Limits.MaxConcurrentStreams)
	return s.endpoint, nil
}

// Endpoint is the address advertised to the mesh.
func (s *Server) Endpoint() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lis == nil {
		return "", fmt.Errorf("rpc: endpoint: %w", ErrServerNotStarted)
	}
	return s.endpoint, nil
}

// Close stops serving and releases the listener. It drains in-flight calls
// until ctx ends, then cuts them off — a stop that waits forever on one stuck
// handler is a stop that never happens.
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	srv, done := s.grpc, s.serveDone
	s.grpc, s.lis = nil, nil
	s.mu.Unlock()

	if srv == nil {
		return nil
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		srv.GracefulStop()
	}()

	select {
	case <-stopped:
	case <-ctx.Done():
		srv.Stop()
		<-stopped
	}
	<-done
	return nil
}

// serverTLSConfig requires and verifies a client certificate against the pinned
// CA. The per-handshake callback is what lets the certificate rotate under a
// listener that must keep its port.
func (s *Server) serverTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		ClientAuth: tls.RequireAndVerifyClientCert,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			m := s.certs.Load()
			if m == nil {
				return nil, fmt.Errorf("rpc: tls handshake: %w", ErrNoCredentials)
			}
			return &tls.Config{
				MinVersion:   tls.VersionTLS13,
				Certificates: []tls.Certificate{m.leaf},
				ClientCAs:    m.roots,
				ClientAuth:   tls.RequireAndVerifyClientCert,
			}, nil
		},
	}
}

// Unary serves one inbound unary call.
//
// It emits no telemetry operation. One logical call is one row, and that row
// belongs to the calling SDK (ADR-0001); a row from the callee would double
// every call in the trace view. The handler runs under the caller's trace
// context so its own nested operations parent to the caller's row.
func (s *Server) Unary(ctx context.Context, req *pb.CallRequest) (*pb.CallResponse, error) {
	if err := s.admit(ctx, req.GetMethod()); err != nil {
		return nil, err
	}
	// Admission first, then the slot: a caller nobody admits must not hold a
	// slot the admitted callers are queueing for.
	if !s.slots.TryAcquire(1) {
		return nil, s.overloaded(req.GetMethod())
	}
	defer s.slots.Release(1)

	ctx = s.withInboundTrace(ctx, req)

	out := s.cfg.Dispatcher.Unary(ctx, req.GetMethod(), req.GetPayload())
	if out.Status != codes.OK {
		return nil, status.Error(out.Status, out.StatusMessage)
	}
	return &pb.CallResponse{
		Payload:      out.Payload,
		ErrorCode:    out.ErrorCode,
		ErrorMessage: out.ErrorMessage,
	}, nil
}

// Stream serves one inbound server-streaming call.
func (s *Server) Stream(req *pb.CallRequest, srv grpc.ServerStreamingServer[pb.StreamChunk]) error {
	ctx := srv.Context()

	if err := s.admit(ctx, req.GetMethod()); err != nil {
		return err
	}
	if !s.slots.TryAcquire(1) {
		return s.overloaded(req.GetMethod())
	}
	defer s.slots.Release(1)

	ctx = s.withInboundTrace(ctx, req)

	sender := &chunkSender{ctx: ctx, srv: srv, method: req.GetMethod()}
	out := s.cfg.Dispatcher.Stream(ctx, req.GetMethod(), req.GetPayload(), sender.send)

	// A send that failed is the transport talking, not the handler. Reporting it
	// as a handler error would tell the caller its request was answered.
	if err := sender.err(); err != nil {
		return err
	}
	if out.Status != codes.OK {
		return status.Error(out.Status, out.StatusMessage)
	}
	if out.ErrorCode != "" {
		if err := srv.Send(&pb.StreamChunk{ErrorCode: out.ErrorCode, ErrorMessage: out.ErrorMessage}); err != nil {
			return fmt.Errorf("rpc: stream %s: send error chunk: %w", req.GetMethod(), err)
		}
	}
	return nil
}

// chunkSender serialises writes to one stream. gRPC forbids concurrent Send on
// a stream, and a handler is free to fan its production out across goroutines,
// so the guard belongs here rather than in the handler contract.
type chunkSender struct {
	ctx    context.Context
	srv    grpc.ServerStreamingServer[pb.StreamChunk]
	method string

	mu     sync.Mutex
	failed error
}

func (c *chunkSender) send(chunk []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failed != nil {
		return c.failed
	}
	// Send blocks on HTTP/2 flow control, which is the backpressure, but a
	// caller that walked away unblocks nothing — ctx is what ends production.
	if err := c.ctx.Err(); err != nil {
		c.failed = fmt.Errorf("rpc: stream %s: caller is gone: %w", c.method, err)
		return c.failed
	}
	if err := c.srv.Send(&pb.StreamChunk{Payload: chunk}); err != nil {
		c.failed = fmt.Errorf("rpc: stream %s: send chunk: %w", c.method, err)
		return c.failed
	}
	return nil
}

func (c *chunkSender) err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed
}

// admit identifies the caller and checks it against the acceptance rules.
func (s *Server) admit(ctx context.Context, method string) error {
	caller, err := PeerFromContext(ctx)
	if err != nil {
		s.log.Warn("rpc: refusing an unidentified caller", "method", method, "error", err)
		return status.Errorf(codes.Unauthenticated, "rpc: %s", err)
	}
	if err := Allow(caller, method, s.policy()); err != nil {
		s.log.Warn("rpc: acceptance denied",
			"method", method,
			"peer_kind", caller.Kind.String(),
			"peer_service_id", caller.ServiceID,
			"error", err)
		return status.Errorf(codes.PermissionDenied, "rpc: %s", err)
	}
	return nil
}

func (s *Server) policy() *pb.PolicyEvaluation {
	if s.cfg.Policies == nil {
		return nil
	}
	return s.cfg.Policies.Policy()
}

// overloaded sheds the call. ResourceExhausted, not a queue: a queue in front of
// a saturated handler pool converts overload into unbounded memory and answers
// every caller late instead of answering most of them on time.
func (s *Server) overloaded(method string) error {
	s.log.Warn("rpc: shedding an inbound call",
		"method", method,
		"max_concurrent_calls", s.cfg.Limits.MaxConcurrentCalls)
	return status.Errorf(codes.ResourceExhausted, "rpc: %s: %s (max_concurrent_calls=%d)",
		method, ErrOverloaded, s.cfg.Limits.MaxConcurrentCalls)
}

// withInboundTrace puts the caller's trace context into ctx so the handler's
// nested operations parent to the caller's row.
//
// The value arrives two ways: the runtime stamps gRPC metadata when it proxies,
// a direct caller fills the request field. Metadata wins when both are present.
// A malformed value is not an error — it means the caller sent no usable trace,
// so the work starts a new tree (ADR-0006 §6). Parsing goes through
// telemetry.ParseHeader, the SDK's only parser (ADR-0006 §7).
func (s *Server) withInboundTrace(ctx context.Context, req *pb.CallRequest) context.Context {
	raw := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get(telemetry.MetadataKey); len(values) > 0 {
			raw = values[0]
		}
	}
	if raw == "" {
		raw = req.GetXSbTrace()
	}

	tc, err := telemetry.ParseHeader(raw)
	if err != nil {
		// The only failure ParseHeader reports is a broken entropy source. A zero
		// trace id would merge unrelated traces into one tree on the runtime side,
		// so the handler runs with no trace context at all instead.
		s.log.Warn("rpc: could not mint a replacement trace", "method", req.GetMethod(), "error", err)
		return ctx
	}
	return telemetry.WithTraceContext(ctx, tc)
}

func boundPort(addr net.Addr) (int, error) {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("rpc: bound address %s is not TCP: %w", addr, ErrServerConfig)
	}
	return tcp.Port, nil
}
