package connection

import (
	"context"
	"crypto/ecdsa"
	"crypto/tls"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc"
	gcreds "google.golang.org/grpc/credentials"
)

// Kinds raised by the session lifecycle. The identity kinds live in errors.go.
const (
	KindSession Kind = "SESSION"
	KindRotate  Kind = "ROTATE"
)

// Sentinels for errors.Is over the lifecycle kinds.
var (
	ErrSession = &Error{Kind: KindSession}
	ErrRotate  = &Error{Kind: KindRotate}
)

// Lease is one leaf certificate plus the identity it carries and the moment it
// stops being usable. It is the unit the whole lifecycle is built around: a
// session is opened for exactly one lease, and rotation is nothing but opening
// a session for the next one.
type Lease struct {
	Identity    Identity
	ServiceName string
	CertDER     []byte
	CAChainDER  []byte
	PrivateKey  *ecdsa.PrivateKey
	TLSCert     tls.Certificate
	NotAfter    time.Time
}

// Credentials is the mTLS material derived from one lease. Everything that
// talks mTLS — the inbound Call server, both outbound transports, the events,
// workflow, job and telemetry clients — holds a copy and must be handed a new
// one on every rotation.
type Credentials struct {
	// Addr is the runtime's host:port.
	Addr string
	// Lease is the certificate behind TLS, exposed for consumers that need the
	// raw DER (peer-to-peer dialling) rather than a *tls.Config.
	Lease Lease
	// TLS is the client config: the current leaf plus the pinned runtime CA.
	TLS *tls.Config
}

// CredentialConsumer is anything holding mTLS material derived from the current
// lease.
//
// UseCredentials must not call back into the CredentialRegistry — the registry
// holds its lock across the call.
type CredentialConsumer interface {
	UseCredentials(ctx context.Context, creds Credentials) error
}

type credentialEntry struct {
	name     string
	consumer CredentialConsumer
}

// CredentialRegistry is the single place mTLS material is handed out.
//
// Consumers register themselves where they are built; rotation makes one pass
// over the registry. There is deliberately no hand-written list of "who to
// update" anywhere: such a list goes stale the moment a new consumer appears,
// and a fleet then runs with half its channels pinned to a certificate that is
// about to expire while the control channel still looks healthy.
type CredentialRegistry struct {
	mu        sync.Mutex
	entries   []credentialEntry
	current   Credentials
	published bool
}

// NewCredentialRegistry builds an empty registry.
func NewCredentialRegistry() *CredentialRegistry {
	return &CredentialRegistry{}
}

// Register adds a consumer under name. When credentials have already been
// published the consumer is switched onto them before Register returns: a
// consumer built between two rotations must never start out holding nothing.
func (r *CredentialRegistry) Register(ctx context.Context, name string, consumer CredentialConsumer) error {
	const op = "register credential consumer"

	if consumer == nil {
		return newError(KindSession, op, "nil consumer for "+name, nil)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.entries = append(r.entries, credentialEntry{name: name, consumer: consumer})
	if !r.published {
		return nil
	}
	if err := consumer.UseCredentials(ctx, r.current); err != nil {
		return newError(KindSession, op, "consumer "+name+" rejected the current credentials", err)
	}
	return nil
}

// Update publishes creds to every registered consumer. Every consumer is
// attempted even after one fails, so a single broken consumer cannot leave the
// rest pinned to the previous certificate; the failures are joined.
func (r *CredentialRegistry) Update(ctx context.Context, creds Credentials) error {
	const op = "publish credentials"

	r.mu.Lock()
	defer r.mu.Unlock()

	r.current = creds
	r.published = true

	var errs []error
	for _, e := range r.entries {
		if err := e.consumer.UseCredentials(ctx, creds); err != nil {
			errs = append(errs, newError(KindSession, op, "consumer "+e.name, err))
		}
	}
	if len(errs) > 0 {
		return newError(KindSession, op, "one or more consumers rejected the credentials", errors.Join(errs...))
	}
	return nil
}

// Current returns the published credentials and whether anything was published.
func (r *CredentialRegistry) Current() (Credentials, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current, r.published
}

// SessionIdentity is the identity of the live session.
type SessionIdentity struct {
	SessionID   string
	ServiceID   string
	ServiceName string
	InstanceID  string
}

// IdentitySource hands out the identity of the CURRENT session.
//
// Consumers must call Identity per use and never copy the result at
// construction: every rotation mints a fresh instanceID for the same serviceID,
// and a copy taken once keeps naming an instance the runtime has already marked
// disconnected.
type IdentitySource interface {
	Identity() SessionIdentity
}

// Provisioner mints a lease from the bootstrap key.
//
// This is the expensive path: the runtime answers Bootstrap.Provision with a
// 64 MiB argon2id hash. The lifecycle calls it only when it holds no lease that
// outlives the rotation lead.
type Provisioner interface {
	Provision(ctx context.Context) (Lease, error)
}

// BootstrapProvisioner mints leases through Bootstrap.Provision.
type BootstrapProvisioner struct {
	Addr string
	Key  BootstrapKey
}

// Provision obtains a fresh leaf certificate with the bootstrap key.
func (p BootstrapProvisioner) Provision(ctx context.Context) (Lease, error) {
	res, err := Provision(ctx, p.Addr, p.Key)
	if err != nil {
		return Lease{}, err
	}
	return Lease{
		Identity:    res.Identity,
		ServiceName: res.ServiceName,
		CertDER:     res.CertDER,
		CAChainDER:  res.CAChainDER,
		PrivateKey:  res.PrivateKey,
		TLSCert:     res.TLSCert,
		NotAfter:    res.NotAfter,
	}, nil
}

// Dialer opens the gRPC channel of one session. The default builds an mTLS
// channel against the runtime; tests substitute an in-memory transport.
type Dialer interface {
	Dial(ctx context.Context, creds Credentials) (*grpc.ClientConn, error)
}

// MTLSDialer dials the runtime with the leaf certificate in creds.
type MTLSDialer struct{}

// Dial builds the channel. grpc.NewClient connects lazily; a channel that
// cannot come up surfaces as an Unavailable on the first stream, which the
// reconnect ladder already handles.
func (MTLSDialer) Dial(_ context.Context, creds Credentials) (*grpc.ClientConn, error) {
	const op = "dial runtime"

	conn, err := grpc.NewClient(creds.Addr, grpc.WithTransportCredentials(gcreds.NewTLS(creds.TLS)))
	if err != nil {
		return nil, newError(KindSession, op, "build channel to "+creds.Addr, err)
	}
	return conn, nil
}

// InboundServer is the inbound Call listener.
//
// Its address must be known before the first RegisterAndWatch, so it comes up
// before the control stream is opened. It takes its certificate from the
// CredentialRegistry like every other consumer, not from Start.
type InboundServer interface {
	// Start binds the listener and returns the address peers dial. It is called
	// on every connect attempt and must be idempotent: a reconnect must not
	// rebind the port or change the advertised address.
	Start(ctx context.Context) (string, error)
	// Close releases the listener. Called once, from Lifecycle.Stop.
	Close(ctx context.Context) error
}

// Registrar owns the Registry.RegisterAndWatch stream of ONE session. It is
// built per session from that session's channel and dies with it, so there is
// no shared stream two overlapping sessions could fight over during rotation.
type Registrar interface {
	// Start opens RegisterAndWatch, advertising endpoint as this instance's
	// inbound Call address. An empty endpoint means caller-only.
	Start(ctx context.Context, endpoint string) error
	// Close tears the stream down. Called exactly once, when the owning session
	// closes.
	Close(ctx context.Context) error
}

// RegistrarFactory builds the registrar bound to a session's channel.
type RegistrarFactory interface {
	NewRegistrar(conn grpc.ClientConnInterface) Registrar
}

// Observer reports lifecycle transitions outward. Callbacks run on the
// lifecycle's own goroutines and must not block.
type Observer interface {
	// Connected fires once per live session, after Welcome.
	Connected(id SessionIdentity)
	// Reconnecting fires before each attempt to rebuild a lost session.
	Reconnecting(attempt int, cause error)
	// Draining fires when the runtime announces it is shutting down.
	Draining(reason string)
	// Disconnected fires once, when the lifecycle has stopped trying.
	Disconnected(cause error)
}

type nopObserver struct{}

func (nopObserver) Connected(SessionIdentity) {}
func (nopObserver) Reconnecting(int, error)   {}
func (nopObserver) Draining(string)           {}
func (nopObserver) Disconnected(error)        {}

// session is one live Control.Open stream and the channel underneath it.
//
// Liveness is the stream: there is no heartbeat (ADR-0005), so the stream
// closing IS the disconnect signal and the first Welcome IS the proof that the
// session came up. Exactly one goroutine — run — reads the stream, and exactly
// one owner — this struct — closes the channel.
type session struct {
	lease     Lease
	conn      *grpc.ClientConn
	stream    grpc.ServerStreamingClient[pb.ServerControl]
	registrar Registrar
	cancel    context.CancelFunc
	log       *slog.Logger
	onDrain   func(reason string)

	welcome chan *pb.Welcome
	done    chan struct{}
	gone    chan struct{}

	// endErr is written by run before done is closed and read only after done is
	// observed closed, so the channel close is the happens-before edge.
	endErr error

	expected atomic.Bool
	closing  atomic.Bool
}

// newSession dials the runtime with creds and opens Control.Open. It returns as
// soon as the stream exists; the Welcome that proves the session live is
// awaited separately, after registration.
//
// ctx governs the session's whole life, not just this call: the stream hangs
// off a child of it, so cancelling ctx tears the session down.
func newSession(ctx context.Context, dialer Dialer, creds Credentials, log *slog.Logger, onDrain func(string)) (*session, error) {
	const op = "open control stream"

	conn, err := dialer.Dial(ctx, creds)
	if err != nil {
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := pb.NewControlClient(conn).Open(streamCtx, &pb.OpenRequest{})
	if err != nil {
		cancel()
		if cerr := conn.Close(); cerr != nil {
			log.Warn("connection: closing channel after a failed Control.Open", "error", cerr)
		}
		return nil, newError(KindSession, op, "Control.Open", err)
	}

	s := &session{
		lease:   creds.Lease,
		conn:    conn,
		stream:  stream,
		cancel:  cancel,
		log:     log,
		onDrain: onDrain,
		welcome: make(chan *pb.Welcome, 1),
		done:    make(chan struct{}),
		gone:    make(chan struct{}),
	}
	go s.run()
	return s, nil
}

// run is the session's only reader. It ends on the first stream error, which is
// the disconnect signal the supervisor waits for.
func (s *session) run() {
	defer close(s.done)

	for {
		msg, err := s.stream.Recv()
		if err != nil {
			s.endErr = newError(KindSession, "read control stream", "Control.Open ended", err)
			return
		}
		switch k := msg.GetKind().(type) {
		case *pb.ServerControl_Welcome:
			// The protocol sends exactly one Welcome, first. The buffered
			// channel absorbs it whether or not anyone is waiting yet; a second
			// one would be a protocol violation and is dropped rather than
			// blocking the reader.
			select {
			case s.welcome <- k.Welcome:
			default:
				s.log.Warn("connection: extra Welcome on a live control stream")
			}
		case *pb.ServerControl_Drain:
			s.onDrain(k.Drain.GetReason())
		default:
			s.log.Warn("connection: unknown control message")
		}
	}
}

// awaitWelcome blocks until the session proves itself live. Every wait point is
// selectable so Stop lands during an in-flight connect.
func (s *session) awaitWelcome(ctx context.Context, timeout time.Duration) (*pb.Welcome, error) {
	const op = "await welcome"

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case w := <-s.welcome:
		return w, nil
	case <-s.done:
		return nil, newError(KindSession, op, "control stream ended before Welcome", s.endErr)
	case <-timer.C:
		return nil, newError(KindSession, op, "no Welcome within "+timeout.String(), nil)
	case <-ctx.Done():
		return nil, newError(KindSession, op, "cancelled", ctx.Err())
	}
}

// dead reports whether the stream has already ended.
func (s *session) dead() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// shutdown tears the session down: registrar first, then the stream, then the
// channel. It is idempotent and synchronous — when it returns, the reader
// goroutine is gone and the channel is closed. An unclosed grpc.ClientConn
// keeps its own reconnect goroutines and backoff timers alive for the life of
// the process, so every path out of a session runs through here.
//
// expected tells the supervisor whether this close is a disconnect worth
// reconnecting from: false makes the supervisor treat it as a lost session.
func (s *session) shutdown(ctx context.Context, expected bool) {
	if !s.closing.CompareAndSwap(false, true) {
		<-s.gone
		return
	}
	defer close(s.gone)

	s.expected.Store(expected)
	if s.registrar != nil {
		if err := s.registrar.Close(ctx); err != nil {
			s.log.Warn("connection: closing the registry stream", "error", err)
		}
	}
	s.cancel()
	<-s.done
	if err := s.conn.Close(); err != nil {
		s.log.Warn("connection: closing the session channel", "error", err)
	}
}
