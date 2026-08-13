package connection

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/service-bridge/sdk/go/internal/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Defaults of the connection lifecycle. See ./README.md.
const (
	DefaultWelcomeTimeout = 10 * time.Second
	DefaultRotateLead     = 30 * time.Minute
	DefaultRotateJitter   = 5 * time.Minute

	// DefaultMinRotateDelay floors the renewal schedule. A runtime handing out
	// leaves shorter than the rotation lead would otherwise put every client
	// into a refresh hot loop the moment it connects.
	DefaultMinRotateDelay = 5 * time.Second
)

// terminalCodes are the gRPC codes no ladder can fix: the identity is rejected,
// the service is unknown, or the request itself is malformed. Retrying them
// hammers the runtime and hides the real failure from the caller, so they end
// the lifecycle instead.
var terminalCodes = map[codes.Code]struct{}{
	codes.Unauthenticated:  {},
	codes.PermissionDenied: {},
	codes.NotFound:         {},
	codes.InvalidArgument:  {},
}

// LifecycleConfig wires the connection lifecycle. See ./README.md.
type LifecycleConfig struct {
	Addr        string
	CACert      *x509.Certificate
	Provisioner Provisioner
	Refresher   Refresher
	Dialer      Dialer
	Credentials *CredentialRegistry
	Inbound     InboundServer
	Registrars  RegistrarFactory
	Observer    Observer
	Backoff     stream.Backoff

	MaxAttempts    int
	WelcomeTimeout time.Duration
	RotateLead     time.Duration
	RotateJitter   time.Duration
	MinRotateDelay time.Duration

	Random func() float64
	Now    func() time.Time
	Logger *slog.Logger
}

// state is everything one live session contributes to the client: the session
// itself, the identity it was welcomed under and the lease behind its
// certificate. It moves as one value so a failed swap can be undone whole.
type state struct {
	sess  *session
	id    SessionIdentity
	lease Lease
	live  bool
}

// Lifecycle owns the control-plane connection: the current session, the
// certificate behind it and the reconnect and renewal schedules.
//
// It has exactly one connect path — connect — travelled by the first connect,
// by every reconnect and by every rotation. Supervision, credential publication
// and the lease cache live inside it because a second path is a second place to
// forget one of them.
type Lifecycle struct {
	cfg LifecycleConfig

	// rotateNow requests a renewal ahead of schedule. Buffered and coalescing:
	// the run goroutine is the only rotator, callers never block on it.
	rotateNow chan struct{}
	done      chan struct{}
	final     sync.Once

	mu        sync.Mutex
	started   bool
	running   bool
	stopped   bool
	cancel    context.CancelFunc
	st        state
	endpoint  string
	inboundUp bool
}

// NewLifecycle validates the config and fills in the defaults.
func NewLifecycle(cfg LifecycleConfig) (*Lifecycle, error) {
	const op = "build connection lifecycle"

	if cfg.Addr == "" {
		return nil, newError(KindSession, op, "runtime address is empty", nil)
	}
	if cfg.CACert == nil {
		return nil, newError(KindSession, op, "no pinned CA: the trust anchor comes from the bootstrap key, never from the wire", nil)
	}
	if cfg.Provisioner == nil {
		return nil, newError(KindSession, op, "no provisioner", nil)
	}
	if cfg.Dialer == nil {
		cfg.Dialer = MTLSDialer{}
	}
	if cfg.Refresher == nil {
		cfg.Refresher = ControlRefresher{}
	}
	if cfg.Credentials == nil {
		cfg.Credentials = NewCredentialRegistry()
	}
	if cfg.Observer == nil {
		cfg.Observer = nopObserver{}
	}
	// A zero-value Backoff has no rungs and panics on Delay.
	if len(cfg.Backoff.Rungs()) == 0 {
		cfg.Backoff = stream.NewBackoff()
	}
	if cfg.WelcomeTimeout <= 0 {
		cfg.WelcomeTimeout = DefaultWelcomeTimeout
	}
	if cfg.RotateLead <= 0 {
		cfg.RotateLead = DefaultRotateLead
	}
	if cfg.RotateJitter < 0 {
		cfg.RotateJitter = DefaultRotateJitter
	}
	if cfg.MinRotateDelay <= 0 {
		cfg.MinRotateDelay = DefaultMinRotateDelay
	}
	if cfg.Random == nil {
		cfg.Random = rand.Float64
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Lifecycle{
		cfg:       cfg,
		rotateNow: make(chan struct{}, 1),
		done:      make(chan struct{}),
	}, nil
}

// Credentials exposes the registry every mTLS consumer registers with.
func (l *Lifecycle) Credentials() *CredentialRegistry { return l.cfg.Credentials }

// Identity reports the identity of the live session. It satisfies
// IdentitySource: consumers must call it per use, because every rotation mints
// a fresh instanceID under the same serviceID.
func (l *Lifecycle) Identity() SessionIdentity {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.st.id
}

// Conn returns the channel of the live session. Consumers that open their own
// streams ask for it per open rather than capturing it: rotation replaces the
// channel underneath them.
func (l *Lifecycle) Conn() (grpc.ClientConnInterface, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.st.live || l.st.sess == nil {
		return nil, newError(KindSession, "current channel", "no live session", nil)
	}
	return l.st.sess.conn, nil
}

// Rotate asks for a certificate renewal now instead of at the scheduled time.
// Non-blocking and coalescing; the renewal itself runs on the lifecycle's own
// goroutine, so it can never overlap a reconnect.
func (l *Lifecycle) Rotate() {
	select {
	case l.rotateNow <- struct{}{}:
	default:
	}
}

// Start provisions the identity, opens the first session and hands supervision
// to the lifecycle goroutine. It returns the failure of the first attempt
// rather than hiding it behind the ladder: a rejected bootstrap key must not
// look like a slow start.
//
// ctx bounds the first attempt only. The session outlives the call — a caller
// passing a request-scoped context must not lose its control plane when that
// request ends.
func (l *Lifecycle) Start(ctx context.Context) error {
	const op = "start connection"

	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return newError(KindSession, op, "lifecycle already stopped", nil)
	}
	if l.started {
		l.mu.Unlock()
		return newError(KindSession, op, "lifecycle already started", nil)
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	l.started = true
	l.cancel = cancel
	l.mu.Unlock()

	if err := l.connect(ctx, runCtx, l.leaseForConnect); err != nil {
		cancel()
		l.closeInbound(ctx)
		l.mu.Lock()
		l.started = false
		l.cancel = nil
		l.mu.Unlock()
		return err
	}

	l.mu.Lock()
	l.running = true
	l.mu.Unlock()
	go l.run(runCtx)
	return nil
}

// Stop tears everything down and waits for the lifecycle goroutine. Safe during
// an in-flight connect: the cancelled context unblocks every wait point, and a
// session adopted after the stop flag is set is refused and closed on the spot.
func (l *Lifecycle) Stop(ctx context.Context) error {
	const op = "stop connection"

	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return nil
	}
	l.stopped = true
	cancel := l.cancel
	l.cancel = nil
	running := l.running
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if running {
		<-l.done
	}

	l.mu.Lock()
	sess := l.st.sess
	l.st = state{id: l.st.id}
	l.mu.Unlock()

	if sess != nil {
		sess.shutdown(ctx)
	}
	l.closeInbound(ctx)
	l.finish(nil)

	if err := ctx.Err(); err != nil {
		return newError(KindSession, op, "stop context ended before the teardown finished", err)
	}
	return nil
}

// run supervises the session: one goroutine owns reconnecting and rotating, so
// the two can never race for the current session.
func (l *Lifecycle) run(ctx context.Context) {
	defer close(l.done)

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	l.armRotation(timer)

	attempt, rotateAttempt := 0, 0

	for {
		sess := l.currentSession()
		if sess == nil {
			if err := l.connect(ctx, ctx, l.leaseForConnect); err != nil {
				if ctx.Err() != nil {
					return
				}
				if isTerminal(err) {
					l.giveUp(ctx, err)
					return
				}
				if !l.waitLadder(ctx, &attempt, err) {
					return
				}
				continue
			}
			attempt = 0
			l.armRotation(timer)
			continue
		}

		select {
		case <-ctx.Done():
			return

		case <-sess.done:
			cause := sess.endErr
			l.cfg.Logger.Info("connection: control stream ended", "session_id", l.Identity().SessionID, "error", cause)
			l.drop(ctx, sess)
			if isTerminal(cause) {
				l.giveUp(ctx, cause)
				return
			}
			if !l.waitLadder(ctx, &attempt, cause) {
				return
			}

		case <-timer.C:
			if !l.rotateOnce(ctx, sess, timer, &rotateAttempt) {
				return
			}

		case <-l.rotateNow:
			if !l.rotateOnce(ctx, sess, timer, &rotateAttempt) {
				return
			}
		}
	}
}

// leaseSource yields the certificate one connect attempt runs on. It is the
// ONLY difference between a first connect, a reconnect and a rotation.
type leaseSource func(ctx context.Context) (Lease, error)

// connect is the connection path. Every session in the SDK is born here, so
// every session is supervised, publishes its credentials to every consumer and
// updates the lease cache — none of which can be forgotten by a caller, because
// no caller performs them.
//
// attemptCtx bounds this attempt; sessionCtx owns the session it produces and
// is always the lifecycle context, never a caller's.
func (l *Lifecycle) connect(attemptCtx, sessionCtx context.Context, source leaseSource) error {
	const op = "connect"

	if err := sessionCtx.Err(); err != nil {
		return newError(KindSession, op, "lifecycle stopped", err)
	}

	lease, err := source(attemptCtx)
	if err != nil {
		return err
	}
	creds := Credentials{
		Addr:  l.cfg.Addr,
		Lease: lease,
		TLS:   MutualTLSConfig(l.cfg.CACert, lease.TLSCert),
	}

	endpoint, err := l.startInbound(attemptCtx)
	if err != nil {
		return err
	}

	sess, err := newSession(sessionCtx, l.cfg.Dialer, creds, l.cfg.Logger, l.onDrain)
	if err != nil {
		return err
	}

	// Welcome is the only proof the session came up. Until it lands, nothing is
	// swapped and nobody is told anything — that is what makes the overlap during
	// rotation safe.
	welcome, err := sess.awaitWelcome(attemptCtx, l.cfg.WelcomeTimeout)
	if err != nil {
		l.discard(attemptCtx, sess)
		return err
	}

	id := SessionIdentity{
		SessionID:   welcome.GetSessionId(),
		ServiceID:   welcome.GetServiceId(),
		ServiceName: welcome.GetServiceName(),
		InstanceID:  lease.Identity.InstanceID,
	}

	if l.cfg.Registrars != nil {
		registrar := l.cfg.Registrars.NewRegistrar(sess.conn)
		if err := registrar.Start(attemptCtx, endpoint); err != nil {
			l.discard(attemptCtx, sess)
			return newError(KindSession, op, "register declarations", err)
		}
		sess.registrar = registrar
	}

	prev, adopted := l.adopt(sess, id, lease)
	if !adopted {
		l.discard(attemptCtx, sess)
		return newError(KindSession, op, "lifecycle stopped", context.Canceled)
	}

	// Identity first, then credentials: a consumer handed a new certificate reads
	// the identity that goes with it on its very next call.
	if err := l.cfg.Credentials.Update(attemptCtx, creds); err != nil {
		// Half the consumers on the new certificate and half on the old one is the
		// exact state this design exists to prevent. Undo the swap and let the
		// ladder retry the whole attempt.
		l.restore(prev)
		l.discard(attemptCtx, sess)
		return newError(KindSession, op, "publish credentials", err)
	}

	if prev.sess != nil {
		prev.sess.shutdown(attemptCtx)
	}
	l.cfg.Observer.Connected(id)
	l.cfg.Logger.Info("connection: session live",
		"session_id", id.SessionID, "service", id.ServiceName, "instance_id", id.InstanceID)
	return nil
}

// leaseForConnect prefers the cached leaf.
//
// Bootstrap.Provision costs the runtime a 64 MiB argon2id hash. Re-provisioning
// on every transport reconnect turns a reconnect storm into a self-inflicted
// denial of service, so the cached leaf is reused until it enters the renewal
// window, where it needs replacing anyway.
func (l *Lifecycle) leaseForConnect(ctx context.Context) (Lease, error) {
	if lease, ok := l.cachedLease(); ok && l.cfg.Now().Before(lease.NotAfter.Add(-l.cfg.RotateLead)) {
		return lease, nil
	}
	return l.cfg.Provisioner.Provision(ctx)
}

// startInbound brings the Call listener up once. The advertised address is
// cached: rebinding on reconnect would republish an endpoint peers already hold.
func (l *Lifecycle) startInbound(ctx context.Context) (string, error) {
	if l.cfg.Inbound == nil {
		return "", nil
	}

	l.mu.Lock()
	if l.inboundUp {
		endpoint := l.endpoint
		l.mu.Unlock()
		return endpoint, nil
	}
	l.mu.Unlock()

	endpoint, err := l.cfg.Inbound.Start(ctx)
	if err != nil {
		return "", newError(KindSession, "start inbound server", "bind the Call listener", err)
	}

	l.mu.Lock()
	l.endpoint = endpoint
	l.inboundUp = true
	l.mu.Unlock()
	return endpoint, nil
}

func (l *Lifecycle) closeInbound(ctx context.Context) {
	l.mu.Lock()
	up := l.inboundUp
	l.inboundUp = false
	l.mu.Unlock()

	if !up || l.cfg.Inbound == nil {
		return
	}
	if err := l.cfg.Inbound.Close(ctx); err != nil {
		l.cfg.Logger.Warn("connection: closing the inbound Call server", "error", err)
	}
}

// discard closes a session that was never adopted. A channel nobody accepted is
// still a channel: left open it keeps gRPC's own reconnect goroutines alive for
// the life of the process.
func (l *Lifecycle) discard(ctx context.Context, sess *session) {
	sess.shutdown(context.WithoutCancel(ctx))
}

// drop closes the session the lifecycle was serving and clears it, but keeps the
// lease and the identity: the next attempt reuses the certificate.
func (l *Lifecycle) drop(ctx context.Context, sess *session) {
	l.mu.Lock()
	if l.st.sess == sess {
		l.st.sess = nil
		l.st.live = false
	}
	l.mu.Unlock()
	sess.shutdown(context.WithoutCancel(ctx))
}

func (l *Lifecycle) adopt(sess *session, id SessionIdentity, lease Lease) (state, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return state{}, false
	}
	prev := l.st
	l.st = state{sess: sess, id: id, lease: lease, live: true}
	return prev, true
}

func (l *Lifecycle) restore(prev state) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.st = prev
}

func (l *Lifecycle) currentSession() *session {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.st.sess
}

func (l *Lifecycle) cachedLease() (Lease, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.st.lease.TLSCert.Leaf == nil {
		return Lease{}, false
	}
	return l.st.lease, true
}

func (l *Lifecycle) onDrain(reason string) {
	l.cfg.Logger.Info("connection: runtime draining", "reason", reason)
	l.cfg.Observer.Draining(reason)
}

// waitLadder sleeps out one rung of the shared reconnect ladder. It reports
// false when the lifecycle must stop: cancelled, or out of attempts.
func (l *Lifecycle) waitLadder(ctx context.Context, attempt *int, cause error) bool {
	if l.cfg.MaxAttempts > 0 && *attempt >= l.cfg.MaxAttempts {
		l.giveUp(ctx, newError(KindSession, "reconnect",
			fmt.Sprintf("gave up after %d attempts", *attempt), cause))
		return false
	}

	delay := l.cfg.Backoff.Delay(*attempt)
	*attempt++
	l.cfg.Observer.Reconnecting(*attempt, cause)
	l.cfg.Logger.Warn("connection: reconnect scheduled",
		"attempt", *attempt, "delay_ms", delay.Milliseconds(), "cause", cause)

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// giveUp reports outward that the lifecycle stopped trying. The session, if any,
// goes with it: a client that has given up must not look half-connected.
func (l *Lifecycle) giveUp(ctx context.Context, cause error) {
	if sess := l.currentSession(); sess != nil {
		l.drop(ctx, sess)
	}
	l.cfg.Logger.Error("connection: giving up", "cause", cause)
	l.finish(cause)
}

func (l *Lifecycle) finish(cause error) {
	l.final.Do(func() { l.cfg.Observer.Disconnected(cause) })
}

// isTerminal reports whether err carries a gRPC code no retry can fix.
func isTerminal(err error) bool {
	var carrier interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &carrier) {
		return false
	}
	st := carrier.GRPCStatus()
	if st == nil {
		return false
	}
	_, terminal := terminalCodes[st.Code()]
	return terminal
}
