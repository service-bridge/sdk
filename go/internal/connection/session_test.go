package connection_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const (
	testServiceID   = "3f2b1c8e-9d4a-4f11-8a7b-0c5d6e7f8a9b"
	testServiceName = "orders"
	testEndpoint    = "10.0.0.7:9100"
)

// ── fake control plane ────────────────────────────────────────────────────────

// controlCall is one live Control.Open call on the server side.
type controlCall struct {
	n     int
	drain chan string
	junk  chan struct{}
	kill  chan error
	ended chan struct{}
}

type fakeControl struct {
	pb.UnimplementedControlServer

	mu       sync.Mutex
	calls    []*controlCall
	opened   chan *controlCall
	welcomes int
	holds    map[int]chan struct{}
	openErr  func(n int) error
	refresh  func(n int, req *pb.RefreshCertRequest) (*pb.RefreshCertResponse, error)
	refreshN int
}

func newFakeControl() *fakeControl {
	return &fakeControl{
		opened:   make(chan *controlCall, 32),
		welcomes: 1,
		holds:    make(map[int]chan struct{}),
	}
}

// holdWelcome withholds the Welcome of the n-th Control.Open until the returned
// channel is closed. It is how a test parks a rotation exactly at the moment
// the new session is dialled but not yet proven.
func (f *fakeControl) holdWelcome(n int) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	gate := make(chan struct{})
	f.holds[n] = gate
	return gate
}

func (f *fakeControl) Open(_ *pb.OpenRequest, srv grpc.ServerStreamingServer[pb.ServerControl]) error {
	f.mu.Lock()
	call := &controlCall{
		n:     len(f.calls) + 1,
		drain: make(chan string, 1),
		junk:  make(chan struct{}, 1),
		kill:  make(chan error, 1),
		ended: make(chan struct{}),
	}
	f.calls = append(f.calls, call)
	welcomes, openErr, gate := f.welcomes, f.openErr, f.holds[call.n]
	f.mu.Unlock()

	defer close(call.ended)
	f.opened <- call

	if openErr != nil {
		if err := openErr(call.n); err != nil {
			return err
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-srv.Context().Done():
			return srv.Context().Err()
		}
	}
	for i := 0; i < welcomes; i++ {
		msg := &pb.ServerControl{Kind: &pb.ServerControl_Welcome{Welcome: &pb.Welcome{
			SessionId:   "session-" + strconv.Itoa(call.n),
			ServiceId:   testServiceID,
			ServiceName: testServiceName,
		}}}
		if err := srv.Send(msg); err != nil {
			return err
		}
	}

	for {
		select {
		case <-call.junk:
			// A control frame the SDK has no case for: forward compatibility, not
			// a reason to drop the session.
			if err := srv.Send(&pb.ServerControl{}); err != nil {
				return err
			}
		case reason := <-call.drain:
			msg := &pb.ServerControl{Kind: &pb.ServerControl_Drain{Drain: &pb.Drain{Reason: reason}}}
			if err := srv.Send(msg); err != nil {
				return err
			}
		case err := <-call.kill:
			return err
		case <-srv.Context().Done():
			return srv.Context().Err()
		}
	}
}

func (f *fakeControl) RefreshCert(_ context.Context, req *pb.RefreshCertRequest) (*pb.RefreshCertResponse, error) {
	f.mu.Lock()
	f.refreshN++
	n, refresh := f.refreshN, f.refresh
	f.mu.Unlock()

	if refresh == nil {
		return nil, status.Error(codes.Unimplemented, "no refresh configured")
	}
	return refresh(n, req)
}

func (f *fakeControl) awaitOpen(t *testing.T) *controlCall {
	t.Helper()
	select {
	case call := <-f.opened:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("no Control.Open reached the runtime")
		return nil
	}
}

func (f *fakeControl) openCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeControl) refreshCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refreshN
}

// die ends one server-side stream with err, the way a runtime restart does.
func (c *controlCall) die(t *testing.T, err error) {
	t.Helper()
	c.kill <- err
	select {
	case <-c.ended:
	case <-time.After(5 * time.Second):
		t.Fatalf("Control.Open %d did not end", c.n)
	}
}

// ── dialer over bufconn ───────────────────────────────────────────────────────

type bufDialer struct {
	lis *bufconn.Listener

	mu    sync.Mutex
	conns []*grpc.ClientConn
	creds []connection.Credentials
	fail  error
}

func (d *bufDialer) Dial(_ context.Context, creds connection.Credentials) (*grpc.ClientConn, error) {
	d.mu.Lock()
	fail := d.fail
	d.mu.Unlock()
	if fail != nil {
		return nil, fail
	}

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return d.lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.creds = append(d.creds, creds)
	d.mu.Unlock()
	return conn, nil
}

func (d *bufDialer) dialed() ([]*grpc.ClientConn, []connection.Credentials) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]*grpc.ClientConn(nil), d.conns...), append([]connection.Credentials(nil), d.creds...)
}

func (d *bufDialer) setFail(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fail = err
}

// assertClosed proves the channel is gone: an open grpc.ClientConn keeps its own
// reconnect goroutines and backoff timers alive for the life of the process.
func assertClosed(t *testing.T, conn *grpc.ClientConn, what string) {
	t.Helper()
	waitFor(t, what+" closed", func() bool { return conn.GetState() == connectivity.Shutdown })
}

// mustConn returns the live channel as the concrete type, so a test can inspect
// its connectivity state.
func mustConn(t *testing.T, life *connection.Lifecycle) *grpc.ClientConn {
	t.Helper()
	conn, err := life.Conn()
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	typed, ok := conn.(*grpc.ClientConn)
	if !ok {
		t.Fatalf("Conn returned %T, want *grpc.ClientConn", conn)
	}
	return typed
}

func assertLive(t *testing.T, conn *grpc.ClientConn, what string) {
	t.Helper()
	if conn.GetState() == connectivity.Shutdown {
		t.Fatalf("%s was closed but had to stay live", what)
	}
}

// ── leases ────────────────────────────────────────────────────────────────────

func newLease(t *testing.T, ca *testCA, instanceID string, notAfter time.Time) connection.Lease {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen leaf key: %v", err)
	}
	id := connection.Identity{ServiceID: testServiceID, InstanceID: instanceID}
	der := ca.issueLeaf(t, &key.PublicKey, id)
	cert, err := connection.NewTLSCertificate(der, ca.der, key)
	if err != nil {
		t.Fatalf("NewTLSCertificate: %v", err)
	}
	return connection.Lease{
		Identity:    id,
		ServiceName: testServiceName,
		CertDER:     der,
		CAChainDER:  ca.der,
		PrivateKey:  key,
		TLSCert:     cert,
		NotAfter:    notAfter,
	}
}

// fakeProvisioner stands in for Bootstrap.Provision — the argon2id path whose
// call count the cache tests assert on.
type fakeProvisioner struct {
	t  *testing.T
	ca *testCA

	mu       sync.Mutex
	calls    int
	notAfter time.Time
	err      error
}

func (p *fakeProvisioner) Provision(context.Context) (connection.Lease, error) {
	p.mu.Lock()
	p.calls++
	n, notAfter, err := p.calls, p.notAfter, p.err
	p.mu.Unlock()

	if err != nil {
		return connection.Lease{}, err
	}
	return newLease(p.t, p.ca, fmt.Sprintf("provisioned-%d", n), notAfter), nil
}

func (p *fakeProvisioner) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *fakeProvisioner) setNotAfter(at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.notAfter = at
}

func (p *fakeProvisioner) setError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// ── consumers, observer, inbound, registrar ───────────────────────────────────

// recordConsumer is one holder of mTLS material: the inbound Call server, an
// outbound transport, the events, workflow, job or telemetry client.
type recordConsumer struct {
	name string

	mu   sync.Mutex
	got  []connection.Credentials
	fail error
}

func (c *recordConsumer) UseCredentials(_ context.Context, creds connection.Credentials) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail != nil {
		return c.fail
	}
	c.got = append(c.got, creds)
	return nil
}

func (c *recordConsumer) last() (connection.Credentials, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.got) == 0 {
		return connection.Credentials{}, false
	}
	return c.got[len(c.got)-1], true
}

func (c *recordConsumer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.got)
}

func (c *recordConsumer) setFail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fail = err
}

type recordObserver struct {
	mu           sync.Mutex
	connected    []connection.SessionIdentity
	reconnects   []int
	drains       []string
	disconnected []error
	done         bool
}

func (o *recordObserver) Connected(id connection.SessionIdentity) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.connected = append(o.connected, id)
}

func (o *recordObserver) Reconnecting(attempt int, _ error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reconnects = append(o.reconnects, attempt)
}

func (o *recordObserver) Draining(reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.drains = append(o.drains, reason)
}

func (o *recordObserver) Disconnected(cause error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.disconnected = append(o.disconnected, cause)
	o.done = true
}

func (o *recordObserver) connectedIDs() []connection.SessionIdentity {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]connection.SessionIdentity(nil), o.connected...)
}

func (o *recordObserver) reconnectCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.reconnects)
}

func (o *recordObserver) drainReasons() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.drains...)
}

func (o *recordObserver) disconnects() []error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]error(nil), o.disconnected...)
}

type fakeInbound struct {
	mu      sync.Mutex
	starts  int
	closes  int
	failErr error
}

func (i *fakeInbound) Start(context.Context) (string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.failErr != nil {
		return "", i.failErr
	}
	i.starts++
	return testEndpoint, nil
}

func (i *fakeInbound) Close(context.Context) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closes++
	return nil
}

func (i *fakeInbound) counts() (int, int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.starts, i.closes
}

type fakeRegistrar struct {
	factory  *fakeRegistrarFactory
	conn     grpc.ClientConnInterface
	endpoint string
	closed   bool
}

func (r *fakeRegistrar) Start(_ context.Context, endpoint string) error {
	r.factory.mu.Lock()
	defer r.factory.mu.Unlock()
	if r.factory.startErr != nil {
		return r.factory.startErr
	}
	r.endpoint = endpoint
	return nil
}

func (r *fakeRegistrar) Close(context.Context) error {
	r.factory.mu.Lock()
	defer r.factory.mu.Unlock()
	r.closed = true
	return nil
}

type fakeRegistrarFactory struct {
	mu       sync.Mutex
	built    []*fakeRegistrar
	startErr error
}

func (f *fakeRegistrarFactory) NewRegistrar(conn grpc.ClientConnInterface) connection.Registrar {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := &fakeRegistrar{factory: f, conn: conn}
	f.built = append(f.built, r)
	return r
}

func (f *fakeRegistrarFactory) registrars() []*fakeRegistrar {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*fakeRegistrar(nil), f.built...)
}

func (f *fakeRegistrarFactory) closedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.built {
		if r.closed {
			n++
		}
	}
	return n
}

// ── harness ───────────────────────────────────────────────────────────────────

// consumerNames mirrors everything that holds mTLS material in a real client.
// Rotation must reach all of them, not the two the control plane happens to use.
var consumerNames = []string{
	"call-server", "outbound-rpc", "outbound-http",
	"events", "workflows", "jobs", "telemetry", "logs",
}

type harness struct {
	t          *testing.T
	ca         *testCA
	control    *fakeControl
	dialer     *bufDialer
	prov       *fakeProvisioner
	creds      *connection.CredentialRegistry
	consumers  map[string]*recordConsumer
	observer   *recordObserver
	inbound    *fakeInbound
	registrars *fakeRegistrarFactory
	life       *connection.Lifecycle
}

func newHarness(t *testing.T, tweak func(cfg *connection.LifecycleConfig)) *harness {
	t.Helper()

	ca := newTestCA(t)
	control := newFakeControl()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterControlServer(srv, control)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	h := &harness{
		t:          t,
		ca:         ca,
		control:    control,
		dialer:     &bufDialer{lis: lis},
		prov:       &fakeProvisioner{t: t, ca: ca, notAfter: time.Now().Add(24 * time.Hour)},
		creds:      connection.NewCredentialRegistry(),
		consumers:  make(map[string]*recordConsumer, len(consumerNames)),
		observer:   &recordObserver{},
		inbound:    &fakeInbound{},
		registrars: &fakeRegistrarFactory{},
	}

	ctx := context.Background()
	for _, name := range consumerNames {
		c := &recordConsumer{name: name}
		h.consumers[name] = c
		if err := h.creds.Register(ctx, name, c); err != nil {
			t.Fatalf("register consumer %s: %v", name, err)
		}
	}

	cfg := connection.LifecycleConfig{
		Addr:        "runtime.test:14445",
		CACert:      ca.cert,
		Provisioner: h.prov,
		Dialer:      h.dialer,
		Credentials: h.creds,
		Inbound:     h.inbound,
		Registrars:  h.registrars,
		Observer:    h.observer,
		// A flat, jitter-free ladder keeps the reconnect tests fast and exact.
		Backoff:        stream.NewBackoff(stream.WithLadder(20*time.Millisecond), stream.WithJitterRatio(0)),
		WelcomeTimeout: 300 * time.Millisecond,
		RotateLead:     30 * time.Minute,
		RotateJitter:   0,
		Random:         func() float64 { return 0 },
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if tweak != nil {
		tweak(&cfg)
	}

	life, err := connection.NewLifecycle(cfg)
	if err != nil {
		t.Fatalf("NewLifecycle: %v", err)
	}
	h.life = life
	return h
}

func (h *harness) start() {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.life.Start(ctx); err != nil {
		h.t.Fatalf("Start: %v", err)
	}
	h.t.Cleanup(h.stop)
}

func (h *harness) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.life.Stop(ctx); err != nil {
		h.t.Errorf("Stop: %v", err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// leafOf returns the leaf certificate a consumer was last handed.
func leafOf(t *testing.T, c *recordConsumer) *x509.Certificate {
	t.Helper()
	creds, ok := c.last()
	if !ok {
		t.Fatalf("consumer %s never received credentials", c.name)
	}
	if creds.TLS == nil || len(creds.TLS.Certificates) != 1 {
		t.Fatalf("consumer %s got a TLS config without exactly one client certificate", c.name)
	}
	leaf := creds.TLS.Certificates[0].Leaf
	if leaf == nil {
		t.Fatalf("consumer %s got a certificate without a parsed leaf", c.name)
	}
	return leaf
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestSessionWelcomeBringsTheClientUp(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.start()

	ids := h.observer.connectedIDs()
	if len(ids) != 1 {
		t.Fatalf("Connected fired %d times, want 1", len(ids))
	}
	if ids[0].SessionID != "session-1" || ids[0].ServiceID != testServiceID || ids[0].ServiceName != testServiceName {
		t.Errorf("session identity from Welcome: %+v", ids[0])
	}
	if ids[0].InstanceID != "provisioned-1" {
		t.Errorf("instanceID: got %q want the one in the lease", ids[0].InstanceID)
	}
	if got := h.life.Identity(); got != ids[0] {
		t.Errorf("Identity(): got %+v want %+v", got, ids[0])
	}
	if _, err := h.life.Conn(); err != nil {
		t.Errorf("Conn on a live session: %v", err)
	}

	starts, _ := h.inbound.counts()
	if starts != 1 {
		t.Errorf("inbound Start called %d times, want 1", starts)
	}
	registrars := h.registrars.registrars()
	if len(registrars) != 1 || registrars[0].endpoint != testEndpoint {
		t.Fatalf("registrar: %d built, endpoint %q", len(registrars), registrars[0].endpoint)
	}

	for _, name := range consumerNames {
		if got := h.consumers[name].count(); got != 1 {
			t.Errorf("consumer %s got %d credential updates, want 1", name, got)
		}
	}
}

func TestSessionWithoutWelcomeIsNeverAccepted(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.control.mu.Lock()
	h.control.welcomes = 0
	h.control.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := h.life.Start(ctx)
	if err == nil {
		t.Fatal("Start accepted a session that was never welcomed")
	}
	if !errors.Is(err, connection.ErrSession) {
		t.Errorf("kind: got %v want ErrSession", err)
	}

	if _, err := h.life.Conn(); err == nil {
		t.Error("an unwelcomed session was adopted")
	}
	for _, name := range consumerNames {
		if got := h.consumers[name].count(); got != 0 {
			t.Errorf("consumer %s was handed credentials of an unproven session (%d times)", name, got)
		}
	}
	conns, _ := h.dialer.dialed()
	if len(conns) != 1 {
		t.Fatalf("dialed %d channels, want 1", len(conns))
	}
	assertClosed(t, conns[0], "unaccepted channel")
}

func TestSessionDrainIsReportedAndTheStreamStaysLive(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.start()

	call := h.control.awaitOpen(t)
	call.drain <- "runtime shutting down"

	waitFor(t, "Draining", func() bool { return len(h.observer.drainReasons()) == 1 })
	if got := h.observer.drainReasons()[0]; got != "runtime shutting down" {
		t.Errorf("drain reason: %q", got)
	}

	// Drain announces the shutdown; the stream itself is still the liveness
	// signal, so nothing reconnects until it actually ends.
	if h.control.openCount() != 1 {
		t.Errorf("Control.Open called %d times after a drain, want 1", h.control.openCount())
	}
	if _, err := h.life.Conn(); err != nil {
		t.Errorf("session died on a drain notice: %v", err)
	}
}

func TestSessionToleratesASecondWelcome(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(*connection.LifecycleConfig) {})
	h.control.mu.Lock()
	h.control.welcomes = 2
	h.control.mu.Unlock()
	h.start()

	// A second Welcome breaks the protocol but must not break the session.
	time.Sleep(100 * time.Millisecond)
	if got := len(h.observer.connectedIDs()); got != 1 {
		t.Errorf("Connected fired %d times for one session", got)
	}
	if _, err := h.life.Conn(); err != nil {
		t.Errorf("session died on an extra Welcome: %v", err)
	}
}

func TestStopClosesEverythingTheSessionOwns(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.life.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	call := h.control.awaitOpen(t)

	if err := h.life.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case <-call.ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the control stream outlived Stop")
	}
	if h.registrars.closedCount() != 1 {
		t.Errorf("registrar closed %d times, want 1", h.registrars.closedCount())
	}
	if _, closes := h.inbound.counts(); closes != 1 {
		t.Errorf("inbound closed %d times, want 1", closes)
	}
	conns, _ := h.dialer.dialed()
	for i, conn := range conns {
		assertClosed(t, conn, fmt.Sprintf("channel %d", i))
	}
	if err := h.life.Stop(ctx); err != nil {
		t.Errorf("second Stop: %v", err)
	}
}
