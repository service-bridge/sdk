package connection_test

import (
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"sync"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ── fake Control.RefreshCert ──────────────────────────────────────────────────

// refreshRuntime issues renewed leaves the way the runtime does: same service,
// same instance, a new key and a new expiry.
type refreshRuntime struct {
	t  *testing.T
	ca *testCA

	mu       sync.Mutex
	notAfter time.Time
	err      error
	// errOnce fails exactly the next refresh, then clears itself.
	errOnce  error
	instance string
	issued   []string
}

func (r *refreshRuntime) handle(_ int, req *pb.RefreshCertRequest) (*pb.RefreshCertResponse, error) {
	r.mu.Lock()
	notAfter, failWith, instanceID := r.notAfter, r.err, r.instance
	if r.errOnce != nil {
		failWith, r.errOnce = r.errOnce, nil
	}
	r.mu.Unlock()

	if failWith != nil {
		return nil, failWith
	}

	csr, err := x509.ParseCertificateRequest(req.GetCsrDer())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "csr parse: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "csr signature: %v", err)
	}
	pub, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "csr key is %T, want ECDSA", csr.PublicKey)
	}
	if instanceID == "" {
		instanceID = "provisioned-1"
	}
	der := r.ca.issueLeaf(r.t, pub, connection.Identity{ServiceID: testServiceID, InstanceID: instanceID})

	r.mu.Lock()
	r.issued = append(r.issued, instanceID)
	r.mu.Unlock()

	return &pb.RefreshCertResponse{
		CertDer:        der,
		CaChainDer:     r.ca.der,
		InstanceId:     instanceID,
		NotAfterUnixMs: notAfter.UnixMilli(),
	}, nil
}

func (r *refreshRuntime) setError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

// withRefresh arms Control.RefreshCert on the harness runtime.
func (h *harness) withRefresh(notAfter time.Time) *refreshRuntime {
	h.t.Helper()
	rr := &refreshRuntime{t: h.t, ca: h.ca, notAfter: notAfter.Truncate(time.Second)}
	h.control.mu.Lock()
	h.control.refresh = rr.handle
	h.control.mu.Unlock()
	return rr
}

// instanceOf reads the instanceID out of the SPIFFE SAN of a leaf.
func instanceOf(t *testing.T, leaf *x509.Certificate) string {
	t.Helper()
	if len(leaf.URIs) != 1 {
		t.Fatalf("leaf carries %d URI SANs, want 1", len(leaf.URIs))
	}
	id, err := connection.ParseSPIFFE(leaf.URIs[0].String())
	if err != nil {
		t.Fatalf("ParseSPIFFE: %v", err)
	}
	return id.InstanceID
}

// presented reads the leaf a TLS configuration hands out on its next handshake.
func presented(t *testing.T, creds connection.Credentials) *x509.Certificate {
	t.Helper()
	cert, err := creds.TLS.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatalf("client certificate: %v", err)
	}
	return cert.Leaf
}

// ── tests ─────────────────────────────────────────────────────────────────────

// A renewal swaps the TLS material and nothing else: the control session, its
// channel and the identity stay. The runtime treats a second Control.Open of
// the same instance as a replacement and aborts the first one, so reopening
// would cut the very session the renewal was meant to keep alive.
func TestRotationKeepsTheSessionAndOnlySwapsTheLeaf(t *testing.T) {
	t.Parallel()
	notAfter := time.Now().Add(24 * time.Hour)
	h := newHarness(t, nil)
	h.withRefresh(notAfter)
	h.start()

	first := h.control.awaitOpen(t)
	oldConn := mustConn(t, h.life)
	_, dialed := h.dialer.dialed()
	before := presented(t, dialed[0])

	h.life.Rotate()
	waitFor(t, "the renewal", func() bool { return h.consumers["call-server"].count() == 2 })

	if got := h.control.openCount(); got != 1 {
		t.Errorf("Control.Open called %d times, want 1: a renewal must not reopen the session", got)
	}
	if current, err := h.life.Conn(); err != nil || current != oldConn {
		t.Errorf("the session channel was replaced by a renewal (err %v)", err)
	}
	select {
	case <-first.ended:
		t.Fatal("the control stream was closed by a renewal")
	default:
	}
	assertLive(t, oldConn, "the session channel")
	if got := h.life.Identity().InstanceID; got != "provisioned-1" {
		t.Errorf("instance after renewal: %q want provisioned-1", got)
	}
	// The configuration the session was dialled with now presents the renewed
	// leaf: the next handshake of every channel picks it up.
	after := presented(t, dialed[0])
	if after.Equal(before) {
		t.Fatal("the dialled TLS configuration still presents the pre-renewal leaf")
	}
	if want := notAfter.Truncate(time.Second); !after.NotAfter.Equal(want.UTC()) && after.NotAfter.Before(before.NotAfter) {
		t.Errorf("presented leaf expires %s, want the renewed one", after.NotAfter)
	}
	if len(h.observer.connectedIDs()) != 1 {
		t.Errorf("a renewal reported a new connection")
	}
}

func TestRotationUpdatesEveryCredentialConsumer(t *testing.T) {
	t.Parallel()
	notAfter := time.Now().Add(12 * time.Hour).Truncate(time.Second)
	h := newHarness(t, nil)
	h.withRefresh(notAfter)
	h.start()

	h.life.Rotate()
	waitFor(t, "the renewal", func() bool { return h.consumers["call-server"].count() == 2 })

	for _, name := range consumerNames {
		consumer := h.consumers[name]
		if got := consumer.count(); got != 2 {
			t.Errorf("consumer %s got %d credential updates, want 2", name, got)
			continue
		}
		creds, _ := consumer.last()
		if creds.Lease.NotAfterUnixMs != notAfter.UnixMilli() {
			t.Errorf("consumer %s got expiry %d, want %d", name, creds.Lease.NotAfterUnixMs, notAfter.UnixMilli())
		}
		if got := instanceOf(t, creds.Lease.TLSCert.Leaf); got != "provisioned-1" {
			t.Errorf("consumer %s holds a leaf of instance %q", name, got)
		}
	}

	late := &recordConsumer{name: "late"}
	if err := h.creds.Register(t.Context(), "late", late); err != nil {
		t.Fatalf("Register after a rotation: %v", err)
	}
	if creds, _ := late.last(); creds.Lease.NotAfterUnixMs != notAfter.UnixMilli() {
		t.Errorf("late consumer got expiry %d", creds.Lease.NotAfterUnixMs)
	}
}

// The renewed leaf is the cached one: a later reconnect reuses it instead of
// re-running the argon2id provisioning on the runtime.
func TestReconnectAfterRotationReusesTheRenewedLeaf(t *testing.T) {
	t.Parallel()
	notAfter := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	h := newHarness(t, nil)
	h.withRefresh(notAfter)
	h.start()

	first := h.control.awaitOpen(t)
	h.life.Rotate()
	waitFor(t, "the renewal", func() bool { return h.consumers["call-server"].count() == 2 })

	first.die(t, status.Error(codes.Unavailable, "transport closing"))
	waitFor(t, "the reconnected session", func() bool { return len(h.observer.connectedIDs()) == 2 })

	if got := h.prov.count(); got != 1 {
		t.Errorf("Provision called %d times, want 1", got)
	}
	_, creds := h.dialer.dialed()
	last := creds[len(creds)-1]
	if last.Lease.NotAfterUnixMs != notAfter.UnixMilli() {
		t.Errorf("the reconnect dialled with expiry %d, want the renewed %d", last.Lease.NotAfterUnixMs, notAfter.UnixMilli())
	}
}

func TestRotationRejectsALeafForAnotherInstance(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	rr := h.withRefresh(time.Now().Add(24 * time.Hour))
	rr.mu.Lock()
	rr.instance = "someone-else"
	rr.mu.Unlock()
	h.start()

	h.life.Rotate()
	waitFor(t, "the rejected renewal", func() bool { return h.control.refreshCount() >= 1 })

	time.Sleep(100 * time.Millisecond)
	for _, name := range consumerNames {
		if got := h.consumers[name].count(); got != 1 {
			t.Errorf("consumer %s adopted a leaf for another instance (%d updates)", name, got)
		}
	}
	if got := h.life.Identity().InstanceID; got != "provisioned-1" {
		t.Errorf("identity: %q", got)
	}
}

// RefreshCert is rate limited on the runtime. ResourceExhausted is not a
// reason to give up: the renewal is retried after the retry interval.
func TestRateLimitedRefreshIsRetried(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(cfg *connection.LifecycleConfig) {
		cfg.RotateRetry = 50 * time.Millisecond
	})
	rr := h.withRefresh(time.Now().Add(24 * time.Hour))
	rr.mu.Lock()
	rr.errOnce = status.Error(codes.ResourceExhausted, "refresh rate limited")
	rr.mu.Unlock()
	h.start()

	h.life.Rotate()
	waitFor(t, "the retried renewal", func() bool { return h.consumers["call-server"].count() == 2 })
	if got := h.control.refreshCount(); got != 2 {
		t.Errorf("RefreshCert called %d times, want 2", got)
	}
	if len(h.observer.disconnects()) != 0 {
		t.Error("a rate-limited renewal stopped the lifecycle")
	}
}

func TestNonRetryableRefreshCodeStopsTheLifecycle(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	rr := h.withRefresh(time.Now().Add(24 * time.Hour))
	rr.setError(status.Error(codes.Unauthenticated, "certificate revoked"))
	h.start()

	h.life.Rotate()
	waitFor(t, "Disconnected", func() bool { return len(h.observer.disconnects()) == 1 })
	if code := status.Code(h.observer.disconnects()[0]); code != codes.Unauthenticated {
		t.Errorf("cause code: got %s want Unauthenticated", code)
	}
	if got := h.control.refreshCount(); got != 1 {
		t.Errorf("RefreshCert retried %d times after a terminal code", got)
	}
}

func TestRenewalIsScheduledAheadOfExpiry(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(cfg *connection.LifecycleConfig) {
		cfg.RotateLead = time.Hour - 100*time.Millisecond
		cfg.MinRotateDelay = time.Millisecond
	})
	h.prov.setNotAfter(time.Now().Add(time.Hour))
	h.withRefresh(time.Now().Add(24 * time.Hour))
	h.start()

	waitFor(t, "the scheduled renewal", func() bool { return h.control.refreshCount() == 1 })
	waitFor(t, "the renewed leaf", func() bool { return h.consumers["call-server"].count() == 2 })
	if got := h.control.openCount(); got != 1 {
		t.Errorf("Control.Open called %d times, want 1", got)
	}
}
