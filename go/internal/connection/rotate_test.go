package connection_test

import (
	"crypto/ecdsa"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
)

// ── fake Control.RefreshCert ──────────────────────────────────────────────────

// refreshRuntime issues renewed leaves the way the runtime does: same serviceID,
// a brand-new instanceID, expiry in SECONDS.
type refreshRuntime struct {
	t  *testing.T
	ca *testCA

	mu           sync.Mutex
	notAfter     time.Time
	err          error
	sameInstance string
	issued       []string
}

func (r *refreshRuntime) handle(n int, req *pb.RefreshCertRequest) (*pb.RefreshCertResponse, error) {
	r.mu.Lock()
	notAfter, failWith, same := r.notAfter, r.err, r.sameInstance
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

	instanceID := same
	if instanceID == "" {
		instanceID = fmt.Sprintf("rotated-%d", n)
	}
	der := r.ca.issueLeaf(r.t, pub, connection.Identity{ServiceID: testServiceID, InstanceID: instanceID})

	r.mu.Lock()
	r.issued = append(r.issued, instanceID)
	r.mu.Unlock()

	return &pb.RefreshCertResponse{
		CertDer:    der,
		CaChainDer: r.ca.der,
		InstanceId: instanceID,
		// not_after_unix is seconds, unlike the unix-ms of every other wire time
		// field (ADR-0006).
		NotAfterUnix: notAfter.Unix(),
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

// ── tests ─────────────────────────────────────────────────────────────────────

func TestRotationSwapsTheSessionOnlyAfterWelcome(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(cfg *connection.LifecycleConfig) {
		cfg.WelcomeTimeout = 10 * time.Second
	})
	h.withRefresh(time.Now().Add(24 * time.Hour))
	gate := h.control.holdWelcome(2)
	h.start()

	first := h.control.awaitOpen(t)
	oldConn := mustConn(t, h.life)

	h.life.Rotate()
	h.control.awaitOpen(t) // the renewed session is dialled and parked

	// While the new session is unproven the old one keeps serving: no swap, no
	// credential update, no teardown.
	time.Sleep(100 * time.Millisecond)
	if got := h.life.Identity().InstanceID; got != "provisioned-1" {
		t.Errorf("identity swapped before Welcome: %q", got)
	}
	if current, err := h.life.Conn(); err != nil || current != oldConn {
		t.Errorf("channel swapped before Welcome (err %v)", err)
	}
	for _, name := range consumerNames {
		if got := h.consumers[name].count(); got != 1 {
			t.Errorf("consumer %s got %d credential updates before Welcome, want 1", name, got)
		}
	}
	select {
	case <-first.ended:
		t.Fatal("the old stream was closed before the new session was welcomed")
	default:
	}

	close(gate)

	waitFor(t, "the renewed session", func() bool { return len(h.observer.connectedIDs()) == 2 })
	if got := h.life.Identity().InstanceID; got != "rotated-1" {
		t.Errorf("identity after rotation: %q want rotated-1", got)
	}
	select {
	case <-first.ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the old stream outlived the accepted rotation")
	}
	assertClosed(t, oldConn, "the channel of the replaced session")
}

// The regression the whole design exists for: after a successful rotation the
// new session must be supervised exactly like the first one. In the Node SDK the
// rotated session was watched by a callback that had already been resolved, so
// its death was silent and the service lost its control plane for good.
func TestStreamDeathAfterRotationReconnects(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.withRefresh(time.Now().Add(24 * time.Hour))
	h.start()

	h.control.awaitOpen(t)
	h.life.Rotate()
	waitFor(t, "the renewed session", func() bool { return len(h.observer.connectedIDs()) == 2 })

	rotated := h.control.awaitOpen(t)
	if rotated.n != 2 {
		t.Fatalf("expected the rotated session to be Control.Open #2, got #%d", rotated.n)
	}
	rotated.die(t, status.Error(codes.Unavailable, "runtime restarted"))

	waitFor(t, "a session after the rotated one died", func() bool {
		return len(h.observer.connectedIDs()) == 3
	})
	if h.observer.reconnectCount() == 0 {
		t.Error("the death of a rotated session did not report a reconnect")
	}
	if got := h.control.openCount(); got != 3 {
		t.Errorf("Control.Open called %d times, want 3", got)
	}
}

// Regression on the second Node bug: only two of eight credential holders were
// switched, so RPC, events, workflow checkpoints, job results and telemetry all
// broke together the moment the first certificate expired.
func TestRotationUpdatesEveryCredentialConsumer(t *testing.T) {
	t.Parallel()
	notAfter := time.Now().Add(12 * time.Hour)
	h := newHarness(t, nil)
	h.withRefresh(notAfter)
	h.start()

	h.life.Rotate()
	waitFor(t, "the renewed session", func() bool { return len(h.observer.connectedIDs()) == 2 })

	for _, name := range consumerNames {
		consumer := h.consumers[name]
		if got := consumer.count(); got != 2 {
			t.Errorf("consumer %s got %d credential updates, want 2", name, got)
			continue
		}
		if got := instanceOf(t, leafOf(t, consumer)); got != "rotated-1" {
			t.Errorf("consumer %s still holds the certificate of instance %q", name, got)
		}
		creds, _ := consumer.last()
		if want := notAfter.Truncate(time.Second).UTC(); !creds.Lease.NotAfter.Equal(want) {
			t.Errorf("consumer %s got expiry %s, want %s: not_after_unix is seconds",
				name, creds.Lease.NotAfter, want)
		}
	}

	// A consumer registered after the rotation must not start out holding nothing.
	late := &recordConsumer{name: "late"}
	if err := h.creds.Register(t.Context(), "late", late); err != nil {
		t.Fatalf("Register after a rotation: %v", err)
	}
	if got := instanceOf(t, leafOf(t, late)); got != "rotated-1" {
		t.Errorf("late consumer got the certificate of instance %q", got)
	}
}

// Regression on the third Node bug: the certificate cache kept the pre-rotation
// leaf, so every later reconnect re-ran the 64 MiB argon2id hash on the runtime.
func TestRotationRefreshesTheLeafCache(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.withRefresh(time.Now().Add(24 * time.Hour))
	h.start()

	h.control.awaitOpen(t)
	h.life.Rotate()
	waitFor(t, "the renewed session", func() bool { return len(h.observer.connectedIDs()) == 2 })

	rotated := h.control.awaitOpen(t)
	rotated.die(t, status.Error(codes.Unavailable, "transport closing"))
	waitFor(t, "the reconnected session", func() bool { return len(h.observer.connectedIDs()) == 3 })

	if got := h.prov.count(); got != 1 {
		t.Errorf("Provision called %d times, want 1: the reconnect must reuse the rotated leaf", got)
	}
	_, creds := h.dialer.dialed()
	last := creds[len(creds)-1]
	if got := instanceOf(t, last.Lease.TLSCert.Leaf); got != "rotated-1" {
		t.Errorf("the reconnect dialled with the certificate of instance %q, want rotated-1", got)
	}
	if got := h.life.Identity().InstanceID; got != "rotated-1" {
		t.Errorf("identity after the reconnect: %q", got)
	}
}

func TestRotationRollsBackWhenTheNewSessionIsNeverWelcomed(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(cfg *connection.LifecycleConfig) {
		// One rotation attempt: the retry rung is longer than the test.
		cfg.Backoff = stream.NewBackoff(stream.WithLadder(30*time.Second), stream.WithJitterRatio(0))
	})
	h.withRefresh(time.Now().Add(24 * time.Hour))
	h.control.holdWelcome(2) // never released
	h.start()

	first := h.control.awaitOpen(t)
	oldConn := mustConn(t, h.life)

	h.life.Rotate()
	h.control.awaitOpen(t)

	// The rotation fails on the Welcome timeout. Its channel goes with it and
	// the old session keeps serving.
	waitFor(t, "the rejected rotation channel to close", func() bool {
		conns, _ := h.dialer.dialed()
		return len(conns) == 2 && conns[1].GetState() == connectivity.Shutdown
	})

	if got := h.life.Identity().InstanceID; got != "provisioned-1" {
		t.Errorf("identity after a failed rotation: %q want the pre-rotation one", got)
	}
	if current, err := h.life.Conn(); err != nil || current != oldConn {
		t.Errorf("the old session did not survive a failed rotation (err %v)", err)
	}
	select {
	case <-first.ended:
		t.Fatal("the old stream was torn down by a failed rotation")
	default:
	}
	assertLive(t, oldConn, "the channel of the surviving session")
	for _, name := range consumerNames {
		if got := h.consumers[name].count(); got != 1 {
			t.Errorf("consumer %s was handed the credentials of an unproven session (%d updates)", name, got)
		}
	}
}

func TestRotationRollsBackWhenAConsumerRejectsTheCredentials(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(cfg *connection.LifecycleConfig) {
		cfg.Backoff = stream.NewBackoff(stream.WithLadder(30*time.Second), stream.WithJitterRatio(0))
	})
	h.withRefresh(time.Now().Add(24 * time.Hour))
	h.start()

	first := h.control.awaitOpen(t)
	oldConn := mustConn(t, h.life)
	h.consumers["telemetry"].setFail(errors.New("telemetry client is closed"))

	h.life.Rotate()

	// Half the consumers on the new certificate and half on the old one is the
	// state the registry exists to prevent: the whole swap is undone.
	waitFor(t, "the rejected rotation channel to close", func() bool {
		conns, _ := h.dialer.dialed()
		return len(conns) == 2 && conns[1].GetState() == connectivity.Shutdown
	})

	if got := h.life.Identity().InstanceID; got != "provisioned-1" {
		t.Errorf("identity after a rejected credential update: %q", got)
	}
	if current, err := h.life.Conn(); err != nil || current != oldConn {
		t.Errorf("the old session did not survive a rejected credential update (err %v)", err)
	}
	select {
	case <-first.ended:
		t.Fatal("the old stream was torn down after a rejected credential update")
	default:
	}
	if got := h.consumers["call-server"].count(); got != 2 {
		t.Errorf("the healthy consumers were not attempted: %d updates", got)
	}
}

func TestRotationRejectsAReusedInstanceID(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	rr := h.withRefresh(time.Now().Add(24 * time.Hour))
	rr.mu.Lock()
	// A runtime that reissues the same instanceID makes the overlapping sessions
	// indistinguishable: closing the old one would mark the new one disconnected.
	rr.sameInstance = "provisioned-1"
	rr.mu.Unlock()
	h.start()

	h.life.Rotate()
	waitFor(t, "the rejected renewal", func() bool { return h.control.refreshCount() >= 1 })

	time.Sleep(100 * time.Millisecond)
	if got := h.control.openCount(); got != 1 {
		t.Errorf("a leaf with a reused instanceID opened %d sessions, want 1", got)
	}
	if got := h.life.Identity().InstanceID; got != "provisioned-1" {
		t.Errorf("identity: %q", got)
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
		// The provisioned leaf lives an hour; renewal is due 100ms in.
		cfg.RotateLead = time.Hour - 100*time.Millisecond
		cfg.MinRotateDelay = time.Millisecond
	})
	h.prov.setNotAfter(time.Now().Add(time.Hour))
	// The renewed leaf lives long enough that the next renewal is a day away.
	h.withRefresh(time.Now().Add(24 * time.Hour))
	h.start()

	waitFor(t, "the scheduled renewal", func() bool { return len(h.observer.connectedIDs()) == 2 })
	if got := h.control.refreshCount(); got != 1 {
		t.Errorf("RefreshCert called %d times, want 1", got)
	}
	if got := h.life.Identity().InstanceID; got != "rotated-1" {
		t.Errorf("identity after the scheduled renewal: %q", got)
	}
}
