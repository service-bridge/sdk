package connection_test

import (
	"context"
	"crypto/x509"
	"errors"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewLifecycleRejectsAnIncompleteConfig(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	prov := &fakeProvisioner{t: t, ca: ca, notAfter: time.Now().Add(time.Hour)}

	cases := map[string]connection.LifecycleConfig{
		"no address":     {CACert: ca.cert, Provisioner: prov},
		"no pinned CA":   {Addr: "runtime:14445", Provisioner: prov},
		"no provisioner": {Addr: "runtime:14445", CACert: ca.cert},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := connection.NewLifecycle(cfg); !errors.Is(err, connection.ErrSession) {
				t.Fatalf("got %v, want ErrSession", err)
			}
		})
	}
}

func TestLifecycleRunsCallerOnlyWithoutAnObserver(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(cfg *connection.LifecycleConfig) {
		// Caller-only: no inbound listener, no registry stream, no observer.
		cfg.Inbound = nil
		cfg.Registrars = nil
		cfg.Observer = nil
	})
	h.start()

	if _, err := h.life.Conn(); err != nil {
		t.Fatalf("caller-only session: %v", err)
	}
	if got := h.life.Identity().InstanceID; got != "provisioned-1" {
		t.Errorf("identity: %q", got)
	}
	if len(h.registrars.registrars()) != 0 {
		t.Error("a caller-only instance registered handlers")
	}
	if h.life.Credentials() != h.creds {
		t.Error("Credentials() does not expose the registry consumers registered with")
	}

	call := h.control.awaitOpen(t)
	call.drain <- "runtime shutting down"
	call.die(t, status.Error(codes.Unavailable, "runtime restarted"))
	waitFor(t, "the reconnect", func() bool { return h.control.openCount() == 2 })
}

func TestSessionSurvivesAnUnknownControlFrame(t *testing.T) {
	t.Parallel()
	h := newHarness(t, nil)
	h.start()

	call := h.control.awaitOpen(t)
	call.junk <- struct{}{}

	time.Sleep(100 * time.Millisecond)
	if _, err := h.life.Conn(); err != nil {
		t.Errorf("an unknown control frame killed the session: %v", err)
	}
	if got := h.control.openCount(); got != 1 {
		t.Errorf("Control.Open called %d times, want 1", got)
	}
}

func TestCredentialRegistryPublishesToLateAndBrokenConsumers(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	registry := connection.NewCredentialRegistry()

	if _, ok := registry.Current(); ok {
		t.Error("an empty registry reports published credentials")
	}
	if err := registry.Register(ctx, "nil-consumer", nil); !errors.Is(err, connection.ErrSession) {
		t.Errorf("registering a nil consumer: got %v want ErrSession", err)
	}

	broken := &recordConsumer{name: "broken"}
	broken.setFail(errors.New("client is closed"))
	healthy := &recordConsumer{name: "healthy"}
	for _, c := range []*recordConsumer{broken, healthy} {
		if err := registry.Register(ctx, c.name, c); err != nil {
			t.Fatalf("Register %s: %v", c.name, err)
		}
	}

	creds := connection.Credentials{Addr: "runtime:14445"}
	err := registry.Update(ctx, creds)
	if !errors.Is(err, connection.ErrSession) {
		t.Errorf("Update with a broken consumer: got %v want ErrSession", err)
	}
	// Every consumer is attempted even after one fails: a single broken client
	// must not leave the rest pinned to the previous certificate.
	if healthy.count() != 1 {
		t.Error("a healthy consumer was skipped after another one failed")
	}
	if got, ok := registry.Current(); !ok || got.Addr != creds.Addr {
		t.Errorf("Current: %+v ok=%v", got, ok)
	}

	// A consumer that rejects the credentials it is handed at registration says
	// so, instead of silently starting out with nothing.
	late := &recordConsumer{name: "late"}
	late.setFail(errors.New("client is closed"))
	if err := registry.Register(ctx, "late", late); !errors.Is(err, connection.ErrSession) {
		t.Errorf("Register a rejecting consumer: got %v want ErrSession", err)
	}
}

func TestMTLSDialerBuildsAPinnedChannel(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	lease := newLease(t, ca, "instance-1", time.Now().Add(time.Hour))
	creds := connection.Credentials{
		Addr:  "127.0.0.1:14445",
		Lease: lease,
		TLS:   connection.MutualTLSConfig(ca.cert, lease.TLSCert),
	}

	conn, err := (connection.MTLSDialer{}).Dial(context.Background(), creds)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	creds.Addr = "\x00not a target"
	if _, err := (connection.MTLSDialer{}).Dial(context.Background(), creds); !errors.Is(err, connection.ErrSession) {
		t.Errorf("dialling a malformed address: got %v want ErrSession", err)
	}
}

func TestBootstrapProvisionerReportsProvisionFailures(t *testing.T) {
	t.Parallel()
	p := connection.BootstrapProvisioner{Addr: "not-a-host-port"}
	if _, err := p.Provision(context.Background()); !errors.Is(err, connection.ErrProvision) {
		t.Errorf("got %v want ErrProvision", err)
	}
}

func TestRefreshRejectsAContradictoryResponse(t *testing.T) {
	t.Parallel()

	cases := map[string]func(t *testing.T, rr *refreshRuntime, resp *pb.RefreshCertResponse, req *pb.RefreshCertRequest){
		"garbage certificate": func(_ *testing.T, _ *refreshRuntime, resp *pb.RefreshCertResponse, _ *pb.RefreshCertRequest) {
			resp.CertDer = []byte("not a certificate")
		},
		"instance_id contradicting the SPIFFE SAN": func(_ *testing.T, _ *refreshRuntime, resp *pb.RefreshCertResponse, _ *pb.RefreshCertRequest) {
			resp.InstanceId = "someone-else"
		},
		"leaf issued for another service": func(t *testing.T, rr *refreshRuntime, resp *pb.RefreshCertResponse, req *pb.RefreshCertRequest) {
			csr, err := x509.ParseCertificateRequest(req.GetCsrDer())
			if err != nil {
				t.Fatalf("parse CSR: %v", err)
			}
			foreign := connection.Identity{
				ServiceID:  "0f0f0f0f-0000-4000-8000-000000000000",
				InstanceID: "rotated-1",
			}
			resp.CertDer = rr.ca.issueLeaf(t, csr.PublicKey, foreign)
			resp.InstanceId = foreign.InstanceID
		},
		"leaf without a SPIFFE SAN": func(t *testing.T, rr *refreshRuntime, resp *pb.RefreshCertResponse, _ *pb.RefreshCertRequest) {
			resp.CertDer = rr.ca.issueServerCert(t).Certificate[0]
		},
	}

	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, nil)
			rr := h.withRefresh(time.Now().Add(24 * time.Hour))
			inner := rr.handle
			h.control.mu.Lock()
			h.control.refresh = func(n int, req *pb.RefreshCertRequest) (*pb.RefreshCertResponse, error) {
				resp, err := inner(n, req)
				if err != nil {
					return nil, err
				}
				corrupt(t, rr, resp, req)
				return resp, nil
			}
			h.control.mu.Unlock()
			h.start()

			h.life.Rotate()
			waitFor(t, "the rejected renewal", func() bool { return h.control.refreshCount() >= 1 })

			// A renewal the SDK cannot trust never becomes a session.
			time.Sleep(100 * time.Millisecond)
			if got := h.control.openCount(); got != 1 {
				t.Errorf("a contradictory renewal opened %d sessions, want 1", got)
			}
			if got := h.life.Identity(); got.InstanceID != "provisioned-1" || got.ServiceID != testServiceID {
				t.Errorf("identity after a rejected renewal: %+v", got)
			}
		})
	}
}
