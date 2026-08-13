package connection_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// ── fake runtime ──────────────────────────────────────────────────────────────

type fakeBootstrap struct {
	pb.UnimplementedBootstrapServer

	ca         *testCA
	t          *testing.T
	failWith   error
	notAfter   time.Time
	serviceID  string
	instanceID string
	svcName    string

	mu   sync.Mutex
	seen *pb.ProvisionRequest
}

func (f *fakeBootstrap) Provision(_ context.Context, req *pb.ProvisionRequest) (*pb.ProvisionResponse, error) {
	f.mu.Lock()
	f.seen = req
	f.mu.Unlock()

	if f.failWith != nil {
		return nil, f.failWith
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
	if pub.Curve != elliptic.P256() {
		return nil, status.Error(codes.InvalidArgument, "csr key is not P-256")
	}

	certDER := f.ca.issueLeaf(f.t, pub, connection.Identity{ServiceID: f.serviceID, InstanceID: f.instanceID})
	return &pb.ProvisionResponse{
		CertDer:      certDER,
		CaChainDer:   f.ca.der,
		ServiceId:    f.serviceID,
		ServiceName:  f.svcName,
		InstanceId:   f.instanceID,
		NotAfterUnix: f.notAfter.Unix(),
	}, nil
}

func (f *fakeBootstrap) request() *pb.ProvisionRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seen
}

// connCounter counts transport connections so a test can prove the single-use
// bootstrap channel was closed.
type connCounter struct {
	mu    sync.Mutex
	begun int
	ended int
}

func (c *connCounter) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (c *connCounter) HandleRPC(context.Context, stats.RPCStats)                         {}
func (c *connCounter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }

func (c *connCounter) HandleConn(_ context.Context, s stats.ConnStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch s.(type) {
	case *stats.ConnBegin:
		c.begun++
	case *stats.ConnEnd:
		c.ended++
	}
}

func (c *connCounter) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.begun, c.ended
}

func (c *connCounter) waitClosed(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		begun, ended := c.counts()
		if begun > 0 && ended >= begun {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	begun, ended := c.counts()
	t.Fatalf("bootstrap channel stayed open: %d connections begun, %d ended", begun, ended)
}

func serveFake(t *testing.T, ca *testCA, svc *fakeBootstrap) (string, *connCounter) {
	t.Helper()
	counter := &connCounter{}
	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(ca.serverConfig(t))),
		grpc.StatsHandler(counter),
	)
	pb.RegisterBootstrapServer(srv, svc)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	return ln.Addr().String(), counter
}

func newFake(t *testing.T, ca *testCA) *fakeBootstrap {
	t.Helper()
	return &fakeBootstrap{
		ca:         ca,
		t:          t,
		notAfter:   time.Now().Add(time.Hour).Truncate(time.Second),
		serviceID:  "3f2b1c8e-9d4a-4f11-8a7b-0c5d6e7f8a9b",
		instanceID: "0123456789ab",
		svcName:    "orders",
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestProvisionSuccess(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	fake := newFake(t, ca)
	addr, counter := serveFake(t, ca, fake)

	keyID := randomBytes(t, 8)
	secret := randomBytes(t, 32)
	key, err := connection.ParseBootstrapKey(encodeKey(t, keyID, secret, ca.der))
	if err != nil {
		t.Fatalf("ParseBootstrapKey: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := connection.Provision(ctx, addr, key)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}

	if res.Identity.ServiceID != fake.serviceID {
		t.Errorf("ServiceID: got %q want %q", res.Identity.ServiceID, fake.serviceID)
	}
	if res.Identity.InstanceID != fake.instanceID {
		t.Errorf("InstanceID: got %q want %q", res.Identity.InstanceID, fake.instanceID)
	}
	if res.ServiceName != fake.svcName {
		t.Errorf("ServiceName: got %q want %q", res.ServiceName, fake.svcName)
	}
	if !res.NotAfter.Equal(fake.notAfter.UTC()) {
		t.Errorf("NotAfter: got %s want %s (seconds, not milliseconds)", res.NotAfter, fake.notAfter.UTC())
	}

	req := fake.request()
	if string(req.GetKeyId()) != string(keyID) || string(req.GetSecret()) != string(secret) {
		t.Error("credentials did not reach the server unchanged")
	}

	// The issued leaf must carry the SPIFFE identity and pair with the CSR key.
	leaf, err := x509.ParseCertificate(res.CertDER)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	if len(leaf.URIs) != 1 {
		t.Fatalf("leaf URI SANs: got %d want 1", len(leaf.URIs))
	}
	id, err := connection.ParseSPIFFE(leaf.URIs[0].String())
	if err != nil {
		t.Fatalf("ParseSPIFFE: %v", err)
	}
	if id != res.Identity {
		t.Errorf("SPIFFE identity: got %+v want %+v", id, res.Identity)
	}
	pub, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok || !pub.Equal(&res.PrivateKey.PublicKey) {
		t.Error("issued cert does not match the generated private key")
	}
	if res.TLSCert.Leaf == nil || !res.TLSCert.Leaf.Equal(leaf) {
		t.Error("TLSCert.Leaf is not the issued cert")
	}

	// The credential must actually complete an mTLS handshake with the runtime.
	serverCfg := ca.serverConfig(t)
	serverCfg.ClientAuth = tls.RequireAndVerifyClientCert
	if err := handshake(t, serverCfg, connection.MutualTLSConfig(key.CACert, res.TLSCert)); err != nil {
		t.Fatalf("mTLS handshake with provisioned credential: %v", err)
	}

	counter.waitClosed(t)
}

func TestProvisionClosesChannelOnError(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	fake := newFake(t, ca)
	fake.failWith = status.Error(codes.Unauthenticated, "invalid credentials")
	addr, counter := serveFake(t, ca, fake)

	key, err := connection.ParseBootstrapKey(encodeKey(t, randomBytes(t, 8), randomBytes(t, 32), ca.der))
	if err != nil {
		t.Fatalf("ParseBootstrapKey: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = connection.Provision(ctx, addr, key)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, connection.ErrProvision) {
		t.Errorf("kind: got %v want ErrProvision", err)
	}
	if status.Code(errors.Unwrap(err)) != codes.Unauthenticated {
		t.Errorf("gRPC status not preserved: %v", err)
	}

	counter.waitClosed(t)
}

func TestProvisionRejectsForeignCA(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	foreign := newTestCA(t)
	addr, _ := serveFake(t, ca, newFake(t, ca))

	// The bootstrap key pins a CA that did not sign the runtime's server cert.
	key, err := connection.ParseBootstrapKey(encodeKey(t, randomBytes(t, 8), randomBytes(t, 32), foreign.der))
	if err != nil {
		t.Fatalf("ParseBootstrapKey: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := connection.Provision(ctx, addr, key); err == nil {
		t.Fatal("provisioned against a runtime signed by a foreign CA")
	}
}

func TestProvisionRejectsBadAddress(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	key, err := connection.ParseBootstrapKey(encodeKey(t, randomBytes(t, 8), randomBytes(t, 32), ca.der))
	if err != nil {
		t.Fatalf("ParseBootstrapKey: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = connection.Provision(ctx, "not-a-host-port", key)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, connection.ErrProvision) {
		t.Errorf("kind: got %v want ErrProvision", err)
	}
}

func TestNotAfterFromUnixSeconds(t *testing.T) {
	t.Parallel()
	want := time.Date(2026, 8, 13, 10, 30, 0, 0, time.UTC)
	if got := connection.NotAfterFromUnixSeconds(want.Unix()); !got.Equal(want) {
		t.Errorf("got %s want %s", got, want)
	}
	// Reading the field as milliseconds would land the expiry in 1970.
	if got := connection.NotAfterFromUnixSeconds(want.UnixMilli()); got.Year() == want.Year() {
		t.Errorf("milliseconds must not decode to the same instant: %s", got)
	}
}
