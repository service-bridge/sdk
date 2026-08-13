package rpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc"
	gcreds "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func outboundTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// outboundCA mirrors the runtime's CA for the outbound tests. It is deliberately
// separate from the inbound suite's fixture: the two halves of this package are
// written by different hands, and a shared test helper turns every rename on one
// side into a broken build on the other.
type outboundCA struct {
	cert *x509.Certificate
	der  []byte
	key  *ecdsa.PrivateKey
}

func newOutboundCA(t *testing.T) *outboundCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          outboundSerial(t),
		Subject:               pkix.Name{CommonName: "servicebridge-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return &outboundCA{cert: cert, der: der, key: key}
}

func (ca *outboundCA) issue(t *testing.T, tmpl *x509.Certificate) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl.SerialNumber = outboundSerial(t)
	tmpl.NotBefore = time.Now().Add(-time.Minute)
	tmpl.NotAfter = time.Now().Add(time.Hour)
	tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der, ca.der}, PrivateKey: key, Leaf: leaf}
}

// serviceLeaf issues the shape every SDK instance gets: a SPIFFE URI SAN.
func (ca *outboundCA) serviceLeaf(t *testing.T, serviceID, instanceID string) tls.Certificate {
	t.Helper()

	uri, err := url.Parse(connection.FormatSPIFFE(connection.Identity{ServiceID: serviceID, InstanceID: instanceID}))
	if err != nil {
		t.Fatalf("parse SPIFFE URI: %v", err)
	}
	return ca.issue(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: "servicebridge-leaf"},
		URIs:    []*url.URL{uri},
	})
}

// sanlessLeaf issues the shape the runtime presents: no SAN at all.
func (ca *outboundCA) sanlessLeaf(t *testing.T) tls.Certificate {
	t.Helper()
	return ca.issue(t, &x509.Certificate{Subject: pkix.Name{CommonName: "servicebridge-runtime"}})
}

func (ca *outboundCA) foreignURILeaf(t *testing.T, raw string) tls.Certificate {
	t.Helper()

	uri, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse URI %q: %v", raw, err)
	}
	return ca.issue(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: "servicebridge-leaf"},
		URIs:    []*url.URL{uri},
	})
}

func (ca *outboundCA) credentials(t *testing.T, leaf tls.Certificate, serviceID, instanceID string) connection.Credentials {
	t.Helper()
	return connection.Credentials{
		Addr: "127.0.0.1:0",
		Lease: connection.Lease{
			Identity:   connection.Identity{ServiceID: serviceID, InstanceID: instanceID},
			CertDER:    leaf.Certificate[0],
			CAChainDER: ca.der,
			TLSCert:    leaf,
		},
		TLS: connection.MutualTLSConfig(ca.cert, leaf),
	}
}

func outboundSerial(t *testing.T) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	return n
}

// peerTarget is the candidate shape the direct transport dials.
func peerTarget(serviceID, instanceID, endpoint string) Candidate {
	return Candidate{
		ServiceID:   serviceID,
		ServiceName: "billing",
		InstanceID:  instanceID,
		Endpoint:    endpoint,
	}
}

// handshakeAgainst runs a real TLS handshake between clientCfg and a server
// presenting serverLeaf. Pinning is a handshake property, so it is tested at
// the handshake, not by inspecting the config.
func handshakeAgainst(t *testing.T, clientCfg *tls.Config, serverLeaf tls.Certificate) error {
	t.Helper()

	clientSide, serverSide := net.Pipe()
	served := make(chan struct{})
	go func() {
		defer close(served)
		s := tls.Server(serverSide, &tls.Config{
			Certificates: []tls.Certificate{serverLeaf},
			MinVersion:   tls.VersionTLS13,
		})
		_ = s.HandshakeContext(context.Background())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := tls.Client(clientSide, clientCfg).HandshakeContext(ctx)

	_ = clientSide.Close()
	_ = serverSide.Close()
	<-served
	return err
}

// TestPeerPinningRejectsAnotherInstancesCertificate is the security invariant of
// the direct path. Every leaf in the mesh is signed by the same CA, so chain
// verification alone lets any service impersonate any other; the URI SAN is the
// only thing that says WHO answered.
func TestPeerPinningRejectsAnotherInstancesCertificate(t *testing.T) {
	ca := newOutboundCA(t)
	callerLeaf := ca.serviceLeaf(t, "svc-caller", "inst-caller")
	base := connection.MutualTLSConfig(ca.cert, callerLeaf)

	want := connection.Identity{ServiceID: "svc-wanted", InstanceID: "inst-wanted"}
	cfg, err := peerTLSConfig(base, want)
	if err != nil {
		t.Fatalf("build peer TLS config: %v", err)
	}

	// A perfectly valid leaf from the same CA — for somebody else.
	impostor := ca.serviceLeaf(t, "svc-other", "inst-other")

	err = handshakeAgainst(t, cfg, impostor)
	if err == nil {
		t.Fatal("a cert signed by the trusted CA but naming another instance MUST be rejected")
	}
	if !errors.Is(err, ErrPeerIdentity) {
		t.Fatalf("rejection must be the identity pin, got %v", err)
	}
	if !strings.Contains(err.Error(), connection.FormatSPIFFE(want)) {
		t.Fatalf("the failure must name the identity that was expected, got %q", err.Error())
	}
}

func TestPeerPinningAcceptsTheExpectedInstance(t *testing.T) {
	ca := newOutboundCA(t)
	callerLeaf := ca.serviceLeaf(t, "svc-caller", "inst-caller")
	base := connection.MutualTLSConfig(ca.cert, callerLeaf)

	want := connection.Identity{ServiceID: "svc-wanted", InstanceID: "inst-wanted"}
	cfg, err := peerTLSConfig(base, want)
	if err != nil {
		t.Fatalf("build peer TLS config: %v", err)
	}

	peerLeaf := ca.serviceLeaf(t, want.ServiceID, want.InstanceID)
	if err := handshakeAgainst(t, cfg, peerLeaf); err != nil {
		t.Fatalf("the expected peer must be accepted, got %v", err)
	}
}

func TestPeerPinningRejectsALeafWithoutAnyURI(t *testing.T) {
	ca := newOutboundCA(t)
	base := connection.MutualTLSConfig(ca.cert, ca.serviceLeaf(t, "svc-caller", "inst-caller"))

	cfg, err := peerTLSConfig(base, connection.Identity{ServiceID: "svc-wanted", InstanceID: "inst-wanted"})
	if err != nil {
		t.Fatalf("build peer TLS config: %v", err)
	}

	err = handshakeAgainst(t, cfg, ca.sanlessLeaf(t))
	if err == nil || !errors.Is(err, ErrPeerIdentity) {
		t.Fatalf("a SAN-less leaf must fail the pin, got %v", err)
	}
}

func TestPeerPinningRejectsAForeignTrustDomain(t *testing.T) {
	ca := newOutboundCA(t)
	base := connection.MutualTLSConfig(ca.cert, ca.serviceLeaf(t, "svc-caller", "inst-caller"))

	want := connection.Identity{ServiceID: "svc-wanted", InstanceID: "inst-wanted"}
	cfg, err := peerTLSConfig(base, want)
	if err != nil {
		t.Fatalf("build peer TLS config: %v", err)
	}

	foreign := ca.foreignURILeaf(t, "spiffe://evil.example/service/svc-wanted/instance/inst-wanted")
	if err := handshakeAgainst(t, cfg, foreign); !errors.Is(err, ErrPeerIdentity) {
		t.Fatalf("a matching path under another trust domain must fail the pin, got %v", err)
	}
}

func TestPeerTLSConfigRefusesABaseThatVerifiesNothing(t *testing.T) {
	naked := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // that is the point of the test
	if _, err := peerTLSConfig(naked, connection.Identity{ServiceID: "s", InstanceID: "i"}); err == nil {
		t.Fatal("a base config that skips verification and verifies nothing must be refused, not used")
	}
	if _, err := peerTLSConfig(nil, connection.Identity{ServiceID: "s", InstanceID: "i"}); !errors.Is(err, ErrNoLease) {
		t.Fatal("a missing base config must report the missing lease")
	}
}

// countingDialer records what was dialled and hands back a lazily-connecting
// channel, so cache behaviour is testable without a listener per instance.
type countingDialer struct {
	dials []string
}

func (d *countingDialer) DialPeer(_ context.Context, endpoint string, _ *tls.Config) (*grpc.ClientConn, error) {
	d.dials = append(d.dials, endpoint)
	return grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func directUnderTest(t *testing.T, ca *outboundCA, clk *stepClock, dialer PeerDialer) *Direct {
	t.Helper()

	d := NewDirect(DirectConfig{Dialer: dialer, Now: clk.now, Logger: outboundTestLogger()})
	creds := ca.credentials(t, ca.serviceLeaf(t, "svc-caller", "inst-caller"), "svc-caller", "inst-caller")
	creds.Lease.NotAfter = time.UnixMilli(clk.now()).Add(24 * time.Hour)
	if err := d.UseCredentials(context.Background(), creds); err != nil {
		t.Fatalf("publish credentials: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// TestChannelCacheSeparatesInstancesSharingAnEndpoint is the k8s bug: pod IPs
// are recycled, and a cache keyed by endpoint alone hands the new owner a
// channel pinned to the previous owner's SPIFFE URI.
func TestChannelCacheSeparatesInstancesSharingAnEndpoint(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(1_700_000_000_000)
	dialer := &countingDialer{}
	d := directUnderTest(t, ca, clk, dialer)

	const endpoint = "10.0.0.7:14446"
	first, err := d.lease(context.Background(), peerTarget("svc-a", "inst-old", endpoint))
	if err != nil {
		t.Fatalf("lease first: %v", err)
	}
	first.release()

	second, err := d.lease(context.Background(), peerTarget("svc-a", "inst-new", endpoint))
	if err != nil {
		t.Fatalf("lease second: %v", err)
	}
	second.release()

	if len(dialer.dials) != 2 {
		t.Fatalf("dialled %d times for two instances on one endpoint, want 2", len(dialer.dials))
	}
	if d.Cached() != 2 {
		t.Fatalf("cached %d channels, want one per instance", d.Cached())
	}
	if first.key == second.key {
		t.Fatal("two instances on one endpoint must not share a cache key")
	}
}

func TestChannelIsReusedForTheSameTarget(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(1_700_000_000_000)
	dialer := &countingDialer{}
	d := directUnderTest(t, ca, clk, dialer)

	target := peerTarget("svc-a", "inst-1", "10.0.0.7:14446")
	for i := 0; i < 5; i++ {
		l, err := d.lease(context.Background(), target)
		if err != nil {
			t.Fatalf("lease %d: %v", i, err)
		}
		l.release()
	}

	if len(dialer.dials) != 1 {
		t.Fatalf("dialled %d times for one target, want 1", len(dialer.dials))
	}
}

func TestCredentialRotationDropsEveryCachedChannel(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(1_700_000_000_000)
	dialer := &countingDialer{}
	d := directUnderTest(t, ca, clk, dialer)

	target := peerTarget("svc-a", "inst-1", "10.0.0.7:14446")
	l, err := d.lease(context.Background(), target)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	l.release()

	rotated := ca.credentials(t, ca.serviceLeaf(t, "svc-caller", "inst-caller-2"), "svc-caller", "inst-caller-2")
	rotated.Lease.NotAfter = time.UnixMilli(clk.now()).Add(24 * time.Hour)
	if err := d.UseCredentials(context.Background(), rotated); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	if d.Cached() != 0 {
		t.Fatalf("rotation left %d channels on the old certificate", d.Cached())
	}
	if _, err := d.lease(context.Background(), target); err != nil {
		t.Fatalf("lease after rotation: %v", err)
	}
	if len(dialer.dials) != 2 {
		t.Fatalf("dialled %d times, want a redial on the new certificate", len(dialer.dials))
	}
}

func TestRetainInstancesClosesDepartedPeers(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(1_700_000_000_000)
	d := directUnderTest(t, ca, clk, &countingDialer{})

	staying := peerTarget("svc-a", "inst-stay", "10.0.0.1:14446")
	leaving := peerTarget("svc-a", "inst-go", "10.0.0.2:14446")
	for _, target := range []Candidate{staying, leaving} {
		l, err := d.lease(context.Background(), target)
		if err != nil {
			t.Fatalf("lease %s: %v", target.InstanceID, err)
		}
		l.release()
	}
	if d.Cached() != 2 {
		t.Fatalf("setup: cached %d, want 2", d.Cached())
	}

	d.RetainInstances(map[string]struct{}{staying.InstanceID: {}})

	if d.Cached() != 1 {
		t.Fatalf("cached %d channels after the departure, want 1", d.Cached())
	}
}

func TestExpiredChannelIsRedialled(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(1_700_000_000_000)
	dialer := &countingDialer{}

	d := NewDirect(DirectConfig{Dialer: dialer, Now: clk.now, Logger: outboundTestLogger()})
	t.Cleanup(func() { _ = d.Close() })

	creds := ca.credentials(t, ca.serviceLeaf(t, "svc-caller", "inst-caller"), "svc-caller", "inst-caller")
	// A certificate expiring in 10 minutes: TTL is 10min − 5min lead = 5min.
	creds.Lease.NotAfter = time.UnixMilli(clk.now()).Add(10 * time.Minute)
	if err := d.UseCredentials(context.Background(), creds); err != nil {
		t.Fatalf("publish credentials: %v", err)
	}

	target := peerTarget("svc-a", "inst-1", "10.0.0.7:14446")
	l, err := d.lease(context.Background(), target)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	l.release()

	clk.advance(5*60*1000 - 1)
	l, err = d.lease(context.Background(), target)
	if err != nil {
		t.Fatalf("lease before expiry: %v", err)
	}
	l.release()
	if len(dialer.dials) != 1 {
		t.Fatalf("dialled %d times before the TTL elapsed, want 1", len(dialer.dials))
	}

	clk.advance(1)
	l, err = d.lease(context.Background(), target)
	if err != nil {
		t.Fatalf("lease after expiry: %v", err)
	}
	l.release()
	if len(dialer.dials) != 2 {
		t.Fatalf("dialled %d times after the TTL elapsed, want a redial", len(dialer.dials))
	}
}

func TestDirectRefusesWithoutALeaseOrAnEndpoint(t *testing.T) {
	d := NewDirect(DirectConfig{Dialer: &countingDialer{}, Logger: outboundTestLogger()})
	t.Cleanup(func() { _ = d.Close() })

	if _, err := d.lease(context.Background(), peerTarget("svc-a", "inst-1", "10.0.0.1:1")); !errors.Is(err, ErrNoLease) {
		t.Fatalf("dialling before the first lease must fail with ErrNoLease, got %v", err)
	}

	ca := newOutboundCA(t)
	creds := ca.credentials(t, ca.serviceLeaf(t, "svc-caller", "inst-caller"), "svc-caller", "inst-caller")
	if err := d.UseCredentials(context.Background(), creds); err != nil {
		t.Fatalf("publish credentials: %v", err)
	}
	if _, err := d.lease(context.Background(), peerTarget("svc-a", "inst-1", "")); !errors.Is(err, ErrNoEndpoint) {
		t.Fatalf("an addressless candidate must fail with ErrNoEndpoint, got %v", err)
	}

	_ = d.Close()
	if _, err := d.lease(context.Background(), peerTarget("svc-a", "inst-1", "10.0.0.1:1")); !errors.Is(err, ErrDirectClosed) {
		t.Fatalf("a closed transport must refuse, got %v", err)
	}
}

// echoPeer is the callee side of the direct path for end-to-end tests.
type echoPeer struct {
	pb.UnimplementedCallServer
	chunks    [][]byte
	errorCode string
	lastReq   *pb.CallRequest
}

func (p *echoPeer) Unary(_ context.Context, req *pb.CallRequest) (*pb.CallResponse, error) {
	p.lastReq = req
	if p.errorCode != "" {
		return &pb.CallResponse{ErrorCode: p.errorCode, ErrorMessage: "rejected"}, nil
	}
	return &pb.CallResponse{Payload: req.GetPayload()}, nil
}

func (p *echoPeer) Stream(req *pb.CallRequest, srv grpc.ServerStreamingServer[pb.StreamChunk]) error {
	p.lastReq = req
	for _, chunk := range p.chunks {
		if err := srv.Send(&pb.StreamChunk{Payload: chunk}); err != nil {
			return err
		}
	}
	return nil
}

// startPeer runs a real mTLS gRPC callee and returns its address.
func startPeer(t *testing.T, ca *outboundCA, leaf tls.Certificate, impl pb.CallServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)

	srv := grpc.NewServer(grpc.Creds(gcreds.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{leaf},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	})))
	pb.RegisterCallServer(srv, impl)

	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-served
	})
	return lis.Addr().String()
}

func TestDirectUnaryReachesThePinnedPeer(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(time.Now().UnixMilli())
	peer := &echoPeer{}
	addr := startPeer(t, ca, ca.serviceLeaf(t, "svc-callee", "inst-callee"), peer)

	d := directUnderTest(t, ca, clk, GRPCPeerDialer{})
	target := peerTarget("svc-callee", "inst-callee", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := d.Unary(ctx, target, &pb.CallRequest{Method: "Ping", Payload: []byte("ping")})
	if err != nil {
		t.Fatalf("direct unary: %v", err)
	}
	if string(got) != "ping" {
		t.Fatalf("payload = %q, want %q", got, "ping")
	}
}

// TestDirectUnaryFailsAgainstAnImpersonatingPeer proves the pin holds through
// the whole gRPC stack, not only in an isolated handshake.
func TestDirectUnaryFailsAgainstAnImpersonatingPeer(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(time.Now().UnixMilli())
	// The listener presents a valid leaf — for a different instance.
	addr := startPeer(t, ca, ca.serviceLeaf(t, "svc-callee", "inst-IMPOSTOR"), &echoPeer{})

	d := directUnderTest(t, ca, clk, GRPCPeerDialer{})
	target := peerTarget("svc-callee", "inst-expected", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := d.Unary(ctx, target, &pb.CallRequest{Method: "Ping", Payload: []byte("ping")})
	if err == nil {
		t.Fatal("a peer presenting another instance's certificate MUST NOT be talked to")
	}
	if !strings.Contains(err.Error(), "peer SPIFFE identity mismatch") {
		t.Fatalf("the failure must name the identity pin, got %v", err)
	}
	if d.Cached() != 0 {
		t.Fatal("a channel that failed its handshake must be evicted, not kept")
	}
}

func TestDirectUnarySurfacesAHandlerError(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(time.Now().UnixMilli())
	addr := startPeer(t, ca, ca.serviceLeaf(t, "svc-callee", "inst-callee"), &echoPeer{errorCode: "VALIDATION"})

	d := directUnderTest(t, ca, clk, GRPCPeerDialer{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := d.Unary(ctx, peerTarget("svc-callee", "inst-callee", addr), &pb.CallRequest{Method: "Charge"})
	var he *HandlerError
	if !errors.As(err, &he) || he.Code != "VALIDATION" {
		t.Fatalf("a body error must surface as a HandlerError, got %v", err)
	}
	if d.Cached() != 1 {
		t.Fatal("a business error must not evict a healthy channel")
	}
}

func TestDirectStreamDeliversEveryChunkThenEOF(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(time.Now().UnixMilli())
	peer := &echoPeer{chunks: [][]byte{[]byte("a"), []byte("b"), []byte("c")}}
	addr := startPeer(t, ca, ca.serviceLeaf(t, "svc-callee", "inst-callee"), peer)

	d := directUnderTest(t, ca, clk, GRPCPeerDialer{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	st, err := d.Stream(ctx, peerTarget("svc-callee", "inst-callee", addr), &pb.CallRequest{Method: "Tokens"})
	if err != nil {
		t.Fatalf("direct stream: %v", err)
	}

	var got []string
	for {
		chunk, rerr := st.Recv()
		if rerr != nil {
			break
		}
		got = append(got, string(chunk))
	}
	if strings.Join(got, "") != "abc" {
		t.Fatalf("chunks = %v, want a, b, c", got)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestIdleChannelsAreSweptAway is the backstop for peers that neither expire
// nor leave the registry: an idle channel still costs gRPC goroutines.
func TestIdleChannelsAreSweptAway(t *testing.T) {
	ca := newOutboundCA(t)
	clk := newStepClock(1_700_000_000_000)
	d := directUnderTest(t, ca, clk, &countingDialer{})

	idle, err := d.lease(context.Background(), peerTarget("svc-a", "inst-idle", "10.0.0.1:14446"))
	if err != nil {
		t.Fatalf("lease idle: %v", err)
	}
	idle.release()

	clk.advance(DefaultIdleTTLMs + 1)

	fresh, err := d.lease(context.Background(), peerTarget("svc-a", "inst-fresh", "10.0.0.2:14446"))
	if err != nil {
		t.Fatalf("lease fresh: %v", err)
	}
	fresh.release()

	if got := d.Cached(); got != 1 {
		t.Fatalf("cached %d channels after the idle sweep, want 1", got)
	}
}
