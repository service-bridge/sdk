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
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

// inboundCA mirrors the runtime's CA: it signs the SPIFFE leaves handed to SDK
// instances and the SAN-less leaf the runtime presents when it proxies.
type inboundCA struct {
	cert *x509.Certificate
	der  []byte
	key  *ecdsa.PrivateKey
}

func newInboundCA(t *testing.T) *inboundCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          certSerial(t),
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
	return &inboundCA{cert: cert, der: der, key: key}
}

func (ca *inboundCA) issue(t *testing.T, tmpl *x509.Certificate) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl.SerialNumber = certSerial(t)
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
	return tls.Certificate{
		Certificate: [][]byte{der, ca.der},
		PrivateKey:  key,
		Leaf:        leaf,
	}
}

// serviceLeaf issues the shape every SDK instance gets: a SPIFFE URI SAN.
func (ca *inboundCA) serviceLeaf(t *testing.T, serviceID, instanceID string) tls.Certificate {
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

// runtimeLeaf issues the shape the runtime presents when it proxies: no SAN at
// all, identified only by its CN.
func (ca *inboundCA) runtimeLeaf(t *testing.T) tls.Certificate {
	t.Helper()
	return ca.issue(t, &x509.Certificate{Subject: pkix.Name{CommonName: RuntimeCommonName}})
}

func (ca *inboundCA) sanlessLeaf(t *testing.T, commonName string) tls.Certificate {
	t.Helper()
	return ca.issue(t, &x509.Certificate{Subject: pkix.Name{CommonName: commonName}})
}

func (ca *inboundCA) foreignURILeaf(t *testing.T, raw string) tls.Certificate {
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

func (ca *inboundCA) credentials(t *testing.T, leaf tls.Certificate, serviceID, instanceID string) connection.Credentials {
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

func certSerial(t *testing.T) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("certSerial: %v", err)
	}
	return n
}

func TestIdentifyPeer(t *testing.T) {
	t.Parallel()

	ca := newInboundCA(t)

	tests := []struct {
		name     string
		cert     *x509.Certificate
		wantKind PeerKind
		wantID   string
		wantErr  bool
	}{
		{
			name:     "SPIFFE leaf is a service",
			cert:     ca.serviceLeaf(t, "svc-a", "inst-1").Leaf,
			wantKind: PeerService,
			wantID:   "svc-a",
		},
		{
			name:     "SAN-less runtime CN is the proxy",
			cert:     ca.runtimeLeaf(t).Leaf,
			wantKind: PeerRuntime,
		},
		{
			name:    "URI SAN that is not SPIFFE is refused",
			cert:    ca.foreignURILeaf(t, "https://example.test/whoami").Leaf,
			wantErr: true,
		},
		{
			name:    "SPIFFE URI in another trust domain is refused",
			cert:    ca.foreignURILeaf(t, "spiffe://elsewhere/service/svc-a/instance/inst-1").Leaf,
			wantErr: true,
		},
		{
			name:    "SAN-less leaf with a foreign CN is refused",
			cert:    ca.sanlessLeaf(t, "servicebridge-leaf").Leaf,
			wantErr: true,
		},
		{
			name:    "no certificate is refused",
			cert:    nil,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := IdentifyPeer(tc.cert)
			if tc.wantErr {
				if !errors.Is(err, ErrPeerUnidentified) {
					t.Fatalf("err = %v, want ErrPeerUnidentified", err)
				}
				if got.Kind != PeerUnknown {
					t.Fatalf("a refused peer must stay unknown, got %v", got.Kind)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Kind != tc.wantKind {
				t.Fatalf("kind = %v, want %v", got.Kind, tc.wantKind)
			}
			if got.ServiceID != tc.wantID {
				t.Fatalf("service id = %q, want %q", got.ServiceID, tc.wantID)
			}
		})
	}
}

func TestIdentifyPeerPrefersTheSPIFFEURIAmongSeveral(t *testing.T) {
	t.Parallel()

	ca := newInboundCA(t)
	other, err := url.Parse("https://example.test/ignored")
	if err != nil {
		t.Fatalf("parse URI: %v", err)
	}
	spiffe, err := url.Parse(connection.FormatSPIFFE(connection.Identity{ServiceID: "svc-a", InstanceID: "inst-1"}))
	if err != nil {
		t.Fatalf("parse SPIFFE URI: %v", err)
	}
	leaf := ca.issue(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: "servicebridge-leaf"},
		URIs:    []*url.URL{other, spiffe},
	})

	got, err := IdentifyPeer(leaf.Leaf)
	if err != nil {
		t.Fatalf("IdentifyPeer: %v", err)
	}
	if got.Kind != PeerService || got.ServiceID != "svc-a" || got.InstanceID != "inst-1" {
		t.Fatalf("got %+v", got)
	}
}

func TestPeerFromContextWithoutTLS(t *testing.T) {
	t.Parallel()

	if _, err := PeerFromContext(context.Background()); !errors.Is(err, ErrPeerUnidentified) {
		t.Fatalf("err = %v, want ErrPeerUnidentified", err)
	}
}

func TestAllow(t *testing.T) {
	t.Parallel()

	service := Peer{Kind: PeerService, ServiceID: "svc-a", InstanceID: "inst-1"}
	runtimeProxy := Peer{Kind: PeerRuntime}

	rule := func(action, peerID, target string) *pb.PolicyRule {
		return &pb.PolicyRule{Action: action, PeerServiceId: peerID, TargetName: target}
	}

	tests := []struct {
		name    string
		peer    Peer
		method  string
		policy  *pb.PolicyEvaluation
		wantErr error
	}{
		{
			name:   "no evaluation yet allows",
			peer:   service,
			method: "charge",
		},
		{
			name:   "empty rule set allows",
			peer:   service,
			method: "charge",
			policy: &pb.PolicyEvaluation{},
		},
		{
			name:   "rules for other actions do not gate rpc",
			peer:   service,
			method: "charge",
			policy: &pb.PolicyEvaluation{Acceptance: []*pb.PolicyRule{rule("event.handle", "svc-z", "orders.*")}},
		},
		{
			name:   "exact peer and method",
			peer:   service,
			method: "charge",
			policy: &pb.PolicyEvaluation{Acceptance: []*pb.PolicyRule{rule(actionRPCHandle, "svc-a", "charge")}},
		},
		{
			name:   "wildcard peer",
			peer:   service,
			method: "charge",
			policy: &pb.PolicyEvaluation{Acceptance: []*pb.PolicyRule{rule(actionRPCHandle, "", "charge")}},
		},
		{
			name:   "wildcard method",
			peer:   service,
			method: "charge",
			policy: &pb.PolicyEvaluation{Acceptance: []*pb.PolicyRule{rule(actionRPCHandle, "svc-a", wildcardTarget)}},
		},
		{
			name:    "another peer is denied",
			peer:    service,
			method:  "charge",
			policy:  &pb.PolicyEvaluation{Acceptance: []*pb.PolicyRule{rule(actionRPCHandle, "svc-b", wildcardTarget)}},
			wantErr: ErrAcceptanceDenied,
		},
		{
			name:    "another method is denied",
			peer:    service,
			method:  "refund",
			policy:  &pb.PolicyEvaluation{Acceptance: []*pb.PolicyRule{rule(actionRPCHandle, "svc-a", "charge")}},
			wantErr: ErrAcceptanceDenied,
		},
		{
			name:   "runtime proxy passes a policy that denies every service",
			peer:   runtimeProxy,
			method: "refund",
			policy: &pb.PolicyEvaluation{Acceptance: []*pb.PolicyRule{rule(actionRPCHandle, "svc-b", "charge")}},
		},
		{
			name:    "unidentified peer is refused",
			peer:    Peer{},
			method:  "charge",
			wantErr: ErrPeerUnidentified,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := Allow(tc.peer, tc.method, tc.policy)
			switch {
			case tc.wantErr == nil && err != nil:
				t.Fatalf("unexpected denial: %v", err)
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}
