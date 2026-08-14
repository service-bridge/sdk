package connection_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/service-bridge/sdk/go/internal/connection"
)

// ── test CA ───────────────────────────────────────────────────────────────────

type testCA struct {
	cert *x509.Certificate
	der  []byte
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(t),
		Subject:               pkix.Name{CommonName: "servicebridge-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
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
	return &testCA{cert: cert, der: der, key: key}
}

// issueServerCert mirrors the runtime: CN only, no DNS or IP SAN, so hostname
// verification cannot pass and the pinned VerifyConnection is what decides.
func (ca *testCA) issueServerCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("gen server key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(t),
		Subject:      pkix.Name{CommonName: "servicebridge-runtime"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse server cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der, ca.der}, PrivateKey: key, Leaf: leaf}
}

func (ca *testCA) issueLeaf(t *testing.T, pub any, id connection.Identity) []byte {
	t.Helper()
	uri, err := url.Parse(connection.FormatSPIFFE(id))
	if err != nil {
		t.Fatalf("parse SPIFFE URI: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(t),
		Subject:      pkix.Name{CommonName: id.InstanceID},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}
	return der
}

func (ca *testCA) serverConfig(t *testing.T) *tls.Config {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return &tls.Config{
		Certificates: []tls.Certificate{ca.issueServerCert(t)},
		MinVersion:   tls.VersionTLS13,
		ClientCAs:    pool,
		ClientAuth:   tls.VerifyClientCertIfGiven,
	}
}

func serial(t *testing.T) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	return n
}

// handshake runs one TLS exchange against a throwaway listener and reports the
// client-side result. A one-byte echo follows the handshake because TLS 1.3
// sends the client cert after the server's Finished: the server only learns
// about a client-auth failure on its first read, and the client only hears
// about it on its next read.
func handshake(t *testing.T, serverCfg, clientCfg *tls.Config) error {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		buf := make([]byte, 1)
		if _, readErr := conn.Read(buf); readErr != nil {
			return
		}
		_, _ = conn.Write(buf)
	}()

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	tlsConn := tls.Client(conn, clientCfg)
	_ = tlsConn.SetDeadline(time.Now().Add(10 * time.Second))

	err = tlsConn.Handshake()
	if err == nil {
		if _, err = tlsConn.Write([]byte{0x2a}); err == nil {
			_, err = tlsConn.Read(make([]byte, 1))
		}
	}
	_ = tlsConn.Close()
	<-done
	return err
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestPinnedTLSConfigAcceptsPinnedCA(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	if err := handshake(t, ca.serverConfig(t), connection.PinnedTLSConfig(ca.cert)); err != nil {
		t.Fatalf("handshake against pinned CA: %v", err)
	}
}

func TestPinnedTLSConfigRejectsForeignCA(t *testing.T) {
	t.Parallel()
	server := newTestCA(t)
	foreign := newTestCA(t)
	err := handshake(t, server.serverConfig(t), connection.PinnedTLSConfig(foreign.cert))
	if err == nil {
		t.Fatal("handshake succeeded against a chain signed by a foreign CA")
	}
}

func TestPinnedTLSConfigRequiresTLS13(t *testing.T) {
	t.Parallel()
	cfg := connection.PinnedTLSConfig(newTestCA(t).cert)
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion: got %#x want %#x", cfg.MinVersion, tls.VersionTLS13)
	}
}

func TestMutualTLSConfigPresentsClientCert(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)

	priv, csrDER, err := connection.NewCSR()
	if err != nil {
		t.Fatalf("NewCSR: %v", err)
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		t.Fatalf("parse CSR: %v", err)
	}
	certDER := ca.issueLeaf(t, csr.PublicKey, connection.Identity{
		ServiceID:  "b1f0a5a4-0000-4000-8000-000000000001",
		InstanceID: "abcdefgh0123",
	})
	clientCert, err := connection.NewTLSCertificate(certDER, ca.der, priv)
	if err != nil {
		t.Fatalf("NewTLSCertificate: %v", err)
	}

	serverCfg := ca.serverConfig(t)
	serverCfg.ClientAuth = tls.RequireAndVerifyClientCert

	if err := handshake(t, serverCfg, connection.MutualTLSConfig(ca.cert, clientCert)); err != nil {
		t.Fatalf("mTLS handshake: %v", err)
	}
}

func TestMutualTLSConfigRejectsForeignCA(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	foreign := newTestCA(t)

	priv, csrDER, err := connection.NewCSR()
	if err != nil {
		t.Fatalf("NewCSR: %v", err)
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		t.Fatalf("parse CSR: %v", err)
	}
	certDER := ca.issueLeaf(t, csr.PublicKey, connection.Identity{
		ServiceID:  "b1f0a5a4-0000-4000-8000-000000000001",
		InstanceID: "abcdefgh0123",
	})
	clientCert, err := connection.NewTLSCertificate(certDER, ca.der, priv)
	if err != nil {
		t.Fatalf("NewTLSCertificate: %v", err)
	}

	if err := handshake(t, ca.serverConfig(t), connection.MutualTLSConfig(foreign.cert, clientCert)); err == nil {
		t.Fatal("mTLS handshake succeeded with a foreign pinned CA")
	}
}

func TestNewTLSCertificateRejectsGarbage(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	priv, _, err := connection.NewCSR()
	if err != nil {
		t.Fatalf("NewCSR: %v", err)
	}

	if _, err := connection.NewTLSCertificate([]byte{0x30, 0x00}, ca.der, priv); err == nil {
		t.Error("accepted a malformed leaf")
	}
	leaf := ca.issueLeaf(t, &priv.PublicKey, connection.Identity{ServiceID: "s", InstanceID: "i"})
	if _, err := connection.NewTLSCertificate(leaf, []byte{0x30, 0x00}, priv); err == nil {
		t.Error("accepted a malformed CA chain")
	}
}
