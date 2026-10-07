package connection

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"google.golang.org/grpc/keepalive"
)

// PinnedTLSConfig trusts exactly one root — the CA carried by the bootstrap key.
//
// Verification requires the pinned CA, ServerAuth and exactly the runtime URI
// SAN. InsecureSkipVerify disables DNS matching, which URI identities replace.
func PinnedTLSConfig(ca *x509.Certificate) *tls.Config {
	roots := x509.NewCertPool()
	roots.AddCert(ca)

	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		RootCAs:            roots,
		InsecureSkipVerify: true, //nolint:gosec // chain verified in VerifyConnection
		VerifyConnection: func(cs tls.ConnectionState) error {
			if err := VerifyServerChain(roots)(cs); err != nil {
				return err
			}
			if len(cs.PeerCertificates[0].URIs) != 1 || cs.PeerCertificates[0].URIs[0].String() != RuntimeSPIFFEURI {
				return fmt.Errorf("connection: runtime peer role mismatch")
			}
			return nil
		},
	}
}

// MutualTLSConfig is PinnedTLSConfig plus one fixed client certificate.
func MutualTLSConfig(ca *x509.Certificate, clientCert tls.Certificate) *tls.Config {
	return RotatingTLSConfig(ca, func() *tls.Certificate { return &clientCert })
}

// RotatingTLSConfig is PinnedTLSConfig whose client certificate is read on
// every handshake. A rotation therefore reaches every NEW connection of every
// channel built from it while the connections already up keep serving: no
// stream, channel or session has to be rebuilt to adopt a renewed leaf.
func RotatingTLSConfig(ca *x509.Certificate, current func() *tls.Certificate) *tls.Config {
	cfg := PinnedTLSConfig(ca)
	cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		if c := current(); c != nil {
			return c, nil
		}
		return nil, fmt.Errorf("connection: no client certificate published yet")
	}
	return cfg
}

// ClientKeepalive is the keepalive every SDK channel dials with: a ping after
// 30 s of silence, dead after 10 s without an answer, pings allowed with no
// call in flight. It matches the callee and runtime enforcement policies
// (MinTime 20 s / 10 s), so no server answers it with GOAWAY too_many_pings.
func ClientKeepalive() keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:                30 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}
}

func VerifyServerChain(roots *x509.CertPool) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("connection: verify peer: no peer certificates")
		}
		opts := x509.VerifyOptions{
			Roots:         roots,
			Intermediates: x509.NewCertPool(),
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		for _, c := range cs.PeerCertificates[1:] {
			opts.Intermediates.AddCert(c)
		}
		if _, err := cs.PeerCertificates[0].Verify(opts); err != nil {
			return fmt.Errorf("connection: verify peer: %w", err)
		}
		return nil
	}
}
