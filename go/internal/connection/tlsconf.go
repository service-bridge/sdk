package connection

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

// PinnedTLSConfig trusts exactly one root — the CA carried by the bootstrap key.
//
// The runtime's server leaf has no DNS or IP SAN (its CN is
// "servicebridge-runtime"), so the standard hostname check can never pass.
// Verification is therefore done by hand in VerifyConnection: the peer chain must
// terminate in the pinned CA. InsecureSkipVerify only switches off the built-in
// hostname/chain step that VerifyConnection replaces — it does not weaken trust.
func PinnedTLSConfig(ca *x509.Certificate) *tls.Config {
	roots := x509.NewCertPool()
	roots.AddCert(ca)

	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, //nolint:gosec // chain verified in VerifyConnection
		VerifyConnection:   verifyAgainst(roots),
	}
}

// MutualTLSConfig is PinnedTLSConfig plus the client cert issued by Provision.
func MutualTLSConfig(ca *x509.Certificate, clientCert tls.Certificate) *tls.Config {
	cfg := PinnedTLSConfig(ca)
	cfg.Certificates = []tls.Certificate{clientCert}
	return cfg
}

func verifyAgainst(roots *x509.CertPool) func(tls.ConnectionState) error {
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
