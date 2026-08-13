package connection

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const csrCommonName = "servicebridge-instance"

// ProvisionResult is the mTLS identity handed back by the runtime.
type ProvisionResult struct {
	Identity    Identity
	ServiceName string
	CertDER     []byte
	CAChainDER  []byte
	PrivateKey  *ecdsa.PrivateKey
	NotAfter    time.Time
	TLSCert     tls.Certificate
}

// Provision obtains a leaf certificate from the runtime's Bootstrap service.
//
// The bootstrap channel is pinned to the CA inside the key and carries no client
// cert — the SDK has none yet. It is single-use and closed on every return path:
// a leaked gRPC channel keeps its own reconnect goroutines and backoff timers
// alive, which turns a reconnect storm into unbounded goroutine growth.
func Provision(ctx context.Context, addr string, key BootstrapKey) (*ProvisionResult, error) {
	const op = "provision"

	if _, _, err := net.SplitHostPort(addr); err != nil {
		return nil, newError(KindProvision, op, "address must be host:port", err)
	}

	priv, csrDER, err := NewCSR()
	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(PinnedTLSConfig(key.CACert))))
	if err != nil {
		return nil, newError(KindProvision, op, "dial runtime", err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := pb.NewBootstrapClient(conn).Provision(ctx, &pb.ProvisionRequest{
		KeyId:  key.KeyID,
		Secret: key.Secret,
		CsrDer: csrDER,
	})
	if err != nil {
		return nil, newError(KindProvision, op, "Bootstrap.Provision", err)
	}

	tlsCert, err := NewTLSCertificate(resp.GetCertDer(), resp.GetCaChainDer(), priv)
	if err != nil {
		return nil, err
	}

	return &ProvisionResult{
		Identity: Identity{
			ServiceID:  resp.GetServiceId(),
			InstanceID: resp.GetInstanceId(),
		},
		ServiceName: resp.GetServiceName(),
		CertDER:     resp.GetCertDer(),
		CAChainDER:  resp.GetCaChainDer(),
		PrivateKey:  priv,
		NotAfter:    NotAfterFromUnixSeconds(resp.GetNotAfterUnix()),
		TLSCert:     tlsCert,
	}, nil
}

// NotAfterFromUnixSeconds converts a cert expiry field off the wire.
//
// Every other time field in the protocol is unix milliseconds (ADR-0006), but
// not_after_unix in ProvisionResponse and RefreshCertResponse is SECONDS — the
// runtime fills it with time.Time.Unix(). Reading it as milliseconds puts the
// expiry in 1970 and makes the client rotate its certificate in a hot loop.
func NotAfterFromUnixSeconds(sec int64) time.Time {
	return time.Unix(sec, 0).UTC()
}

// NewCSR generates a fresh ECDSA P-256 key pair and a PKCS#10 CSR for it. The
// subject is decorative: the runtime ignores it and stamps the SPIFFE URI SAN
// itself when issuing the leaf.
func NewCSR() (*ecdsa.PrivateKey, []byte, error) {
	const op = "create CSR"

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, newError(KindTLS, op, "generate P-256 key", err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: csrCommonName},
	}, priv)
	if err != nil {
		return nil, nil, newError(KindTLS, op, "encode PKCS#10", err)
	}
	return priv, der, nil
}

// NewTLSCertificate assembles the client credential from an issued leaf, the CA
// chain behind it and the private key the CSR was signed with.
func NewTLSCertificate(certDER, caChainDER []byte, priv *ecdsa.PrivateKey) (tls.Certificate, error) {
	const op = "build client certificate"

	leaf, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, newError(KindTLS, op, "parse issued leaf", err)
	}
	chain, err := x509.ParseCertificates(caChainDER)
	if err != nil {
		return tls.Certificate{}, newError(KindTLS, op, "parse CA chain", err)
	}

	out := tls.Certificate{Certificate: [][]byte{certDER}, PrivateKey: priv, Leaf: leaf}
	for _, c := range chain {
		out.Certificate = append(out.Certificate, c.Raw)
	}
	return out, nil
}
