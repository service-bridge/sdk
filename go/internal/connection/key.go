package connection

import (
	"crypto/x509"
	"encoding/base64"
	"strings"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/protobuf/proto"
)

const (
	bootstrapKeyPrefix = "sb."
	keyIDLen           = 8
	secretLen          = 32
)

// BootstrapKey is a parsed bootstrap key.
//
// The CA cert travels inside the key so the SDK has a trust anchor before it
// owns any certificate of its own: the very first connection to the runtime is
// already pinned, with no trust-on-first-use window.
type BootstrapKey struct {
	KeyID     []byte
	Secret    []byte
	CACertDER []byte
	CACert    *x509.Certificate
}

// ParseBootstrapKey decodes the wire form "sb.<base64url-raw(BootstrapKeyPayload)>".
func ParseBootstrapKey(raw string) (BootstrapKey, error) {
	const op = "parse bootstrap key"

	body, found := strings.CutPrefix(raw, bootstrapKeyPrefix)
	if !found {
		return BootstrapKey{}, newError(KindKey, op, `missing "sb." prefix`, nil)
	}

	blob, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return BootstrapKey{}, newError(KindKey, op, "invalid base64url body", err)
	}

	var payload pb.BootstrapKeyPayload
	if err := proto.Unmarshal(blob, &payload); err != nil {
		return BootstrapKey{}, newError(KindKey, op, "invalid payload encoding", err)
	}

	if len(payload.GetKeyId()) != keyIDLen {
		return BootstrapKey{}, newError(KindKey, op, "key_id must be 8 bytes", nil)
	}
	if len(payload.GetSecret()) != secretLen {
		return BootstrapKey{}, newError(KindKey, op, "secret must be 32 bytes", nil)
	}
	if len(payload.GetCaCertDer()) == 0 {
		return BootstrapKey{}, newError(KindKey, op, "ca_cert_der is empty", nil)
	}

	caCert, err := x509.ParseCertificate(payload.GetCaCertDer())
	if err != nil {
		return BootstrapKey{}, newError(KindKey, op, "ca_cert_der is not a valid certificate", err)
	}

	return BootstrapKey{
		KeyID:     payload.GetKeyId(),
		Secret:    payload.GetSecret(),
		CACertDER: payload.GetCaCertDer(),
		CACert:    caCert,
	}, nil
}
