package connection

import (
	"net/url"
	"strconv"
	"strings"
)

// SPIFFETrustDomain is the trust domain of every ServiceBridge identity. It must
// stay byte-identical to connection.SPIFFETrustDomain in the runtime and to
// SPIFFE_TRUST_DOMAIN in the Node SDK — a mismatch rejects every peer cert.
const SPIFFETrustDomain = "service-bridge"

// Identity is the SPIFFE identity carried in a leaf cert URI SAN.
type Identity struct {
	ServiceID  string
	InstanceID string
}

// FormatSPIFFE renders id as spiffe://service-bridge/service/<id>/instance/<id>.
func FormatSPIFFE(id Identity) string {
	return "spiffe://" + SPIFFETrustDomain + "/service/" + id.ServiceID + "/instance/" + id.InstanceID
}

// ParseSPIFFE parses a SPIFFE URI of the form
// spiffe://service-bridge/service/<serviceID>/instance/<instanceID>.
func ParseSPIFFE(raw string) (Identity, error) {
	const op = "parse SPIFFE"

	u, err := url.Parse(raw)
	if err != nil {
		return Identity{}, newError(KindIdentity, op, "malformed URI", err)
	}
	if u.Scheme != "spiffe" {
		return Identity{}, newError(KindIdentity, op, "unexpected scheme "+strconv.Quote(u.Scheme), nil)
	}
	if u.Host != SPIFFETrustDomain {
		return Identity{}, newError(KindIdentity, op, "unexpected trust domain "+strconv.Quote(u.Host), nil)
	}

	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "service" || parts[2] != "instance" {
		return Identity{}, newError(KindIdentity, op, "unexpected path "+strconv.Quote(u.Path), nil)
	}
	if parts[1] == "" {
		return Identity{}, newError(KindIdentity, op, "empty serviceID in path "+strconv.Quote(u.Path), nil)
	}
	if parts[3] == "" {
		return Identity{}, newError(KindIdentity, op, "empty instanceID in path "+strconv.Quote(u.Path), nil)
	}

	return Identity{ServiceID: parts[1], InstanceID: parts[3]}, nil
}
