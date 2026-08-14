package rpc

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// RuntimeCommonName is the Subject CN of the runtime's own leaf certificate. It
// must stay byte-identical to the CN stamped in runtime/internal/tlsca; a
// mismatch makes every proxied call look like an unidentified peer.
const RuntimeCommonName = "servicebridge-runtime"

// actionRPCHandle is the PolicyRule action that governs inbound RPC.
const actionRPCHandle = "rpc.handle"

// wildcardTarget matches any method name in a PolicyRule.
const wildcardTarget = "*"

// Admission failures.
var (
	// ErrPeerUnidentified means the caller's certificate does not say who the
	// caller is. It is a refusal, never a pass: an identity that cannot be read
	// cannot be checked against any rule.
	ErrPeerUnidentified = errors.New("caller identity could not be established")

	// ErrAcceptanceDenied means the caller is known and the acceptance rules do
	// not admit it for this method.
	ErrAcceptanceDenied = errors.New("acceptance policy denied the caller")
)

// PeerKind is what kind of party is on the other end of an inbound call.
type PeerKind uint8

const (
	// PeerUnknown is the zero value and never admits a call.
	PeerUnknown PeerKind = iota
	// PeerService is another SDK instance dialling directly. Its leaf carries a
	// SPIFFE URI SAN, so its service identity is known exactly.
	PeerService
	// PeerRuntime is the runtime proxying a call it has already gated.
	PeerRuntime
)

func (k PeerKind) String() string {
	switch k {
	case PeerService:
		return "service"
	case PeerRuntime:
		return "runtime"
	default:
		return "unknown"
	}
}

// Peer is the identified caller of one inbound call.
type Peer struct {
	Kind       PeerKind
	ServiceID  string
	InstanceID string
}

// IdentifyPeer reads the caller's identity off its verified leaf certificate.
//
// Identification is positive on both branches, never residual. A leaf with URI
// SANs must yield a ServiceBridge SPIFFE identity — if none of its URIs parse,
// the peer is refused rather than waved through, because a parse failure that
// opens the door is a parse failure an attacker can arrange. A leaf with no URI
// SAN at all is only the runtime's proxy, and only when its CN says so; the
// runtime never issues a SAN-less leaf to a service, so the two shapes cannot
// be confused.
//
// The certificate reaching this function has already been verified against the
// pinned CA by the TLS handshake, so CN is a trustworthy claim here and nowhere
// else.
func IdentifyPeer(cert *x509.Certificate) (Peer, error) {
	const op = "rpc: identify peer"

	if cert == nil {
		return Peer{}, fmt.Errorf("%s: no client certificate: %w", op, ErrPeerUnidentified)
	}

	if len(cert.URIs) > 0 {
		for _, u := range cert.URIs {
			id, err := connection.ParseSPIFFE(u.String())
			if err != nil {
				continue
			}
			return Peer{Kind: PeerService, ServiceID: id.ServiceID, InstanceID: id.InstanceID}, nil
		}
		return Peer{}, fmt.Errorf("%s: no URI SAN carries a ServiceBridge identity: %w", op, ErrPeerUnidentified)
	}

	if cert.Subject.CommonName == RuntimeCommonName {
		return Peer{Kind: PeerRuntime}, nil
	}
	return Peer{}, fmt.Errorf("%s: no URI SAN and CN %q is not the runtime: %w",
		op, cert.Subject.CommonName, ErrPeerUnidentified)
}

// PeerFromContext identifies the caller of the inbound gRPC call carried by ctx.
func PeerFromContext(ctx context.Context) (Peer, error) {
	const op = "rpc: peer from context"

	p, ok := peer.FromContext(ctx)
	if !ok {
		return Peer{}, fmt.Errorf("%s: call carries no peer: %w", op, ErrPeerUnidentified)
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return Peer{}, fmt.Errorf("%s: call is not over mTLS: %w", op, ErrPeerUnidentified)
	}
	if len(info.State.PeerCertificates) == 0 {
		return Peer{}, fmt.Errorf("%s: handshake presented no client certificate: %w", op, ErrPeerUnidentified)
	}
	return IdentifyPeer(info.State.PeerCertificates[0])
}

// Allow decides whether peer may invoke method on this instance.
//
// The runtime's proxy passes: the bilateral gate on the caller side has already
// run against the originating service, and re-deriving that decision here from
// an identity the proxy does not carry would only invent a second answer.
//
// A nil evaluation and an empty rule set both mean allow. That is not a
// fallback: no acceptance rule is the runtime stating that this service accepts
// everyone, and the SDK must not invent a stricter policy than the operator
// configured.
func Allow(peer Peer, method string, policy *pb.PolicyEvaluation) error {
	switch peer.Kind {
	case PeerRuntime:
		return nil
	case PeerService:
	default:
		return fmt.Errorf("rpc: acceptance: peer kind %s: %w", peer.Kind, ErrPeerUnidentified)
	}

	if policy == nil {
		return nil
	}

	matched := false
	for _, rule := range policy.GetAcceptance() {
		if rule.GetAction() != actionRPCHandle {
			continue
		}
		matched = true
		if ruleAdmits(rule, peer.ServiceID, method) {
			return nil
		}
	}
	if !matched {
		return nil
	}

	return fmt.Errorf("rpc: acceptance: caller %s on method %q: %w", peer.ServiceID, method, ErrAcceptanceDenied)
}

// ruleAdmits matches one acceptance rule. An empty peer_service_id is the
// runtime's wildcard for "any caller"; target_name "*" is its wildcard for "any
// method".
func ruleAdmits(rule *pb.PolicyRule, callerServiceID, method string) bool {
	peerOK := rule.GetPeerServiceId() == "" || rule.GetPeerServiceId() == callerServiceID
	methodOK := rule.GetTargetName() == wildcardTarget || rule.GetTargetName() == method
	return peerOK && methodOK
}
