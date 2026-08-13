import "reflect-metadata";
import * as x509 from "@peculiar/x509";
import { SPIFFE_TRUST_DOMAIN } from "../connection/spiffe";
import type { PolicyEvaluation } from "../pb/servicebridge/v1/registry";

// RUNTIME_PEER_COMMON_NAME is the subject CN the runtime puts on the cert it
// presents when it proxies a call to an SDK callee (tlsca.issueServerCert).
// That cert carries no SAN at all, whereas every SDK leaf (tlsca.Issue) carries
// a SPIFFE URI SAN — so "no URI SAN + this CN" positively identifies the
// runtime instead of guessing from a failed parse.
export const RUNTIME_PEER_COMMON_NAME = "servicebridge-runtime";

// SpiffeIdentity is the parsed SPIFFE URI for a ServiceBridge peer.
export interface SpiffeIdentity {
	serviceId: string;
	instanceId: string;
}

// PeerIdentity is the outcome of classifying a peer certificate.
//   service — a ServiceBridge SDK instance, identified by SPIFFE URI SAN
//   runtime — the runtime proxying a call on behalf of an originating service
//   unknown — identity could not be established; the caller must reject
export type PeerIdentity =
	| { kind: "service"; serviceId: string }
	| { kind: "runtime" }
	| { kind: "unknown"; reason: string };

// parsePeerSpiffeUri parses a full SPIFFE URI of the form:
// spiffe://service-bridge/service/<serviceId>/instance/<instanceId>
// Returns null for any other URI.
export function parsePeerSpiffeUri(uri: string): SpiffeIdentity | null {
	const prefix = `spiffe://${SPIFFE_TRUST_DOMAIN}/service/`;
	if (!uri.startsWith(prefix)) return null;
	const rest = uri.slice(prefix.length);
	const parts = rest.split("/instance/");
	if (parts.length !== 2 || !parts[0] || !parts[1]) return null;
	return { serviceId: parts[0], instanceId: parts[1] };
}

// PeerCertLike is the subset of Node's TLS PeerCertificate this module reads.
export interface PeerCertLike {
	subjectaltname?: string;
	subject?: { CN?: string };
	raw?: Buffer | Uint8Array;
}

interface CertFacts {
	uriSans: string[];
	commonName: string;
}

// uriSansFromAltName pulls the URI entries out of Node's flattened
// `subjectaltname` string (e.g. "URI:spiffe://…, DNS:host").
function uriSansFromAltName(altName: string): string[] {
	const out: string[] = [];
	for (const part of altName.split(",")) {
		const trimmed = part.trim();
		if (trimmed.startsWith("URI:")) out.push(trimmed.slice(4));
	}
	return out;
}

// certFactsFromDer parses the DER form, which is authoritative: Node leaves
// `subjectaltname` empty for certs whose SAN holds only URI entries, so the
// string form cannot distinguish "no URI SAN" from "URI SAN not surfaced".
// Returns null when the DER cannot be parsed at all.
function certFactsFromDer(raw: Buffer | Uint8Array): CertFacts | null {
	try {
		const parsed = new x509.X509Certificate(raw);
		const uriSans: string[] = [];
		const sanExt = parsed.getExtension(x509.SubjectAlternativeNameExtension);
		if (sanExt) {
			for (const name of sanExt.names.items) {
				if (name.type === "url") uriSans.push(name.value);
			}
		}
		const cn = parsed.subjectName.getField("CN")[0] ?? "";
		return { uriSans, commonName: cn };
	} catch {
		return null;
	}
}

// classifyPeer establishes who the TLS peer is. The chain itself is already
// verified by the TLS layer (createSsl with the CA chain and
// checkClientCertificate=true), so this only resolves identity — it never
// decides trust on its own.
//
// A cert that asserts a URI SAN we cannot resolve to a ServiceBridge SPIFFE
// identity is `unknown`, not "probably the runtime": treating a parse failure
// as the runtime would be fail-open on exactly the input an attacker controls.
export function classifyPeer(cert: PeerCertLike): PeerIdentity {
	let facts: CertFacts;
	if (cert.raw) {
		const fromDer = certFactsFromDer(cert.raw);
		if (!fromDer) {
			return { kind: "unknown", reason: "peer certificate DER is unparseable" };
		}
		facts = fromDer;
	} else {
		facts = {
			uriSans: cert.subjectaltname
				? uriSansFromAltName(cert.subjectaltname)
				: [],
			commonName: cert.subject?.CN ?? "",
		};
	}

	for (const uri of facts.uriSans) {
		const id = parsePeerSpiffeUri(uri);
		if (id) return { kind: "service", serviceId: id.serviceId };
	}
	if (facts.uriSans.length > 0) {
		return {
			kind: "unknown",
			reason:
				"peer certificate carries a URI SAN that is not a ServiceBridge SPIFFE identity",
		};
	}
	if (facts.commonName === RUNTIME_PEER_COMMON_NAME) {
		return { kind: "runtime" };
	}
	return {
		kind: "unknown",
		reason: "peer certificate has no SPIFFE URI SAN and is not the runtime",
	};
}

// checkAcceptance evaluates whether callerServiceId may call methodName on
// this service, based on the service's current PolicyEvaluation.acceptance
// rules. Default-allow when acceptance rules are empty.
//
// Rule matching:
//   - peerServiceId == "" → wildcard (any caller)
//   - targetName == "*"   → any method
//   - otherwise exact match on both fields
export function checkAcceptance(
	callerServiceId: string,
	methodName: string,
	policy: PolicyEvaluation,
): boolean {
	const rules = policy.acceptance.filter((r) => r.action === "rpc.handle");
	if (rules.length === 0) return true; // default-allow

	for (const rule of rules) {
		const peerMatch =
			rule.peerServiceId === "" || rule.peerServiceId === callerServiceId;
		const methodMatch =
			rule.targetName === "*" || rule.targetName === methodName;
		if (peerMatch && methodMatch) return true;
	}
	return false;
}

// evaluatePeerAcceptance is the pure decision function used by CallServer for
// every incoming direct call. Returns a denial reason string when the call
// should be rejected, or null when it should proceed.
//
// Default-allow when policy is null or has no rpc.handle rules. Once rules
// exist the check is fail-closed: any peer whose identity cannot be established
// is rejected.
export function evaluatePeerAcceptance(
	policy: PolicyEvaluation | null,
	call: object,
	methodName: string,
): string | null {
	if (!policy) return null;
	const rpcHandleRules = policy.acceptance.filter(
		(r) => r.action === "rpc.handle",
	);
	if (rpcHandleRules.length === 0) return null;
	const cert = getPeerCertFromCall(call);
	if (!cert) {
		return "rpc: could not extract peer certificate from direct call";
	}
	const peer = classifyPeer(cert);
	if (peer.kind === "unknown") {
		return `rpc: peer identity not established — ${peer.reason}`;
	}
	if (peer.kind === "runtime") {
		// The runtime already enforced caller-side gate #3 against the originating
		// service. Local acceptance rules only describe direct SDK→SDK peers.
		return null;
	}
	if (!checkAcceptance(peer.serviceId, methodName, policy)) {
		return `rpc: acceptance denied for caller ${peer.serviceId} method ${methodName}`;
	}
	return null;
}

// getPeerCertFromCall extracts the peer TLS certificate from a grpc-js server
// call. Primary path is the public `getAuthContext()` on ServerUnaryCall /
// ServerWritableStream, which grpc-js implements by reading the TLS socket and
// which only populates `sslPeerCertificate` when the cert has a `raw` DER
// (server-interceptors.js BaseServerInterceptingCall.getAuthContext).
//
// The walk to `call.call.stream.session.socket` is the same socket reached
// directly; it is kept because `getAuthContext()` returns `{}` for any call
// whose socket grpc-js does not recognize as a TLSSocket.
// Returns null if the call is not over TLS or the cert is unavailable.
export function getPeerCertFromCall(call: object): PeerCertLike | null {
	try {
		const outer = call as {
			getAuthContext?: () => { sslPeerCertificate?: PeerCertLike } | null;
			call?: Record<string, unknown>;
			stream?: Record<string, unknown>;
		};

		if (typeof outer.getAuthContext === "function") {
			const ctx = outer.getAuthContext();
			if (ctx?.sslPeerCertificate) return ctx.sslPeerCertificate;
		}

		// ServerUnaryCall.call → BaseServerInterceptingCall.stream → http2 stream
		// → session.socket (TLSSocket). getPeerCertificate(true) returns the
		// detailed form, which carries `raw` and `subject`.
		const candidates: unknown[] = [];
		const inner = outer.call;
		if (inner) {
			const stream = inner.stream as Record<string, unknown> | undefined;
			if (stream?.session) candidates.push(stream.session);
		}
		const directStream = outer.stream;
		if (directStream?.session) candidates.push(directStream.session);

		for (const session of candidates) {
			const s = session as Record<string, unknown>;
			const socket = s.socket as Record<string, unknown> | undefined;
			if (!socket) continue;
			const getCert = socket.getPeerCertificate;
			if (typeof getCert !== "function") continue;
			const cert = (getCert as (detailed?: boolean) => unknown).call(
				socket,
				true,
			);
			if (cert && typeof cert === "object") {
				return cert as PeerCertLike;
			}
		}

		return null;
	} catch {
		return null;
	}
}
