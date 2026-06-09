import "reflect-metadata";
import * as x509 from "@peculiar/x509";
import { SPIFFE_TRUST_DOMAIN } from "../connection/spiffe";
import type { PolicyEvaluation } from "../pb/servicebridge/v1/registry";

// SpiffeIdentity is the parsed SPIFFE URI for a ServiceBridge peer.
export interface SpiffeIdentity {
	serviceId: string;
	instanceId: string;
}

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

// extractSpiffeServiceId extracts the caller's service UUID from a
// PeerCertificate-like object. Tries both `subjectaltname` (Node TLS short
// form) and `raw` (DER buffer) — Node sometimes leaves `subjectaltname` empty
// for URI SAN-only certs, in which case we parse the DER ourselves via
// @peculiar/x509 (the SubjectAlternativeName extension carries the URI).
export function extractSpiffeServiceId(cert: {
	subjectaltname?: string;
	raw?: Buffer | Uint8Array;
}): string | null {
	const altName = cert.subjectaltname;
	if (altName) {
		for (const part of altName.split(",")) {
			const trimmed = part.trim();
			if (!trimmed.startsWith("URI:")) continue;
			const uri = trimmed.slice(4);
			const id = parsePeerSpiffeUri(uri);
			if (id) return id.serviceId;
		}
	}
	// Node TLS sometimes leaves `subjectaltname` empty when the SAN extension
	// contains only URI entries — fall through to parsing the raw DER.
	if (cert.raw) {
		try {
			const parsed = new x509.X509Certificate(cert.raw);
			const sanExt = parsed.getExtension(x509.SubjectAlternativeNameExtension);
			if (sanExt) {
				for (const name of sanExt.names.items) {
					if (name.type === "url") {
						const id = parsePeerSpiffeUri(name.value);
						if (id) return id.serviceId;
					}
				}
			}
		} catch {
			// fall through
		}
	}
	return null;
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
// Fail-closed when peerCertAccessorOk = false (smoke-test failed at startup).
// Default-allow when policy is null or has no rpc.handle rules.
export function evaluatePeerAcceptance(
	peerCertAccessorOk: boolean,
	policy: PolicyEvaluation | null,
	call: object,
	methodName: string,
): string | null {
	if (!peerCertAccessorOk) {
		return "rpc: peer-cert accessor broken — direct calls rejected (fail-closed)";
	}
	if (!policy) return null;
	const rpcHandleRules = policy.acceptance.filter(
		(r) => r.action === "rpc.handle",
	);
	if (rpcHandleRules.length === 0) return null;
	const cert = getPeerCertFromCall(call);
	if (!cert) {
		return "rpc: could not extract peer certificate from direct call";
	}
	const callerServiceId = extractSpiffeServiceId(cert);
	if (!callerServiceId) {
		// No SPIFFE service URI ⇒ peer is the runtime proxying a call. The
		// runtime has already enforced caller-side gate #3 against the
		// originating service. Trust that and let the call through. (Local
		// callee identity is only meaningful for direct SDK→SDK connections,
		// where the leaf cert carries a SPIFFE URI.)
		return null;
	}
	if (!checkAcceptance(callerServiceId, methodName, policy)) {
		return `rpc: acceptance denied for caller ${callerServiceId} method ${methodName}`;
	}
	return null;
}

// getPeerCertFromCall extracts the peer TLS certificate from a grpc-js server
// call. Uses the public `getAuthContext()` API exposed on ServerUnaryCall /
// ServerWritableStream — internally grpc-js reads it via
// `stream.session.socket.getPeerCertificate()` (server-interceptors.js:785).
// Returns null if the call is not over TLS or the cert is unavailable.
//
// Test helpers may pass a plain object exposing the same legacy private path
// `call.handler.session.socket.getPeerCertificate()`; that fallback is kept
// for unit tests where we cannot construct a real grpc-js call.
export function getPeerCertFromCall(
	call: object,
): { subjectaltname?: string; [k: string]: unknown } | null {
	try {
		// Walk to the underlying TLS socket through the documented grpc-js layout:
		//   ServerUnaryCall.call → BaseServerInterceptingCall.stream → http2 stream
		//   → session.socket (TLSSocket).
		// Test helpers may instead use the legacy `call.handler.session.socket`
		// shape — we accept both. getPeerCertificate(true) returns the detailed
		// form which includes `subjectaltname`.
		const candidates: unknown[] = [];
		const outer = call as Record<string, unknown>;
		const inner = outer.call as Record<string, unknown> | undefined;
		if (inner) {
			const stream = inner.stream as Record<string, unknown> | undefined;
			if (stream?.session) candidates.push(stream.session);
			const handler = inner.handler as Record<string, unknown> | undefined;
			if (handler?.session) candidates.push(handler.session);
		}
		// Also try outer.stream / outer.session for completeness.
		const directStream = outer.stream as Record<string, unknown> | undefined;
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
				return cert as { subjectaltname?: string };
			}
		}

		// Last-resort fallback for getAuthContext-based mocks.
		const c = call as {
			getAuthContext?: () => {
				sslPeerCertificate?: { subjectaltname?: string };
			};
		};
		if (typeof c.getAuthContext === "function") {
			const ctx = c.getAuthContext();
			if (ctx?.sslPeerCertificate) return ctx.sslPeerCertificate;
		}
		return null;
	} catch {
		return null;
	}
}
