import type { PeerCertificate } from "node:tls";
export const SPIFFE_TRUST_DOMAIN = "service-bridge";
export const RUNTIME_SPIFFE_URI = `spiffe://${SPIFFE_TRUST_DOMAIN}/runtime`;

// Node TLS has already checked the chain and server EKU. Require exactly one
// URI identity: another leaf signed by our CA is not the expected role/peer.
export function makeSpiffeCheck(
	expectedUri: string,
): (hostname: string, cert: PeerCertificate) => Error | undefined {
	return (_hostname, cert) => {
		const uris = (cert.subjectaltname ?? "")
			.split(",")
			.map((value) => value.trim())
			.filter((value) => value.startsWith("URI:"))
			.map((value) => value.slice(4));
		if (uris.length !== 1 || uris[0] !== expectedUri)
			return new Error(
				`TLS SPIFFE mismatch: expected ${expectedUri}, got ${uris.join(", ") || "<none>"}`,
			);
		return undefined;
	};
}
