// Bootstrap key format:
//   sb.<base64url(proto.Marshal(BootstrapKeyPayload))>
//
// The CA cert is embedded in the key so the SDK can construct mTLS channel
// credentials without a separate trust-on-first-use step (which is unreliable
// on runtimes that don't expose the full cert chain through node:tls, e.g.
// Bun's getPeerCertificate).

import { BootstrapKeyPayload } from "../pb/servicebridge/v1/bootstrap";

const PREFIX = "sb.";

export interface BootstrapKey {
	keyID: Buffer;
	secret: Buffer;
	caCertDer: Buffer;
}

/**
 * Parses a ServiceBridge bootstrap key string into its components.
 * Throws on malformed input (wrong prefix, truncated, invalid base64url,
 * or proto message with empty required fields).
 */
export function parseBootstrapKey(raw: string): BootstrapKey {
	if (!raw.startsWith(PREFIX)) {
		throw new Error(`bootstrap key: missing "sb." prefix`);
	}
	const bytes = Buffer.from(raw.slice(PREFIX.length), "base64url");
	const msg = BootstrapKeyPayload.decode(bytes);
	if (!msg.keyId || msg.keyId.length === 0) {
		throw new Error("bootstrap key: key_id is empty");
	}
	if (!msg.secret || msg.secret.length === 0) {
		throw new Error("bootstrap key: secret is empty");
	}
	if (!msg.caCertDer || msg.caCertDer.length === 0) {
		throw new Error("bootstrap key: ca_cert_der is empty");
	}
	return {
		keyID: Buffer.from(msg.keyId),
		secret: Buffer.from(msg.secret),
		caCertDer: Buffer.from(msg.caCertDer),
	};
}
