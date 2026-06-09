/**
 * derToPem wraps a DER buffer in PEM (label defaults to "CERTIFICATE").
 */
export function derToPem(der: Buffer, label = "CERTIFICATE"): Buffer {
	const b64 = der.toString("base64");
	const lines = b64.match(/.{1,64}/g) ?? [];
	return Buffer.from(
		`-----BEGIN ${label}-----\n${lines.join("\n")}\n-----END ${label}-----\n`,
	);
}
