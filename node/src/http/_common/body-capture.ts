// body-capture.ts — serialize an HTTP request/response body into the raw-JSON
// payload bytes the runtime stores verbatim (mirrors runtime
// telemetry.ContractRawJSON). HTTP bodies have no proto schema, so they are
// captured as-is and rendered without proto decoding.
// @internal — см. ../README.md

/** Contract-hash marker for already-JSON payloads. Must equal the runtime's
 *  telemetry.ContractRawJSON so DecodePayload returns the bytes verbatim. */
export const RAW_JSON_CONTRACT = "raw/json";

const encoder = new TextEncoder();

/**
 * Best-effort serialize an HTTP body to bytes for payload capture. Returns
 * null when there is nothing worth capturing (empty / `{}` / unserializable),
 * so bodyless requests (e.g. GET) don't emit empty Input payloads.
 */
export function bodyToBytes(body: unknown): Uint8Array | null {
	if (body === undefined || body === null) return null;
	if (body instanceof Uint8Array) return body.byteLength ? body : null;
	if (typeof body === "string") {
		const s = body.trim();
		if (!s || s === "{}" || s === "null") return null;
		return encoder.encode(body);
	}
	try {
		const json = JSON.stringify(body);
		if (!json || json === "{}" || json === "null") return null;
		return encoder.encode(json);
	} catch {
		return null;
	}
}
