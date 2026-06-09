// Canonical JSON serializer for workflow graph fingerprinting (ADR-W-002).
//
// Rules:
//   - Object keys sorted lexicographically (Unicode codepoint order).
//   - No whitespace.
//   - Arrays preserve order (semantic — DAG steps).
//   - Numbers, booleans, null, strings — JSON.stringify defaults.
//   - `undefined` properties and functions are omitted (functions appear in
//     LocalStep.fn — they are not part of the graph fingerprint).
//
// Fingerprint = sha256(canonical_bytes), hex-encoded.
//
// @internal — см. ./README.md

import { createHash } from "node:crypto";

export function canonicalize(value: unknown): string {
	return stringify(value);
}

export function fingerprint(value: unknown): string {
	const canonical = canonicalize(value);
	return createHash("sha256").update(canonical).digest("hex");
}

function stringify(v: unknown): string {
	if (v === null) return "null";
	const t = typeof v;
	if (t === "string") return JSON.stringify(v);
	if (t === "number") {
		if (!Number.isFinite(v)) {
			throw new Error(
				`workflow/canonical: non-finite number not representable in JSON: ${String(v)}`,
			);
		}
		return JSON.stringify(v);
	}
	if (t === "boolean") return v ? "true" : "false";
	if (t === "undefined" || t === "function") {
		// undefined / function never appear as JSON values; callers must
		// strip them before serialization. The case is reached only for
		// element-of-array contexts.
		return "null";
	}
	if (Array.isArray(v)) {
		return `[${v.map((x) => stringify(stripUnencodable(x))).join(",")}]`;
	}
	// object
	const obj = v as Record<string, unknown>;
	const keys = Object.keys(obj)
		.filter((k) => {
			const val = obj[k];
			return val !== undefined && typeof val !== "function";
		})
		.sort();
	const parts: string[] = [];
	for (const k of keys) {
		parts.push(`${JSON.stringify(k)}:${stringify(obj[k])}`);
	}
	return `{${parts.join(",")}}`;
}

function stripUnencodable(x: unknown): unknown {
	return x === undefined || typeof x === "function" ? null : x;
}
