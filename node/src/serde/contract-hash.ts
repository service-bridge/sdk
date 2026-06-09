import { createHash } from "node:crypto";
import type { SchemaPair, Serializer } from "./serializer";

// computeContractHash returns the canonical SHA-256 (hex) of a SchemaPair.
// The hash identifies an exact (input, output) schema combination — caller
// and callee that load the SAME schema source MUST produce the SAME hash.
//
// Algorithm:
//   1. Take input.toJsonSchema() and output.toJsonSchema() — both are
//      protobufjs Type descriptors (deterministic structure).
//   2. Canonicalize each (sorted keys, no whitespace) into a JSON string.
//      Arrays preserve order; objects sort by key recursively.
//   3. Concatenate as `<input_canonical>:<output_canonical>`.
//   4. SHA-256, hex-encode.
//
// This is the SINGLE source of truth for contract hashing across the SDK.
// Runtime stores the hash opaque and does NOT recompute (ADR 0005, variant 3).
//
// @public — см. ./README.md
export function computeContractHash(pair: SchemaPair): string {
	const inputCanonical = canonicalize(pair.input.toJsonSchema());
	const outputCanonical = canonicalize(pair.output.toJsonSchema());
	return createHash("sha256")
		.update(`${inputCanonical}:${outputCanonical}`)
		.digest("hex");
}

// computeSerializerHash hashes a single Serializer (used when only one side
// of the pair is relevant — e.g. event schemas in future branches).
export function computeSerializerHash(s: Serializer): string {
	const canonical = canonicalize(s.toJsonSchema());
	return createHash("sha256").update(canonical).digest("hex");
}

// canonicalize produces the deterministic JSON string used for hashing.
// Rules:
//   - null / primitives → JSON.stringify (standard)
//   - arrays → preserve element order, recurse on each element
//   - objects → sort keys lexicographically, recurse on each value
//   - no whitespace, no trailing commas
//
// @internal — см. ./README.md
export function canonicalize(value: unknown): string {
	if (value === null || typeof value !== "object") {
		return JSON.stringify(value);
	}
	if (Array.isArray(value)) {
		return `[${value.map(canonicalize).join(",")}]`;
	}
	const obj = value as Record<string, unknown>;
	const keys = Object.keys(obj).sort();
	return `{${keys
		.map((k) => `${JSON.stringify(k)}:${canonicalize(obj[k])}`)
		.join(",")}}`;
}
