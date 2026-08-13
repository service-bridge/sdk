import { createHash } from "node:crypto";
import protobuf from "protobufjs";
import type { SchemaPair, Serializer } from "./serializer";

// HASH_PREFIX tags the wire descriptor algorithm. It travels inside the hash
// string so a mesh running mixed algorithm generations never matches a hash
// computed under different rules.
const HASH_PREFIX = "v2";

type Cardinality = "opt" | "rep" | "map" | "req";

interface FieldDescriptor {
	n: number;
	c: Cardinality;
	t: string;
	k?: string;
}

interface OneofDescriptor {
	f: number[];
}

interface MessageDescriptor {
	f: FieldDescriptor[];
	o?: OneofDescriptor[];
}

interface EnumDescriptor {
	e: number[];
}

type ReferencedType = MessageDescriptor | EnumDescriptor;

// wireDescriptors holds the canonical wire descriptor of every Serializer that
// serde builds, keyed by identity. It is a side table rather than a Serializer
// member because the descriptor describes the protobuf Type behind the
// serializer, not the encode/decode contract other domains depend on.
const wireDescriptors = new WeakMap<object, string>();

// attachWireDescriptor binds a serializer to the canonical descriptor of its
// message. Called by every serializer factory in this module's domain.
//
// @internal — см. ./README.md
export function attachWireDescriptor(
	serializer: Serializer,
	type: protobuf.Type,
): void {
	wireDescriptors.set(serializer, canonicalMessageDescriptor(type));
}

// computeContractHash returns the routing identity of an (input, output) pair.
//
//	contractHash = "v2:" + hex(sha256(canon(input) + ":" + canon(output)))
//
// canon() is the language-neutral wire descriptor built by
// canonicalMessageDescriptor: field numbers, cardinality and types only. Field
// names are excluded, so a rename — wire-compatible in protobuf — keeps traffic
// flowing, while a number, type or cardinality change routes to a new identity.
//
// This is the SINGLE source of truth for contract hashing across the SDK.
// Runtime stores the hash opaque and does NOT recompute it.
//
// @public — см. ./README.md
export function computeContractHash(pair: SchemaPair): string {
	const input = wireDescriptor(pair.input);
	const output = wireDescriptor(pair.output);
	const digest = createHash("sha256")
		.update(`${input}:${output}`)
		.digest("hex");
	return `${HASH_PREFIX}:${digest}`;
}

// wireDescriptor returns the canonical descriptor string a serializer was built
// with — the exact bytes that feed the contract hash.
//
// @internal — см. ./README.md
export function wireDescriptor(serializer: Serializer): string {
	const descriptor = wireDescriptors.get(serializer);
	if (descriptor === undefined) {
		throw new Error(
			"serde: serializer has no wire descriptor — build the pair through buildSchemaPair",
		);
	}
	return descriptor;
}

// canonicalMessageDescriptor renders a protobuf message as the canonical JSON
// string that feeds the contract hash. Shape:
//
//	{"f":[{"n":1,"c":"opt","t":"string"}],
//	 "o":[{"f":[4,5]}],
//	 "r":{".pkg.Item":{"f":[...]},".pkg.Kind":{"e":[0,1]}}}
//
// `o` is omitted when the message has no oneof group, `r` when it references no
// other type. Full type names carry a leading dot.
//
// @internal — см. ./README.md
export function canonicalMessageDescriptor(type: protobuf.Type): string {
	const referenced: Record<string, ReferencedType> = {};
	const shape = messageShape(type, referenced);
	const doc: Record<string, unknown> = { f: shape.f };
	if (shape.o) {
		doc.o = shape.o;
	}
	if (Object.keys(referenced).length > 0) {
		doc.r = referenced;
	}
	return canonicalize(doc);
}

function messageShape(
	type: protobuf.Type,
	referenced: Record<string, ReferencedType>,
): MessageDescriptor {
	const fields = type.fieldsArray
		.map((field) => fieldDescriptor(field, referenced))
		.sort((a, b) => a.n - b.n);

	const oneofs = type.oneofsArray
		// A proto3 `optional` field is a single-member synthetic oneof in the
		// descriptor model; on the wire it is a plain singular field.
		.filter((oneof) => !oneof.isProto3Optional)
		.map((oneof) => ({
			f: oneof.fieldsArray.map((field) => field.id).sort((a, b) => a - b),
		}))
		.filter((group) => group.f.length > 0)
		.sort((a, b) => (a.f[0] as number) - (b.f[0] as number));

	const shape: MessageDescriptor = { f: fields };
	if (oneofs.length > 0) {
		shape.o = oneofs;
	}
	return shape;
}

function fieldDescriptor(
	field: protobuf.Field,
	referenced: Record<string, ReferencedType>,
): FieldDescriptor {
	try {
		field.resolve();
	} catch (err) {
		throw new Error(
			`serde: resolve field ${field.fullName}: ${(err as Error).message}`,
		);
	}

	const t = typeRef(field, referenced);
	if (field instanceof protobuf.MapField) {
		return { n: field.id, c: "map", k: field.keyType, t };
	}
	const c: Cardinality = field.repeated
		? "rep"
		: field.required
			? "req"
			: "opt";
	return { n: field.id, c, t };
}

function typeRef(
	field: protobuf.Field,
	referenced: Record<string, ReferencedType>,
): string {
	const resolved = field.resolvedType;
	if (resolved === null) {
		return field.type;
	}

	const name = resolved.fullName;
	if (resolved instanceof protobuf.Enum) {
		if (!(name in referenced)) {
			referenced[name] = { e: enumNumbers(resolved) };
		}
		return `e:${name}`;
	}

	if (!(name in referenced)) {
		// Claim the slot before recursing: a cyclic reference resolves by name
		// against this placeholder instead of recursing forever.
		referenced[name] = { f: [] };
		referenced[name] = messageShape(resolved as protobuf.Type, referenced);
	}
	return `m:${name}`;
}

function enumNumbers(enm: protobuf.Enum): number[] {
	return [...new Set(Object.values(enm.values))].sort((a, b) => a - b);
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
