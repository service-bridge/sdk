import protobuf from "protobufjs";
import { attachWireDescriptor } from "./contract-hash";
import {
	buildSchemaPairFromJsonFile,
	type JsonSchemaFileSpec,
} from "./json-schema-serde";

// ProtoFileSpec references a user .proto file. `input`/`output` are optional —
// when omitted, the SDK resolves them from the file's `service` definition by
// matching the registered method name (gRPC-native, the only supported path).
// Pass `input`/`output` explicitly for .proto files without a service block.
export interface ProtoFileSpec {
	protoFile: string;
	// Optional: explicit message names. Required only when the .proto file has
	// no matching `service { rpc <method>(In) returns (Out); }` entry.
	input?: string;
	output?: string;
	// Optional: method name to resolve against the service definition. Defaults
	// to the name passed to `Handle.rpc`/`stream` (set automatically by Handle).
	method?: string;
}

// SchemaSpec is the public type for handler/caller schema declaration.
// Two sources supported: .proto file or .schema.json file (with explicit
// fieldNumber per property). See ./README.md and ./json-schema-serde.ts.
export type SchemaSpec = ProtoFileSpec | JsonSchemaFileSpec;

function isProtoFileSpec(spec: SchemaSpec): spec is ProtoFileSpec {
	return (spec as ProtoFileSpec).protoFile !== undefined;
}

// Serializer encodes/decodes a single Protobuf message type at runtime,
// produced from a user-supplied .proto file via protobufjs.
export interface Serializer {
	encode(value: unknown): Uint8Array;
	decode(bytes: Uint8Array): unknown;
	toJsonSchema(): Record<string, unknown>;
}

export interface SchemaPair {
	input: Serializer;
	output: Serializer;
}

// buildSchemaPair dispatches to the correct loader based on SchemaSpec kind.
// .proto files load via protobufjs.load (async); .schema.json files via
// json-schema-serde (sync). Result is always a SchemaPair with stable contract
// hashes.
export async function buildSchemaPair(spec: SchemaSpec): Promise<SchemaPair> {
	if (isProtoFileSpec(spec)) {
		let root: protobuf.Root;
		try {
			root = await protobuf.load(spec.protoFile);
		} catch (err) {
			throw new Error(
				`serde: load proto file ${spec.protoFile}: ${(err as Error).message}`,
			);
		}
		const { input, output } = resolveProtoMessages(root, spec);
		return {
			input: typeToSerializer(root, spec.protoFile, input),
			output: typeToSerializer(root, spec.protoFile, output),
		};
	}
	// JsonSchemaFileSpec — synchronous file read + dynamic Type build.
	return buildSchemaPairFromJsonFile(spec);
}

// resolveProtoMessages picks input/output message names from a ProtoFileSpec.
// Resolution order:
//   1. Explicit spec.input + spec.output → used as-is.
//   2. spec.method provided + service { rpc <method>(In) returns (Out); } found
//      → take requestType/responseType from the service definition.
//
// Throws with a clear error otherwise — convention-based and unique-pair
// fallbacks were removed (ADR 0001 / cleanup #32) because they hid contract
// mistakes behind name similarity instead of failing loudly.
function resolveProtoMessages(
	root: protobuf.Root,
	spec: ProtoFileSpec,
): { input: string; output: string } {
	if (spec.input && spec.output) {
		return { input: spec.input, output: spec.output };
	}

	const method = spec.method;
	if (method) {
		const fromService = findInService(root, method);
		if (fromService) return fromService;
	}

	throw new Error(
		`serde: cannot resolve input/output for ${spec.protoFile}` +
			(method ? ` (method=${method})` : "") +
			". Add a `service { rpc <method>(In) returns (Out); }` block or pass `input` and `output` explicitly.",
	);
}

function findInService(
	root: protobuf.Root,
	method: string,
): { input: string; output: string } | null {
	let found: { input: string; output: string } | null = null;
	walk(root, (obj) => {
		if (!(obj instanceof protobuf.Service)) return;
		const m = obj.methods[method];
		if (m) {
			found = { input: m.requestType, output: m.responseType };
		}
	});
	return found;
}

function walk(
	obj: protobuf.NamespaceBase,
	fn: (item: protobuf.ReflectionObject) => void,
): void {
	fn(obj);
	const nested = obj.nested;
	if (!nested) return;
	for (const child of Object.values(nested)) {
		if (child instanceof protobuf.Namespace) {
			walk(child, fn);
		} else {
			fn(child);
		}
	}
}

function typeToSerializer(
	root: protobuf.Root,
	file: string,
	messageName: string,
): Serializer {
	let type: protobuf.Type;
	try {
		type = root.lookupType(messageName);
	} catch (err) {
		throw new Error(
			`serde: message ${messageName} not found in ${file}: ${(err as Error).message}`,
		);
	}

	const jsonSchema = type.toJSON() as unknown as Record<string, unknown>;

	const serializer: Serializer = {
		encode(value: unknown): Uint8Array {
			const err = type.verify(value as object);
			if (err) {
				throw new Error(`serde: encode ${messageName}: ${err}`);
			}
			const msg = type.fromObject(value as object);
			return type.encode(msg).finish();
		},
		decode(bytes: Uint8Array): unknown {
			const msg = type.decode(bytes);
			// Without longs, a 64-bit field decodes to a {low, high, unsigned}
			// object, so arithmetic on it silently yields NaN and a Go peer sending
			// the same field reads back a number. Values past 2^53 lose precision
			// here — a narrower failure than every 64-bit field being unusable.
			return type.toObject(msg, { defaults: true, longs: Number });
		},
		toJsonSchema(): Record<string, unknown> {
			return jsonSchema;
		},
	};
	attachWireDescriptor(serializer, type);
	return serializer;
}
