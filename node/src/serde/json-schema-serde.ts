import { readFileSync } from "node:fs";
import protobuf from "protobufjs";
import { attachWireDescriptor } from "./contract-hash";
import type { SchemaPair, Serializer } from "./serializer";

// JsonSchemaFileSpec references a .schema.json file describing both input and
// output messages with explicit field numbers per property. Unlike inline JSON
// schema (removed permanently), this format is safe for schema evolution
// because field numbers are pinned by the schema author.
export interface JsonSchemaFileSpec {
	schemaFile: string;
}

// .schema.json file shape:
//   {
//     "input":  { "<MessageName>": { "<field>": { type, fieldNumber }, ... } },
//     "output": { "<MessageName>": { ... } }
//   }
interface SchemaFile {
	input: Record<string, SchemaFields>;
	output: Record<string, SchemaFields>;
}

type SchemaFields = Record<string, SchemaField>;

interface SchemaField {
	type: ProtoFieldType;
	fieldNumber?: number;
	// For type="array": the element type and its own field number rules.
	// The repeated field itself uses the outer `fieldNumber`.
	array?: { type: ProtoFieldType; nested?: SchemaFields };
	// For type="object": nested message fields with their own field numbers.
	nested?: SchemaFields;
}

type ProtoFieldType =
	| "string"
	| "bool"
	| "int32"
	| "int64"
	| "uint32"
	| "uint64"
	| "float"
	| "double"
	| "bytes"
	| "object"
	| "array";

// buildSchemaPairFromJsonFile loads the schema file, validates field numbers,
// constructs protobufjs Types dynamically, and returns a SchemaPair compatible
// with the rest of the rpc/serde stack.
export function buildSchemaPairFromJsonFile(
	spec: JsonSchemaFileSpec,
): SchemaPair {
	let raw: string;
	try {
		raw = readFileSync(spec.schemaFile, "utf8");
	} catch (err) {
		throw new Error(
			`serde: read schema file ${spec.schemaFile}: ${(err as Error).message}`,
		);
	}

	let parsed: SchemaFile;
	try {
		parsed = JSON.parse(raw) as SchemaFile;
	} catch (err) {
		throw new Error(
			`serde: parse schema file ${spec.schemaFile}: ${(err as Error).message}`,
		);
	}

	if (!parsed.input || !parsed.output) {
		throw new Error(
			`serde: schema file ${spec.schemaFile} must have top-level "input" and "output" objects`,
		);
	}

	const root = new protobuf.Root();
	const inputType = buildType(root, "Input", firstMessage(parsed.input));
	const outputType = buildType(root, "Output", firstMessage(parsed.output));
	root.add(inputType);
	root.add(outputType);
	// The wire descriptor walks resolvedType, which only exists after the root
	// links every field reference to its target.
	root.resolveAll();

	return {
		input: typeToSerializer(inputType),
		output: typeToSerializer(outputType),
	};
}

// firstMessage extracts the single message definition from { "<MessageName>": fields }.
// Throws if the section is empty or has more than one entry — exactly one
// message per input/output section is supported.
function firstMessage(section: Record<string, SchemaFields>): SchemaFields {
	const names = Object.keys(section);
	if (names.length !== 1) {
		throw new Error(
			`serde: each of input/output must declare exactly one message, got ${names.length}`,
		);
	}
	return section[names[0]!]!;
}

// buildType recursively constructs a protobufjs.Type from a SchemaFields tree.
function buildType(
	root: protobuf.Root,
	typeName: string,
	fields: SchemaFields,
): protobuf.Type {
	const type = new protobuf.Type(typeName);
	const seenNumbers = new Set<number>();

	for (const [name, field] of Object.entries(fields)) {
		if (typeof field.fieldNumber !== "number") {
			throw new Error(
				`serde: field "${name}" missing required fieldNumber — schema evolution requires explicit numbers`,
			);
		}
		if (seenNumbers.has(field.fieldNumber)) {
			throw new Error(
				`serde: duplicate fieldNumber ${field.fieldNumber} in ${typeName}`,
			);
		}
		seenNumbers.add(field.fieldNumber);

		if (field.type === "object") {
			if (!field.nested) {
				throw new Error(
					`serde: field "${name}" type=object requires nested fields`,
				);
			}
			const nestedTypeName = `${typeName}_${name}`;
			const nestedType = buildType(root, nestedTypeName, field.nested);
			root.add(nestedType);
			type.add(new protobuf.Field(name, field.fieldNumber, nestedTypeName));
			continue;
		}

		if (field.type === "array") {
			if (!field.array) {
				throw new Error(
					`serde: field "${name}" type=array requires array spec`,
				);
			}
			if (field.array.type === "object") {
				if (!field.array.nested) {
					throw new Error(
						`serde: array field "${name}" of objects requires nested fields`,
					);
				}
				const nestedTypeName = `${typeName}_${name}_elem`;
				const nestedType = buildType(root, nestedTypeName, field.array.nested);
				root.add(nestedType);
				type.add(
					new protobuf.Field(
						name,
						field.fieldNumber,
						nestedTypeName,
						"repeated",
					),
				);
			} else {
				type.add(
					new protobuf.Field(
						name,
						field.fieldNumber,
						field.array.type,
						"repeated",
					),
				);
			}
			continue;
		}

		// Scalar.
		type.add(new protobuf.Field(name, field.fieldNumber, field.type));
	}

	return type;
}

function typeToSerializer(type: protobuf.Type): Serializer {
	const jsonSchema = type.toJSON() as unknown as Record<string, unknown>;
	const serializer: Serializer = {
		encode(value: unknown): Uint8Array {
			const err = type.verify(value as object);
			if (err) {
				throw new Error(`serde: encode ${type.name}: ${err}`);
			}
			const msg = type.fromObject(value as object);
			return type.encode(msg).finish();
		},
		decode(bytes: Uint8Array): unknown {
			const msg = type.decode(bytes);
			// Same reason as the .proto path: a 64-bit field would otherwise arrive
			// as a {low, high, unsigned} object and break arithmetic silently.
			return type.toObject(msg, { defaults: true, longs: Number });
		},
		toJsonSchema(): Record<string, unknown> {
			return jsonSchema;
		},
	};
	attachWireDescriptor(serializer, type);
	return serializer;
}
