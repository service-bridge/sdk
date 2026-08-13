import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import protobuf from "protobufjs";
import {
	canonicalize,
	canonicalMessageDescriptor,
	computeContractHash,
	wireDescriptor,
} from "./contract-hash";
import { buildSchemaPair } from "./serializer";

const testdata = join(import.meta.dir, "testdata");
const protoFile = join(testdata, "payment.proto");
const protoFormatted = join(testdata, "payment_formatted.proto");
const jsonFile = join(testdata, "payment.schema.json");

const vectorsFile = join(
	import.meta.dir,
	"..",
	"..",
	"..",
	"contract-hash-vectors.json",
);

interface Vector {
	name: string;
	source: "proto" | "schema.json";
	fixture: string;
	inputMessage?: string;
	outputMessage?: string;
	canonicalInput: string;
	canonicalOutput: string;
	contractHash: string;
}

interface VectorProperty {
	name: string;
	assert: "equal" | "different";
	vectors: string[];
}

const goldens = JSON.parse(readFileSync(vectorsFile, "utf8")) as {
	version: string;
	vectors: Vector[];
	properties: VectorProperty[];
};

const fixturePath = (vector: Vector): string =>
	join(testdata, vector.fixture.split("/").pop() as string);

const pairOf = (vector: Vector) =>
	vector.source === "proto"
		? buildSchemaPair({
				protoFile: fixturePath(vector),
				input: vector.inputMessage,
				output: vector.outputMessage,
			})
		: buildSchemaPair({ schemaFile: fixturePath(vector) });

describe("golden vectors", () => {
	it("file declares the v2 algorithm and a non-empty vector set", () => {
		expect(goldens.version).toBe("v2");
		expect(goldens.vectors.length).toBeGreaterThan(0);
		expect(goldens.properties.length).toBeGreaterThan(0);
	});

	for (const vector of goldens.vectors) {
		it(`${vector.name} reproduces its canonical descriptors and hash`, async () => {
			const pair = await pairOf(vector);
			expect(wireDescriptor(pair.input)).toBe(vector.canonicalInput);
			expect(wireDescriptor(pair.output)).toBe(vector.canonicalOutput);
			expect(computeContractHash(pair)).toBe(vector.contractHash);
		});
	}

	it("every recorded hash is sha256 over the recorded canonical strings", () => {
		for (const vector of goldens.vectors) {
			const digest = createHash("sha256")
				.update(`${vector.canonicalInput}:${vector.canonicalOutput}`)
				.digest("hex");
			expect(vector.contractHash).toBe(`v2:${digest}`);
		}
	});

	for (const property of goldens.properties) {
		it(`property: ${property.name}`, async () => {
			const hashes: string[] = [];
			for (const name of property.vectors) {
				const vector = goldens.vectors.find((v) => v.name === name);
				expect(vector).toBeDefined();
				hashes.push(computeContractHash(await pairOf(vector as Vector)));
			}
			const first = hashes[0] as string;
			if (property.assert === "equal") {
				for (const h of hashes) expect(h).toBe(first);
			} else {
				expect(new Set(hashes).size).toBe(hashes.length);
			}
		});
	}
});

describe("computeContractHash", () => {
	const chargePair = () =>
		buildSchemaPair({
			protoFile,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});

	it("carries the v2 prefix and a sha256 hex digest", async () => {
		expect(computeContractHash(await chargePair())).toMatch(
			/^v2:[0-9a-f]{64}$/,
		);
	});

	it("is stable across repeated builds of the same schema", async () => {
		expect(computeContractHash(await chargePair())).toBe(
			computeContractHash(await chargePair()),
		);
	});

	it("ignores whitespace and comments in the .proto", async () => {
		const b = await buildSchemaPair({
			protoFile: protoFormatted,
			input: "ChargeRequest",
			output: "ChargeResponse",
		});
		expect(computeContractHash(await chargePair())).toBe(
			computeContractHash(b),
		);
	});

	it("swapping input and output changes the hash", async () => {
		const swapped = await buildSchemaPair({
			protoFile,
			input: "ChargeResponse",
			output: "ChargeRequest",
		});
		expect(computeContractHash(await chargePair())).not.toBe(
			computeContractHash(swapped),
		);
	});

	it("works for the .schema.json source", async () => {
		const pair = await buildSchemaPair({ schemaFile: jsonFile });
		expect(computeContractHash(pair)).toMatch(/^v2:[0-9a-f]{64}$/);
	});

	it("rejects a pair not built by buildSchemaPair", () => {
		const stub = {
			encode: () => new Uint8Array(),
			decode: () => ({}),
			toJsonSchema: () => ({}),
		};
		expect(() => computeContractHash({ input: stub, output: stub })).toThrow(
			/no wire descriptor/,
		);
	});
});

describe("canonicalMessageDescriptor", () => {
	const load = (source: string, message: string): protobuf.Type => {
		const root = protobuf.parse(source, { keepCase: true }).root;
		root.resolveAll();
		return root.lookupType(message);
	};

	it("sorts fields by number regardless of declaration order", () => {
		const declared = load(
			`syntax="proto3";package t;message M{string c=9;int32 a=1;bool b=4;}`,
			"t.M",
		);
		const reordered = load(
			`syntax="proto3";package t;message M{bool b=4;string c=9;int32 a=1;}`,
			"t.M",
		);
		expect(canonicalMessageDescriptor(declared)).toBe(
			'{"f":[{"c":"opt","n":1,"t":"int32"},{"c":"opt","n":4,"t":"bool"},{"c":"opt","n":9,"t":"string"}]}',
		);
		expect(canonicalMessageDescriptor(reordered)).toBe(
			canonicalMessageDescriptor(declared),
		);
	});

	it("renders an empty message as an empty field list", () => {
		expect(
			canonicalMessageDescriptor(
				load(`syntax="proto3";package t;message M{}`, "t.M"),
			),
		).toBe('{"f":[]}');
	});

	it("expresses a map inline without a synthetic entry type", () => {
		expect(
			canonicalMessageDescriptor(
				load(
					`syntax="proto3";package t;message M{map<int32,string> m=1;}`,
					"t.M",
				),
			),
		).toBe('{"f":[{"c":"map","k":"int32","n":1,"t":"string"}]}');
	});

	it("keeps proto3 optional singular and out of the oneof list", () => {
		expect(
			canonicalMessageDescriptor(
				load(
					`syntax="proto3";package t;message M{optional string s=1;}`,
					"t.M",
				),
			),
		).toBe('{"f":[{"c":"opt","n":1,"t":"string"}]}');
	});

	it("groups a real oneof by field number", () => {
		expect(
			canonicalMessageDescriptor(
				load(
					`syntax="proto3";package t;message M{oneof k{string b=5;int32 a=2;}}`,
					"t.M",
				),
			),
		).toBe(
			'{"f":[{"c":"opt","n":2,"t":"int32"},{"c":"opt","n":5,"t":"string"}],"o":[{"f":[2,5]}]}',
		);
	});

	it("collapses enum aliases and sorts values", () => {
		expect(
			canonicalMessageDescriptor(
				load(
					`syntax="proto3";package t;enum E{option allow_alias=true;Z=0;B=7;A=7;C=3;}message M{E e=1;}`,
					"t.M",
				),
			),
		).toBe('{"f":[{"c":"opt","n":1,"t":"e:.t.E"}],"r":{".t.E":{"e":[0,3,7]}}}');
	});

	it("terminates on a self-referencing message", () => {
		expect(
			canonicalMessageDescriptor(
				load(
					`syntax="proto3";package t;message M{string id=1;repeated M kids=2;}`,
					"t.M",
				),
			),
		).toBe(
			'{"f":[{"c":"opt","n":1,"t":"string"},{"c":"rep","n":2,"t":"m:.t.M"}],"r":{".t.M":{"f":[{"c":"opt","n":1,"t":"string"},{"c":"rep","n":2,"t":"m:.t.M"}]}}}',
		);
	});

	it("terminates on mutual recursion and lists each type once", () => {
		const canonical = canonicalMessageDescriptor(
			load(
				`syntax="proto3";package t;message A{B b=1;}message B{A a=1;}`,
				"t.A",
			),
		);
		const parsed = JSON.parse(canonical) as { r: Record<string, unknown> };
		expect(Object.keys(parsed.r).sort()).toEqual([".t.A", ".t.B"]);
	});

	it("ignores field names", () => {
		const a = load(
			`syntax="proto3";package t;message M{string user_id=1;Inner inner=2;}message Inner{int32 qty=1;}`,
			"t.M",
		);
		const b = load(
			`syntax="proto3";package t;message M{string account=1;Inner payload=2;}message Inner{int32 amount=1;}`,
			"t.M",
		);
		expect(canonicalMessageDescriptor(a)).toBe(canonicalMessageDescriptor(b));
	});
});

describe("canonicalize", () => {
	it("sorts object keys", () => {
		expect(canonicalize({ z: 1, a: 2 })).toBe(canonicalize({ a: 2, z: 1 }));
		expect(canonicalize({ z: 1, a: 2 })).toBe('{"a":2,"z":1}');
	});

	it("preserves array order", () => {
		expect(canonicalize([3, 1, 2])).toBe("[3,1,2]");
	});

	it("handles nested structures", () => {
		expect(canonicalize({ z: [3, 1], a: { y: 1, x: 2 } })).toBe(
			'{"a":{"x":2,"y":1},"z":[3,1]}',
		);
	});

	it("primitives via JSON.stringify", () => {
		expect(canonicalize("hello")).toBe('"hello"');
		expect(canonicalize(42)).toBe("42");
		expect(canonicalize(null)).toBe("null");
		expect(canonicalize(true)).toBe("true");
	});

	it("no whitespace in output", () => {
		expect(canonicalize({ a: 1, b: [1, 2] })).not.toMatch(/\s/);
	});
});
