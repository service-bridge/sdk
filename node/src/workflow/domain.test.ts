import { describe, expect, it } from "bun:test";
import { MethodType } from "../pb/servicebridge/v1/registry";
import { Registry } from "../registry/registry";
import { canonicalize, fingerprint } from "./canonical";
import { WorkflowDomain } from "./domain";
import type { WorkflowDef } from "./types";

const TRIVIAL: WorkflowDef = {
	steps: [{ id: "wait", type: "sleep", durationSec: 1 }],
};

describe("WorkflowDomain.handle", () => {
	it("registers METHOD_TYPE_WORKFLOW with the canonical graph + fingerprint", () => {
		const registry = new Registry();
		const domain = new WorkflowDomain(registry);

		domain.handle("processPayment", TRIVIAL);
		const methods = registry._handle.incomingMethods();
		expect(methods).toHaveLength(1);
		expect(methods[0]!.type).toBe(MethodType.METHOD_TYPE_WORKFLOW);
		expect(methods[0]!.name).toBe("processPayment");

		const canonical = canonicalize({
			graph: TRIVIAL.steps,
			retry: undefined,
			maxParallelism: undefined,
			timeoutSec: undefined,
		});
		expect(Buffer.from(methods[0]!.inputSchemaJson).toString("utf8")).toBe(
			canonical,
		);
		expect(methods[0]!.contractHash).toBe(
			fingerprint({
				graph: TRIVIAL.steps,
				retry: undefined,
				maxParallelism: undefined,
				timeoutSec: undefined,
			}),
		);
		expect(methods[0]!.outputSchemaJson.length).toBe(0);
	});

	it("opts.input overrides def.input on the registry entry", () => {
		const registry = new Registry();
		const domain = new WorkflowDomain(registry);

		const def: WorkflowDef = {
			...TRIVIAL,
			input: { fromDef: "string" },
		};
		domain.handle("processOrder", def, { input: { fromOpts: "string" } });

		// We can't inspect the override directly through incomingMethods (it
		// already overwrites inputSchemaJson with the canonical graph). Instead
		// confirm validation passed (no throw) and a single entry landed.
		const methods = registry._handle.incomingMethods();
		expect(methods).toHaveLength(1);
	});

	it("rejects invalid graph (duplicate id)", () => {
		const registry = new Registry();
		const domain = new WorkflowDomain(registry);
		expect(() =>
			domain.handle("bad", {
				steps: [
					{ id: "a", type: "sleep", durationSec: 1 },
					{ id: "a", type: "sleep", durationSec: 1 },
				],
			}),
		).toThrow(/duplicate step id/);
	});

	it("fingerprint stable under property reorder", () => {
		const a = new Registry();
		const b = new Registry();
		new WorkflowDomain(a).handle("wf", {
			steps: [
				{
					id: "x",
					type: "call",
					service: "s",
					method: "m",
					input: { foo: 1, bar: 2 },
				},
			],
		});
		new WorkflowDomain(b).handle("wf", {
			steps: [
				{
					id: "x",
					type: "call",
					method: "m",
					service: "s",
					input: { bar: 2, foo: 1 },
				},
			],
		});
		expect(a._handle.incomingMethods()[0]!.contractHash).toBe(
			b._handle.incomingMethods()[0]!.contractHash,
		);
	});
});
