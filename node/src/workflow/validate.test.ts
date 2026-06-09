import { describe, expect, it } from "bun:test";
import type { WorkflowDef } from "./types";
import { validate, WorkflowValidationError } from "./validate";

function def(steps: WorkflowDef["steps"]): WorkflowDef {
	return { steps };
}

describe("validate — id format", () => {
	it("accepts ^[a-z0-9_]+$", () => {
		validate(def([{ id: "a_1", type: "sleep", durationSec: 1 }]), {
			workflowName: "wf",
		});
	});

	it("rejects uppercase / dash / dot", () => {
		expect(() =>
			validate(def([{ id: "A", type: "sleep", durationSec: 1 }]), {
				workflowName: "wf",
			}),
		).toThrow(WorkflowValidationError);
		expect(() =>
			validate(def([{ id: "a-1", type: "sleep", durationSec: 1 }]), {
				workflowName: "wf",
			}),
		).toThrow(WorkflowValidationError);
	});

	it("rejects duplicates", () => {
		expect(() =>
			validate(
				def([
					{ id: "a", type: "sleep", durationSec: 1 },
					{ id: "a", type: "sleep", durationSec: 1 },
				]),
				{ workflowName: "wf" },
			),
		).toThrow(/duplicate step id/);
	});
});

describe("validate — waitFor", () => {
	it("rejects unknown reference", () => {
		expect(() =>
			validate(
				def([
					{
						id: "a",
						type: "sleep",
						durationSec: 1,
						waitFor: ["missing"],
					},
				]),
				{ workflowName: "wf" },
			),
		).toThrow(/unknown step "missing"/);
	});

	it("rejects cycle", () => {
		expect(() =>
			validate(
				def([
					{
						id: "a",
						type: "sleep",
						durationSec: 1,
						waitFor: ["b"],
					},
					{
						id: "b",
						type: "sleep",
						durationSec: 1,
						waitFor: ["a"],
					},
				]),
				{ workflowName: "wf" },
			),
		).toThrow(/cycle/);
	});

	it("accepts linear chain", () => {
		validate(
			def([
				{ id: "a", type: "sleep", durationSec: 1 },
				{
					id: "b",
					type: "sleep",
					durationSec: 1,
					waitFor: ["a"],
				},
			]),
			{ workflowName: "wf" },
		);
	});
});

describe("validate — compensate", () => {
	it("rejects compensate on sleep", () => {
		expect(() =>
			validate(
				def([
					{
						id: "a",
						type: "sleep",
						durationSec: 1,
						compensate: { input: {} },
					} as unknown as WorkflowDef["steps"][number],
				]),
				{ workflowName: "wf" },
			),
		).toThrow(/compensate allowed only/);
	});

	it("accepts compensate on call/publish", () => {
		validate(
			def([
				{
					id: "a",
					type: "call",
					service: "s",
					method: "m",
					input: { x: 1 },
					compensate: { input: { x: 1 } },
				},
			]),
			{ workflowName: "wf" },
		);
	});
});

describe("validate — workflow self-ref", () => {
	it("rejects literal self-ref", () => {
		expect(() =>
			validate(
				def([
					{
						id: "a",
						type: "workflow",
						workflow: "myWf",
						input: {},
					},
				]),
				{ workflowName: "myWf" },
			),
		).toThrow(/self-reference/);
	});

	it("accepts dynamic workflow target", () => {
		validate(
			def([
				{
					id: "a",
					type: "workflow",
					workflow: "$.input.target",
					input: {},
				},
			]),
			{ workflowName: "myWf" },
		);
	});
});

describe("validate — forEach", () => {
	it("accepts forEach with parseable path", () => {
		validate(
			def([
				{
					id: "g",
					type: "parallel",
					forEach: { from: "$.list_items.items[*]", as: "item" },
					steps: [
						{
							id: "inner",
							type: "call",
							service: "s",
							method: "m",
							input: { id: "$.item.id" },
						},
					],
				},
			]),
			{ workflowName: "wf" },
		);
	});

	it("rejects bad forEach.as", () => {
		expect(() =>
			validate(
				def([
					{
						id: "g",
						type: "parallel",
						forEach: { from: "$.x", as: "BAD-NAME" },
						steps: [
							{
								id: "inner",
								type: "sleep",
								durationSec: 1,
							},
						],
					},
				]),
				{ workflowName: "wf" },
			),
		).toThrow(/forEach.as/);
	});
});

describe("validate — does NOT validate opts (ADR-W-018)", () => {
	it("accepts malformed opts.retry; surfaces at runtime via sb.rpc.call", () => {
		validate(
			def([
				{
					id: "a",
					type: "call",
					service: "s",
					method: "m",
					input: { x: 1 },
					opts: {
						// intentionally bogus — proves validator ignores opts (ADR-W-018)
						retry: { maxAttempts: "not-a-number" as unknown as number },
					},
				},
			]),
			{ workflowName: "wf" },
		);
	});
});

describe("validate — JsonExpression syntax", () => {
	it("rejects malformed $-path", () => {
		expect(() =>
			validate(
				def([
					{
						id: "a",
						type: "call",
						service: "s",
						method: "m",
						input: { x: "$.foo[abc]" },
					},
				]),
				{ workflowName: "wf" },
			),
		).toThrow(/invalid JsonExpression/);
	});

	it("accepts well-formed literal escape", () => {
		validate(
			def([
				{
					id: "a",
					type: "call",
					service: "s",
					method: "m",
					input: { x: { literal: "$.weird" } },
				},
			]),
			{ workflowName: "wf" },
		);
	});
});
