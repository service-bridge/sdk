// Workflow graph static validation.
//
// Per ADR-W-018, this validator covers ONLY graph control:
//   - id uniqueness and format `^[a-z0-9_]+$`
//   - waitFor topological acyclicity
//   - depth and size caps
//   - compensate only on call/publish
//   - forEach.from JSONPath parseable
//   - workflow self-ref check
//   - JsonExpression syntax (parses every $.… expression)
//
// It does NOT validate `opts` (retry, transport, idempotencyKey, timeout, …).
// Those live with the owner module — checks happen at step execution time via
// `sb.rpc.call`/`sb.event.publish`/`sb.workflow.start`.
//
// @internal — см. ./README.md

import type { JsonExpression } from "./jsonpath";
import { evalPath, JsonPathError } from "./jsonpath";
import type { CompensateSpec, Step, WorkflowDef } from "./types";

export class WorkflowValidationError extends Error {
	constructor(message: string) {
		super(`workflow/validate: ${message}`);
		this.name = "WorkflowValidationError";
	}
}

const ID_RE = /^[a-z0-9_]+$/;
const MAX_DEPTH = 16;
const MAX_STEPS = 512;

export interface ValidateOptions {
	// Workflow name — used for self-ref check on `type: "workflow"` steps.
	workflowName: string;
}

export function validate(def: WorkflowDef, opts: ValidateOptions): void {
	if (!def || !Array.isArray(def.steps)) {
		throw new WorkflowValidationError("def.steps must be an array");
	}
	const counter = { count: 0 };
	const allIds = new Set<string>();
	walkSteps(def.steps, 0, allIds, counter, opts);
	validateWaitFor(def.steps, allIds);
}

function walkSteps(
	steps: Step[],
	depth: number,
	allIds: Set<string>,
	counter: { count: number },
	opts: ValidateOptions,
): void {
	if (depth > MAX_DEPTH) {
		throw new WorkflowValidationError(
			`max nesting depth ${MAX_DEPTH} exceeded`,
		);
	}
	for (const step of steps) {
		counter.count += 1;
		if (counter.count > MAX_STEPS) {
			throw new WorkflowValidationError(`max step count ${MAX_STEPS} exceeded`);
		}
		validateStep(step, depth, allIds, counter, opts);
	}
}

function validateStep(
	step: Step,
	depth: number,
	allIds: Set<string>,
	counter: { count: number },
	opts: ValidateOptions,
): void {
	if (typeof step.id !== "string" || !ID_RE.test(step.id)) {
		throw new WorkflowValidationError(
			`step id must match ${ID_RE} (got "${String(step.id)}")`,
		);
	}
	if (allIds.has(step.id)) {
		throw new WorkflowValidationError(`duplicate step id "${step.id}"`);
	}
	allIds.add(step.id);

	// compensate — only on call/publish
	const hasCompensate = (step as { compensate?: CompensateSpec }).compensate;
	if (hasCompensate && step.type !== "call" && step.type !== "publish") {
		throw new WorkflowValidationError(
			`step "${step.id}": compensate allowed only on call/publish steps (got ${step.type})`,
		);
	}
	if (hasCompensate) {
		validateJsonExpression(hasCompensate.input, `${step.id}.compensate.input`);
	}

	switch (step.type) {
		case "call": {
			validateMaybePathOrLiteral(step.service, `${step.id}.service`);
			validateMaybePathOrLiteral(step.method, `${step.id}.method`);
			validateJsonExpression(step.input, `${step.id}.input`);
			break;
		}
		case "publish": {
			validateMaybePathOrLiteral(step.event, `${step.id}.event`);
			validateJsonExpression(step.input, `${step.id}.input`);
			break;
		}
		case "sleep": {
			if (
				typeof step.durationSec !== "number" ||
				!Number.isInteger(step.durationSec) ||
				step.durationSec < 0
			) {
				throw new WorkflowValidationError(
					`step "${step.id}": durationSec must be a non-negative integer`,
				);
			}
			break;
		}
		case "wait_event": {
			if (typeof step.event !== "string" || step.event.length === 0) {
				throw new WorkflowValidationError(
					`step "${step.id}": wait_event.event must be a non-empty string`,
				);
			}
			if (step.filter) {
				for (const [k, v] of Object.entries(step.filter)) {
					validateJsonExpression(v, `${step.id}.filter.${k}`);
				}
			}
			break;
		}
		case "wait_signal": {
			if (typeof step.signal !== "string" || step.signal.length === 0) {
				throw new WorkflowValidationError(
					`step "${step.id}": wait_signal.signal must be a non-empty string`,
				);
			}
			break;
		}
		case "workflow": {
			validateMaybePathOrLiteral(step.workflow, `${step.id}.workflow`);
			if (
				typeof step.workflow === "string" &&
				!step.workflow.startsWith("$.") &&
				step.workflow === opts.workflowName
			) {
				throw new WorkflowValidationError(
					`step "${step.id}": workflow self-reference to "${opts.workflowName}" rejected (ADR-W-015 static)`,
				);
			}
			validateJsonExpression(step.input, `${step.id}.input`);
			break;
		}
		case "parallel":
		case "sequence": {
			if (step.forEach) {
				validateMaybePathOrLiteral(
					step.forEach.from,
					`${step.id}.forEach.from`,
				);
				if (
					typeof step.forEach.as !== "string" ||
					!ID_RE.test(step.forEach.as)
				) {
					throw new WorkflowValidationError(
						`step "${step.id}": forEach.as must match ${ID_RE}`,
					);
				}
			}
			if (!Array.isArray(step.steps) || step.steps.length === 0) {
				throw new WorkflowValidationError(
					`step "${step.id}": ${step.type}.steps must be a non-empty array`,
				);
			}
			walkSteps(step.steps, depth + 1, allIds, counter, opts);
			break;
		}
		case "local": {
			if (typeof step.fn !== "function") {
				throw new WorkflowValidationError(
					`step "${step.id}": local.fn must be a function`,
				);
			}
			break;
		}
		default: {
			const _exhaustive: never = step;
			throw new WorkflowValidationError(
				`unknown step type: ${JSON.stringify(_exhaustive)}`,
			);
		}
	}
}

function validateMaybePathOrLiteral(
	v: string | JsonExpression,
	ctx: string,
): void {
	if (typeof v === "string") {
		if (v.startsWith("$.") || v === "$") {
			validatePathSyntax(v, ctx);
		}
		return;
	}
	validateJsonExpression(v, ctx);
}

function validateJsonExpression(expr: JsonExpression, ctx: string): void {
	if (expr === null) return;
	if (typeof expr === "string") {
		if (expr.startsWith("$.") || expr === "$") {
			validatePathSyntax(expr, ctx);
		}
		return;
	}
	if (
		typeof expr === "number" ||
		typeof expr === "boolean" ||
		typeof expr === "undefined"
	) {
		return;
	}
	if (Array.isArray(expr)) {
		for (let i = 0; i < expr.length; i++) {
			validateJsonExpression(expr[i] as JsonExpression, `${ctx}[${i}]`);
		}
		return;
	}
	// object — { literal } passes through, else recurse
	const obj = expr as Record<string, JsonExpression>;
	if (Object.keys(obj).length === 1 && typeof obj.literal === "string") return;
	for (const [k, v] of Object.entries(obj)) {
		validateJsonExpression(v, `${ctx}.${k}`);
	}
}

function validatePathSyntax(expr: string, ctx: string): void {
	try {
		evalPath(expr, {});
	} catch (err) {
		if (err instanceof JsonPathError) {
			throw new WorkflowValidationError(
				`${ctx}: invalid JsonExpression: ${err.message}`,
			);
		}
		throw err;
	}
}

// validateWaitFor — flat topo on top-level + recursive on groups; reports
// cycles and unknown references.
function validateWaitFor(steps: Step[], allIds: Set<string>): void {
	const visited = new Set<string>();
	const onStack = new Set<string>();
	const byId = new Map<string, Step>();
	indexById(steps, byId);

	function visit(id: string): void {
		if (visited.has(id)) return;
		if (onStack.has(id)) {
			throw new WorkflowValidationError(
				`waitFor cycle detected at step "${id}"`,
			);
		}
		const step = byId.get(id);
		if (!step) return; // unknown id surfaces below
		onStack.add(id);
		const deps = step.waitFor ?? [];
		for (const dep of deps) {
			if (!allIds.has(dep)) {
				throw new WorkflowValidationError(
					`step "${id}": waitFor references unknown step "${dep}"`,
				);
			}
			visit(dep);
		}
		onStack.delete(id);
		visited.add(id);
	}

	for (const id of byId.keys()) visit(id);
}

function indexById(steps: Step[], out: Map<string, Step>): void {
	for (const s of steps) {
		out.set(s.id, s);
		if (s.type === "parallel" || s.type === "sequence") {
			indexById(s.steps, out);
		}
	}
}
