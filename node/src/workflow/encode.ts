// Encodes the workflow DSL into the WorkflowDefinition proto. The runtime
// validates, freezes and fingerprints it; the SDK only keeps the local
// functions, which cannot travel.
//
// @internal — см. ./README.md

import type {
	CallStepOptions,
	Expr,
	Compensation as PbCompensation,
	Predicate as PbPredicate,
	RetryPolicy as PbRetryPolicy,
	Step as PbStep,
	PublishStepOptions,
	WorkflowDefinition,
} from "../pb/servicebridge/v1/workflows";
import type {
	Compensation,
	JsonExpression,
	LocalStep,
	Predicate,
	RetryPolicy,
	Step,
	WorkflowCallOpts,
	WorkflowDef,
	WorkflowPublishOpts,
} from "./types";

// LocalFns maps a step id to its local function.
export type LocalFns = Map<string, LocalStep["fn"]>;

export function encodeDefinition(
	name: string,
	def: WorkflowDef,
): { definition: WorkflowDefinition; locals: LocalFns } {
	const locals: LocalFns = new Map();
	const definition: WorkflowDefinition = {
		name,
		version: def.version ?? "",
		inputSchemaJson: def.input
			? Buffer.from(JSON.stringify(def.input), "utf8")
			: Buffer.alloc(0),
		steps: encodeSteps(def.steps, locals),
		retry: encodeRetry(def.retry),
		maxParallelism: def.maxParallelism ?? 0,
		timeoutMs: def.timeoutMs ?? 0,
	};
	return { definition, locals };
}

function encodeSteps(steps: Step[], locals: LocalFns): PbStep[] {
	return steps.map((s) => encodeStep(s, locals));
}

function encodeStep(s: Step, locals: LocalFns): PbStep {
	const out: PbStep = {
		id: s.id,
		waitFor: s.waitFor ?? [],
		when: s.when === undefined ? undefined : encodePredicate(s.when),
		timeoutMs: s.timeoutMs ?? 0,
		retry: encodeRetry(s.retry),
	};
	switch (s.type) {
		case "call":
			out.call = {
				service: encodeExpr(s.service),
				method: encodeExpr(s.method),
				input: optExpr(s.input),
				opts: encodeCallOpts(s.opts),
				compensate: encodeCompensation(s.compensate, "call"),
			};
			break;
		case "publish":
			out.publish = {
				event: encodeExpr(s.event),
				input: optExpr(s.input),
				opts: encodePublishOpts(s.opts),
				compensate: encodeCompensation(s.compensate, "publish"),
			};
			break;
		case "local":
			locals.set(s.id, s.fn);
			out.local = {};
			break;
		case "sleep":
			out.sleep = { durationMs: s.durationMs };
			break;
		case "wait_event":
			out.waitEvent = { event: s.event, filter: encodeExprMap(s.filter) };
			break;
		case "wait_signal":
			out.waitSignal = { signal: s.signal };
			break;
		case "workflow":
			out.workflow = {
				service: optExpr(s.service),
				workflow: encodeExpr(s.workflow),
				input: optExpr(s.input),
				idempotencyKey: optExpr(s.idempotencyKey),
				timeoutMs: s.childTimeoutMs ?? 0,
			};
			break;
		case "parallel":
			out.parallel = {
				steps: encodeSteps(s.steps, locals),
				forEach: s.forEach,
			};
			break;
		case "sequence":
			out.sequence = {
				steps: encodeSteps(s.steps, locals),
				forEach: s.forEach,
			};
			break;
		default: {
			const exhaustive: never = s;
			throw new Error(
				`workflow: unknown step type ${JSON.stringify(exhaustive)}`,
			);
		}
	}
	return out;
}

function isLiteralEscape(v: unknown): v is { literal: unknown } {
	return (
		typeof v === "object" &&
		v !== null &&
		!Array.isArray(v) &&
		Object.keys(v).length === 1 &&
		"literal" in v
	);
}

export function encodeExpr(v: JsonExpression): Expr {
	if (typeof v === "string" && v.startsWith("$")) return { path: v };
	if (isLiteralEscape(v))
		return { literal: Buffer.from(JSON.stringify(v.literal ?? null), "utf8") };
	if (Array.isArray(v)) return { list: { items: v.map(encodeExpr) } };
	if (typeof v === "object" && v !== null)
		return { object: { fields: encodeExprMap(v) } };
	return { literal: Buffer.from(JSON.stringify(v), "utf8") };
}

function optExpr(v: JsonExpression | undefined): Expr | undefined {
	return v === undefined ? undefined : encodeExpr(v);
}

function encodeExprMap(
	m: Record<string, JsonExpression> | undefined,
): Record<string, Expr> {
	const out: Record<string, Expr> = {};
	for (const [k, v] of Object.entries(m ?? {})) {
		if (v !== undefined) out[k] = encodeExpr(v);
	}
	return out;
}

function encodePredicate(p: Predicate): PbPredicate {
	if (typeof p === "string") return { truthy: encodeExpr(p) };
	if ("not" in p) return { not: encodePredicate(p.not) };
	if ("equals" in p)
		return {
			equals: { left: encodeExpr(p.equals[0]), right: encodeExpr(p.equals[1]) },
		};
	if ("in" in p)
		return { in: { left: encodeExpr(p.in[0]), right: encodeExpr(p.in[1]) } };
	if ("and" in p) return { and: { items: p.and.map(encodePredicate) } };
	return { or: { items: p.or.map(encodePredicate) } };
}

function encodeRetry(r: RetryPolicy | undefined): PbRetryPolicy | undefined {
	if (!r) return undefined;
	return {
		maxAttempts: r.maxAttempts ?? 0,
		baseDelayMs: r.baseDelayMs ?? 0,
		factor: r.factor ?? 0,
		maxDelayMs: r.maxDelayMs ?? 0,
		jitter: r.jitter ?? 0,
	};
}

function encodeCallOpts(
	o: WorkflowCallOpts | undefined,
): CallStepOptions | undefined {
	if (!o) return undefined;
	return {
		timeoutMs: o.timeoutMs ?? 0,
		transport: o.transport ?? "",
		idempotencyKey: optExpr(o.idempotencyKey),
		requestId: optExpr(o.requestId),
		retry: encodeRetry(o.retry),
	};
}

function encodePublishOpts(
	o: WorkflowPublishOpts | undefined,
): PublishStepOptions | undefined {
	if (!o) return undefined;
	return {
		idempotencyKey: optExpr(o.idempotencyKey),
		partitionKey: optExpr(o.partitionKey),
		headers: encodeExprMap(o.headers),
	};
}

function encodeCompensation(
	c: Compensation | undefined,
	stepType: "call" | "publish",
): PbCompensation | undefined {
	if (!c) return undefined;
	const out: PbCompensation = {
		input: optExpr(c.input),
		retry: encodeRetry(c.retry),
	};
	const type = c.type ?? stepType;
	if (type === "call")
		out.call = {
			service: optExpr(c.service),
			method: optExpr(c.method),
			opts: encodeCallOpts(c.callOpts),
		};
	if (type === "publish")
		out.publish = {
			event: optExpr(c.event),
			opts: encodePublishOpts(c.publishOpts),
		};
	return out;
}
