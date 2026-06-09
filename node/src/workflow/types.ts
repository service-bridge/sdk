// Workflow graph type system.
//
// Per ADR-W-018, `CallStep`/`PublishStep`/`WorkflowStep` compose their options
// from the owner modules through `TemplatableOpts<T>` — they do not redeclare
// `transport`/`idempotencyKey`/`requestId`/`retry`/`timeout` and so on.
// Adding a new field to `CallOpts` (rpc/client.ts) automatically surfaces in
// `CallStep.opts` without touching workflow code.
//
// @public — см. ./README.md

import type { PublishOpts } from "../events/publisher";
import type { CallOpts, RetryOpts } from "../rpc/client";
import type { CompensateSpec } from "./compensate";
import type { JsonExpression, Predicate } from "./jsonpath";
import type { WorkflowStartOpts } from "./start-opts";

export type { CompensateSpec } from "./compensate";
export type { JsonExpression, Predicate } from "./jsonpath";
export type { WorkflowStartOpts } from "./start-opts";

// TemplatableOpts<T> — every field of T can be either the original literal
// type or a JsonExpression (state path that resolves to the same shape).
// Preserves Partial/optional modifiers because the mapping does not unwrap them.
export type TemplatableOpts<T> = {
	[K in keyof T]: T[K] | JsonExpression;
};

export type TemplatableCallOpts = TemplatableOpts<CallOpts>;
export type TemplatablePublishOpts = TemplatableOpts<PublishOpts>;
export type TemplatableWorkflowStartOpts = TemplatableOpts<WorkflowStartOpts>;

// StepControlFields — workflow-level fields that govern *the step itself*,
// NOT the underlying RPC/publish call. These are owned by the workflow module
// and never duplicate fields that live in CallOpts/PublishOpts/WorkflowStartOpts.
//
// `timeoutSec` here is the workflow-control timeout: expiry → run enters
// `compensating` (ADR-W-007). DIFFERENT from `opts.timeout` (CallOpts) which is
// the RPC-level timeout — see ADR-W-018 §"Семантика двух timeout'ов".
export interface StepControlFields {
	id: string;
	waitFor?: string[];
	when?: Predicate;
	compensate?: CompensateSpec;
	timeoutSec?: number;
	retry?: Partial<RetryOpts>;
}

// CallStep — `sb.rpc.call(service, method, input, opts)`. ADR-W-018.
export interface CallStep extends StepControlFields {
	type: "call";
	service: string | JsonExpression;
	method: string | JsonExpression;
	input: JsonExpression;
	opts?: TemplatableCallOpts;
}

// PublishStep — `sb.event.publish(event, payload, opts)`. ADR-W-018.
export interface PublishStep extends StepControlFields {
	type: "publish";
	event: string | JsonExpression;
	input: JsonExpression;
	opts?: TemplatablePublishOpts;
}

// SleepStep — durable timer in runtime.
export interface SleepStep extends StepControlFields {
	type: "sleep";
	durationSec: number;
}

// WaitEventStep — park until matching ingested event.
export interface WaitEventStep extends StepControlFields {
	type: "wait_event";
	event: string;
	filter?: Record<string, JsonExpression>;
}

// WaitSignalStep — park until external Signal RPC.
export interface WaitSignalStep extends StepControlFields {
	type: "wait_signal";
	signal: string;
}

// WorkflowStep — `sb.workflow.start(workflow, input, opts)` + completion wait.
// ADR-W-018.
export interface WorkflowStep extends StepControlFields {
	type: "workflow";
	workflow: string | JsonExpression;
	input: JsonExpression;
	opts?: TemplatableWorkflowStartOpts;
}

// ForEachSpec — dynamic fan-out for parallel/sequence groups.
export interface ForEachSpec {
	from: JsonExpression;
	as: string;
}

// ParallelStep — group: all inner steps start at once, completes when all done.
export interface ParallelStep extends StepControlFields {
	type: "parallel";
	steps: Step[];
	forEach?: ForEachSpec;
}

// SequenceStep — group: inner steps run one after another.
export interface SequenceStep extends StepControlFields {
	type: "sequence";
	steps: Step[];
	forEach?: ForEachSpec;
}

// LocalStep — arbitrary JS executed inside the SDK. Use sparingly.
export interface LocalStep extends StepControlFields {
	type: "local";
	fn: (state: Record<string, unknown>) => Promise<unknown>;
}

export type Step =
	| CallStep
	| PublishStep
	| SleepStep
	| WaitEventStep
	| WaitSignalStep
	| WorkflowStep
	| ParallelStep
	| SequenceStep
	| LocalStep;

// SchemaShape — JSON Schema description for workflow input (ADR-W-009).
export type SchemaShape = Record<string, unknown>;

// WorkflowDef — passed to `sb.workflow.handle(name, def)`.
export interface WorkflowDef {
	input?: SchemaShape;
	steps: Step[];
	retry?: Partial<RetryOpts>;
	maxParallelism?: number;
	timeoutSec?: number;
}
