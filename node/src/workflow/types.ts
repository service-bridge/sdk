// Workflow definition DSL. A definition is a description: the runtime freezes
// it, interprets the DAG and leases task steps (local / call / publish /
// compensation) to an instance of the owner service (runtime ADR 0003).
//
// @public — см. ./README.md

// JsonExpression is resolved by the runtime against run state:
//   - a string starting with "$" is a JSONPath-lite path ($.a.b, $.list[0],
//     $.list[*].field);
//   - { literal: value } is the value itself (escape for strings that start
//     with "$");
//   - an array or an object is resolved member by member;
//   - anything else is a JSON literal.
export type JsonExpression =
	| string
	| number
	| boolean
	| null
	| { literal: unknown }
	| JsonExpression[]
	| { [key: string]: JsonExpression };

// Predicate gates a step through `when`: a path (truthy test) or a
// combination.
export type Predicate =
	| string
	| { not: Predicate }
	| { equals: [JsonExpression, JsonExpression] }
	| { in: [JsonExpression, JsonExpression] }
	| { and: Predicate[] }
	| { or: Predicate[] };

// RetryPolicy is applied by the runtime: the attempt after n failures waits
// min(maxDelayMs, baseDelayMs·factor^(n-1))·(1±jitter).
export interface RetryPolicy {
	maxAttempts?: number; // attempts including the first; default 1
	baseDelayMs?: number; // default 200
	factor?: number; // default 2
	maxDelayMs?: number; // default 5000
	jitter?: number; // fraction in [0,1]
}

export interface StepControl {
	id: string;
	waitFor?: string[];
	when?: Predicate;
	// Step deadline: on expiry the step fails and the run stops.
	timeoutMs?: number;
	// Retry policy of a task step (call / publish / local).
	retry?: RetryPolicy;
}

export interface WorkflowCallOpts {
	timeoutMs?: number;
	transport?: "auto" | "direct" | "proxy";
	idempotencyKey?: JsonExpression;
	requestId?: JsonExpression;
	// RPC-level retry inside the client, separate from the step retry.
	retry?: RetryPolicy;
}

export interface WorkflowPublishOpts {
	idempotencyKey?: JsonExpression;
	partitionKey?: JsonExpression;
	headers?: Record<string, JsonExpression>;
}

// Compensation reverses a successful call or publish step when the run stops.
// Without `type` it mirrors the step; empty targets reuse the step's.
export interface Compensation {
	type?: "call" | "publish";
	service?: JsonExpression;
	method?: JsonExpression;
	event?: JsonExpression;
	input?: JsonExpression;
	callOpts?: WorkflowCallOpts;
	publishOpts?: WorkflowPublishOpts;
	retry?: RetryPolicy;
}

export interface CallStep extends StepControl {
	type: "call";
	service: JsonExpression;
	method: JsonExpression;
	input?: JsonExpression;
	opts?: WorkflowCallOpts;
	compensate?: Compensation;
}

export interface PublishStep extends StepControl {
	type: "publish";
	event: JsonExpression;
	input?: JsonExpression;
	opts?: WorkflowPublishOpts;
	compensate?: Compensation;
}

// LocalContext is what a local function receives besides the run state.
export interface LocalContext {
	// Aborted when the lease is lost, the step deadline passes or the client stops.
	signal: AbortSignal;
	runId: string;
	stepId: string;
	attempt: number;
}

export interface LocalStep extends StepControl {
	type: "local";
	fn: (state: Record<string, unknown>, ctx: LocalContext) => unknown;
}

export interface SleepStep extends StepControl {
	type: "sleep";
	durationMs: number;
}

export interface WaitEventStep extends StepControl {
	type: "wait_event";
	event: string;
	// Filter Expression: payload path → expected value (an expression resolved
	// when the step parks). All pairs must match.
	filter?: Record<string, JsonExpression>;
}

export interface WaitSignalStep extends StepControl {
	type: "wait_signal";
	signal: string;
}

export interface WorkflowStep extends StepControl {
	type: "workflow";
	// Owner service of the child workflow; default — this service.
	service?: JsonExpression;
	workflow: JsonExpression;
	input?: JsonExpression;
	idempotencyKey?: JsonExpression;
	// Child run timeout; default — the child definition's.
	childTimeoutMs?: number;
}

export interface ForEachSpec {
	from: string; // path resolving to an array
	as: string; // name the element is bound to inside the group
}

export interface ParallelStep extends StepControl {
	type: "parallel";
	steps: Step[];
	forEach?: ForEachSpec;
}

export interface SequenceStep extends StepControl {
	type: "sequence";
	steps: Step[];
	forEach?: ForEachSpec;
}

export type Step =
	| CallStep
	| PublishStep
	| LocalStep
	| SleepStep
	| WaitEventStep
	| WaitSignalStep
	| WorkflowStep
	| ParallelStep
	| SequenceStep;

// WorkflowDef is passed to `sb.workflow.handle(name, def)`.
export interface WorkflowDef {
	// Identifies the code behind local steps; part of the fingerprint. Bump it
	// when a local function changes.
	version?: string;
	// JSON Schema of the run input.
	input?: Record<string, unknown>;
	steps: Step[];
	// Default retry policy of task steps that declare none.
	retry?: RetryPolicy;
	// Maximum concurrently executing task steps of one run; 0 = unlimited.
	maxParallelism?: number;
	// Run timeout; ends the run as timed_out.
	timeoutMs?: number;
}

export interface WorkflowStartOpts {
	idempotencyKey?: string;
	// Run timeout overriding the definition's.
	timeoutMs?: number;
}

export interface WorkflowSignalOpts {
	// Idempotency key: a repeated signal with the same id is not enqueued again.
	signalId?: string;
}

export type RunStatus =
	| "active"
	| "compensating"
	| "success"
	| "failed"
	| "cancelled"
	| "timed_out"
	| "failed_compensated";

export type StepStatus =
	| "pending"
	| "leased"
	| "parked"
	| "success"
	| "failed"
	| "compensated";

export interface StepSnapshot {
	stepId: string;
	parentStepId: string;
	kind: string;
	status: StepStatus;
	attempt: number;
	output: unknown;
	errorCode: string;
	errorMessage: string;
	waitingReason: string;
	waitKey: string;
	childRunId: string;
	compensatesStepId: string;
	startedAtMs: number;
	endedAtMs: number;
}

export interface RunSnapshot {
	runId: string;
	service: string;
	workflow: string;
	status: RunStatus;
	stopReason: string;
	waitingReason: string;
	input: unknown;
	output: Record<string, unknown> | null;
	errorCode: string;
	errorMessage: string;
	parentRunId: string;
	startedAtMs: number;
	endedAtMs: number;
	steps: StepSnapshot[];
	signals: Array<{
		signalName: string;
		signalId: string;
		payload: unknown;
		enqueuedAtMs: number;
	}>;
}
