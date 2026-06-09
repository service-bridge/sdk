// Thin workflow runner — executes a frozen plan on the owner SDK.
//
// ADR-W-018: each case body is exactly
//   (1) eval JsonExpressions in opts/input → (2) await sb.<domain>.<op>(...)
//   → (3) Complete/FailStep.
// The runner does NOT implement its own retry / backoff / idempotency /
// circuit-breaker / timeout loop for call/publish/workflow — those live in
// sb.rpc.call / sb.event.publish / sb.workflow.start and are applied
// transitively.
//
// @internal — см. ./README.md

import { buildCompensateOpts } from "./compensate-opts";
import type { JsonExpression, Predicate, State } from "./jsonpath";
import { evalLiteralOrPath, evalPredicate } from "./jsonpath";
import type {
	CallStep,
	CompensateSpec,
	LocalStep,
	ParallelStep,
	PublishStep,
	SequenceStep,
	SleepStep,
	Step,
	WaitEventStep,
	WaitSignalStep,
	WorkflowStep,
} from "./types";

// SbDomains — the surface area the runner depends on (ADR-W-018: thin).
// The real implementation is `ServiceBridge.{rpc,event,workflow}`; tests
// substitute a mock that observes argument shapes.
export interface SbDomains {
	rpc: {
		call(
			service: string,
			method: string,
			payload: unknown,
			opts?: Record<string, unknown>,
		): Promise<unknown>;
	};
	event: {
		publish(
			name: string,
			payload: unknown,
			opts?: Record<string, unknown>,
		): Promise<unknown>;
	};
	workflow: {
		start(
			name: string,
			input: unknown,
			opts?: Record<string, unknown>,
		): Promise<{ runId: string }>;
		await(runId: string): Promise<unknown>;
	};
}

// RuntimeOps — the runtime-facing checkpoint surface. Real impl talks to
// gRPC WorkflowsClient; tests can mock.
export interface RuntimeOps {
	beginStep(args: {
		runId: string;
		stepId: string;
		parentStepId: string;
		kind: Step["type"];
		inputSnapshot: unknown;
		leaseEpoch: number;
	}): Promise<{ alreadyDone: boolean; cachedOutput?: unknown }>;
	completeStep(args: {
		runId: string;
		stepId: string;
		output: unknown;
		leaseEpoch: number;
	}): Promise<void>;
	failStep(args: {
		runId: string;
		stepId: string;
		errorCode: string;
		errorMessage: string;
		leaseEpoch: number;
		retriable: boolean;
	}): Promise<{ nextAction: string }>;
	park(args: {
		runId: string;
		stepId: string;
		leaseEpoch: number;
		sleep?: { durationSec: number };
		eventWait?: {
			event: string;
			filterJson: string;
			timeoutSec: number;
		};
		signalWait?: { signal: string; timeoutSec: number };
		nestedRun?: { childRunId: string };
	}): Promise<void>;
	completeRun(args: {
		runId: string;
		finalState: unknown;
		leaseEpoch: number;
		terminalStatus?: string;
	}): Promise<void>;
}

export interface RunContext {
	runId: string;
	leaseEpoch: number;
	state: State;
	compensating: boolean;
	maxParallelism: number; // 0 = unlimited
}

// StepSpanInfo describes the USER.SUBOP step span the runner asks the caller to
// open around a unit of workflow execution. `role` distinguishes a plain step
// from the two fanout container levels (the group span, and one branch span per
// iteration) so the caller can label them and the UI can render the
// group → branch → step nesting.
export interface StepSpanInfo {
	runId: string;
	stepId: string;
	stepName: string;
	role: "step" | "group" | "branch" | "compensation";
	isCompensation?: boolean;
	compensatesForStepId?: string;
}

export interface RunnerDeps {
	sb: SbDomains;
	ops: RuntimeOps;
	// Optional hook the runner calls around every executed unit (each step,
	// each fanout group, each fanout branch, each compensation). The caller
	// emits one USER.SUBOP step span and runs `fn` inside a child trace context
	// rooted at that span's op_id, so the step's nested sb.rpc.call /
	// sb.event.publish / sub-workflow.start inherit the span as their parent
	// and the trace renders as a tree. Absent in unit tests that do not assert
	// on telemetry — execution then runs unwrapped.
	wrapStep?: <T>(info: StepSpanInfo, fn: () => Promise<T>) => Promise<T>;
}

// run executes a frozen plan to completion (or until a Park happens, which
// surrenders control back to runtime). Returns the final state.
export async function run(
	steps: Step[],
	ctx: RunContext,
	deps: RunnerDeps,
): Promise<State> {
	if (ctx.compensating) {
		await runCompensation(steps, ctx, deps);
		return ctx.state;
	}

	const byId = new Map<string, Step>();
	indexById(steps, byId);
	const completed = new Set<string>(Object.keys(ctx.state));
	completed.delete("input"); // input is not a step id

	// Dependency-based scheduling — repeatedly find steps whose waitFor is
	// satisfied and run them, honoring maxParallelism.
	while (true) {
		const ready: Step[] = [];
		for (const step of steps) {
			if (completed.has(step.id)) continue;
			const deps = step.waitFor ?? [];
			if (deps.every((d) => completed.has(d))) ready.push(step);
		}
		if (ready.length === 0) break;

		const cap =
			ctx.maxParallelism > 0
				? Math.min(ready.length, ctx.maxParallelism)
				: ready.length;
		const batch = ready.slice(0, cap);

		await Promise.all(batch.map((s) => executeStep(s, ctx, deps, completed)));
	}

	return ctx.state;
}

function indexById(steps: Step[], out: Map<string, Step>): void {
	for (const s of steps) {
		out.set(s.id, s);
		if (s.type === "parallel" || s.type === "sequence") {
			indexById(s.steps, out);
		}
	}
}

async function executeStep(
	step: Step,
	ctx: RunContext,
	deps: RunnerDeps,
	completed: Set<string>,
): Promise<void> {
	// `when` predicate — skip if false.
	if (
		step.when !== undefined &&
		!evalPredicate(step.when as Predicate, ctx.state)
	) {
		ctx.state[step.id] = null;
		completed.add(step.id);
		return;
	}

	const begin = await deps.ops.beginStep({
		runId: ctx.runId,
		stepId: step.id,
		parentStepId: "",
		kind: step.type,
		inputSnapshot: snapshotInput(step, ctx.state),
		leaseEpoch: ctx.leaseEpoch,
	});

	if (begin.alreadyDone) {
		ctx.state[step.id] = begin.cachedOutput ?? null;
		completed.add(step.id);
		return;
	}

	try {
		// One USER.SUBOP step span per executed unit. Group steps
		// (parallel/sequence) get role "group" so dispatchGroup's branch spans
		// nest under it; everything else is a plain "step". The span scope
		// established by wrapStep is what makes the step's nested rpc/event/
		// sub-workflow ops parent to the span instead of the run root.
		const role: StepSpanInfo["role"] =
			step.type === "parallel" || step.type === "sequence" ? "group" : "step";
		const runStep = () => dispatch(step, ctx, deps);
		const output = deps.wrapStep
			? await deps.wrapStep(
					{
						runId: ctx.runId,
						stepId: step.id,
						stepName: step.id,
						role,
					},
					runStep,
				)
			: await runStep();
		// Park-typed steps surrender control; they have no synchronous output.
		// dispatch returns the sentinel `PARKED` and the runtime will resume
		// the run after the timer/event/signal fires.
		if (output === PARKED) {
			throw new RunnerParkedError(step.id);
		}
		await deps.ops.completeStep({
			runId: ctx.runId,
			stepId: step.id,
			output,
			leaseEpoch: ctx.leaseEpoch,
		});
		ctx.state[step.id] = output ?? null;
		completed.add(step.id);
	} catch (err) {
		if (err instanceof RunnerParkedError) throw err;
		const { code, message } = unpackError(err);
		await deps.ops.failStep({
			runId: ctx.runId,
			stepId: step.id,
			errorCode: code,
			errorMessage: message,
			leaseEpoch: ctx.leaseEpoch,
			retriable: false,
		});
		throw err;
	}
}

const PARKED = Symbol("workflow.parked");

export class RunnerParkedError extends Error {
	constructor(public readonly stepId: string) {
		super(`workflow/runner: parked at step "${stepId}" — runtime will resume`);
		this.name = "RunnerParkedError";
	}
}

function snapshotInput(step: Step, state: State): unknown {
	switch (step.type) {
		case "call":
		case "publish":
		case "workflow":
			return evalLiteralOrPath(step.input as JsonExpression, state);
		case "sleep":
			return { durationSec: step.durationSec };
		case "wait_event":
			return { event: step.event, filter: step.filter ?? null };
		case "wait_signal":
			return { signal: step.signal };
		default:
			return null;
	}
}

async function dispatch(
	step: Step,
	ctx: RunContext,
	deps: RunnerDeps,
): Promise<unknown> {
	switch (step.type) {
		case "call":
			return dispatchCall(step, ctx, deps);
		case "publish":
			return dispatchPublish(step, ctx, deps);
		case "workflow":
			return dispatchSubWorkflow(step, ctx, deps);
		case "sleep":
			return dispatchSleep(step, ctx, deps);
		case "wait_event":
			return dispatchWaitEvent(step, ctx, deps);
		case "wait_signal":
			return dispatchWaitSignal(step, ctx, deps);
		case "parallel":
		case "sequence":
			return dispatchGroup(step, ctx, deps);
		case "local":
			return dispatchLocal(step, ctx, deps);
		default: {
			const _exhaustive: never = step;
			throw new Error(
				`workflow/runner: unknown step type ${JSON.stringify(_exhaustive)}`,
			);
		}
	}
}

// dispatchCall — single sb.rpc.call. No retry, no backoff, no
// idempotency-lookup, no circuit-breaker, no timeout-loop here. Those live in
// sb.rpc.call (ADR-W-018).
async function dispatchCall(
	step: CallStep,
	ctx: RunContext,
	deps: RunnerDeps,
): Promise<unknown> {
	const service = evalLiteralOrPath(step.service as JsonExpression, ctx.state);
	const method = evalLiteralOrPath(step.method as JsonExpression, ctx.state);
	const input = evalLiteralOrPath(step.input, ctx.state);
	const opts = step.opts ? evalOpts(step.opts, ctx.state) : undefined;
	return deps.sb.rpc.call(asString(service), asString(method), input, opts);
}

async function dispatchPublish(
	step: PublishStep,
	ctx: RunContext,
	deps: RunnerDeps,
): Promise<unknown> {
	const event = evalLiteralOrPath(step.event as JsonExpression, ctx.state);
	const payload = evalLiteralOrPath(step.input, ctx.state);
	const opts = step.opts ? evalOpts(step.opts, ctx.state) : undefined;
	return deps.sb.event.publish(asString(event), payload, opts);
}

async function dispatchSubWorkflow(
	step: WorkflowStep,
	ctx: RunContext,
	deps: RunnerDeps,
): Promise<unknown> {
	const name = evalLiteralOrPath(step.workflow as JsonExpression, ctx.state);
	const input = evalLiteralOrPath(step.input, ctx.state);
	const baseOpts = step.opts ? evalOpts(step.opts, ctx.state) : undefined;
	// Propagate parent run id so the runtime can enforce dynamic cycle
	// detection across the ancestor chain (ADR-W-019).
	const opts = { ...(baseOpts ?? {}), parentRunId: ctx.runId };
	const { runId } = await deps.sb.workflow.start(asString(name), input, opts);
	return deps.sb.workflow.await(runId);
}

async function dispatchSleep(
	step: SleepStep,
	ctx: RunContext,
	deps: RunnerDeps,
): Promise<unknown> {
	await deps.ops.park({
		runId: ctx.runId,
		stepId: step.id,
		leaseEpoch: ctx.leaseEpoch,
		sleep: { durationSec: step.durationSec },
	});
	return PARKED;
}

async function dispatchWaitEvent(
	step: WaitEventStep,
	ctx: RunContext,
	deps: RunnerDeps,
): Promise<unknown> {
	const filter = step.filter
		? JSON.stringify(
				Object.fromEntries(
					Object.entries(step.filter).map(([k, v]) => [
						k,
						evalLiteralOrPath(v, ctx.state),
					]),
				),
			)
		: "";
	await deps.ops.park({
		runId: ctx.runId,
		stepId: step.id,
		leaseEpoch: ctx.leaseEpoch,
		eventWait: {
			event: step.event,
			filterJson: filter,
			timeoutSec: step.timeoutSec ?? 0,
		},
	});
	return PARKED;
}

async function dispatchWaitSignal(
	step: WaitSignalStep,
	ctx: RunContext,
	deps: RunnerDeps,
): Promise<unknown> {
	await deps.ops.park({
		runId: ctx.runId,
		stepId: step.id,
		leaseEpoch: ctx.leaseEpoch,
		signalWait: { signal: step.signal, timeoutSec: step.timeoutSec ?? 0 },
	});
	return PARKED;
}

async function dispatchGroup(
	step: ParallelStep | SequenceStep,
	ctx: RunContext,
	deps: RunnerDeps,
): Promise<unknown> {
	// Iterations: each one is a (template-steps[], extra state bindings) pair.
	const iterations: { steps: Step[]; bindings: State; suffix: string }[] = [];
	if (step.forEach) {
		const items = evalLiteralOrPath(step.forEach.from, ctx.state);
		if (!Array.isArray(items)) {
			throw new Error(
				`workflow/runner: forEach.from must resolve to array (got ${typeof items})`,
			);
		}
		items.forEach((item, idx) => {
			iterations.push({
				steps: step.steps.map((s) => rewriteStepIds(s, `${idx}`)),
				bindings: { [step.forEach!.as]: item },
				suffix: `${idx}`,
			});
		});
	} else {
		iterations.push({ steps: step.steps, bindings: {}, suffix: "" });
	}

	const groupOutput: Record<string, unknown> = {};
	const runIteration = async (
		it: (typeof iterations)[number],
	): Promise<void> => {
		const subState: State = { ...ctx.state, ...it.bindings };
		const subCtx: RunContext = { ...ctx, state: subState };
		const runBranchBody = async (): Promise<void> => {
			if (step.type === "parallel") {
				await Promise.all(
					it.steps.map((s) =>
						executeStep(s, subCtx, deps, new Set(Object.keys(subState))),
					),
				);
			} else {
				const completed = new Set<string>(Object.keys(subState));
				for (const s of it.steps) {
					await executeStep(s, subCtx, deps, completed);
				}
			}
		};
		// One branch span per iteration, nested under the group span (R1: each
		// branch enters its own span scope inside its own async scope, so
		// parallel siblings never leak each other's parentOpId via shared ALS).
		// The non-forEach single iteration (suffix "") needs no extra branch
		// level — its steps already nest directly under the group span.
		if (it.suffix !== "" && deps.wrapStep) {
			await deps.wrapStep(
				{
					runId: ctx.runId,
					stepId: `${step.id}:${it.suffix}`,
					stepName: `${step.id}[${it.suffix}]`,
					role: "branch",
				},
				runBranchBody,
			);
		} else {
			await runBranchBody();
		}
		for (const s of it.steps) {
			groupOutput[s.id] = subState[s.id] ?? null;
		}
	};

	if (step.type === "parallel") {
		await Promise.all(iterations.map(runIteration));
	} else {
		for (const it of iterations) await runIteration(it);
	}
	return groupOutput;
}

function rewriteStepIds(step: Step, suffix: string): Step {
	const copy: Step = { ...step, id: `${step.id}:${suffix}` };
	if (copy.type === "parallel" || copy.type === "sequence") {
		copy.steps = copy.steps.map((c) => rewriteStepIds(c, suffix));
	}
	return copy;
}

async function dispatchLocal(
	step: LocalStep,
	ctx: RunContext,
	_deps: RunnerDeps,
): Promise<unknown> {
	return step.fn(ctx.state);
}

function evalOpts(
	opts: Record<string, JsonExpression>,
	state: State,
): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (const [k, v] of Object.entries(opts)) {
		out[k] = evalLiteralOrPath(v, state);
	}
	return out;
}

function asString(v: unknown): string {
	if (typeof v !== "string") {
		throw new Error(
			`workflow/runner: expected string from JsonExpression, got ${typeof v}`,
		);
	}
	return v;
}

function unpackError(err: unknown): { code: string; message: string } {
	if (err instanceof Error) {
		const raw = (err as unknown as { code?: string | number }).code;
		const code = raw !== undefined ? String(raw) : "UNKNOWN";
		return { code, message: err.message };
	}
	return { code: "UNKNOWN", message: String(err) };
}

// runCompensation — reverse-order traversal of completed call/publish steps
// that declare `compensate`. Per ADR-W-007: shape inputs against final state
// and replay through the corresponding owner-module operation.
async function runCompensation(
	steps: Step[],
	ctx: RunContext,
	deps: RunnerDeps,
): Promise<void> {
	const flat: Step[] = [];
	flatten(steps, flat);
	const reversed = [...flat].reverse();
	for (const step of reversed) {
		if (!(step.type === "call" || step.type === "publish")) continue;
		const comp = (step as { compensate?: CompensateSpec }).compensate;
		if (!comp) continue;
		if (ctx.state[step.id] === undefined || ctx.state[step.id] === null) {
			continue; // not completed → nothing to compensate
		}
		const input = evalLiteralOrPath(comp.input, ctx.state);
		const compStepId = `${step.id}.compensate`;
		await deps.ops.beginStep({
			runId: ctx.runId,
			stepId: compStepId,
			parentStepId: step.id,
			kind: comp.type ?? (step.type === "call" ? "call" : "publish"),
			inputSnapshot: input,
			leaseEpoch: ctx.leaseEpoch,
		});
		const kind = comp.type ?? step.type;
		// Forward CompensateSpec fields as opts to the owner-module op. The
		// runner itself does NOT implement any policy loop — it only transports
		// declarative options down to sb.rpc.call / sb.event.publish, which
		// own their semantics (ADR-W-018).
		const opts = buildCompensateOpts(comp);

		const executeCompensateOp = async (): Promise<unknown> => {
			if (kind === "call") {
				return deps.sb.rpc.call(
					comp.service ?? ((step as CallStep).service as string),
					comp.method ?? ((step as CallStep).method as string),
					input,
					opts,
				);
			}
			return deps.sb.event.publish(
				comp.event ?? ((step as PublishStep).event as string),
				input,
				opts,
			);
		};

		const output = deps.wrapStep
			? await deps.wrapStep(
					{
						runId: ctx.runId,
						stepId: compStepId,
						stepName: `compensate ${step.id}`,
						role: "compensation",
						isCompensation: true,
						compensatesForStepId: step.id,
					},
					executeCompensateOp,
				)
			: await executeCompensateOp();

		await deps.ops.completeStep({
			runId: ctx.runId,
			stepId: compStepId,
			output,
			leaseEpoch: ctx.leaseEpoch,
		});
	}
}

function flatten(steps: Step[], out: Step[]): void {
	for (const s of steps) {
		out.push(s);
		if (s.type === "parallel" || s.type === "sequence") {
			flatten(s.steps, out);
		}
	}
}
