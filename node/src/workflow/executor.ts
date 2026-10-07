// Owner-side task executor: consumes `Workflows.Subscribe`, runs each leased
// step task (local function, RPC call, event publish, compensation), keeps the
// lease alive with heartbeats and reports the result by task token. It holds
// no DAG logic — readiness, retries, parks and compensation order belong to the
// runtime (ADR 0003).
//
// @internal — см. ./README.md

import type { ClientReadableStream, ServiceError } from "@grpc/grpc-js";
import { HandlerError, ServiceBridgeError } from "../errors";
import type {
	StepTask,
	TaskKind,
	WorkflowsClient,
} from "../pb/servicebridge/v1/workflows";
import { TaskKind as Kind } from "../pb/servicebridge/v1/workflows";
import { StreamSupervisor } from "../registry/stream-supervisor";
import { runWithTrace } from "../telemetry/context";
import { parseXSbTrace } from "../telemetry/wire-trace";
import type { ReconnectDelayOptions } from "../utils/reconnect-ladder";
import type { LocalDefinition } from "./domain";

// gRPC codes the report path distinguishes.
const ABORTED = 10;
const UNAVAILABLE = 14;
const DEADLINE_EXCEEDED = 4;
const RESOURCE_EXHAUSTED = 8;

// Report retries for a transient channel failure; after them the lease runs
// out and the runtime re-leases the step (at-least-once).
const REPORT_BACKOFF_MS = [200, 1000, 3000];

// SpanInfo describes the USER.SUBOP span opened around a local step or a
// compensation. Call and publish steps get no wrapping span: their own
// RPC.CALL / EVENT.PUBLISH op hangs under the run root.
// @internal
export interface SpanInfo {
	runId: string;
	stepId: string;
	workflow: string;
	isCompensation: boolean;
	compensatesStepId: string;
}

// @internal
export interface ExecutorDeps {
	rpc: WorkflowsClient;
	// Session identity; null until the first Welcome.
	identity: () => { serviceId: string; instanceId: string } | null;
	sb: {
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
	};
	definition: (workflow: string) => LocalDefinition | undefined;
	wrapSpan?: <T>(info: SpanInfo, fn: () => Promise<T>) => Promise<T>;
	logger: {
		warn(msg: string, ...args: unknown[]): void;
		error(msg: string, ...args: unknown[]): void;
	};
	reconnectOpts?: ReconnectDelayOptions;
	onSchedule?: (delayMs: number) => void;
}

interface Running {
	controller: AbortController;
	heartbeatMs: number;
	lastBeat: number;
	deadline: ReturnType<typeof setTimeout> | null;
	lost: boolean;
}

export class WorkflowExecutor {
	private closed = true;
	private readonly running = new Map<string, Running>();
	private readonly supervisor: StreamSupervisor<
		ClientReadableStream<StepTask>,
		StepTask
	>;
	private beatTimer: ReturnType<typeof setInterval> | null = null;

	constructor(private readonly d: ExecutorDeps) {
		this.supervisor = new StreamSupervisor({
			open: () => (this.d.identity() ? this.d.rpc.subscribe({}) : null),
			onData: (task) => {
				void this.execute(task).catch((err) =>
					this.d.logger.error(
						`workflow task ${task.runId}/${task.stepId}: ${(err as Error).message}`,
					),
				);
			},
			onError: (err) =>
				this.d.logger.warn("workflow executor: stream error", err.message),
			reconnectOpts: d.reconnectOpts,
			onSchedule: d.onSchedule,
		});
	}

	start(): void {
		if (!this.closed) return;
		this.closed = false;
		this.supervisor.start();
		const timer = setInterval(() => this.heartbeat(), 250);
		timer.unref();
		this.beatTimer = timer;
	}

	// close stops the stream and aborts every running task; their leases expire
	// and the runtime re-leases the steps.
	close(): void {
		this.closed = true;
		this.supervisor.stop();
		if (this.beatTimer) clearInterval(this.beatTimer);
		this.beatTimer = null;
		for (const r of this.running.values()) {
			r.lost = true;
			r.controller.abort(new Error("workflow executor closed"));
			if (r.deadline) clearTimeout(r.deadline);
		}
		this.running.clear();
	}

	// inFlight reports how many tasks are executing (tests, drain).
	inFlight(): number {
		return this.running.size;
	}

	private async execute(task: StepTask): Promise<void> {
		if (this.closed || this.running.has(task.taskToken)) return;
		const r: Running = {
			controller: new AbortController(),
			heartbeatMs: Math.max(task.heartbeatIntervalMs, 250),
			lastBeat: Date.now(),
			deadline: null,
			lost: false,
		};
		if (task.deadlineUnixMs > 0) {
			r.deadline = setTimeout(
				() => r.controller.abort(new Error("workflow step deadline passed")),
				Math.max(task.deadlineUnixMs - Date.now(), 0),
			);
			r.deadline.unref();
		}
		this.running.set(task.taskToken, r);
		let output: unknown;
		let failure: { code: string; message: string; permanent: boolean } | null =
			null;
		try {
			const trace = parseXSbTrace(task.xSbTrace);
			const run = () => this.perform(task, r.controller.signal);
			output = trace ? await runWithTrace(trace, run) : await run();
		} catch (err) {
			failure = describe(err);
		} finally {
			if (r.deadline) clearTimeout(r.deadline);
			this.running.delete(task.taskToken);
		}
		// A lost lease means another attempt may already run: report nothing.
		if (r.lost || this.closed) return;
		if (failure) await this.report("fail", task, failure);
		else await this.report("complete", task, output);
	}

	private async perform(task: StepTask, signal: AbortSignal): Promise<unknown> {
		const input = parse(task.input);
		switch (task.kind as TaskKind) {
			case Kind.TASK_KIND_LOCAL: {
				const def = this.d.definition(task.workflow);
				const fn =
					def?.version === task.version
						? def.locals.get(task.templateStepId)
						: undefined;
				if (!fn)
					throw new UnsupportedVersionError(
						`unsupported_version: ${task.workflow}@${task.version} step ${task.templateStepId}`,
					);
				const state = (parse(task.state) ?? {}) as Record<string, unknown>;
				const call = async () =>
					fn(state, {
						signal,
						runId: task.runId,
						stepId: task.stepId,
						attempt: task.attempt,
					});
				return this.wrap(task, call);
			}
			case Kind.TASK_KIND_CALL: {
				const o = task.callOpts;
				const opts: Record<string, unknown> = { signal };
				if (o?.timeoutMs) opts.timeout = `${o.timeoutMs}ms`;
				if (o?.transport) opts.transport = o.transport;
				if (o?.idempotencyKey) opts.idempotencyKey = o.idempotencyKey;
				if (o?.requestId) opts.requestId = o.requestId;
				if (o?.retry) opts.retry = definedRetry(o.retry);
				return this.wrap(task, () =>
					this.d.sb.rpc.call(task.service, task.method, input, opts),
				);
			}
			case Kind.TASK_KIND_PUBLISH: {
				const o = task.publishOpts;
				const opts: Record<string, unknown> = {};
				if (o?.idempotencyKey) opts.idempotencyKey = o.idempotencyKey;
				if (o?.partitionKey) opts.partitionKey = o.partitionKey;
				if (o && Object.keys(o.headers).length > 0) opts.headers = o.headers;
				return this.wrap(task, () =>
					this.d.sb.event.publish(task.event, input, opts),
				);
			}
		}
		throw new UnsupportedVersionError(`unknown task kind ${task.kind}`);
	}

	// wrap opens the USER.SUBOP span for local steps and compensations.
	private wrap<T>(task: StepTask, fn: () => Promise<T>): Promise<T> {
		const span = task.kind === Kind.TASK_KIND_LOCAL || task.isCompensation;
		if (!span || !this.d.wrapSpan) return fn();
		return this.d.wrapSpan(
			{
				runId: task.runId,
				stepId: task.stepId,
				workflow: task.workflow,
				isCompensation: task.isCompensation,
				compensatesStepId: task.compensatesStepId,
			},
			fn,
		);
	}

	private heartbeat(): void {
		if (this.closed || this.running.size === 0 || !this.d.identity()) return;
		const now = Date.now();
		const due: string[] = [];
		for (const [token, r] of this.running) {
			if (now - r.lastBeat >= r.heartbeatMs) {
				r.lastBeat = now;
				due.push(token);
			}
		}
		if (due.length === 0) return;
		try {
			this.d.rpc.heartbeat({ taskTokens: due }, (err, resp) => {
				if (err) {
					this.d.logger.warn(`workflow heartbeat failed: ${err.message}`);
					return;
				}
				for (const token of resp.lostTokens) this.loseLease(token);
			});
		} catch (err) {
			this.d.logger.warn(`workflow heartbeat threw: ${(err as Error).message}`);
		}
	}

	// loseLease aborts an execution whose lease the runtime no longer
	// recognizes (expired, run stopped, re-leased elsewhere).
	private loseLease(token: string): void {
		const r = this.running.get(token);
		if (!r) return;
		r.lost = true;
		r.controller.abort(new Error("workflow task lease lost"));
	}

	private async report(
		kind: "complete" | "fail",
		task: StepTask,
		result: unknown,
	): Promise<void> {
		for (let attempt = 0; ; attempt++) {
			try {
				await new Promise<void>((resolve, reject) => {
					const done = (err: ServiceError | null) =>
						err ? reject(err) : resolve();
					if (kind === "complete")
						this.d.rpc.completeTask(
							{ taskToken: task.taskToken, output: json(result) },
							done,
						);
					else {
						const f = result as {
							code: string;
							message: string;
							permanent: boolean;
						};
						this.d.rpc.failTask(
							{
								taskToken: task.taskToken,
								errorCode: f.code,
								errorMessage: f.message,
								nonRetriable: f.permanent,
							},
							done,
						);
					}
				});
				return;
			} catch (err) {
				const code = (err as ServiceError).code;
				const transient =
					code === UNAVAILABLE ||
					code === DEADLINE_EXCEEDED ||
					code === RESOURCE_EXHAUSTED;
				if (
					code === ABORTED ||
					!transient ||
					attempt >= REPORT_BACKOFF_MS.length ||
					this.closed
				) {
					if (code !== ABORTED)
						this.d.logger.warn(
							`workflow task ${task.runId}/${task.stepId}: ${kind} not recorded: ${(err as Error).message}`,
						);
					return;
				}
				await new Promise((r) => setTimeout(r, REPORT_BACKOFF_MS[attempt]));
			}
		}
	}
}

class UnsupportedVersionError extends Error {
	readonly code = "UNSUPPORTED_VERSION";
}

function describe(err: unknown): {
	code: string;
	message: string;
	permanent: boolean;
} {
	if (err instanceof UnsupportedVersionError)
		return { code: err.code, message: err.message, permanent: true };
	// A failed call reports the callee's business code, so a workflow can tell
	// failures apart; any other SDK failure reports its error code.
	if (err instanceof HandlerError)
		return { code: err.handlerCode, message: err.message, permanent: false };
	if (err instanceof ServiceBridgeError)
		return { code: err.code, message: err.message, permanent: false };
	if (err instanceof Error) {
		const raw = (err as { code?: string | number }).code;
		return {
			code: raw === undefined ? "ERROR" : String(raw),
			message: err.message,
			permanent: false,
		};
	}
	return { code: "ERROR", message: String(err), permanent: false };
}

function json(v: unknown): Buffer {
	return Buffer.from(JSON.stringify(v ?? null), "utf8");
}

function parse(b: Uint8Array | undefined): unknown {
	if (!b || b.length === 0) return null;
	return JSON.parse(Buffer.from(b).toString("utf8"));
}

function definedRetry(r: {
	maxAttempts: number;
	baseDelayMs: number;
	factor: number;
	maxDelayMs: number;
	jitter: number;
}): Record<string, number> {
	const out: Record<string, number> = {};
	if (r.maxAttempts) out.maxAttempts = r.maxAttempts;
	if (r.baseDelayMs) out.baseDelayMs = r.baseDelayMs;
	if (r.factor) out.factor = r.factor;
	if (r.maxDelayMs) out.maxDelayMs = r.maxDelayMs;
	if (r.jitter) out.jitter = r.jitter;
	return out;
}
