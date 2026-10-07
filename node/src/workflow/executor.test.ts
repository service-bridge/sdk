import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import { HandlerError, NoLiveInstanceError } from "../errors";
import type {
	StepTask,
	WorkflowsClient,
} from "../pb/servicebridge/v1/workflows";
import { TaskKind } from "../pb/servicebridge/v1/workflows";
import { currentTraceContext } from "../telemetry/context";
import type { LocalFns } from "./encode";
import { type ExecutorDeps, WorkflowExecutor } from "./executor";

class FakeStream extends EventEmitter {
	cancel(): void {
		this.emit("end");
	}
}

interface Reports {
	completed: Array<{ taskToken: string; output: unknown }>;
	failed: Array<{
		taskToken: string;
		errorCode: string;
		nonRetriable: boolean;
	}>;
	beats: string[][];
}

function fakeClient(opts: { lost?: string[]; failCompleteOnce?: number } = {}) {
	const stream = new FakeStream();
	const reports: Reports = { completed: [], failed: [], beats: [] };
	let completeFailures = opts.failCompleteOnce ?? 0;
	const client = {
		subscribe: () => stream,
		completeTask: (
			req: { taskToken: string; output: Buffer },
			cb: (e: unknown) => void,
		) => {
			if (completeFailures > 0) {
				completeFailures--;
				return cb(Object.assign(new Error("unavailable"), { code: 14 }));
			}
			reports.completed.push({
				taskToken: req.taskToken,
				output: JSON.parse(req.output.toString()),
			});
			cb(null);
		},
		failTask: (
			req: { taskToken: string; errorCode: string; nonRetriable: boolean },
			cb: (e: unknown) => void,
		) => {
			reports.failed.push(req);
			cb(null);
		},
		heartbeat: (
			req: { taskTokens: string[] },
			cb: (e: unknown, r: unknown) => void,
		) => {
			reports.beats.push(req.taskTokens);
			cb(null, {
				lostTokens: req.taskTokens.filter((t) => opts.lost?.includes(t)),
			});
		},
	};
	return { stream, reports, client: client as unknown as WorkflowsClient };
}

function task(over: Partial<StepTask>): StepTask {
	return {
		taskToken: "tok",
		runId: "run",
		workflow: "wf",
		version: "v1",
		fingerprint: "fp",
		stepId: "s",
		templateStepId: "s",
		kind: TaskKind.TASK_KIND_LOCAL,
		attempt: 1,
		service: "",
		method: "",
		callOpts: undefined,
		event: "",
		publishOpts: undefined,
		input: Buffer.alloc(0),
		state: Buffer.from(JSON.stringify({ input: { n: 2 } })),
		isCompensation: false,
		compensatesStepId: "",
		leaseTtlMs: 30_000,
		heartbeatIntervalMs: 10_000,
		deadlineUnixMs: 0,
		xSbTrace: "",
		...over,
	};
}

function executor(
	client: WorkflowsClient,
	locals: LocalFns,
	extra: Partial<ExecutorDeps> = {},
) {
	const calls: unknown[][] = [];
	const ex = new WorkflowExecutor({
		rpc: client,
		identity: () => ({ serviceId: "svc", instanceId: "inst" }),
		sb: {
			rpc: {
				call: async (...args) => {
					calls.push(["call", ...args]);
					return { ok: true };
				},
			},
			event: {
				publish: async (...args) => {
					calls.push(["publish", ...args]);
					return { eventId: "e1" };
				},
			},
		},
		definition: (name) =>
			name === "wf" ? { version: "v1", locals } : undefined,
		logger: { warn: () => {}, error: () => {} },
		...extra,
	});
	return { ex, calls };
}

const until = async (cond: () => boolean) => {
	const deadline = Date.now() + 3000;
	while (!cond()) {
		if (Date.now() > deadline) throw new Error("timed out");
		await new Promise((r) => setTimeout(r, 5));
	}
};

describe("WorkflowExecutor", () => {
	it("runs local steps with state and reports the output", async () => {
		const { stream, reports, client } = fakeClient();
		const spans: string[] = [];
		const { ex } = executor(
			client,
			new Map([
				[
					"s",
					(state: Record<string, unknown>) =>
						(state.input as { n: number }).n * 2,
				],
			]),
			{
				wrapSpan: async (info, fn) => {
					spans.push(info.stepId);
					return fn();
				},
			},
		);
		ex.start();
		stream.emit("data", task({}));
		await until(() => reports.completed.length === 1);
		expect(reports.completed[0]).toEqual({ taskToken: "tok", output: 4 });
		expect(spans).toEqual(["s"]);
		ex.close();
	});

	it("runs call and publish tasks with resolved options, no wrapping span, inside the run trace", async () => {
		const { stream, reports, client } = fakeClient();
		const spans: string[] = [];
		let traced: string | undefined;
		const { ex, calls } = executor(client, new Map(), {
			wrapSpan: async (info, fn) => {
				spans.push(info.stepId);
				return fn();
			},
			sb: {
				rpc: {
					call: async (...args) => {
						traced = currentTraceContext()?.traceId;
						calls.push(["call", ...args]);
						return 1;
					},
				},
				event: {
					publish: async (...args) => {
						calls.push(["publish", ...args]);
						return 2;
					},
				},
			},
		});
		ex.start();
		stream.emit(
			"data",
			task({
				taskToken: "c",
				kind: TaskKind.TASK_KIND_CALL,
				service: "billing",
				method: "charge",
				input: Buffer.from('{"a":1}'),
				callOpts: {
					timeoutMs: 500,
					transport: "proxy",
					idempotencyKey: "k",
					requestId: "",
					retry: undefined,
				},
				xSbTrace:
					"01a11369-e617-7683-9b36-3551ddd8a6a8-01a11369-e617-7683-9b36-3551ddd8a6a9",
			}),
		);
		stream.emit(
			"data",
			task({
				taskToken: "p",
				kind: TaskKind.TASK_KIND_PUBLISH,
				event: "order.done",
				input: Buffer.from("[1]"),
				publishOpts: {
					idempotencyKey: "",
					partitionKey: "pk",
					headers: { h: "v" },
				},
			}),
		);
		await until(() => reports.completed.length === 2);
		const call = calls.find((c) => c[0] === "call")!;
		expect(call.slice(1, 4)).toEqual(["billing", "charge", { a: 1 }]);
		expect(call[4]).toMatchObject({
			timeout: "500ms",
			transport: "proxy",
			idempotencyKey: "k",
		});
		const pub = calls.find((c) => c[0] === "publish")!;
		expect(pub.slice(1)).toEqual([
			"order.done",
			[1],
			{ partitionKey: "pk", headers: { h: "v" } },
		]);
		expect(spans).toEqual([]);
		expect(traced).toBe("01a11369-e617-7683-9b36-3551ddd8a6a8");
		ex.close();
	});

	it("wraps compensations in a span and reports failures with the error code", async () => {
		const { stream, reports, client } = fakeClient();
		const spans: Array<{ id: string; comp: boolean }> = [];
		const { ex } = executor(client, new Map(), {
			wrapSpan: async (info, fn) => {
				spans.push({ id: info.compensatesStepId, comp: info.isCompensation });
				return fn();
			},
			sb: {
				rpc: {
					call: async () => {
						throw new HandlerError("DECLINED", "declined");
					},
				},
				event: { publish: async () => null },
			},
		});
		ex.start();
		stream.emit(
			"data",
			task({
				kind: TaskKind.TASK_KIND_CALL,
				service: "b",
				method: "refund",
				isCompensation: true,
				compensatesStepId: "charge",
			}),
		);
		await until(() => reports.failed.length === 1);
		expect(reports.failed[0]).toMatchObject({
			errorCode: "DECLINED",
			nonRetriable: false,
		});
		expect(spans).toEqual([{ id: "charge", comp: true }]);
		ex.close();
	});

	it("reports an SDK failure of a call task with its error code", async () => {
		const { stream, reports, client } = fakeClient();
		const { ex } = executor(client, new Map(), {
			sb: {
				rpc: {
					call: async () => {
						throw new NoLiveInstanceError("nobody serves b.m");
					},
				},
				event: { publish: async () => null },
			},
		});
		ex.start();
		stream.emit(
			"data",
			task({ kind: TaskKind.TASK_KIND_CALL, service: "b", method: "m" }),
		);
		await until(() => reports.failed.length === 1);
		expect(reports.failed[0]).toMatchObject({
			errorCode: "NO_LIVE_INSTANCE",
			nonRetriable: false,
		});
		ex.close();
	});

	it("reports an unknown version as a permanent failure", async () => {
		const { stream, reports, client } = fakeClient();
		const { ex } = executor(client, new Map([["s", () => 1]]));
		ex.start();
		stream.emit("data", task({ version: "v9" }));
		await until(() => reports.failed.length === 1);
		expect(reports.failed[0]).toMatchObject({
			errorCode: "UNSUPPORTED_VERSION",
			nonRetriable: true,
		});
		ex.close();
	});

	it("aborts an execution whose lease is lost and reports nothing", async () => {
		const { stream, reports, client } = fakeClient({ lost: ["gone"] });
		let aborted = false;
		const { ex } = executor(
			client,
			new Map([
				[
					"s",
					(_: unknown, ctx: { signal: AbortSignal }) =>
						new Promise((_, reject) => {
							ctx.signal.addEventListener("abort", () => {
								aborted = true;
								reject(new Error("aborted"));
							});
						}),
				],
			]),
		);
		ex.start();
		stream.emit("data", task({ taskToken: "gone", heartbeatIntervalMs: 250 }));
		await until(() => aborted);
		await new Promise((r) => setTimeout(r, 30));
		expect(reports.beats.flat()).toContain("gone");
		expect(reports.completed).toHaveLength(0);
		expect(reports.failed).toHaveLength(0);
		expect(ex.inFlight()).toBe(0);
		ex.close();
	});

	it("aborts at the step deadline", async () => {
		const { stream, reports, client } = fakeClient();
		const { ex } = executor(
			client,
			new Map([
				[
					"s",
					(_: unknown, ctx: { signal: AbortSignal }) =>
						new Promise((_, reject) =>
							ctx.signal.addEventListener("abort", () =>
								reject(new Error("deadline")),
							),
						),
				],
			]),
		);
		ex.start();
		stream.emit("data", task({ deadlineUnixMs: Date.now() + 20 }));
		await until(() => reports.failed.length === 1);
		ex.close();
	});

	it("retries a completion over a transient channel failure", async () => {
		const { stream, reports, client } = fakeClient({ failCompleteOnce: 1 });
		const { ex } = executor(client, new Map([["s", () => "x"]]));
		ex.start();
		stream.emit("data", task({}));
		await until(() => reports.completed.length === 1);
		ex.close();
	});
});
