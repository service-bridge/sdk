// subscriber.trace.test.ts — trace-context propagation for job dispatch.
//
// Keystone (service-bridge.ts::runHandlerWithTrace): a job handler must run
// inside the ALS trace context parsed from JobExecution.xSbTrace so any nested
// sb.rpc.call / sb.event.publish inherits the inbound traceId + parentOpId
// instead of minting its own root. The composition under test is exactly the
// one ServiceBridge injects: parseXSbTrace(...) ? runWithTrace(parsed, fn) : fn().
//
// We inject that real composition as JobSubscriber.runWithTrace and assert the
// handler observes the right currentTraceContext(). This exercises (a) which
// xSbTrace the subscriber forwards, (b) all parse branches, (c) async
// inheritance, (d) no leakage between executions.

import { describe, expect, it } from "bun:test";
import { Registry } from "../registry/registry";
import { currentTraceContext, runWithTrace } from "../telemetry/context";
import type { ParsedXSbTrace } from "../telemetry/wire-trace";
import { formatXSbTrace, parseXSbTrace } from "../telemetry/wire-trace";
import { JobDomain } from "./domain";
import type { SubscriberDeps } from "./subscriber";
import { JobSubscriber } from "./subscriber";
import type { JobHandler } from "./types";

function newDomain(): JobDomain {
	return new JobDomain(new Registry());
}

// Exact keystone composition (mirror of ServiceBridge.runHandlerWithTrace).
const runHandlerWithTrace = (
	xSbTrace: string,
	fn: () => Promise<void>,
): Promise<void> => {
	const parsed = parseXSbTrace(xSbTrace);
	return parsed ? runWithTrace(parsed, fn) : fn();
};

const TRACE_ID = "0192f000-0000-7000-8000-000000000abc";
const PARENT_OP = "0192f000-0000-7000-8000-000000000def";

interface FakeStream {
	on(event: string, cb: (...args: unknown[]) => void): void;
	emit(event: string, ...args: unknown[]): void;
	cancel(): void;
}

function makeFakeStream(): FakeStream {
	const listeners = new Map<string, Array<(...a: unknown[]) => void>>();
	return {
		on(event, cb) {
			const list = listeners.get(event) ?? [];
			list.push(cb);
			listeners.set(event, list);
		},
		emit(event, ...args) {
			for (const cb of listeners.get(event) ?? []) cb(...args);
		},
		cancel() {},
	};
}

function makeExec(
	overrides: Partial<{
		jobName: string;
		xSbTrace: string;
		executionId: string;
	}>,
) {
	return {
		executionId: overrides.executionId ?? "exec-1",
		jobName: overrides.jobName ?? "nightly",
		scheduledAtUnixMs: Date.now(),
		localScheduledAtUnixMs: Date.now(),
		attempt: 1,
		leaseEpoch: 1,
		idempotencyKey: "idem-1",
		xSbTrace: overrides.xSbTrace ?? "",
	};
}

function makeSubscriber(
	domain: JobDomain,
	stream: FakeStream,
): { sub: JobSubscriber; results: unknown[] } {
	const results: unknown[] = [];
	const deps: SubscriberDeps = {
		rpcClient: {
			subscribe: () => stream,
			jobResult: (req: unknown, cb: (err: unknown) => void) => {
				results.push(req);
				cb(null);
			},
			heartbeat: () => {},
			// biome-ignore lint/suspicious/noExplicitAny: minimal grpc stub
		} as any,
		identity: () => ({ serviceId: "svc-1", instanceId: "inst-1" }),
		domain,
		logger: { warn: () => {}, error: () => {} },
		runWithTrace: runHandlerWithTrace,
	};
	const sub = new JobSubscriber(deps);
	return { sub, results };
}

describe("JobSubscriber trace propagation", () => {
	it("HandlerRunsInsideParsedTraceContext", async () => {
		const domain = newDomain();
		let seen: ReturnType<typeof currentTraceContext>;
		const fn: JobHandler = async () => {
			seen = currentTraceContext();
		};
		domain.handle("nightly", { trigger: { interval: 1000 } }, fn);

		const stream = makeFakeStream();
		const { sub } = makeSubscriber(domain, stream);
		sub.start();

		stream.emit(
			"data",
			makeExec({ xSbTrace: formatXSbTrace(TRACE_ID, PARENT_OP) }),
		);
		await Bun.sleep(10);

		expect(seen).toBeDefined();
		expect(seen!.traceId).toBe(TRACE_ID);
		expect(seen!.parentOpId).toBe(PARENT_OP);
		await sub.stop();
	});

	it("NestedAsyncWorkInheritsTraceContext", async () => {
		const domain = newDomain();
		let nestedTrace: string | undefined;
		// Simulate sb.rpc.call inside the handler: a nested async hop that reads
		// the ambient context the same way RpcClient does before sending X-SB-Trace.
		async function nestedCall(): Promise<void> {
			await Promise.resolve();
			nestedTrace = currentTraceContext()?.traceId;
		}
		domain.handle("nightly", { trigger: { interval: 1000 } }, async () => {
			await Promise.resolve();
			await nestedCall();
		});

		const stream = makeFakeStream();
		const { sub } = makeSubscriber(domain, stream);
		sub.start();

		stream.emit(
			"data",
			makeExec({ xSbTrace: formatXSbTrace(TRACE_ID, PARENT_OP) }),
		);
		await Bun.sleep(10);

		expect(nestedTrace).toBe(TRACE_ID);
		await sub.stop();
	});

	it("EmptyXSbTrace_HandlerStillRunsWithoutContext", async () => {
		const domain = newDomain();
		let ran = false;
		let seen: ReturnType<typeof currentTraceContext>;
		domain.handle("nightly", { trigger: { interval: 1000 } }, async () => {
			ran = true;
			seen = currentTraceContext();
		});

		const stream = makeFakeStream();
		const { sub, results } = makeSubscriber(domain, stream);
		sub.start();

		stream.emit("data", makeExec({ xSbTrace: "" }));
		await Bun.sleep(10);

		expect(ran).toBe(true);
		expect(seen).toBeUndefined();
		// Handler success still reports a result to the runtime.
		expect(results).toHaveLength(1);
		await sub.stop();
	});

	it("MalformedXSbTrace_HandlerStillRunsWithoutContext", async () => {
		const domain = newDomain();
		let ran = false;
		let seen: ReturnType<typeof currentTraceContext>;
		domain.handle("nightly", { trigger: { interval: 1000 } }, async () => {
			ran = true;
			seen = currentTraceContext();
		});

		const stream = makeFakeStream();
		const { sub } = makeSubscriber(domain, stream);
		sub.start();

		// Too short / not "<uuid>-<uuid>" → parseXSbTrace returns null → no scope.
		stream.emit("data", makeExec({ xSbTrace: "not-a-valid-trace-header" }));
		await Bun.sleep(10);

		expect(ran).toBe(true);
		expect(seen).toBeUndefined();
		await sub.stop();
	});

	// A traced execution followed by a non-traced one: the second must observe
	// no ambient context. Guards the graceful-empty branch against any future
	// change that would establish context process-wide instead of per-dispatch.
	it("ContextDoesNotLeakBetweenSequentialExecutions", async () => {
		const domain = newDomain();
		const observed: Array<ParsedXSbTrace | undefined> = [];
		domain.handle("nightly", { trigger: { interval: 1000 } }, async () => {
			const ctx = currentTraceContext();
			observed.push(ctx ? { ...ctx } : undefined);
		});

		const stream = makeFakeStream();
		const { sub } = makeSubscriber(domain, stream);
		sub.start();

		const traceA = "0192f000-0000-7000-8000-00000000aaaa";
		const opA = "0192f000-0000-7000-8000-00000000bbbb";
		// First execution: traced.
		stream.emit(
			"data",
			makeExec({ executionId: "e-a", xSbTrace: formatXSbTrace(traceA, opA) }),
		);
		await Bun.sleep(10);
		// Second execution: no trace — must NOT see traceA bleeding through.
		stream.emit("data", makeExec({ executionId: "e-b", xSbTrace: "" }));
		await Bun.sleep(10);

		expect(observed).toHaveLength(2);
		expect(observed[0]?.traceId).toBe(traceA);
		expect(observed[1]).toBeUndefined();
		await sub.stop();
	});
});
