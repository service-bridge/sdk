// subscriber.trace.test.ts — trace-context propagation for event delivery.
//
// Keystone (service-bridge.ts::runHandlerWithTrace): an event handler must run
// inside the ALS trace context parsed from EventEnvelope.xSbTrace so any nested
// sb.rpc.call / sb.event.publish inherits the inbound traceId + parentOpId
// instead of minting its own root. The composition under test is exactly the
// one ServiceBridge injects: parseXSbTrace(...) ? runWithTrace(parsed, fn) : fn().
//
// The Subscriber wraps the handler→ack cycle in runWithTrace for BOTH the
// no-partition (parallel) path and the partition-keyed (serial) path; both are
// covered. We inject the real keystone composition and assert the handler
// observes the right currentTraceContext().

import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import { currentTraceContext, runWithTrace } from "../telemetry/context";
import { formatXSbTrace, parseXSbTrace } from "../telemetry/wire-trace";
import type { SubscriberDeps } from "./subscriber";
import { Subscriber } from "./subscriber";

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

function makeSchema() {
	return {
		input: {
			encode: (_v: unknown) => new Uint8Array(),
			decode: (_b: Uint8Array) => ({ amount: 42 }),
			toJsonSchema: () => ({}),
		},
		output: {
			encode: (_v: unknown) => new Uint8Array(),
			decode: (_b: Uint8Array) => ({}),
			toJsonSchema: () => ({}),
		},
	};
}

function makeFakeStream() {
	const emitter = new EventEmitter();
	const written: unknown[] = [];
	return {
		emitter,
		written,
		stream: {
			write: (msg: unknown) => written.push(msg),
			cancel: () => {},
			end: () => {},
			on: (event: string, cb: (...a: unknown[]) => void) =>
				emitter.on(event, cb),
		},
	};
}

function makeDelivery(
	deliveryId: string,
	xSbTrace: string,
	partitionKey = "",
	name = "order.created",
) {
	return {
		delivery: {
			deliveryId,
			envelope: {
				id: `ev-${deliveryId}`,
				name,
				payload: new Uint8Array([1, 2, 3]),
				partitionKey,
				xSbTrace,
			},
			attempt: 1,
		},
	};
}

function makeDeps(handlerFn: (p: unknown) => Promise<void>): {
	deps: SubscriberDeps;
	fake: ReturnType<typeof makeFakeStream>;
} {
	const fake = makeFakeStream();
	const deps: SubscriberDeps = {
		// biome-ignore lint/suspicious/noExplicitAny: minimal grpc stub
		rpcClient: { subscribe: () => fake.stream } as any,
		schemaIndex: {
			get: (name: string) =>
				name === "order.created"
					? { contractHash: "h", pair: makeSchema() }
					: undefined,
		},
		identity: () => ({ serviceId: "svc-1", instanceId: "inst-1" }),
		handlers: (pattern) => (pattern === "order.created" ? [handlerFn] : []),
		maxInFlight: 32,
		logger: { warn: () => {}, error: () => {} },
		runWithTrace: runHandlerWithTrace,
	};
	return { deps, fake };
}

describe("Subscriber trace propagation", () => {
	it("HandlerRunsInsideParsedTraceContext_NoPartition", async () => {
		let seen: ReturnType<typeof currentTraceContext>;
		const { deps, fake } = makeDeps(async () => {
			seen = currentTraceContext();
		});
		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit(
			"data",
			makeDelivery("d-1", formatXSbTrace(TRACE_ID, PARENT_OP)),
		);
		await Bun.sleep(10);

		expect(seen).toBeDefined();
		expect(seen!.traceId).toBe(TRACE_ID);
		expect(seen!.parentOpId).toBe(PARENT_OP);
		await sub.stop();
	});

	it("HandlerRunsInsideParsedTraceContext_PartitionKeyed", async () => {
		// Partition-keyed deliveries go through the serial-queue path, which is a
		// distinct wrapping site for runWithTrace.
		let seen: ReturnType<typeof currentTraceContext>;
		const { deps, fake } = makeDeps(async () => {
			seen = currentTraceContext();
		});
		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit(
			"data",
			makeDelivery("d-pk", formatXSbTrace(TRACE_ID, PARENT_OP), "tenant-7"),
		);
		await Bun.sleep(10);

		expect(seen).toBeDefined();
		expect(seen!.traceId).toBe(TRACE_ID);
		await sub.stop();
	});

	it("NestedAsyncWorkInheritsTraceContext", async () => {
		let nestedTrace: string | undefined;
		async function nestedCall(): Promise<void> {
			await Promise.resolve();
			nestedTrace = currentTraceContext()?.traceId;
		}
		const { deps, fake } = makeDeps(async () => {
			await Promise.resolve();
			await nestedCall();
		});
		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit(
			"data",
			makeDelivery("d-2", formatXSbTrace(TRACE_ID, PARENT_OP)),
		);
		await Bun.sleep(10);

		expect(nestedTrace).toBe(TRACE_ID);
		await sub.stop();
	});

	it("EmptyXSbTrace_HandlerStillRunsWithoutContext_AndAcks", async () => {
		let ran = false;
		let seen: ReturnType<typeof currentTraceContext>;
		const { deps, fake } = makeDeps(async () => {
			ran = true;
			seen = currentTraceContext();
		});
		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit("data", makeDelivery("d-3", ""));
		await Bun.sleep(10);

		expect(ran).toBe(true);
		expect(seen).toBeUndefined();
		const acked = fake.written.find(
			// biome-ignore lint/suspicious/noExplicitAny: test assertion
			(m: any) => m.ack?.deliveryId === "d-3",
		);
		expect(acked).toBeDefined();
		await sub.stop();
	});

	it("MalformedXSbTrace_HandlerStillRunsWithoutContext", async () => {
		let ran = false;
		let seen: ReturnType<typeof currentTraceContext>;
		const { deps, fake } = makeDeps(async () => {
			ran = true;
			seen = currentTraceContext();
		});
		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit("data", makeDelivery("d-4", "garbage-not-a-trace"));
		await Bun.sleep(10);

		expect(ran).toBe(true);
		expect(seen).toBeUndefined();
		await sub.stop();
	});

	it("NonTracedDeliveryAfterTracedSeesNoAmbientContext", async () => {
		const observed: Array<string | undefined> = [];
		const { deps, fake } = makeDeps(async () => {
			observed.push(currentTraceContext()?.traceId);
		});
		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit(
			"data",
			makeDelivery("d-a", formatXSbTrace(TRACE_ID, PARENT_OP)),
		);
		await Bun.sleep(10);
		fake.emitter.emit("data", makeDelivery("d-b", ""));
		await Bun.sleep(10);

		expect(observed).toHaveLength(2);
		expect(observed[0]).toBe(TRACE_ID);
		expect(observed[1]).toBeUndefined();
		await sub.stop();
	});
});
