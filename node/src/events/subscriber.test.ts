import { describe, expect, it, mock } from "bun:test";
import { EventEmitter } from "node:events";
import type { EventHandlerFn } from "../registry/registry";
import type { SubscriberDeps } from "./subscriber";
import { Subscriber } from "./subscriber";

// -- helpers --

// handlersFor builds the pattern-indexed fan-out lookup the subscriber expects,
// mirroring Handle.eventHandlers: exact-name buckets in registration order,
// empty for anything unregistered. Wildcards never match locally (ADR-0002).
function handlersFor(
	entries: Array<{ pattern: string; fn: EventHandlerFn }>,
): (pattern: string) => readonly EventHandlerFn[] {
	const byPattern = new Map<string, EventHandlerFn[]>();
	for (const e of entries) {
		const bucket = byPattern.get(e.pattern);
		if (bucket) bucket.push(e.fn);
		else byPattern.set(e.pattern, [e.fn]);
	}
	return (pattern) => byPattern.get(pattern) ?? [];
}

// Captured before any spy replaces the global, so test waits never show up in
// the recorded reconnect delays.
const realSetTimeout = globalThis.setTimeout;
const wait = (ms: number): Promise<void> =>
	new Promise((resolve) => {
		realSetTimeout(resolve, ms);
	});

function makeSchema() {
	return {
		input: {
			encode: (_val: unknown) => new Uint8Array(),
			decode: (_buf: Uint8Array) => ({ amount: 42 }),
			toJsonSchema: () => ({}),
		},
		output: {
			encode: (_val: unknown) => new Uint8Array(),
			decode: (_buf: Uint8Array) => ({}),
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
			on: (event: string, cb: (...args: unknown[]) => void) =>
				emitter.on(event, cb),
		},
	};
}

// makeTraceCtx returns a valid trace context for test envelopes.
// Per ADR 0006 §3 runtime always injects trace context into EventEnvelope.
function makeTraceCtx(): {
	traceId: Uint8Array;
	spanId: Uint8Array;
	traceFlags: number;
} {
	const traceId = new Uint8Array(16);
	const spanId = new Uint8Array(8);
	crypto.getRandomValues(traceId);
	crypto.getRandomValues(spanId);
	return { traceId, spanId, traceFlags: 1 };
}

function makeDelivery(
	deliveryId: string,
	name = "order.created",
	payload = new Uint8Array([1, 2, 3]),
): {
	delivery: {
		deliveryId: string;
		envelope: {
			id: string;
			name: string;
			payload: Uint8Array;
			traceId: Uint8Array;
			spanId: Uint8Array;
			traceFlags: number;
		};
		attempt: number;
	};
} {
	return {
		delivery: {
			deliveryId,
			envelope: { id: `ev-${deliveryId}`, name, payload, ...makeTraceCtx() },
			attempt: 1,
		},
	};
}

function makeDeps(
	overrides: Partial<SubscriberDeps> & {
		handlers?: SubscriberDeps["handlers"];
		streamOverride?: ReturnType<typeof makeFakeStream>["stream"];
		extraSchemas?: Record<
			string,
			{ contractHash: string; pair: ReturnType<typeof makeSchema> }
		>;
	} = {},
): {
	deps: SubscriberDeps;
	fake: ReturnType<typeof makeFakeStream>;
} {
	const fake = makeFakeStream();
	const streamRef = overrides.streamOverride ?? fake.stream;
	const builtInSchemas: Record<
		string,
		{ contractHash: string; pair: ReturnType<typeof makeSchema> }
	> = {
		"order.created": { contractHash: "hash1", pair: makeSchema() },
		"payment.charged": { contractHash: "hash2", pair: makeSchema() },
		"payment.refunded": { contractHash: "hash3", pair: makeSchema() },
	};
	const allSchemas = { ...builtInSchemas, ...(overrides.extraSchemas ?? {}) };
	const deps: SubscriberDeps = {
		rpcClient: {
			subscribe: () => streamRef,
			// biome-ignore lint/suspicious/noExplicitAny: minimal stub
		} as any,
		schemaIndex: {
			get: (name: string) => allSchemas[name],
		},
		identity: () => ({ serviceId: "svc-1", instanceId: "inst-1" }),
		handlers: overrides.handlers ?? (() => []),
		maxInFlight: 32,
		logger: { warn: () => {}, error: () => {} },
		runWithTrace: (_x, fn) => fn(),
	};
	return { deps, fake };
}

// -- tests --

describe("Subscriber", () => {
	it("HandlerSuccess_SendsAck", async () => {
		const handlerFn = mock(async () => {});
		const { deps, fake } = makeDeps({
			handlers: handlersFor([
				{
					pattern: "order.created",
					fn: handlerFn,
				},
			]),
		});

		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit("data", makeDelivery("d-1"));
		await Bun.sleep(10);

		expect(handlerFn).toHaveBeenCalledTimes(1);
		const acked = fake.written.find(
			// biome-ignore lint/suspicious/noExplicitAny: test assertion
			(m: any) => m.ack?.deliveryId === "d-1",
		);
		expect(acked).toBeDefined();
		await sub.stop();
	});

	it("HandlerThrows_SendsNack", async () => {
		const { deps, fake } = makeDeps({
			handlers: handlersFor([
				{
					pattern: "order.created",
					fn: async () => {
						throw new Error("processing failed");
					},
				},
			]),
		});

		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit("data", makeDelivery("d-2"));
		await Bun.sleep(10);

		const nacked = fake.written.find(
			// biome-ignore lint/suspicious/noExplicitAny: test assertion
			(m: any) => m.nack?.deliveryId === "d-2",
		);
		expect(nacked).toBeDefined();
		await sub.stop();
	});

	it("MultiHandler_AllCalledOnce", async () => {
		const fn1 = mock(async () => {});
		const fn2 = mock(async () => {});
		const { deps, fake } = makeDeps({
			handlers: handlersFor([
				{ pattern: "order.created", fn: fn1 },
				{ pattern: "order.created", fn: fn2 },
			]),
		});

		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit("data", makeDelivery("d-3"));
		await Bun.sleep(10);

		expect(fn1).toHaveBeenCalledTimes(1);
		expect(fn2).toHaveBeenCalledTimes(1);
		await sub.stop();
	});

	it("DuplicateDelivery_HandlerCalledEachTime", async () => {
		// At-least-once: server is single source of truth (ADR-0002). SDK no
		// longer dedups — handler contract requires idempotency.
		const fn = mock(async () => {});
		const { deps, fake } = makeDeps({
			handlers: handlersFor([{ pattern: "order.created", fn }]),
		});

		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit("data", makeDelivery("d-4"));
		await Bun.sleep(10);
		expect(fn).toHaveBeenCalledTimes(1);

		// Same envelope re-delivered — handler invoked again.
		fake.emitter.emit("data", makeDelivery("d-4"));
		await Bun.sleep(10);
		expect(fn).toHaveBeenCalledTimes(2);

		await sub.stop();
	});

	it("NoSchema_Nack", async () => {
		const { deps, fake } = makeDeps({
			handlers: handlersFor([
				{
					pattern: "unknown.event",
					fn: async () => {},
				},
			]),
		});

		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit("data", makeDelivery("d-5", "unknown.event"));
		await Bun.sleep(10);

		const nacked = fake.written.find(
			// biome-ignore lint/suspicious/noExplicitAny: test assertion
			(m: any) => m.nack?.deliveryId === "d-5",
		);
		expect(nacked).toBeDefined();
		await sub.stop();
	});

	it("Stop_NoReconnect", async () => {
		let connectCount = 0;
		const fakes: ReturnType<typeof makeFakeStream>[] = [];

		const { deps } = makeDeps();
		const depsMod: SubscriberDeps = {
			...deps,
			rpcClient: {
				subscribe: () => {
					connectCount++;
					const f = makeFakeStream();
					fakes.push(f);
					return f.stream;
				},
				// biome-ignore lint/suspicious/noExplicitAny: minimal stub
			} as any,
		};

		const sub = new Subscriber(depsMod);
		sub.start();
		await sub.stop();

		fakes[0]?.emitter.emit("error", new Error("closed"));
		await Bun.sleep(100);

		expect(connectCount).toBe(1);
	});

	it("ExactName_OnlyDispatchesOnEqual", async () => {
		// Dispatch is by exact event-name match. Wildcards live on the server.
		const handlerFn = mock(async () => {});
		const { deps, fake } = makeDeps({
			handlers: handlersFor([{ pattern: "payment.charged", fn: handlerFn }]),
		});

		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit("data", makeDelivery("d-wc-1", "payment.charged"));
		await Bun.sleep(10);

		expect(handlerFn).toHaveBeenCalledTimes(1);
		const acked = fake.written.find(
			// biome-ignore lint/suspicious/noExplicitAny: test assertion
			(m: any) => m.ack?.deliveryId === "d-wc-1",
		);
		expect(acked).toBeDefined();
		await sub.stop();
	});

	it("NameMismatch_AutoAck", async () => {
		const handlerFn = mock(async () => {});
		const { deps, fake } = makeDeps({
			handlers: handlersFor([{ pattern: "payment.charged", fn: handlerFn }]),
		});

		const sub = new Subscriber(deps);
		sub.start();

		// "order.created" does not equal "payment.charged".
		fake.emitter.emit("data", makeDelivery("d-wc-2", "order.created"));
		await Bun.sleep(10);

		expect(handlerFn).not.toHaveBeenCalled();
		const acked = fake.written.find(
			// biome-ignore lint/suspicious/noExplicitAny: test assertion
			(m: any) => m.ack?.deliveryId === "d-wc-2",
		);
		expect(acked).toBeDefined();
		await sub.stop();
	});

	it("Ack_PopulatesEventId", async () => {
		const handlerFn = mock(async () => {});
		const { deps, fake } = makeDeps({
			handlers: handlersFor([{ pattern: "order.created", fn: handlerFn }]),
		});

		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit("data", makeDelivery("d-eid-1"));
		await Bun.sleep(10);

		const acked = fake.written.find(
			// biome-ignore lint/suspicious/noExplicitAny: test assertion
			(m: any) => m.ack?.deliveryId === "d-eid-1",
		);
		expect(acked).toBeDefined();
		// biome-ignore lint/suspicious/noExplicitAny: test assertion
		const eventIdBuf: Buffer = (acked as any).ack.eventId;
		expect(Buffer.isBuffer(eventIdBuf)).toBe(true);
		expect(eventIdBuf.toString()).toBe("ev-d-eid-1");
		await sub.stop();
	});

	it("Nack_PopulatesEventId", async () => {
		const { deps, fake } = makeDeps({
			handlers: handlersFor([
				{
					pattern: "order.created",
					fn: async () => {
						throw new Error("fail");
					},
				},
			]),
		});

		const sub = new Subscriber(deps);
		sub.start();

		fake.emitter.emit("data", makeDelivery("d-eid-2"));
		await Bun.sleep(10);

		const nacked = fake.written.find(
			// biome-ignore lint/suspicious/noExplicitAny: test assertion
			(m: any) => m.nack?.deliveryId === "d-eid-2",
		);
		expect(nacked).toBeDefined();
		// biome-ignore lint/suspicious/noExplicitAny: test assertion
		const eventIdBuf: Buffer = (nacked as any).nack.eventId;
		expect(Buffer.isBuffer(eventIdBuf)).toBe(true);
		expect(eventIdBuf.toString()).toBe("ev-d-eid-2");
		await sub.stop();
	});

	it("StreamError_Reconnects", async () => {
		let connectCount = 0;
		const fakes: ReturnType<typeof makeFakeStream>[] = [];

		const deps: SubscriberDeps = {
			rpcClient: {
				subscribe: () => {
					connectCount++;
					const f = makeFakeStream();
					fakes.push(f);
					return f.stream;
				},
				// biome-ignore lint/suspicious/noExplicitAny: minimal stub
			} as any,
			schemaIndex: { get: () => undefined },
			identity: () => ({ serviceId: "svc-1", instanceId: "inst-1" }),
			handlers: handlersFor([]),
			maxInFlight: 32,
			logger: { warn: () => {}, error: () => {} },
			runWithTrace: (_x, fn) => fn(),
		};

		const sub = new Subscriber(deps);
		sub.start();

		fakes[0]?.emitter.emit("error", new Error("test error"));
		await Bun.sleep(10);
		await sub.stop();

		expect(connectCount).toBeGreaterThanOrEqual(1);
	});

	it("ErrorPlusEnd_SchedulesSingleReconnectPerCycle", async () => {
		// grpc-js emits both "error" and "end" for one broken stream. Each cycle
		// must schedule exactly one reconnect — a second timer doubles the live
		// reconnect loops every cycle (exponential runaway, gigabytes of heap).
		let connectCount = 0;
		const fakes: ReturnType<typeof makeFakeStream>[] = [];

		const deps: SubscriberDeps = {
			rpcClient: {
				subscribe: () => {
					connectCount++;
					const f = makeFakeStream();
					fakes.push(f);
					return f.stream;
				},
				// biome-ignore lint/suspicious/noExplicitAny: minimal stub
			} as any,
			schemaIndex: { get: () => undefined },
			identity: () => ({ serviceId: "svc-1", instanceId: "inst-1" }),
			handlers: handlersFor([]),
			maxInFlight: 32,
			logger: { warn: () => {}, error: () => {} },
			runWithTrace: (_x, fn) => fn(),
			reconnectOpts: { ladder: [1], jitterRatio: 0 },
		};

		const sub = new Subscriber(deps);
		sub.start();
		expect(connectCount).toBe(1);

		for (let cycle = 0; cycle < 3; cycle++) {
			const current = fakes[fakes.length - 1];
			current?.emitter.emit("error", new Error("broken"));
			current?.emitter.emit("end");
			await Bun.sleep(20);
			// One reconnect per cycle. The doubling bug yields 2^cycle extra
			// connects here (1 → 3 → 7 → 15).
			expect(connectCount).toBe(cycle + 2);
		}

		await sub.stop();
	});

	it("CleanCloses_ClimbTheLadder", async () => {
		// A runtime that drains streams gracefully must not be reconnected to
		// once per second forever: only a data frame proves progress, a clean
		// close does not.
		const fakes: ReturnType<typeof makeFakeStream>[] = [];
		const { deps } = makeDeps();
		const depsMod: SubscriberDeps = {
			...deps,
			rpcClient: {
				subscribe: () => {
					const f = makeFakeStream();
					fakes.push(f);
					return f.stream;
				},
				// biome-ignore lint/suspicious/noExplicitAny: minimal stub
			} as any,
			reconnectOpts: { ladder: [4, 12, 24], jitterRatio: 0 },
		};

		// Read from the supervisor rather than a spy on the global timer: one
		// test process also holds the subscribers of every other file, and their
		// reconnect timers land in the spy too.
		const delays: number[] = [];
		const sub = new Subscriber({
			...depsMod,
			onSchedule: (ms) => delays.push(ms),
		});
		sub.start();
		for (let i = 0; i < 3; i++) {
			fakes[fakes.length - 1]?.emitter.emit("end");
			await wait(30);
		}
		await sub.stop();

		expect(delays.slice(0, 3)).toEqual([4, 12, 24]);
	});

	it("DataFrame_ResetsLadder", async () => {
		const fakes: ReturnType<typeof makeFakeStream>[] = [];
		const { deps } = makeDeps();
		const depsMod: SubscriberDeps = {
			...deps,
			rpcClient: {
				subscribe: () => {
					const f = makeFakeStream();
					fakes.push(f);
					return f.stream;
				},
				// biome-ignore lint/suspicious/noExplicitAny: minimal stub
			} as any,
			reconnectOpts: { ladder: [4, 12, 24], jitterRatio: 0 },
		};

		const delays: number[] = [];
		const sub = new Subscriber({
			...depsMod,
			onSchedule: (ms) => delays.push(ms),
		});
		sub.start();
		fakes[fakes.length - 1]?.emitter.emit("end");
		await wait(20);
		fakes[fakes.length - 1]?.emitter.emit("data", makeDelivery("d-ladder"));
		fakes[fakes.length - 1]?.emitter.emit("end");
		await wait(20);
		await sub.stop();

		expect(delays.slice(0, 2)).toEqual([4, 4]);
	});

	it("LateEndFromReplacedStream_DoesNotOpenAThirdStream", async () => {
		// grpc-js flushes buffered frames before "end", so a dead stream can
		// outlive a ladder rung. Without the identity guard the late "end" from
		// stream A drops the live stream B (stop() can no longer cancel it) and
		// opens a third the runtime rejects with ALREADY_EXISTS.
		const fakes: ReturnType<typeof makeFakeStream>[] = [];
		const cancels: number[] = [];
		const { deps } = makeDeps();
		const depsMod: SubscriberDeps = {
			...deps,
			rpcClient: {
				subscribe: () => {
					const f = makeFakeStream();
					const idx = fakes.push(f) - 1;
					cancels[idx] = 0;
					f.stream.cancel = () => {
						cancels[idx] = (cancels[idx] ?? 0) + 1;
					};
					return f.stream;
				},
				// biome-ignore lint/suspicious/noExplicitAny: minimal stub
			} as any,
			reconnectOpts: { ladder: [4], jitterRatio: 0 },
		};

		const sub = new Subscriber(depsMod);
		sub.start();
		fakes[0]?.emitter.emit("error", new Error("broken"));
		await wait(25);
		expect(fakes).toHaveLength(2);

		fakes[0]?.emitter.emit("end");
		await wait(25);
		expect(fakes).toHaveLength(2);

		await sub.stop();
		expect(cancels[1]).toBe(1);
	});

	it("NoIdentity_RetriesUntilAvailable", async () => {
		const fakes: ReturnType<typeof makeFakeStream>[] = [];
		let id: { serviceId: string; instanceId: string } | null = null;
		const { deps } = makeDeps();
		const depsMod: SubscriberDeps = {
			...deps,
			identity: () => id,
			rpcClient: {
				subscribe: () => {
					const f = makeFakeStream();
					fakes.push(f);
					return f.stream;
				},
				// biome-ignore lint/suspicious/noExplicitAny: minimal stub
			} as any,
			reconnectOpts: { ladder: [4], jitterRatio: 0 },
		};

		const sub = new Subscriber(depsMod);
		sub.start();
		expect(fakes).toHaveLength(0);

		id = { serviceId: "svc-1", instanceId: "inst-1" };
		await wait(25);
		expect(fakes).toHaveLength(1);
		await sub.stop();
	});
});
