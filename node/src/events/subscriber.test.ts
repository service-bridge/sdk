import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import { silentLogger } from "../logger";
import type { EventsClient } from "../pb/servicebridge/v1/events";
import type {
	EventHandlerContext,
	EventHandlerFn,
	SubscriptionEntry,
} from "../registry/registry";
import type { SchemaPair } from "../serde/serializer";
import { Subscriber, type SubscriberDeps } from "./subscriber";

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

function fakeStream() {
	const emitter = new EventEmitter();
	const written: Record<string, unknown>[] = [];
	return {
		emitter,
		written,
		stream: {
			write: (msg: Record<string, unknown>) => written.push(msg),
			cancel: () => {},
			end: () => {},
			on: (event: string, cb: (...args: unknown[]) => void) =>
				emitter.on(event, cb),
		},
	};
}

const decodingPair = {
	input: { decode: (_b: Uint8Array) => ({ amount: 42 }) },
} as unknown as SchemaPair;

function entry(
	pattern: string,
	fn: EventHandlerFn,
	schemaPair?: SchemaPair,
): SubscriptionEntry {
	return { pattern, filter: "", fn, schemaPair };
}

function delivery(
	id: string,
	matchedPatterns: string[],
	over: { partitionKey?: string } = {},
) {
	return {
		delivery: {
			deliveryId: id,
			attempt: 1,
			leaseToken: `lt-${id}`,
			matchedPatterns,
			envelope: {
				id: `ev-${id}`,
				name: "order.created",
				payload: new Uint8Array([1, 2, 3]),
				payloadJson: new Uint8Array(),
				contractHash: "",
				partitionKey: over.partitionKey ?? "",
				idempotencyKey: "",
				headers: { h: "v" },
				occurredAtUnixMs: 1700000000000,
				xSbTrace: "",
			},
		},
	};
}

function makeSubscriber(
	entries: SubscriptionEntry[],
	over: Partial<SubscriberDeps> = {},
) {
	const streams: ReturnType<typeof fakeStream>[] = [];
	const byPattern = new Map(entries.map((e) => [e.pattern, e]));
	const deps: SubscriberDeps = {
		client: () =>
			({
				subscribe: () => {
					const f = fakeStream();
					streams.push(f);
					return f.stream;
				},
			}) as unknown as EventsClient,
		identity: () => ({ serviceId: "svc-1", instanceId: "inst-1" }),
		subscription: (p) => byPattern.get(p),
		maxInFlight: 32,
		logger: silentLogger,
		runWithTrace: (_x, fn) => fn(),
		reconnectOpts: { ladder: [1], jitterRatio: 0 },
		...over,
	};
	const sub = new Subscriber(deps);
	sub.start();
	const current = () =>
		streams[streams.length - 1] as ReturnType<typeof fakeStream>;
	return { sub, streams, current };
}

const acks = (s: ReturnType<typeof fakeStream>) =>
	s.written
		.filter((m) => "ack" in m)
		.map((m) => m.ack as { deliveryId: string });
const nacks = (s: ReturnType<typeof fakeStream>) =>
	s.written
		.filter((m) => "nack" in m)
		.map((m) => m.nack as { deliveryId: string; errorMessage: string });

describe("Subscriber routing by matched_patterns", () => {
	it("opens the stream with SubscribeInit and acks a handled delivery", async () => {
		const seen: unknown[] = [];
		const { sub, current } = makeSubscriber([
			entry(
				"order.created",
				(p) => {
					seen.push(p);
				},
				decodingPair,
			),
		]);
		expect(current().written[0]).toMatchObject({
			init: {
				subscriberServiceId: "svc-1",
				subscriberInstanceId: "inst-1",
				maxInFlight: 32,
			},
		});
		current().emitter.emit("data", delivery("d1", ["order.created"]));
		await wait(5);
		expect(seen).toEqual([{ amount: 42 }]);
		expect(acks(current())).toHaveLength(1);
		const ack = acks(current())[0] as {
			deliveryId: string;
			leaseToken?: string;
		};
		expect(ack.deliveryId).toBe("d1");
		sub.stop();
	});

	it("without a schema the handler gets the raw payload bytes", async () => {
		let got: unknown;
		const { sub, current } = makeSubscriber([
			entry("order.*", (p) => {
				got = p;
			}),
		]);
		current().emitter.emit("data", delivery("d1", ["order.*"]));
		await wait(5);
		expect(got).toEqual(new Uint8Array([1, 2, 3]));
		sub.stop();
	});

	it("runs the handler of every matched pattern once, ack only after all", async () => {
		const calls: string[] = [];
		const { sub, current } = makeSubscriber([
			entry("order.created", () => {
				calls.push("exact");
			}),
			entry("order.*", () => {
				calls.push("wildcard");
			}),
		]);
		current().emitter.emit(
			"data",
			delivery("d1", ["order.created", "order.*"]),
		);
		await wait(5);
		expect(calls).toEqual(["exact", "wildcard"]);
		expect(acks(current())).toHaveLength(1);
		sub.stop();
	});

	it("never matches wildcards locally: a pattern absent from matched_patterns is not run", async () => {
		const calls: string[] = [];
		const { sub, current } = makeSubscriber([
			entry("order.*", () => {
				calls.push("wildcard");
			}),
			entry("order.created", () => {
				calls.push("exact");
			}),
		]);
		current().emitter.emit("data", delivery("d1", ["order.created"]));
		await wait(5);
		expect(calls).toEqual(["exact"]);
		sub.stop();
	});

	it("nacks when none of the matched patterns has a handler here (rolling deploy)", async () => {
		const { sub, current } = makeSubscriber([entry("order.created", () => {})]);
		current().emitter.emit("data", delivery("d1", ["invoice.*"]));
		await wait(5);
		expect(acks(current())).toHaveLength(0);
		expect(nacks(current())[0]?.errorMessage).toContain(
			"no handler for matched patterns",
		);
		sub.stop();
	});

	it("nacks with the handler's message when it throws", async () => {
		const { sub, current } = makeSubscriber([
			entry("order.created", () => {
				throw new Error("db down");
			}),
		]);
		current().emitter.emit("data", delivery("d1", ["order.created"]));
		await wait(5);
		expect(nacks(current())[0]?.errorMessage).toBe("db down");
		sub.stop();
	});

	it("hands the handler the delivery context", async () => {
		let ctx: EventHandlerContext | undefined;
		const { sub, current } = makeSubscriber([
			entry("order.created", (_p, c) => {
				ctx = c;
			}),
		]);
		current().emitter.emit(
			"data",
			delivery("d1", ["order.created"], { partitionKey: "o-1" }),
		);
		await wait(5);
		expect(ctx).toMatchObject({
			eventId: "ev-d1",
			eventName: "order.created",
			attempt: 1,
			deliveryId: "d1",
			leaseToken: "lt-d1",
			partitionKey: "o-1",
			headers: { h: "v" },
			occurredAtMs: 1700000000000,
		});
		expect(ctx?.signal.aborted).toBe(false);
		sub.stop();
	});

	it("serialises deliveries sharing a partition key", async () => {
		const order: string[] = [];
		const { sub, current } = makeSubscriber([
			entry("order.created", async (_p, c) => {
				order.push(`start ${c.deliveryId}`);
				await wait(c.deliveryId === "d1" ? 20 : 1);
				order.push(`end ${c.deliveryId}`);
			}),
		]);
		current().emitter.emit(
			"data",
			delivery("d1", ["order.created"], { partitionKey: "k" }),
		);
		current().emitter.emit(
			"data",
			delivery("d2", ["order.created"], { partitionKey: "k" }),
		);
		await wait(40);
		expect(order).toEqual(["start d1", "end d1", "start d2", "end d2"]);
		sub.stop();
	});
});

describe("Subscriber drain and lifecycle", () => {
	it("drain leaves new deliveries unanswered and waits for the running ones", async () => {
		let release!: () => void;
		let finished = false;
		const { sub, current } = makeSubscriber([
			entry("order.created", async () => {
				await new Promise<void>((r) => {
					release = r;
				});
				finished = true;
			}),
		]);
		current().emitter.emit("data", delivery("d1", ["order.created"]));
		await wait(2);
		const draining = sub.drain(1_000);
		current().emitter.emit("data", delivery("d2", ["order.created"]));
		await wait(2);
		expect(nacks(current())).toHaveLength(0);
		release();
		await draining;
		expect(finished).toBe(true);
		expect(acks(current()).map((a) => a.deliveryId)).toEqual(["d1"]);
		sub.stop();
	});

	it("drain gives up at its deadline", async () => {
		const { sub, current } = makeSubscriber([
			entry("order.created", () => new Promise(() => {})),
		]);
		current().emitter.emit("data", delivery("d1", ["order.created"]));
		await wait(2);
		const started = Date.now();
		await sub.drain(20);
		expect(Date.now() - started).toBeLessThan(500);
		sub.stop();
	});

	it("reopens the stream once per broken cycle", async () => {
		const { sub, streams } = makeSubscriber([entry("order.created", () => {})]);
		for (let cycle = 0; cycle < 3; cycle++) {
			const s = streams[streams.length - 1];
			s?.emitter.emit("error", new Error("broken"));
			s?.emitter.emit("end");
			await wait(15);
			expect(streams).toHaveLength(cycle + 2);
		}
		sub.stop();
	});

	it("waits for an identity before opening the stream", async () => {
		let identity: { serviceId: string; instanceId: string } | null = null;
		const { sub, streams } = makeSubscriber(
			[entry("order.created", () => {})],
			{
				identity: () => identity,
			},
		);
		await wait(5);
		expect(streams).toHaveLength(0);
		identity = { serviceId: "s", instanceId: "i" };
		await wait(15);
		expect(streams.length).toBeGreaterThan(0);
		sub.stop();
	});

	it("stop opens no further streams", async () => {
		const { sub, streams } = makeSubscriber([entry("order.created", () => {})]);
		sub.stop();
		streams[0]?.emitter.emit("error", new Error("x"));
		await wait(15);
		expect(streams).toHaveLength(1);
	});
});
