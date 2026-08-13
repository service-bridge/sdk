// transport.test.ts — TelemetryTransport unit tests with a mock gRPC client.
// Verifies: stream open, batch building, at-least-once re-send after a stream
// drop, advisory (non-pausing) backpressure, reconnect backoff growth + reset on
// first ack, graceful drain, and drop observability.

import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import { EventEmitter } from "node:events";
import type {
	Log,
	MetricPoint,
	OpReport,
	PayloadAttachment,
	TelemetryAck,
	TelemetryBatch,
} from "../pb/servicebridge/v1/telemetry";
import { RECONNECT_LADDER_MS } from "../utils/reconnect-ladder";
import { Channel, OpHandle, RpcCall, Status } from "./ops";
import { TelemetryRing } from "./ring";
import {
	type ClientTelemetryStream,
	type TelemetryClientLike,
	TelemetryTransport,
} from "./transport";

class MockStream extends EventEmitter {
	written: TelemetryBatch[] = [];
	ended = false;
	write(msg: TelemetryBatch): boolean {
		this.written.push(msg);
		return true;
	}
	end(): void {
		this.ended = true;
		this.emit("end");
	}
	ack(partial: Partial<TelemetryAck> = {}): void {
		this.emit("data", {
			receivedBytesHighWater: 0,
			backpressureLevel: 0,
			dropCountServerSide: 0,
			drainReason: "",
			...partial,
		} as TelemetryAck);
	}
}

class MockClient implements TelemetryClientLike {
	streams: MockStream[] = [];
	openStream(): ClientTelemetryStream {
		const s = new MockStream();
		this.streams.push(s);
		return s as unknown as ClientTelemetryStream;
	}
	current(): MockStream {
		return this.streams[this.streams.length - 1]!;
	}
}

function makeOpParams() {
	return {
		traceId: "01900000-0000-7000-8000-0000000000aa",
		opId: "01900000-0000-7000-8000-0000000000bb",
		channel: Channel.RPC,
		kind: RpcCall,
		subject: "rpc.call:svc/m",
		businessKey: "k",
	};
}

function op(opId: string): OpReport {
	return {
		traceId: "01900000-0000-7000-8000-000000000001",
		opId,
		parentOpId: "",
		channel: Channel.RPC,
		kind: RpcCall,
		subject: "rpc.call:s/m",
		peerServiceId: "",
		businessKey: "k",
		attempt: 0,
		startedAtMs: 1,
		finishedAtMs: undefined,
		status: Status.PENDING,
		statusMessage: "",
		metaJson: Buffer.alloc(0),
		attrsJson: Buffer.alloc(0),
	};
}

function totalOps(batches: TelemetryBatch[]): number {
	return batches
		.filter((b) => b.ops)
		.reduce((acc, b) => acc + b.ops!.items.length, 0);
}

describe("TelemetryTransport", () => {
	let ring: TelemetryRing;
	let client: MockClient;
	let transport: TelemetryTransport;

	beforeEach(() => {
		ring = new TelemetryRing();
		client = new MockClient();
	});

	afterEach(async () => {
		await transport?.stop();
	});

	test("opens a stream on start and flushes ops batches as typed items", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
		});
		await transport.start();

		OpHandle.start(ring, makeOpParams());
		await transport.flushNow();

		const stream = client.current();
		expect(stream.written.length).toBeGreaterThan(0);
		const batch = stream.written[0]!;
		expect(batch.ops).toBeDefined();
		expect(batch.ops!.items).toHaveLength(1);
		expect(batch.ops!.items[0]!.traceId).toBe(makeOpParams().traceId);
	});

	test("groups multiple ring items into a single OpBatch", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
		});
		await transport.start();
		for (let i = 0; i < 5; i++) {
			ring.push("ops", op(`01900000-0000-7000-8000-00000000000${i}`));
		}
		await transport.flushNow();
		expect(totalOps(client.current().written)).toBe(5);
	});

	test("does not re-write in-flight items on repeated flush before an ack", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 1000,
			maxBatchItems: 100,
		});
		await transport.start();

		OpHandle.start(ring, makeOpParams());
		// Two flushes on the SAME stream with no ack in between. The op must be
		// written exactly once — it is in-flight after the first write.
		await transport.flushNow();
		await transport.flushNow();

		expect(totalOps(client.current().written)).toBe(1);
	});

	test("at-least-once: un-acked ops are re-sent on the next stream after a drop", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
			reconnectOpts: { ladder: [0], jitterRatio: 0 },
		});
		await transport.start();

		OpHandle.start(ring, makeOpParams());
		await transport.flushNow();
		const first = client.current();
		expect(totalOps(first.written)).toBe(1);

		// Stream dies BEFORE any ack — the op was never confirmed.
		first.emit("error", new Error("transport gone"));
		await new Promise((r) => setTimeout(r, 5));

		// A new stream opened; flushing re-sends the same un-acked op.
		expect(client.streams.length).toBe(2);
		await transport.flushNow();
		const second = client.current();
		expect(totalOps(second.written)).toBe(1);
		expect(second.written[0]!.ops!.items[0]!.opId).toBe(makeOpParams().opId);
	});

	test("confirmed ops are NOT re-sent after a later stream drop", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
			reconnectOpts: { ladder: [0], jitterRatio: 0 },
		});
		await transport.start();

		OpHandle.start(ring, makeOpParams());
		await transport.flushNow();
		const first = client.current();
		// The runtime acks on a fixed ticker and the ack names no batch, so a single
		// ack cannot confirm a write that may have raced it in transit. The write is
		// confirmed by the following ack, which is emitted an ack interval later.
		first.ack();
		first.ack();

		first.emit("error", new Error("transport gone"));
		await new Promise((r) => setTimeout(r, 5));

		await transport.flushNow();
		// Nothing left to send — the op was confirmed and released.
		expect(totalOps(client.current().written)).toBe(0);
	});

	test("an ack does NOT release a write that raced it — the write survives the stream death", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 1000,
			maxBatchItems: 100,
			reconnectOpts: { ladder: [0], jitterRatio: 0 },
		});
		await transport.start();
		const first = client.current();

		// Op A is written and confirmed by two acks.
		ring.push("ops", op("01900000-0000-7000-8000-0000000000a1"));
		await transport.flushNow();
		first.ack();

		// Op B is written by a flush that slips out AFTER the runtime built its ack
		// but BEFORE that ack lands here — the runtime has not seen B yet.
		ring.push("ops", op("01900000-0000-7000-8000-0000000000b2"));
		await transport.flushNow();
		first.ack();

		// Stream dies before any further ack. A was confirmed, B was not.
		first.emit("error", new Error("transport gone"));
		await new Promise((r) => setTimeout(r, 5));
		await transport.flushNow();

		const resent = client
			.current()
			.written.filter((b) => b.ops)
			.flatMap((b) => b.ops!.items.map((i) => i.opId));
		expect(resent).toEqual(["01900000-0000-7000-8000-0000000000b2"]);
	});

	test("backpressure level 2 does NOT pause the flusher (advisory only)", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
		});
		await transport.start();
		const stream = client.current();

		stream.ack({ backpressureLevel: 2 });
		OpHandle.start(ring, makeOpParams());
		await transport.flushNow();

		// Flusher keeps sending — pausing would oldest-drop START frames.
		expect(totalOps(stream.written)).toBe(1);
	});

	test("reconnect ladder has 5 rungs sourced from RECONNECT_LADDER_MS, not a shorter local copy", async () => {
		// The pre-fix transport.ts kept its own 4-rung DEFAULT_RECONNECT_DELAYS
		// array, saturating at 30_000ms. A leftover local copy would silently cap
		// growth one rung short of the shared ladder's 60_000ms top rung — this
		// test only passes once transport.ts defers entirely to
		// utils/reconnect-ladder for both rungs and jitter.
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 1000,
			maxBatchItems: 100,
			// random()=0.5 → jitter offset 0, pinning each delay to the exact rung.
			reconnectOpts: { random: () => 0.5 },
		});
		await transport.start();

		const scheduled: number[] = [];
		const setTimeoutSpy = spyOn(globalThis, "setTimeout").mockImplementation(((
			fn: () => void,
			ms?: number,
		) => {
			scheduled.push(ms ?? 0);
			fn();
			return 0 as unknown as ReturnType<typeof setTimeout>;
		}) as typeof setTimeout);
		try {
			for (let i = 0; i < RECONNECT_LADDER_MS.length; i++) {
				client.current().emit("error", new Error(`drop${i}`));
			}
			expect(scheduled).toEqual([...RECONNECT_LADDER_MS]);
		} finally {
			setTimeoutSpy.mockRestore();
		}
	});

	test("reconnect backoff grows across consecutive drops without an ack", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 1000,
			maxBatchItems: 100,
			reconnectOpts: { ladder: [10, 40, 120], jitterRatio: 0 },
		});
		await transport.start();

		const start = Date.now();
		client.current().emit("error", new Error("drop1"));
		await waitFor(() => client.streams.length === 2);
		const afterFirst = Date.now() - start;

		client.current().emit("error", new Error("drop2"));
		await waitFor(() => client.streams.length === 3);
		const afterSecond = Date.now() - start - afterFirst;

		// Second reconnect waited the longer ladder rung.
		expect(afterSecond).toBeGreaterThanOrEqual(afterFirst);
	});

	test("first ack resets the reconnect backoff ladder", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 1000,
			maxBatchItems: 100,
			reconnectOpts: { ladder: [10, 200], jitterRatio: 0 },
		});
		await transport.start();

		// Burn one rung: drop with no ack advances the ladder.
		client.current().emit("error", new Error("drop1"));
		await waitFor(() => client.streams.length === 2);

		// Healthy ack on the new stream resets the ladder to rung 0.
		client.current().ack();

		const before = Date.now();
		client.current().emit("error", new Error("drop2"));
		await waitFor(() => client.streams.length === 3);
		const waited = Date.now() - before;

		// Reset means the short rung (10ms) was used, not the 200ms rung.
		expect(waited).toBeLessThan(150);
	});

	test("graceful drain: drainReason ack flushes and ends the stream", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
		});
		await transport.start();
		OpHandle.start(ring, makeOpParams());

		const stream = client.current();
		stream.ack({ drainReason: "shutdown" });
		await new Promise((r) => setTimeout(r, 5));

		expect(stream.ended).toBe(true);
		expect(totalOps(stream.written)).toBeGreaterThan(0);
	});

	test("reopens stream after disconnect", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
			reconnectOpts: { ladder: [0, 0, 0], jitterRatio: 0 },
		});
		await transport.start();
		expect(client.streams.length).toBe(1);

		client.current().emit("error", new Error("transport gone"));
		await new Promise((r) => setTimeout(r, 5));
		expect(client.streams.length).toBe(2);
	});

	test("flushes logs into LogBatch", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
		});
		await transport.start();
		const log: Log = {
			atUnixMs: Date.now(),
			level: 2,
			message: "hi",
			fieldsJson: Buffer.alloc(0),
			traceId: "",
			opId: "",
			instanceId: "01900000-0000-7000-8000-000000000011",
			source: "sdk",
		};
		ring.push("logs", log);
		await transport.flushNow();
		const batches = client.current().written;
		expect(batches.some((b) => b.logs?.items.length)).toBe(true);
	});

	test("flushes payloads into PayloadBatch", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
		});
		await transport.start();
		const pay: PayloadAttachment = {
			traceId: "01900000-0000-7000-8000-0000000000aa",
			opId: "01900000-0000-7000-8000-0000000000bb",
			direction: 1,
			bytes: Buffer.from([1, 2, 3]),
			originalSize: 3,
			contractHash: "h",
		};
		ring.push("payloads", pay);
		await transport.flushNow();
		const batches = client.current().written;
		const pb = batches.find((b) => b.payloads?.items.length);
		expect(pb?.payloads?.items[0]?.direction).toBe(1);
		expect(pb?.payloads?.items[0]?.contractHash).toBe("h");
	});

	test("drop observability: onDrop fires when server drop count rises", async () => {
		const seen: number[] = [];
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
			onDrop: (info) => seen.push(info.serverDrops),
		});
		await transport.start();
		const stream = client.current();

		stream.ack({ dropCountServerSide: 0 }); // no rise
		stream.ack({ dropCountServerSide: 7 }); // rise → fires
		stream.ack({ dropCountServerSide: 7 }); // flat → no fire

		expect(seen).toEqual([7]);
	});

	test("drop observability: onDrop fires when local ring drops rise", async () => {
		const seen: Array<{ ring: number }> = [];
		// Tiny metrics budget to force ring drops.
		ring = new TelemetryRing({ metrics: 100 });
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
			onDrop: (info) => seen.push({ ring: info.ringDrops }),
		});
		await transport.start();
		for (let i = 0; i < 5; i++) {
			const m: MetricPoint = {
				atUnixMs: 1,
				name: `metric_${i}`,
				kind: 1,
				labels: {},
				instanceId: "i",
				value: 1,
				unit: "1",
				bucketsJson: Buffer.alloc(0),
			};
			ring.push("metrics", m);
		}
		client.current().ack();
		expect(seen.length).toBe(1);
		expect(seen[0]!.ring).toBeGreaterThan(0);
	});

	test("an ack immediately writes the next batch instead of waiting for the timer", async () => {
		// flushIntervalMs is far longer than the test — anything written after the
		// first flush can only come from the ack path.
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 60_000,
			maxBatchItems: 2,
		});
		await transport.start();
		const stream = client.current();

		// Ops produced after the flush cycle have no timer tick left to carry them.
		for (let i = 0; i < 6; i++) {
			ring.push("ops", op(`01900000-0000-7000-8000-00000000000${i}`));
		}
		expect(totalOps(stream.written)).toBe(0);

		stream.ack();
		expect(totalOps(stream.written)).toBe(6);
	});

	test("2000 frames drain in one flush cycle — throughput is not capped at one batch per tick", async () => {
		// A single 256-item batch per 250ms tick caps the SDK near 1024 frames/s,
		// i.e. ~512 ops/s once every op costs a START and an END frame. Worse, with
		// in-flight items pinned at the ring head, a peek capped at the batch size
		// returns nothing but in-flight and the transport stalls entirely until the
		// next ack (2s on the runtime ticker). One flush cycle must move the whole
		// backlog.
		const frames = 2000;
		ring = new TelemetryRing({ ops: 4 * 1024 * 1024 });
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 60_000,
			maxBatchItems: 256,
		});
		await transport.start();
		const stream = client.current();

		for (let i = 0; i < frames; i++) {
			ring.push(
				"ops",
				op(`01900000-0000-7000-8000-${String(i).padStart(12, "0")}`),
			);
		}
		expect(ring.dropCount("ops")).toBe(0);

		await transport.flushNow();

		expect(totalOps(stream.written)).toBe(frames);
		// 2000 frames in one 250ms tick is ≥ 8000 frames/s ≥ 4000 ops/s.
		expect(totalOps(stream.written) / (250 / 1000)).toBeGreaterThan(512 * 2);
	});

	test("sustained produce/ack cycles deliver every frame without loss", async () => {
		const perCycle = 600;
		const cycles = 5;
		ring = new TelemetryRing({ ops: 4 * 1024 * 1024 });
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 60_000,
			maxBatchItems: 256,
		});
		await transport.start();
		const stream = client.current();

		const produced: string[] = [];
		for (let c = 0; c < cycles; c++) {
			for (let i = 0; i < perCycle; i++) {
				const id = `01900000-0000-7000-8000-${String(c * perCycle + i).padStart(12, "0")}`;
				produced.push(id);
				ring.push("ops", op(id));
			}
			await transport.flushNow();
			stream.ack();
		}

		expect(ring.dropCount("ops")).toBe(0);
		const sent = stream.written
			.filter((b) => b.ops)
			.flatMap((b) => b.ops!.items.map((i) => i.opId));
		expect(new Set(sent).size).toBe(produced.length);
		expect([...new Set(sent)].sort()).toEqual([...produced].sort());
	});

	test("materialises accumulated metric series into the batch", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 60_000,
			maxBatchItems: 100,
		});
		await transport.start();

		// The aggregator holds series state, not points — nothing reaches the ring
		// until a flush cycle closes the aggregation window.
		const hits = ring.metrics.counter("inst-1", "http_hits");
		hits.inc();
		hits.inc(2);
		ring.metrics.gauge("inst-1", "queue_depth").set(7);

		await transport.flushNow();
		const points = client
			.current()
			.written.filter((b) => b.metrics)
			.flatMap((b) => b.metrics!.items);
		expect(points.map((m) => m.name).sort()).toEqual([
			"http_hits",
			"queue_depth",
		]);
		// Three producer calls collapsed into one point per series.
		expect(points.find((m) => m.name === "http_hits")?.value).toBe(3);
		expect(points.find((m) => m.name === "queue_depth")?.value).toBe(7);
	});

	test("an unchanged series emits nothing on the next flush cycle", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 60_000,
			maxBatchItems: 100,
		});
		await transport.start();

		ring.metrics.counter("inst-1", "http_hits").inc();
		await transport.flushNow();
		const afterFirst = client
			.current()
			.written.filter((b) => b.metrics)
			.flatMap((b) => b.metrics!.items).length;
		expect(afterFirst).toBe(1);

		await transport.flushNow();
		const afterSecond = client
			.current()
			.written.filter((b) => b.metrics)
			.flatMap((b) => b.metrics!.items).length;
		expect(afterSecond).toBe(1);
	});

	test("stop() closes the stream and cancels timers", async () => {
		transport = new TelemetryTransport({
			client,
			ring,
			flushIntervalMs: 25,
			maxBatchItems: 100,
		});
		await transport.start();
		const stream = client.current();
		await transport.stop();
		expect(stream.ended).toBe(true);
	});
});

async function waitFor(cond: () => boolean, timeoutMs = 1000): Promise<void> {
	const start = Date.now();
	while (!cond()) {
		if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout");
		await new Promise((r) => setTimeout(r, 2));
	}
}
