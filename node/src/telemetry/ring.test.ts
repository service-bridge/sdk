import { describe, expect, test } from "bun:test";
import type {
	Log,
	MetricPoint,
	OpReport,
} from "../pb/servicebridge/v1/telemetry";
import { Channel, Status } from "../pb/servicebridge/v1/telemetry";
import { type RingItem, TelemetryRing } from "./ring";

function op(opId: string, metaBytes = 0): OpReport {
	return {
		traceId: "01900000-0000-7000-8000-000000000001",
		opId,
		parentOpId: "",
		channel: Channel.RPC,
		kind: 1,
		subject: "rpc.call:s/m",
		peerServiceId: "",
		businessKey: "k",
		attempt: 0,
		startedAtMs: 1,
		finishedAtMs: undefined,
		status: Status.PENDING,
		statusMessage: "",
		metaJson: Buffer.alloc(metaBytes),
		attrsJson: Buffer.alloc(0),
	};
}

function logMsg(i: number): Log {
	return {
		atUnixMs: i,
		level: 2,
		message: "m",
		fieldsJson: Buffer.alloc(0),
		traceId: "",
		opId: "",
		instanceId: "01900000-0000-7000-8000-000000000011",
		source: "sdk",
	};
}

function metric(
	name: string,
	labels: Record<string, string> = {},
): MetricPoint {
	return {
		atUnixMs: 1,
		name,
		kind: 1,
		labels,
		instanceId: "i",
		value: 1,
		unit: "1",
		bucketsJson: Buffer.alloc(0),
	};
}

describe("TelemetryRing", () => {
	test("push and peek ops returns typed message", () => {
		const ring = new TelemetryRing();
		ring.push("ops", op("01900000-0000-7000-8000-0000000000aa"));

		const items = ring.peek(100);
		expect(items).toHaveLength(1);
		expect(items[0]!.kind).toBe("ops");
		expect((items[0]!.message as OpReport).opId).toBe(
			"01900000-0000-7000-8000-0000000000aa",
		);
	});

	test("peek does not remove items — at-least-once until commit", () => {
		const ring = new TelemetryRing();
		ring.push("ops", op("01900000-0000-7000-8000-0000000000aa"));
		const first = ring.peek(100);
		const second = ring.peek(100);
		expect(first).toHaveLength(1);
		expect(second).toHaveLength(1);
		expect(first[0]!.id).toBe(second[0]!.id);
	});

	test("commit releases acked items", () => {
		const ring = new TelemetryRing();
		ring.push("ops", op("01900000-0000-7000-8000-0000000000aa"));
		const items = ring.peek(100);
		ring.commit(items);
		expect(ring.peek(100)).toHaveLength(0);
		expect(ring.size("ops")).toBe(0);
	});

	test("commit of a subset leaves uncommitted items resendable", () => {
		const ring = new TelemetryRing();
		ring.push("ops", op("01900000-0000-7000-8000-0000000000a1"));
		ring.push("ops", op("01900000-0000-7000-8000-0000000000a2"));
		const items = ring.peek(100);
		ring.commit([items[0]!]); // ack only the first
		const remaining = ring.peek(100);
		expect(remaining).toHaveLength(1);
		expect((remaining[0]!.message as OpReport).opId).toBe(
			"01900000-0000-7000-8000-0000000000a2",
		);
	});

	test("un-acked peeked batch stays resendable after a simulated stream drop", () => {
		const ring = new TelemetryRing();
		ring.push("ops", op("01900000-0000-7000-8000-0000000000a1"));
		const inflight = ring.peek(100);
		expect(inflight).toHaveLength(1);
		// Stream dropped before ack — we do NOT commit. A fresh op arrives.
		ring.push("ops", op("01900000-0000-7000-8000-0000000000a2"));

		// Next stream re-peeks: a1 (never acked) is still FIRST, a2 after.
		const items = ring.peek(100);
		expect(items).toHaveLength(2);
		expect((items[0]!.message as OpReport).opId).toBe(
			"01900000-0000-7000-8000-0000000000a1",
		);
		expect((items[1]!.message as OpReport).opId).toBe(
			"01900000-0000-7000-8000-0000000000a2",
		);
	});

	test("oldest-drop on overflow (explicit small budget)", () => {
		// meta padding sizes each op at ~1000 bytes (base 64 + ids/subject + 850
		// meta); budget = 4KiB → 4 fit, the 5th evicts the oldest.
		const ring = new TelemetryRing({ ops: 4 * 1024 });
		for (let i = 0; i < 5; i++) {
			ring.push("ops", op(`01900000-0000-7000-8000-00000000000${i}`, 850));
		}
		expect(ring.size("ops")).toBe(4);
		expect(ring.dropCount("ops")).toBe(1);
	});

	test("default ops budget holds a dense step-span burst without dropping", () => {
		const ring = new TelemetryRing();
		for (let i = 0; i < 128; i++) {
			ring.push("ops", op(`01900000-0000-7000-8000-0000000${i + 100}`, 200));
		}
		expect(ring.dropCount("ops")).toBe(0);
		expect(ring.size("ops")).toBe(128);
	});

	test("peek respects maxPerKind", () => {
		const ring = new TelemetryRing();
		for (let i = 0; i < 10; i++) ring.push("logs", logMsg(i));
		const items = ring.peek(3);
		expect(items).toHaveLength(3);
		expect(ring.size("logs")).toBe(10); // peek does not remove
	});

	test("drop count increments on overflow", () => {
		const ring = new TelemetryRing({ metrics: 200 });
		// estimateSize base is 64; name length pads each item over the budget after a few.
		for (let i = 0; i < 5; i++) ring.push("metrics", metric(`metric_${i}`));
		expect(ring.dropCount("metrics")).toBeGreaterThan(0);
	});

	test("item id is unique and monotonically increasing", () => {
		const ring = new TelemetryRing();
		ring.push("ops", op("01900000-0000-7000-8000-0000000000a1"));
		ring.push("ops", op("01900000-0000-7000-8000-0000000000a2"));
		const items = ring.peek(100);
		expect(items[0]!.id).toBeLessThan(items[1]!.id);
	});

	test("totalDropCount sums all kinds", () => {
		const ring = new TelemetryRing({ metrics: 100 });
		for (let i = 0; i < 5; i++) ring.push("metrics", metric(`metric_${i}`));
		expect(ring.totalDropCount()).toBe(ring.dropCount("metrics"));
		expect(ring.totalDropCount()).toBeGreaterThan(0);
	});

	test("overflow keeps FIFO order and counts every eviction", () => {
		// meta padding sizes each op at ~1000 bytes; budget = 10KiB → 10 fit.
		const ring = new TelemetryRing({ ops: 10 * 1024 });
		for (let i = 0; i < 100; i++) {
			ring.push("ops", op(`01900000-0000-7000-8000-0000000${1000 + i}`, 850));
		}
		const items = ring.peek(1000);
		expect(items).toHaveLength(10);
		expect(ring.size("ops")).toBe(10);
		expect(ring.dropCount("ops")).toBe(90);
		// The survivors are the newest 10, still oldest-first.
		expect(items.map((it) => (it.message as OpReport).opId)).toEqual(
			Array.from(
				{ length: 10 },
				(_, i) => `01900000-0000-7000-8000-0000000${1090 + i}`,
			),
		);
	});

	test("FIFO order survives interleaved commits and evictions", () => {
		const ring = new TelemetryRing({ ops: 10 * 1024 });
		for (let i = 0; i < 10; i++) {
			ring.push("ops", op(`01900000-0000-7000-8000-0000000${1000 + i}`, 850));
		}
		// Ack a non-prefix subset: leaves holes in the middle of the buffer.
		const all = ring.peek(1000);
		ring.commit([all[2]!, all[5]!, all[6]!]);
		expect(ring.size("ops")).toBe(7);
		// Refill past the budget so eviction has to walk over the holes.
		for (let i = 0; i < 6; i++) {
			ring.push("ops", op(`01900000-0000-7000-8000-0000000${2000 + i}`, 850));
		}
		const ids = ring.peek(1000).map((it) => (it.message as OpReport).opId);
		expect(ids).toHaveLength(10);
		expect(ids).toEqual([...ids].sort());
		expect(ids.slice(-6)).toEqual(
			Array.from(
				{ length: 6 },
				(_, i) => `01900000-0000-7000-8000-0000000${2000 + i}`,
			),
		);
	});

	test("un-acked items outlive repeated peeks after neighbours were acked", () => {
		const ring = new TelemetryRing();
		for (let i = 0; i < 5; i++) {
			ring.push("ops", op(`01900000-0000-7000-8000-0000000000b${i}`));
		}
		const all = ring.peek(100);
		ring.commit([all[0]!, all[1]!, all[3]!]);
		const first = ring.peek(100).map((it) => (it.message as OpReport).opId);
		const second = ring.peek(100).map((it) => (it.message as OpReport).opId);
		expect(first).toEqual([
			"01900000-0000-7000-8000-0000000000b2",
			"01900000-0000-7000-8000-0000000000b4",
		]);
		expect(second).toEqual(first);
	});

	test("committing an already-evicted id is a no-op", () => {
		const ring = new TelemetryRing({ ops: 4 * 1024 });
		ring.push("ops", op("01900000-0000-7000-8000-0000000000c0", 850));
		const stale = ring.peek(100);
		for (let i = 0; i < 8; i++) {
			ring.push("ops", op(`01900000-0000-7000-8000-0000000000c${i + 1}`, 850));
		}
		const before = ring.size("ops");
		ring.commit(stale);
		expect(ring.size("ops")).toBe(before);
	});

	test("metric labels count towards the byte budget", () => {
		const bare = new TelemetryRing({ metrics: 16 * 1024 });
		const labelled = new TelemetryRing({ metrics: 16 * 1024 });
		bare.push("metrics", metric("m"));
		labelled.push(
			"metrics",
			metric("m", { route: "/orders", region: "eu-central-1" }),
		);
		expect(labelled.bytes("metrics")).toBe(
			bare.bytes("metrics") +
				"route".length +
				"/orders".length +
				"region".length +
				"eu-central-1".length,
		);
	});

	test("sustained overflow holds capacity, FIFO and drop accounting", () => {
		const ring = new TelemetryRing({ ops: 10 * 1024 });
		for (let i = 0; i < 200_000; i++) {
			ring.push("ops", op(`01900000-0000-7000-8000-00000${1000000 + i}`, 850));
		}
		expect(ring.size("ops")).toBe(10);
		expect(ring.dropCount("ops")).toBe(199_990);
		const ids = ring.peek(1000).map((it) => (it.message as OpReport).opId);
		expect(ids).toEqual(
			Array.from(
				{ length: 10 },
				(_, i) => `01900000-0000-7000-8000-00000${1199990 + i}`,
			),
		);
		expect(ring.bytes("ops")).toBeLessThanOrEqual(10 * 1024);
	});

	test("push into a saturated ring does not degrade with ring depth", () => {
		// Each op is ~1000 bytes. The small ring holds ~8 items, the large one
		// ~1000, and every push over budget evicts one. Cost per push must not
		// scale with how deep the ring is.
		const PUSHES = 40_000;
		const run = (budgetBytes: number): number => {
			const ring = new TelemetryRing({ ops: budgetBytes });
			const msg = op("01900000-0000-7000-8000-0000000000d0", 850);
			const started = performance.now();
			for (let i = 0; i < PUSHES; i++) ring.push("ops", msg);
			return performance.now() - started;
		};
		run(8 * 1024); // warm the JIT so the first measured run is not penalised
		const small = run(8 * 1024);
		const large = run(1000 * 1024);
		expect(large).toBeLessThan(small * 6 + 100);
	});

	test("steady-state peek/commit/push does not degrade with ring depth", () => {
		// The transport peeks a fixed batch, acks it and keeps producing. Rebuilding
		// the backing array on every ack costs O(depth) and allocates a fresh array
		// per ack; releasing slots in place and short-circuiting the head prefix
		// costs O(batch). Growing the ring 80x must not grow the time in step.
		const BATCH = 256;
		const ROUNDS = 400;
		const msg = op("01900000-0000-7000-8000-0000000000e0");
		const run = (depth: number): number => {
			const ring = new TelemetryRing({ ops: depth * 4096 });
			for (let i = 0; i < depth; i++) ring.push("ops", msg);
			const started = performance.now();
			for (let r = 0; r < ROUNDS; r++) {
				ring.commit(ring.peek(BATCH));
				for (let i = 0; i < BATCH; i++) ring.push("ops", msg);
			}
			return performance.now() - started;
		};
		run(300); // warm the JIT
		const shallow = run(300);
		const deep = run(24_000);
		expect(deep).toBeLessThan(shallow * 6 + 100);
	});

	test("push and peek payloads", () => {
		const ring = new TelemetryRing();
		ring.push("payloads", {
			traceId: "01900000-0000-7000-8000-0000000000aa",
			opId: "01900000-0000-7000-8000-0000000000bb",
			direction: 1,
			bytes: Buffer.from([9, 8, 7]),
			originalSize: 3,
			contractHash: "h",
		});
		const items: RingItem[] = ring.peek(100);
		expect(items).toHaveLength(1);
		expect(items[0]!.kind).toBe("payloads");
	});
});
