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

function metric(name: string): MetricPoint {
	return {
		atUnixMs: 1,
		name,
		kind: 1,
		labels: {},
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
