import { afterEach, beforeEach, describe, expect, mock, test } from "bun:test";
import type { MetricPoint } from "../pb/servicebridge/v1/telemetry";
import { MetricKind } from "../pb/servicebridge/v1/telemetry";
import { cpuPercent, ProcessSampler } from "./process-sampler";
import { TelemetryRing } from "./ring";

// ── cpuPercent pure function ──────────────────────────────────────────────────

describe("cpuPercent", () => {
	test("zero usage when no CPU time consumed", () => {
		const prev = { user: 1_000_000, system: 500_000 };
		const cur = { user: 1_000_000, system: 500_000 };
		expect(cpuPercent(prev, cur, 1_000)).toBeCloseTo(0);
	});

	test("100% when cpu micros equal elapsed micros (single core)", () => {
		const prev = { user: 0, system: 0 };
		// elapsedMs=1000 → elapsedMicros=1_000_000; cpu delta=1_000_000 → 100%
		const cur = { user: 800_000, system: 200_000 };
		expect(cpuPercent(prev, cur, 1_000)).toBeCloseTo(100);
	});

	test("200% when cpu micros are 2x elapsed (multi-core)", () => {
		const prev = { user: 0, system: 0 };
		const cur = { user: 1_500_000, system: 500_000 };
		expect(cpuPercent(prev, cur, 1_000)).toBeCloseTo(200);
	});

	test("partial usage", () => {
		const prev = { user: 1_000_000, system: 1_000_000 };
		// delta: user+100_000 system+50_000 = 150_000 micros over 500ms = 500_000 micros
		const cur = { user: 1_100_000, system: 1_050_000 };
		// 150_000 / 500_000 * 100 = 30%
		expect(cpuPercent(prev, cur, 500)).toBeCloseTo(30);
	});

	test("returns 0 for zero elapsed time (guards division by zero)", () => {
		const prev = { user: 0, system: 0 };
		const cur = { user: 100_000, system: 50_000 };
		expect(cpuPercent(prev, cur, 0)).toBe(0);
	});
});

// ── ProcessSampler lifecycle ──────────────────────────────────────────────────

describe("ProcessSampler tick", () => {
	let ring: TelemetryRing;

	beforeEach(() => {
		ring = new TelemetryRing();
	});

	test("tick enqueues exactly two metric points", () => {
		const sampler = new ProcessSampler(ring, () => "inst-abc", 30_000);
		sampler.tick();

		const items = ring.peek(10);
		expect(items).toHaveLength(2);
	});

	test("tick enqueues process.cpu_percent with instance_id", () => {
		const sampler = new ProcessSampler(ring, () => "inst-xyz", 30_000);
		sampler.tick();

		const pts = ring.peek(10).map((i) => i.message as MetricPoint);
		const cpu = pts.find((p) => p.name === "process.cpu_percent");
		expect(cpu).toBeDefined();
		expect(cpu!.instanceId).toBe("inst-xyz");
		expect(cpu!.kind).toBe(MetricKind.METRIC_KIND_GAUGE);
	});

	test("tick enqueues process.rss_bytes with instance_id", () => {
		const sampler = new ProcessSampler(ring, () => "inst-xyz", 30_000);
		sampler.tick();

		const pts = ring.peek(10).map((i) => i.message as MetricPoint);
		const rss = pts.find((p) => p.name === "process.rss_bytes");
		expect(rss).toBeDefined();
		expect(rss!.instanceId).toBe("inst-xyz");
		expect(rss!.kind).toBe(MetricKind.METRIC_KIND_GAUGE);
		expect(rss!.value).toBeGreaterThan(0);
	});

	test("tick cpu_percent value is finite and non-negative", () => {
		const sampler = new ProcessSampler(ring, () => "inst-1", 30_000);
		sampler.tick();

		const pts = ring.peek(10).map((i) => i.message as MetricPoint);
		const cpu = pts.find((p) => p.name === "process.cpu_percent")!;
		expect(Number.isFinite(cpu.value)).toBe(true);
		expect(cpu.value).toBeGreaterThanOrEqual(0);
	});
});

describe("ProcessSampler start emits immediately", () => {
	test("start() pushes a first sample without waiting for the interval", () => {
		const ring = new TelemetryRing();
		const sampler = new ProcessSampler(ring, () => "inst-1", 30_000);
		sampler.start();
		try {
			// Two points (cpu + rss) present right after start, before any tick fires.
			expect(ring.size("metrics")).toBe(2);
			const pts = ring.peek(10).map((i) => i.message as MetricPoint);
			const cpu = pts.find((p) => p.name === "process.cpu_percent")!;
			expect(Number.isFinite(cpu.value)).toBe(true);
			expect(cpu.value).toBeGreaterThanOrEqual(0);
			const rss = pts.find((p) => p.name === "process.rss_bytes")!;
			expect(rss.value).toBeGreaterThan(0);
		} finally {
			sampler.close();
		}
	});
});

describe("ProcessSampler close stops sampler", () => {
	test("no further enqueues after close()", () => {
		const ring = new TelemetryRing();
		const sampler = new ProcessSampler(ring, () => "inst-1", 30_000);
		sampler.start();
		sampler.close();
		// After close, tick should be a no-op (closed flag set)
		const sizeBefore = ring.size("metrics");
		sampler.tick();
		expect(ring.size("metrics")).toBe(sizeBefore);
	});

	test("start/close cycle does not throw", () => {
		const ring = new TelemetryRing();
		const sampler = new ProcessSampler(ring, () => "inst-1", 30_000);
		expect(() => {
			sampler.start();
			sampler.close();
		}).not.toThrow();
	});
});
