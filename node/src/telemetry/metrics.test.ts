import { describe, expect, test } from "bun:test";
import type { MetricPoint } from "../pb/servicebridge/v1/telemetry";
import { MetricKind, makeCounter, makeGauge, makeHistogram } from "./metrics";
import { TelemetryRing } from "./ring";

function firstMetric(ring: TelemetryRing): MetricPoint {
	return ring.peek(10)[0]!.message as MetricPoint;
}

describe("counter", () => {
	test("inc enqueues a counter metric", () => {
		const ring = new TelemetryRing();
		const c = makeCounter(ring, "inst-1", "my_counter");
		c.inc(5);

		const items = ring.peek(10);
		expect(items).toHaveLength(1);
		const decoded = items[0]!.message as MetricPoint;
		expect(decoded.name).toBe("my_counter");
		expect(decoded.kind).toBe(MetricKind.METRIC_KIND_COUNTER);
		expect(decoded.value).toBe(5);
	});
});

describe("gauge", () => {
	test("set enqueues a gauge metric", () => {
		const ring = new TelemetryRing();
		const g = makeGauge(ring, "inst-1", "cpu_usage");
		g.set(0.72);

		const decoded = firstMetric(ring);
		expect(decoded.kind).toBe(MetricKind.METRIC_KIND_GAUGE);
		expect(decoded.value).toBeCloseTo(0.72);
	});
});

describe("histogram", () => {
	test("observe enqueues a histogram metric", () => {
		const ring = new TelemetryRing();
		const h = makeHistogram(ring, "inst-1", "request_duration", "s");
		h.observe(0.123);

		const decoded = firstMetric(ring);
		expect(decoded.kind).toBe(MetricKind.METRIC_KIND_HISTOGRAM);
		expect(decoded.unit).toBe("s");
		expect(decoded.value).toBeCloseTo(0.123);
	});
});
