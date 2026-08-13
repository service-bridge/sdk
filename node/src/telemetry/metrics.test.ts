import { describe, expect, test } from "bun:test";
import type { MetricPoint } from "../pb/servicebridge/v1/telemetry";
import {
	DEFAULT_HISTOGRAM_BOUNDS,
	type HistogramBucket,
	MetricKind,
	MetricsAggregator,
	makeCounter,
	makeGauge,
	makeHistogram,
} from "./metrics";
import { TelemetryRing } from "./ring";

// Closes an aggregation window the way the transport's flush cycle does, then
// reads what landed in the ring. The ring never drains the aggregator on its
// own — the flush call IS the window boundary.
function metricPoints(ring: TelemetryRing): MetricPoint[] {
	ring.metrics.flush();
	return ring
		.peek(1000)
		.filter((it) => it.kind === "metrics")
		.map((it) => it.message as MetricPoint);
}

function ringMetrics(ring: TelemetryRing): MetricPoint[] {
	return ring
		.peek(1000)
		.filter((it) => it.kind === "metrics")
		.map((it) => it.message as MetricPoint);
}

function buckets(point: MetricPoint): HistogramBucket[] {
	return JSON.parse(point.bucketsJson.toString("utf8")) as HistogramBucket[];
}

describe("counter", () => {
	test("1000 increments produce ONE point with value 1000", () => {
		const ring = new TelemetryRing();
		const c = makeCounter(ring, "inst-1", "req_total");
		for (let i = 0; i < 1000; i++) c.inc();

		const points = metricPoints(ring);
		expect(points).toHaveLength(1);
		expect(points[0]!.name).toBe("req_total");
		expect(points[0]!.kind).toBe(MetricKind.METRIC_KIND_COUNTER);
		expect(points[0]!.value).toBe(1000);
		expect(points[0]!.unit).toBe("1");
		expect(ring.dropCount("metrics")).toBe(0);
	});

	test("inc sums explicit amounts", () => {
		const ring = new TelemetryRing();
		const c = makeCounter(ring, "inst-1", "bytes_total");
		c.inc(5);
		c.inc(7);
		expect(metricPoints(ring)[0]!.value).toBe(12);
	});

	test("counter resets after a drain — a second window starts at zero", () => {
		const agg = new MetricsAggregator({ push: () => {} });
		const c = agg.counter("inst-1", "req_total");
		c.inc(3);
		expect(agg.drain(1).map((p) => p.value)).toEqual([3]);
		c.inc(4);
		expect(agg.drain(2).map((p) => p.value)).toEqual([4]);
	});

	test("an untouched counter emits nothing", () => {
		const agg = new MetricsAggregator({ push: () => {} });
		agg.counter("inst-1", "req_total").inc(1);
		expect(agg.drain(1)).toHaveLength(1);
		expect(agg.drain(2)).toHaveLength(0);
	});

	test("two handles for the same series share one accumulator", () => {
		const ring = new TelemetryRing();
		makeCounter(ring, "inst-1", "req_total", { route: "/a" }).inc(2);
		makeCounter(ring, "inst-1", "req_total", { route: "/a" }).inc(3);

		const points = metricPoints(ring);
		expect(points).toHaveLength(1);
		expect(points[0]!.value).toBe(5);
	});

	test("different labels are different series", () => {
		const ring = new TelemetryRing();
		makeCounter(ring, "inst-1", "req_total", { route: "/a" }).inc(2);
		makeCounter(ring, "inst-1", "req_total", { route: "/b" }).inc(3);

		const points = metricPoints(ring);
		expect(points).toHaveLength(2);
		expect(points.map((p) => p.labels.route).sort()).toEqual(["/a", "/b"]);
	});

	test("different instance ids are different series", () => {
		const ring = new TelemetryRing();
		makeCounter(ring, "inst-1", "req_total").inc(2);
		makeCounter(ring, "inst-2", "req_total").inc(3);
		expect(metricPoints(ring)).toHaveLength(2);
	});
});

describe("series key", () => {
	test("label order does not change identity", () => {
		const ring = new TelemetryRing();
		makeCounter(ring, "inst-1", "req_total", { a: "1", b: "2" }).inc(1);
		makeCounter(ring, "inst-1", "req_total", { b: "2", a: "1" }).inc(1);

		const points = metricPoints(ring);
		expect(points).toHaveLength(1);
		expect(points[0]!.value).toBe(2);
		expect(points[0]!.labels).toEqual({ a: "1", b: "2" });
	});

	test("label separators cannot be forged into a collision", () => {
		const agg = new MetricsAggregator({ push: () => {} });
		agg.counter("i", "m", { a: "1 b", c: "" }).inc(1);
		agg.counter("i", "m", { a: "1", b: "", c: "" }).inc(1);
		expect(agg.seriesCount).toBe(2);
	});

	test("labels are copied — later mutation does not move the series", () => {
		const ring = new TelemetryRing();
		const labels = { route: "/a" };
		const c = makeCounter(ring, "inst-1", "req_total", labels);
		labels.route = "/mutated";
		c.inc(1);

		const points = metricPoints(ring);
		expect(points).toHaveLength(1);
		expect(points[0]!.labels).toEqual({ route: "/a" });
	});
});

describe("gauge", () => {
	test("last value wins inside a window", () => {
		const ring = new TelemetryRing();
		const g = makeGauge(ring, "inst-1", "queue_depth");
		g.set(5);
		g.set(9);
		g.set(7);

		const points = metricPoints(ring);
		expect(points).toHaveLength(1);
		expect(points[0]!.kind).toBe(MetricKind.METRIC_KIND_GAUGE);
		expect(points[0]!.value).toBe(7);
	});

	test("an untouched gauge is not re-emitted, but keeps its value", () => {
		const agg = new MetricsAggregator({ push: () => {} });
		const g = agg.gauge("inst-1", "queue_depth");
		g.set(4);
		expect(agg.drain(1).map((p) => p.value)).toEqual([4]);
		expect(agg.drain(2)).toHaveLength(0);
		g.set(4);
		expect(agg.drain(3).map((p) => p.value)).toEqual([4]);
	});
});

describe("histogram", () => {
	test("observations land in cumulative buckets", () => {
		const ring = new TelemetryRing();
		const h = makeHistogram(ring, "inst-1", "rpc_duration", "s");
		h.observe(0.003); // <= 0.005
		h.observe(0.02); // <= 0.025
		h.observe(0.02);
		h.observe(42); // > 10 → +Inf only

		const points = metricPoints(ring);
		expect(points).toHaveLength(1);
		const p = points[0]!;
		expect(p.kind).toBe(MetricKind.METRIC_KIND_HISTOGRAM);
		expect(p.unit).toBe("s");
		expect(p.value).toBeCloseTo(0.003 + 0.02 + 0.02 + 42);

		const b = buckets(p);
		expect(b).toHaveLength(DEFAULT_HISTOGRAM_BOUNDS.length + 1);
		expect(b[0]).toEqual({ le: 0.005, count: 1 });
		expect(b[1]).toEqual({ le: 0.01, count: 1 });
		expect(b[2]).toEqual({ le: 0.025, count: 3 });
		expect(b[b.length - 2]).toEqual({ le: 10, count: 3 });
		expect(b[b.length - 1]).toEqual({ le: "+Inf", count: 4 });
	});

	test("bucketsJson is non-empty and monotonically non-decreasing", () => {
		const ring = new TelemetryRing();
		const h = makeHistogram(ring, "inst-1", "rpc_duration");
		for (const v of [0.001, 0.06, 0.3, 3, 20]) h.observe(v);

		const p = metricPoints(ring)[0]!;
		expect(p.bucketsJson.length).toBeGreaterThan(0);
		const b = buckets(p);
		for (let i = 1; i < b.length; i++) {
			expect(b[i]!.count).toBeGreaterThanOrEqual(b[i - 1]!.count);
		}
		expect(b[b.length - 1]).toEqual({ le: "+Inf", count: 5 });
	});

	test("value on the boundary counts into that bucket (le semantics)", () => {
		const agg = new MetricsAggregator({ push: () => {} });
		agg.histogram("i", "h", "s", {}, [1, 2]).observe(1);
		const b = JSON.parse(
			agg.drain(1)[0]!.bucketsJson.toString("utf8"),
		) as HistogramBucket[];
		expect(b).toEqual([
			{ le: 1, count: 1 },
			{ le: 2, count: 1 },
			{ le: "+Inf", count: 1 },
		]);
	});

	test("custom bounds are honoured", () => {
		const ring = new TelemetryRing();
		const h = makeHistogram(
			ring,
			"inst-1",
			"payload_bytes",
			"By",
			{},
			[100, 1000],
		);
		h.observe(50);
		h.observe(500);
		h.observe(5000);

		expect(buckets(metricPoints(ring)[0]!)).toEqual([
			{ le: 100, count: 1 },
			{ le: 1000, count: 2 },
			{ le: "+Inf", count: 3 },
		]);
	});

	test("buckets reset after a drain", () => {
		const agg = new MetricsAggregator({ push: () => {} });
		const h = agg.histogram("i", "h", "s", {}, [1]);
		h.observe(0.5);
		agg.drain(1);
		h.observe(5);
		const b = JSON.parse(
			agg.drain(2)[0]!.bucketsJson.toString("utf8"),
		) as HistogramBucket[];
		expect(b).toEqual([
			{ le: 1, count: 0 },
			{ le: "+Inf", count: 1 },
		]);
	});

	test("rejects empty, non-finite or unordered bounds", () => {
		const agg = new MetricsAggregator({ push: () => {} });
		expect(() => agg.histogram("i", "h", "s", {}, [])).toThrow(
			/must not be empty/,
		);
		expect(() =>
			agg.histogram("i", "h", "s", {}, [1, Number.POSITIVE_INFINITY]),
		).toThrow(/finite/);
		expect(() => agg.histogram("i", "h", "s", {}, [2, 1])).toThrow(
			/strictly ascending/,
		);
		expect(() => agg.histogram("i", "h", "s", {}, [1, 1])).toThrow(
			/strictly ascending/,
		);
	});

	test("re-registering one series with different bounds fails loudly", () => {
		const agg = new MetricsAggregator({ push: () => {} });
		agg.histogram("i", "h", "s", {}, [1, 2]);
		expect(() => agg.histogram("i", "h", "s", {}, [1, 3])).toThrow(
			/different bounds/,
		);
	});

	test("re-registering one series with a different unit fails loudly", () => {
		const agg = new MetricsAggregator({ push: () => {} });
		agg.histogram("i", "h", "s");
		expect(() => agg.histogram("i", "h", "By")).toThrow(/already registered/);
	});
});

describe("aggregator ↔ ring", () => {
	test("a saturated hot loop no longer overflows the metrics ring", () => {
		const ring = new TelemetryRing();
		const c = makeCounter(ring, "inst-1", "req_total", { route: "/orders" });
		const h = makeHistogram(ring, "inst-1", "rpc_duration", "s", {
			route: "/orders",
		});
		for (let i = 0; i < 100_000; i++) {
			c.inc();
			h.observe(i / 100_000);
		}
		expect(metricPoints(ring)).toHaveLength(2);
		expect(ring.dropCount("metrics")).toBe(0);
	});

	test("counter kinds carry no buckets", () => {
		const ring = new TelemetryRing();
		makeCounter(ring, "inst-1", "req_total").inc();
		makeGauge(ring, "inst-1", "queue_depth").set(1);
		for (const p of metricPoints(ring)) expect(p.bucketsJson.length).toBe(0);
	});

	test("drain hands the points to the caller instead of the sink", () => {
		const ring = new TelemetryRing();
		makeCounter(ring, "inst-1", "req_total").inc(4);
		const points = ring.metrics.drain(1234);
		expect(points).toHaveLength(1);
		expect(points[0]!.atUnixMs).toBe(1234);
		// drain consumed the state, so a later flush has nothing left to push.
		expect(metricPoints(ring)).toHaveLength(0);
	});

	test("peek alone never materialises a point — flush owns the boundary", () => {
		const ring = new TelemetryRing();
		makeCounter(ring, "inst-1", "req_total").inc(4);
		expect(ringMetrics(ring)).toHaveLength(0);
		expect(ring.size("metrics")).toBe(0);

		ring.metrics.flush();
		expect(ringMetrics(ring)).toHaveLength(1);
	});

	test("a second flush inside one window does not duplicate points", () => {
		const ring = new TelemetryRing();
		makeCounter(ring, "inst-1", "req_total").inc(4);
		ring.metrics.flush();
		ring.metrics.flush();
		expect(ringMetrics(ring)).toHaveLength(1);
	});

	test("un-acked points stay in the ring across flush cycles", () => {
		const ring = new TelemetryRing();
		const c = makeCounter(ring, "inst-1", "req_total");
		c.inc(4);
		ring.metrics.flush();
		// Cycle two: nothing acked yet, and the counter moved again.
		c.inc(6);
		ring.metrics.flush();
		expect(ringMetrics(ring).map((p) => p.value)).toEqual([4, 6]);
	});
});
