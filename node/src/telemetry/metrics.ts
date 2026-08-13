// metrics.ts — counter/gauge/histogram aggregation for the telemetry ring.
// @public — см. ./README.md

import { MetricKind, type MetricPoint } from "../pb/servicebridge/v1/telemetry";

export { MetricKind };

export type Labels = Record<string, string>;

/**
 * MetricSink receives the points a drain produced. `TelemetryRing` satisfies it.
 * @internal
 */
export interface MetricSink {
	push(kind: "metrics", message: MetricPoint): void;
}

/**
 * MetricsHost owns a `MetricsAggregator`. `TelemetryRing` satisfies it; the
 * `make*` factories accept the host so callers keep passing the ring.
 * @internal
 */
export interface MetricsHost {
	readonly metrics: MetricsAggregator;
}

/** @public — см. ./README.md */
export interface Counter {
	inc(amount?: number): void;
}

/** @public — см. ./README.md */
export interface Gauge {
	set(value: number): void;
}

/** @public — см. ./README.md */
export interface Histogram {
	observe(value: number): void;
}

/**
 * Cumulative bucket entry as it is written to `MetricPoint.buckets_json`.
 * `count` is the number of observations `<= le`, so the `"+Inf"` entry carries
 * the total observation count. The overflow bound is the string `"+Inf"`
 * because JSON has no infinity literal.
 * @public — см. ./README.md
 */
export interface HistogramBucket {
	le: number | "+Inf";
	count: number;
}

/**
 * Default histogram bounds, in seconds — the Prometheus latency ladder.
 * @public — см. ./README.md
 */
export const DEFAULT_HISTOGRAM_BOUNDS: readonly number[] = [
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
];

const EMPTY_BUCKETS = Buffer.alloc(0);

interface SeriesBase {
	readonly name: string;
	readonly instanceId: string;
	readonly labels: Labels;
	readonly unit: string;
	dirty: boolean;
}

type CounterSeries = SeriesBase & {
	readonly kind: MetricKind.METRIC_KIND_COUNTER;
	sum: number;
};

type GaugeSeries = SeriesBase & {
	readonly kind: MetricKind.METRIC_KIND_GAUGE;
	value: number;
};

type HistogramSeries = SeriesBase & {
	readonly kind: MetricKind.METRIC_KIND_HISTOGRAM;
	readonly bounds: readonly number[];
	readonly counts: number[];
	sum: number;
};

type Series = CounterSeries | GaugeSeries | HistogramSeries;

// NUL separates key segments. A printable separator would let a label value
// forge one: with "|", `{a: "1|b"}` and `{a: "1", b: ""}` yield the same key and
// silently merge two series.
const SEP = "\u0000";

// Series identity. Label keys are sorted so {a:1,b:2} and {b:2,a:1} address the
// same state.
function seriesKey(
	kind: MetricKind,
	name: string,
	instanceId: string,
	sortedLabelKeys: string[],
	labels: Labels,
): string {
	let key = `${kind}${SEP}${name}${SEP}${instanceId}`;
	for (const k of sortedLabelKeys) key += `${SEP}${k}${SEP}${labels[k]}`;
	return key;
}

function normalizeLabels(labels: Labels): { keys: string[]; copy: Labels } {
	const keys = Object.keys(labels).sort();
	const copy: Labels = {};
	for (const k of keys) copy[k] = labels[k] as string;
	return { keys, copy };
}

function validateBounds(bounds: readonly number[]): readonly number[] {
	if (bounds.length === 0) {
		throw new Error("histogram bounds must not be empty");
	}
	for (let i = 0; i < bounds.length; i++) {
		const b = bounds[i] as number;
		if (!Number.isFinite(b)) {
			throw new Error(
				`histogram bound at index ${i} must be finite, got ${String(b)}`,
			);
		}
		if (i > 0 && b <= (bounds[i - 1] as number)) {
			throw new Error(
				`histogram bounds must be strictly ascending, got ${String(bounds[i - 1])} then ${String(b)}`,
			);
		}
	}
	return bounds;
}

function sameBounds(a: readonly number[], b: readonly number[]): boolean {
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
	return true;
}

function encodeBuckets(
	bounds: readonly number[],
	counts: readonly number[],
): Buffer {
	const out: HistogramBucket[] = [];
	let cumulative = 0;
	for (let i = 0; i < bounds.length; i++) {
		cumulative += counts[i] as number;
		out.push({ le: bounds[i] as number, count: cumulative });
	}
	cumulative += counts[bounds.length] as number;
	out.push({ le: "+Inf", count: cumulative });
	return Buffer.from(JSON.stringify(out));
}

/**
 * MetricsAggregator accumulates metric state per `(kind, name, instance, labels)`
 * series and emits exactly one `MetricPoint` per series per drain.
 *
 * Without it every `.inc()` was one ring item: a service at 1000 rps with one
 * counter increment per request produced 1000 points/s into a 16 KiB ring that
 * holds ~190 of them, so the ring silently oldest-dropped almost everything.
 *
 * Hot path (`inc`/`set`/`observe`) touches only the series object the handle
 * closes over — no map lookup, no allocation. The map is consulted once, when a
 * handle is created.
 *
 * @public — см. ./README.md
 */
export class MetricsAggregator {
	private readonly sink: MetricSink;
	private readonly series = new Map<string, Series>();

	constructor(sink: MetricSink) {
		this.sink = sink;
	}

	counter(instanceId: string, name: string, labels: Labels = {}): Counter {
		const s = this.resolve(
			MetricKind.METRIC_KIND_COUNTER,
			instanceId,
			name,
			labels,
			"1",
			() => ({ sum: 0 }),
		) as CounterSeries;
		return {
			inc(amount = 1): void {
				s.sum += amount;
				s.dirty = true;
			},
		};
	}

	gauge(instanceId: string, name: string, labels: Labels = {}): Gauge {
		const s = this.resolve(
			MetricKind.METRIC_KIND_GAUGE,
			instanceId,
			name,
			labels,
			"1",
			() => ({ value: 0 }),
		) as GaugeSeries;
		return {
			set(value: number): void {
				s.value = value;
				s.dirty = true;
			},
		};
	}

	histogram(
		instanceId: string,
		name: string,
		unit = "s",
		labels: Labels = {},
		bounds: readonly number[] = DEFAULT_HISTOGRAM_BOUNDS,
	): Histogram {
		const checked = validateBounds(bounds);
		const s = this.resolve(
			MetricKind.METRIC_KIND_HISTOGRAM,
			instanceId,
			name,
			labels,
			unit,
			() => ({
				bounds: checked,
				counts: new Array<number>(checked.length + 1).fill(0),
				sum: 0,
			}),
		) as HistogramSeries;
		if (!sameBounds(s.bounds, checked)) {
			throw new Error(
				`histogram "${name}" already registered with different bounds — one series has one bucket layout`,
			);
		}
		const bnds = s.bounds;
		const counts = s.counts;
		return {
			observe(value: number): void {
				let i = 0;
				while (i < bnds.length && value > (bnds[i] as number)) i++;
				counts[i] = (counts[i] as number) + 1;
				s.sum += value;
				s.dirty = true;
			},
		};
	}

	/**
	 * drain returns one point per series that changed since the previous drain and
	 * resets the accumulators. Counters and histograms reset to zero; a gauge keeps
	 * its last value but is not re-emitted until it is set again — re-sending an
	 * unchanged gauge every flush tick would put 4 points/s/series on the wire for
	 * no new information and re-create the ring overflow this class exists to fix.
	 * The runtime stores raw points, so the last emitted value stays readable.
	 */
	drain(nowMs: number = Date.now()): MetricPoint[] {
		const out: MetricPoint[] = [];
		for (const s of this.series.values()) {
			if (!s.dirty) continue;
			s.dirty = false;
			out.push(this.toPoint(s, nowMs));
		}
		return out;
	}

	/** flush drains and pushes every produced point into the sink. */
	flush(nowMs: number = Date.now()): number {
		const points = this.drain(nowMs);
		for (const p of points) this.sink.push("metrics", p);
		return points.length;
	}

	/** Number of distinct series currently tracked. @internal */
	get seriesCount(): number {
		return this.series.size;
	}

	private toPoint(s: Series, nowMs: number): MetricPoint {
		const base = {
			atUnixMs: nowMs,
			name: s.name,
			kind: s.kind,
			labels: s.labels,
			instanceId: s.instanceId,
			unit: s.unit,
		};
		if (s.kind === MetricKind.METRIC_KIND_COUNTER) {
			const value = s.sum;
			s.sum = 0;
			return { ...base, value, bucketsJson: EMPTY_BUCKETS };
		}
		if (s.kind === MetricKind.METRIC_KIND_GAUGE) {
			return { ...base, value: s.value, bucketsJson: EMPTY_BUCKETS };
		}
		const bucketsJson = encodeBuckets(s.bounds, s.counts);
		const value = s.sum;
		s.sum = 0;
		s.counts.fill(0);
		return { ...base, value, bucketsJson };
	}

	private resolve(
		kind: MetricKind,
		instanceId: string,
		name: string,
		labels: Labels,
		unit: string,
		init: () => object,
	): Series {
		const { keys, copy } = normalizeLabels(labels);
		const key = seriesKey(kind, name, instanceId, keys, copy);
		const existing = this.series.get(key);
		if (existing) {
			if (existing.unit !== unit) {
				throw new Error(
					`metric "${name}" already registered with unit "${existing.unit}", got "${unit}"`,
				);
			}
			return existing;
		}
		const created = {
			kind,
			name,
			instanceId,
			labels: copy,
			unit,
			dirty: false,
			...init(),
		} as Series;
		this.series.set(key, created);
		return created;
	}
}

// Counter is a monotonically increasing metric.
// @public — см. ./README.md
export function makeCounter(
	host: MetricsHost,
	instanceId: string,
	name: string,
	labels: Labels = {},
): Counter {
	return host.metrics.counter(instanceId, name, labels);
}

// Gauge can go up or down.
// @public — см. ./README.md
export function makeGauge(
	host: MetricsHost,
	instanceId: string,
	name: string,
	labels: Labels = {},
): Gauge {
	return host.metrics.gauge(instanceId, name, labels);
}

// Histogram records distribution observations into cumulative buckets.
// @public — см. ./README.md
export function makeHistogram(
	host: MetricsHost,
	instanceId: string,
	name: string,
	unit = "s",
	labels: Labels = {},
	bounds: readonly number[] = DEFAULT_HISTOGRAM_BOUNDS,
): Histogram {
	return host.metrics.histogram(instanceId, name, unit, labels, bounds);
}
