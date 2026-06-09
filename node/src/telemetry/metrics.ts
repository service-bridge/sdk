// metrics.ts — counter/gauge/histogram emission into the telemetry ring.
// @public — см. ./README.md

import { MetricKind, type MetricPoint } from "../pb/servicebridge/v1/telemetry";
import type { TelemetryRing } from "./ring";

export { MetricKind };

export type Labels = Record<string, string>;

function encodeLabels(labels: Labels): Labels {
	return labels;
}

function pushPoint(
	ring: TelemetryRing,
	_instanceId: string,
	point: MetricPoint,
): void {
	ring.push("metrics", point);
}

// Counter is a monotonically increasing metric.
// @public — см. ./README.md
export function makeCounter(
	ring: TelemetryRing,
	instanceId: string,
	name: string,
	labels: Labels = {},
) {
	return {
		inc(amount = 1): void {
			pushPoint(ring, instanceId, {
				atUnixMs: Date.now(),
				name,
				kind: MetricKind.METRIC_KIND_COUNTER,
				labels: encodeLabels(labels),
				instanceId,
				value: amount,
				unit: "1",
				bucketsJson: Buffer.alloc(0),
			});
		},
	};
}

// Gauge can go up or down.
// @public — см. ./README.md
export function makeGauge(
	ring: TelemetryRing,
	instanceId: string,
	name: string,
	labels: Labels = {},
) {
	return {
		set(value: number): void {
			pushPoint(ring, instanceId, {
				atUnixMs: Date.now(),
				name,
				kind: MetricKind.METRIC_KIND_GAUGE,
				labels: encodeLabels(labels),
				instanceId,
				value,
				unit: "1",
				bucketsJson: Buffer.alloc(0),
			});
		},
	};
}

// Histogram records distribution observations.
// @public — см. ./README.md
export function makeHistogram(
	ring: TelemetryRing,
	instanceId: string,
	name: string,
	unit = "s",
	labels: Labels = {},
) {
	return {
		observe(value: number): void {
			pushPoint(ring, instanceId, {
				atUnixMs: Date.now(),
				name,
				kind: MetricKind.METRIC_KIND_HISTOGRAM,
				labels: encodeLabels(labels),
				instanceId,
				value,
				unit,
				bucketsJson: Buffer.alloc(0),
			});
		},
	};
}
