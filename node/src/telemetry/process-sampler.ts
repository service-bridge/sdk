// process-sampler.ts — periodic per-process CPU% and RSS gauge emission.
// @public — см. ./README.md

import { MetricKind } from "../pb/servicebridge/v1/telemetry";
import type { TelemetryRing } from "./ring";

const SAMPLE_INTERVAL_MS = 30_000;

/**
 * cpuPercent computes process CPU utilisation as a percentage.
 *
 * Formula: (userDeltaMicros + systemDeltaMicros) / (elapsedMs * 1_000) * 100
 *
 * Result is normalised to a single-core-equivalent percent — a process
 * saturating 2 cores over the interval returns ~200. elapsedMs=0 returns 0.
 *
 * @public — см. ./README.md
 */
export function cpuPercent(
	prev: NodeJS.CpuUsage,
	cur: NodeJS.CpuUsage,
	elapsedMs: number,
): number {
	if (elapsedMs <= 0) return 0;
	const cpuDeltaMicros = cur.user - prev.user + (cur.system - prev.system);
	return (cpuDeltaMicros / (elapsedMs * 1_000)) * 100;
}

/**
 * ProcessSampler owns a periodic timer that emits `process.cpu_percent` and
 * `process.rss_bytes` gauges into the telemetry ring. One instance per SDK
 * client; lifecycle mirrors the client: start() on connect, close() on stop.
 *
 * `getInstanceId` is resolved at each tick — the same lazy-identity pattern
 * used by the rest of the telemetry API; the sampler can be started before
 * Welcome and will carry the correct instance_id once it arrives.
 *
 * @public — см. ./README.md
 */
export class ProcessSampler {
	private readonly ring: TelemetryRing;
	private readonly getInstanceId: () => string;
	private readonly intervalMs: number;
	private timer: ReturnType<typeof setInterval> | null = null;
	private closed = false;
	private prevCpu: NodeJS.CpuUsage = process.cpuUsage();
	private prevAt = Date.now();

	constructor(
		ring: TelemetryRing,
		getInstanceId: () => string,
		intervalMs = SAMPLE_INTERVAL_MS,
	) {
		this.ring = ring;
		this.getInstanceId = getInstanceId;
		this.intervalMs = intervalMs;
	}

	start(): void {
		if (this.closed) return;
		if (this.timer) return;
		// Первый сэмпл — сразу при старте, чтобы только что подключившийся
		// сервис показывал ресурсы с момента инициализации, а не только после
		// первого интервала. CPU% на этой точке — среднее за всё время жизни
		// процесса (baseline = старт процесса); tick() затем сбросит prev* на
		// "сейчас", и последующие тики считают дельту за интервал.
		this.prevCpu = { user: 0, system: 0 };
		this.prevAt = Date.now() - Math.round(process.uptime() * 1000);
		this.tick();
		const t = setInterval(() => this.tick(), this.intervalMs);
		// Do not keep the process alive solely due to the sampler.
		if (typeof t.unref === "function") t.unref();
		this.timer = t;
	}

	close(): void {
		this.closed = true;
		if (this.timer) {
			clearInterval(this.timer);
			this.timer = null;
		}
	}

	/** tick samples CPU and RSS and pushes two gauge points into the ring. */
	tick(): void {
		if (this.closed) return;
		const now = Date.now();
		const curCpu = process.cpuUsage();
		const elapsed = now - this.prevAt;

		const cpu = cpuPercent(this.prevCpu, curCpu, elapsed);
		const rss = process.memoryUsage().rss;

		this.prevCpu = curCpu;
		this.prevAt = now;

		const atUnixMs = now;
		const empty = Buffer.alloc(0);

		const instanceId = this.getInstanceId();

		this.ring.push("metrics", {
			atUnixMs,
			name: "process.cpu_percent",
			kind: MetricKind.METRIC_KIND_GAUGE,
			labels: {},
			instanceId,
			value: cpu,
			unit: "%",
			bucketsJson: empty,
		});

		this.ring.push("metrics", {
			atUnixMs,
			name: "process.rss_bytes",
			kind: MetricKind.METRIC_KIND_GAUGE,
			labels: {},
			instanceId,
			value: rss,
			unit: "By",
			bucketsJson: empty,
		});
	}
}
