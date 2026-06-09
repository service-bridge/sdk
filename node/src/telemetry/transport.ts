// transport.ts — real bidi gRPC Telemetry.Report client.
// Peeks typed messages from the TelemetryRing into TelemetryBatch frames, writes
// them on the long-lived bidi stream, and commits (releases) them only after the
// runtime acks — at-least-once delivery (C1). Reopens the stream on disconnect
// with an exponential backoff that resets on the first successful ack (C2).
// @public — см. ./README.md

import type { ClientDuplexStream } from "@grpc/grpc-js";
import type {
	Log,
	MetricPoint,
	OpReport,
	PayloadAttachment,
} from "../pb/servicebridge/v1/telemetry";
import {
	LogBatch,
	MetricBatch,
	OpBatch,
	PayloadBatch,
	type TelemetryAck,
	type TelemetryBatch,
	type TelemetryClient,
} from "../pb/servicebridge/v1/telemetry";
import type { RingItem, TelemetryRing } from "./ring";

/**
 * Minimal stream shape we depend on. The generated gRPC client returns
 * `ClientDuplexStream<TelemetryBatch, TelemetryAck>` which satisfies this.
 * Test mocks implement the same shape with a plain EventEmitter.
 * @internal
 */
export interface ClientTelemetryStream {
	write(msg: TelemetryBatch): boolean;
	end(): void;
	on(event: "data", listener: (ack: TelemetryAck) => void): unknown;
	on(event: "end", listener: () => void): unknown;
	on(event: "error", listener: (err: Error) => void): unknown;
	on(event: string, listener: (...args: never[]) => void): unknown;
}

/**
 * Minimal client shape. Production wires a real `TelemetryClient` via the
 * adapter inside `service-bridge.ts`. Test mocks implement this directly.
 * @internal
 */
export interface TelemetryClientLike {
	openStream(): ClientTelemetryStream;
}

/** @internal */
export function adaptTelemetryClient(
	client: TelemetryClient,
): TelemetryClientLike {
	return {
		openStream(): ClientTelemetryStream {
			const stream = client.report() as ClientDuplexStream<
				TelemetryBatch,
				TelemetryAck
			>;
			return stream as unknown as ClientTelemetryStream;
		},
	};
}

/**
 * Observability hook. Fires when the SDK learns about dropped telemetry — either
 * from the runtime (`serverDrops` rises) or from the local ring overflow
 * (`ringDrops` rises). Lets the host surface a metric/log so backpressure is not
 * silent (C4). @public — см. ./README.md
 */
export type DropObserver = (info: {
	serverDrops: number;
	ringDrops: number;
	backpressureLevel: number;
}) => void;

/** @public — см. ./README.md */
export interface TelemetryTransportOptions {
	client: TelemetryClientLike;
	ring: TelemetryRing;
	/** Periodic flush interval in ms. Default 250ms. */
	flushIntervalMs?: number;
	/** Max items per batch (per kind). Default 256. */
	maxBatchItems?: number;
	/** Reconnect backoff ladder in ms. Default [1000, 5000, 15000, 30000]. */
	reconnectDelays?: number[];
	/** Called when server-side or ring drop counts rise. Optional. */
	onDrop?: DropObserver;
}

const DEFAULT_FLUSH_INTERVAL_MS = 250;
const DEFAULT_MAX_BATCH_ITEMS = 256;
const DEFAULT_RECONNECT_DELAYS = [1_000, 5_000, 15_000, 30_000];

/**
 * TelemetryTransport owns the lifecycle of the Telemetry.Report bidi stream:
 *
 * - Periodically peeks the ring and writes TelemetryBatch frames, tracking the
 *   peeked items as in-flight.
 * - Commits (releases) in-flight items only when the runtime acks — at-least-once.
 * - On `error`/`end` from the stream — drops the in-flight marker (items stay in
 *   the ring) and reopens with an exponential backoff.
 * - Reconnect backoff resets on the FIRST successful ack, not on openStream.
 * - On `drainReason` ack — flushes once more, closes the local end gracefully.
 *
 * @public — см. ./README.md
 */
export class TelemetryTransport {
	private readonly client: TelemetryClientLike;
	private readonly ring: TelemetryRing;
	private readonly flushIntervalMs: number;
	private readonly maxBatchItems: number;
	private readonly reconnectDelays: number[];
	private readonly onDrop?: DropObserver;

	private stream: ClientTelemetryStream | null = null;
	private flushTimer: ReturnType<typeof setInterval> | null = null;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	private backpressureLevel = 0;
	private draining = false;
	private stopped = false;
	private reconnectAttempt = 0;
	// Items written on the current stream but not yet released. Committed on the
	// next ack; left in the ring (uncommitted) if the stream dies first.
	private inflight: RingItem[] = [];
	// Ids currently in-flight, so repeated flushes before an ack do not re-write
	// the same items (peek leaves them in the ring).
	private inflightIds = new Set<number>();
	// Last drop counts we reported via onDrop, to fire only on increase.
	private lastServerDrops = 0;
	private lastRingDrops = 0;

	constructor(opts: TelemetryTransportOptions) {
		this.client = opts.client;
		this.ring = opts.ring;
		this.flushIntervalMs = opts.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
		this.maxBatchItems = opts.maxBatchItems ?? DEFAULT_MAX_BATCH_ITEMS;
		this.reconnectDelays = opts.reconnectDelays ?? DEFAULT_RECONNECT_DELAYS;
		this.onDrop = opts.onDrop;
	}

	async start(): Promise<void> {
		this.stopped = false;
		this.openStream();
		this.flushTimer = setInterval(() => {
			void this.flushNow();
		}, this.flushIntervalMs);
	}

	async stop(): Promise<void> {
		this.stopped = true;
		if (this.flushTimer) {
			clearInterval(this.flushTimer);
			this.flushTimer = null;
		}
		if (this.reconnectTimer) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = null;
		}
		if (this.stream) {
			try {
				// Best-effort final flush so in-flight ops reach the runtime on shutdown.
				this.writeBatchToStream();
				this.stream.end();
			} catch {
				// Stream already torn down — nothing to do.
			}
			this.stream = null;
		}
	}

	/**
	 * Force an immediate flush. Exposed for tests; production also calls it
	 * periodically via the flush timer. The runtime's backpressure level is
	 * advisory only — we never pause the flusher, because pausing while producers
	 * keep pushing causes oldest-drop of START frames in the ring (worse than
	 * sending). The runtime sheds load on its side via the windowed level.
	 */
	async flushNow(): Promise<void> {
		if (this.stopped) return;
		if (!this.stream) return;
		this.writeBatchToStream();
	}

	// writeBatchToStream peeks the ring, writes one TelemetryBatch per non-empty
	// kind, and records the peeked items as in-flight (released on the next ack).
	// Peek does NOT remove from the ring, so a stream death before the ack leaves
	// the items in place for the next stream (at-least-once).
	private writeBatchToStream(): void {
		if (!this.stream) return;
		// Peek leaves items in the ring, so a repeated flush before an ack sees the
		// same items again — skip the ones already on the wire (in-flight) so we do
		// not double-write them every tick.
		const items = this.ring
			.peek(this.maxBatchItems)
			.filter((it) => !this.inflightIds.has(it.id));
		if (items.length === 0) return;
		const byKind = groupByKind(items);
		if (byKind.ops.length > 0) {
			this.stream.write({ ops: OpBatch.fromPartial({ items: byKind.ops }) });
		}
		if (byKind.logs.length > 0) {
			this.stream.write({ logs: LogBatch.fromPartial({ items: byKind.logs }) });
		}
		if (byKind.metrics.length > 0) {
			this.stream.write({
				metrics: MetricBatch.fromPartial({ items: byKind.metrics }),
			});
		}
		if (byKind.payloads.length > 0) {
			this.stream.write({
				payloads: PayloadBatch.fromPartial({ items: byKind.payloads }),
			});
		}
		// Track everything written on this stream as in-flight. The next ack
		// confirms the runtime processed (or at least received) it.
		for (const it of items) this.inflightIds.add(it.id);
		this.inflight.push(...items);
	}

	private openStream(): void {
		if (this.stopped) return;
		const stream = this.client.openStream();
		this.stream = stream;
		stream.on("data", (ack: TelemetryAck) => {
			this.handleAck(ack);
		});
		stream.on("error", () => {
			this.handleStreamGone();
		});
		stream.on("end", () => {
			this.handleStreamGone();
		});
	}

	private handleAck(ack: TelemetryAck): void {
		// First successful ack proves the stream is healthy — reset the backoff
		// ladder so a future disconnect starts from the shortest delay (C2/I12).
		this.reconnectAttempt = 0;

		this.backpressureLevel = ack.backpressureLevel;

		// Release everything written before this ack: the runtime has received it.
		if (this.inflight.length > 0) {
			this.ring.commit(this.inflight);
			this.inflight = [];
			this.inflightIds.clear();
		}

		this.reportDrops(ack);

		if (ack.drainReason && !this.draining) {
			this.draining = true;
			// Final flush of everything still in the ring, then graceful local close.
			// The runtime sends EOF; handleStreamGone reopens after a delay so future
			// ops are not silently dropped if the process keeps running.
			this.writeBatchToStream();
			try {
				this.stream?.end();
			} catch {
				// Already torn down.
			}
		}
	}

	private reportDrops(ack: TelemetryAck): void {
		if (!this.onDrop) return;
		const serverDrops = Number(ack.dropCountServerSide);
		const ringDrops = this.ring.totalDropCount();
		if (serverDrops > this.lastServerDrops || ringDrops > this.lastRingDrops) {
			this.lastServerDrops = serverDrops;
			this.lastRingDrops = ringDrops;
			this.onDrop({
				serverDrops,
				ringDrops,
				backpressureLevel: ack.backpressureLevel,
			});
		}
	}

	private handleStreamGone(): void {
		// In-flight items were never acked — drop the marker, leave them in the
		// ring. They are re-peeked and resent on the next stream (at-least-once).
		this.inflight = [];
		this.inflightIds.clear();
		this.draining = false;
		this.stream = null;
		if (this.stopped) return;
		const delay =
			this.reconnectDelays[
				Math.min(this.reconnectAttempt, this.reconnectDelays.length - 1)
			] ??
			this.reconnectDelays[this.reconnectDelays.length - 1] ??
			1000;
		this.reconnectAttempt++;
		if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = null;
			if (this.stopped) return;
			this.openStream();
		}, delay);
	}
}

interface Grouped {
	ops: OpReport[];
	logs: Log[];
	metrics: MetricPoint[];
	payloads: PayloadAttachment[];
}

function groupByKind(items: RingItem[]): Grouped {
	const out: Grouped = { ops: [], logs: [], metrics: [], payloads: [] };
	for (const it of items) {
		if (it.kind === "ops") out.ops.push(it.message as OpReport);
		else if (it.kind === "logs") out.logs.push(it.message as Log);
		else if (it.kind === "metrics") out.metrics.push(it.message as MetricPoint);
		else if (it.kind === "payloads")
			out.payloads.push(it.message as PayloadAttachment);
	}
	return out;
}
