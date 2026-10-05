// transport.ts — real bidi gRPC Telemetry.Report client.
// Peeks typed messages from the TelemetryRing into TelemetryBatch frames, writes
// them on the long-lived bidi stream, and commits (releases) them only after the
// runtime cumulative sequence acks — ingress acceptance/disposal, not persistence. Reopens the stream on disconnect
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
import {
	type ReconnectDelayOptions,
	reconnectDelay,
} from "../utils/reconnect-ladder";
import type { RingItem, RingKind, TelemetryRing } from "./ring";

/**
 * Minimal stream shape we depend on. The generated gRPC client returns
 * `ClientDuplexStream<TelemetryBatch, TelemetryAck>` which satisfies this.
 * Test mocks implement the same shape with a plain EventEmitter.
 * @internal
 */
export interface ClientTelemetryStream {
	write(msg: TelemetryBatch): boolean;
	end(): void;
	cancel?(): void;
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
	maxInflightItems?: number;
	/**
	 * Reconnect backoff options, shared with the events/job/workflow
	 * subscribers (see ../utils/reconnect-ladder.ts). Default: the shared
	 * RECONNECT_LADDER_MS ladder with ±20% jitter.
	 */
	reconnectOpts?: ReconnectDelayOptions;
	/** Called when server-side or ring drop counts rise. Optional. */
	onDrop?: DropObserver;
}

const DEFAULT_FLUSH_INTERVAL_MS = 250;
const DEFAULT_MAX_BATCH_ITEMS = 256;

/** One in-flight item plus the stream sequence of its batch. */
interface InflightEntry {
	sequence: number;
	item: RingItem;
}

/**
 * TelemetryTransport owns the lifecycle of the Telemetry.Report bidi stream:
 *
 * - Drains the ring into TelemetryBatch frames on every flush tick and on every
 *   ack, tracking written items as in-flight.
 * - Commits (releases) in-flight items only once an ack proves the runtime saw
 *   them for ingress acceptance/disposal.
 * - On `error`/`end` from the stream — drops the in-flight marker (items stay in
 *   the ring) and reopens with an exponential backoff.
 * - Reconnect backoff resets on the FIRST successful ack, not on openStream.
 * - On `drainReason` ack — stops writing and closes the local end gracefully.
 *
 * @public — см. ./README.md
 */
export class TelemetryTransport {
	private readonly client: TelemetryClientLike;
	private readonly ring: TelemetryRing;
	private readonly flushIntervalMs: number;
	private readonly maxBatchItems: number;
	private readonly reconnectOpts?: ReconnectDelayOptions;
	private readonly onDrop?: DropObserver;

	private stream: ClientTelemetryStream | null = null;
	private flushTimer: ReturnType<typeof setInterval> | null = null;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	private backpressureLevel = 0;
	private draining = false;
	private stopped = false;
	private reconnectAttempt = 0;
	// Items written on the current stream but not yet released, each tagged with
	// the stream sequence it was written in. Released once an ack proves the runtime
	// received them; left in the ring (uncommitted) if the stream dies first.
	private inflight: InflightEntry[] = [];
	// Ids currently in-flight, so repeated flushes before an ack do not re-write
	// the same items (peek leaves them in the ring).
	private inflightIds = new Set<number>();
	// Sequence numbers are strictly increasing within one stream.
	private sequence = 0;
	private writeBlocked = false;
	private readonly maxInflightItems: number;
	// Last drop counts we reported via onDrop, to fire only on increase.
	private lastServerDrops = 0;
	private lastRingDrops = 0;

	constructor(opts: TelemetryTransportOptions) {
		this.client = opts.client;
		this.ring = opts.ring;
		this.flushIntervalMs = opts.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
		this.maxBatchItems = opts.maxBatchItems ?? DEFAULT_MAX_BATCH_ITEMS;
		this.maxInflightItems = opts.maxInflightItems ?? 1024;
		this.reconnectOpts = opts.reconnectOpts;
		this.onDrop = opts.onDrop;
	}

	async start(): Promise<void> {
		this.stopped = false;
		this.openStream();
		const timer = setInterval(() => {
			void this.flushNow();
		}, this.flushIntervalMs);
		// The flusher must not keep the process alive on its own.
		if (typeof timer.unref === "function") timer.unref();
		this.flushTimer = timer;
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
				this.pump();
				this.stream.end();
			} catch {
				// Stream already torn down — nothing to do.
			}
			this.stream?.cancel?.();
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
		this.pump();
	}

	// pump materialises the accumulated metric series into the ring, then writes
	// batch after batch until nothing is left un-written. Throughput is bounded by
	// what the ring holds, not by one batch per timer tick: a single batch per
	// 250ms tick caps the SDK at maxBatchItems*4 frames/s regardless of how fast
	// the runtime consumes them. Each pass strictly grows inflightIds, so the loop
	// terminates.
	private pump(): void {
		// One aggregation window per flush cycle. Draining here rather than per
		// written batch keeps a cycle's metric points in one batch instead of
		// splintering them across the batches the loop below emits.
		this.ring.metrics.flush();
		let wrote = true;
		while (
			wrote &&
			!this.writeBlocked &&
			!this.draining &&
			this.inflightIds.size < this.maxInflightItems
		)
			wrote = this.writeBatchToStream();
	}

	// writeBatchToStream selects the next un-written slice of the ring, writes one
	// TelemetryBatch per non-empty kind, and records the items as in-flight. Peek
	// does NOT remove from the ring, so a stream death before confirmation leaves
	// the items in place for the next stream (at-least-once). Returns whether
	// anything was written.
	private writeBatchToStream(): boolean {
		if (!this.stream) return false;
		const items = this.selectNextBatch();
		if (items.length === 0) return false;
		for (const kind of ["ops", "logs", "metrics", "payloads"] as const) {
			if (this.writeBlocked) break;
			const group = items.filter((it) => it.kind === kind);
			if (!group.length) continue;
			const sequence = ++this.sequence;
			for (const item of group) {
				this.inflightIds.add(item.id);
				this.inflight.push({ sequence, item });
			}
			const byKind = groupByKind(group);
			const batch: TelemetryBatch = { sequence };
			if (kind === "ops")
				batch.ops = OpBatch.fromPartial({ items: byKind.ops });
			if (kind === "logs")
				batch.logs = LogBatch.fromPartial({ items: byKind.logs });
			if (kind === "metrics")
				batch.metrics = MetricBatch.fromPartial({ items: byKind.metrics });
			if (kind === "payloads")
				batch.payloads = PayloadBatch.fromPartial({ items: byKind.payloads });
			try {
				this.writeBlocked = !this.stream.write(batch);
			} catch {
				this.handleStreamGone();
				return false;
			}
		}
		return true;
	}

	// selectNextBatch returns up to maxBatchItems per kind that are not already on
	// the wire. It peeks PAST the in-flight prefix: peek() returns the ring head
	// oldest-first and in-flight items sit at that head until they are committed,
	// so a peek capped at maxBatchItems would return in-flight items only and the
	// transport would write nothing at all until the next ack.
	private selectNextBatch(): RingItem[] {
		const peeked = this.ring.peek(this.maxBatchItems + this.inflightIds.size);
		const perKind: Record<RingKind, number> = {
			ops: 0,
			logs: 0,
			metrics: 0,
			payloads: 0,
		};
		const taken: RingItem[] = [];
		for (const it of peeked) {
			if (this.inflightIds.has(it.id)) continue;
			if (perKind[it.kind] >= this.maxBatchItems) continue;
			perKind[it.kind]++;
			taken.push(it);
			if (taken.length >= this.maxInflightItems - this.inflightIds.size) break;
		}
		return taken;
	}

	private openStream(): void {
		if (this.stopped) return;
		const stream = this.client.openStream();
		this.stream = stream;
		this.sequence = 0;
		this.writeBlocked = false;
		stream.on("drain", () => {
			if (this.stream !== stream || this.stopped) return;
			this.writeBlocked = false;
			this.pump();
		});
		stream.on("data", (ack: TelemetryAck) => {
			if (this.stream === stream) this.handleAck(ack);
		});
		stream.on("error", () => {
			if (this.stream === stream) this.handleStreamGone();
		});
		stream.on("end", () => {
			if (this.stream === stream) this.handleStreamGone();
		});
	}

	private handleAck(ack: TelemetryAck): void {
		// First successful ack proves the stream is healthy — reset the backoff
		// ladder so a future disconnect starts from the shortest delay (C2/I12).
		this.reconnectAttempt = 0;

		this.backpressureLevel = ack.backpressureLevel;

		this.releaseConfirmed(ack.acknowledgedSequence);
		this.reportDrops(ack);

		// Credit-based pipelining: an ack is the cheapest signal that the runtime
		// is keeping up, so refill the wire immediately instead of idling until the
		// next timer tick. Costs nothing when the ring is empty.
		if (!this.stopped && !ack.drainReason) this.pump();

		if (ack.drainReason && !this.draining) {
			this.draining = true;
			// Graceful local close after the final flush above. The runtime sends
			// EOF; handleStreamGone reopens after a delay so future ops are not
			// silently dropped if the process keeps running.
			try {
				this.stream?.end();
			} catch {
				// Already torn down.
			}
		}
	}

	// Cumulative sequence acknowledges runtime ingress acceptance/disposal, not
	// a durable database commit. Never infer confirmation from elapsed time.
	private releaseConfirmed(sequence: number): void {
		if (
			!Number.isSafeInteger(sequence) ||
			sequence < 0 ||
			sequence > this.sequence
		)
			return;
		const confirmed = this.inflight
			.filter((entry) => entry.sequence <= sequence)
			.map((entry) => entry.item);
		this.inflight = this.inflight.filter((entry) => entry.sequence > sequence);
		this.ring.commit(confirmed);
		for (const item of confirmed) this.inflightIds.delete(item.id);
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
		this.writeBlocked = false;
		const retired = this.stream;
		this.stream = null;
		retired?.cancel?.();
		if (this.stopped) return;
		const delay = reconnectDelay(this.reconnectAttempt, this.reconnectOpts);
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
