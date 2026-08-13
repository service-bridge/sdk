// ring.ts — per-kind byte-budgeted ring buffer for typed telemetry messages.
// Oldest-drop on overflow. Defaults: ops=256KiB, logs=64KiB, metrics=16KiB.
// Stores decoded proto messages (not bytes): the transport feeds them straight
// into a TelemetryBatch and grpc-js serializes once at write — no decode round
// trip. Byte budget uses a cheap per-kind size estimate, never a full encode.
// @public — см. ./README.md

import type {
	Log,
	MetricPoint,
	OpReport,
	PayloadAttachment,
} from "../pb/servicebridge/v1/telemetry";
import { MetricsAggregator } from "./metrics";

export type RingKind = "ops" | "logs" | "metrics" | "payloads";

// RingMessage is the typed message stored for each kind.
// @public — см. ./README.md
export type RingMessage = {
	ops: OpReport;
	logs: Log;
	metrics: MetricPoint;
	payloads: PayloadAttachment;
};

// RingItem is one buffered, typed telemetry message ready for transport.
// @public — см. ./README.md
export interface RingItem<K extends RingKind = RingKind> {
	id: number;
	kind: K;
	message: RingMessage[K];
	bytes: number;
}

// RingBudgets overrides the per-kind byte budget. Any omitted kind keeps its
// default. @public — см. ./README.md
export type RingBudgets = Partial<Record<RingKind, number>>;

// Per-kind byte budgets. The ops budget holds the dense USER.SUBOP step-span
// emission a workflow run produces between 250ms flush ticks — a 4KiB budget
// (≈13 frames) silently dropped step spans once per-step spans landed, so the
// default is sized for ≥800 typical op frames.
const DEFAULT_KIND_BUDGETS: Record<RingKind, number> = {
	ops: 256 * 1024,
	logs: 64 * 1024,
	metrics: 16 * 1024,
	payloads: 256 * 1024,
};

let _nextId = 1;

// estimateSize returns a cheap byte-cost estimate for memory accounting. It
// never serializes the message — the transport does that once at write. The
// estimate covers the variable-length fields that dominate memory; a fixed base
// covers the rest. Accuracy is not required: this only bounds buffer growth.
function estimateSize(kind: RingKind, msg: RingMessage[RingKind]): number {
	const base = 64;
	switch (kind) {
		case "ops": {
			const m = msg as OpReport;
			return (
				base +
				m.traceId.length +
				m.opId.length +
				m.parentOpId.length +
				m.subject.length +
				m.peerServiceId.length +
				m.businessKey.length +
				m.statusMessage.length +
				byteLen(m.metaJson) +
				byteLen(m.attrsJson)
			);
		}
		case "logs": {
			const m = msg as Log;
			return base + m.message.length + byteLen(m.fieldsJson);
		}
		case "metrics": {
			const m = msg as MetricPoint;
			let size = base + m.name.length + byteLen(m.bucketsJson);
			for (const [k, v] of Object.entries(m.labels))
				size += k.length + v.length;
			return size;
		}
		case "payloads": {
			const m = msg as PayloadAttachment;
			return base + byteLen(m.bytes) + m.contractHash.length;
		}
	}
}

function byteLen(b: Uint8Array | undefined): number {
	return b ? b.byteLength : 0;
}

// Dead slots tolerated before a repack. Keeps small rings from repacking on
// every removal while bounding the wasted array span to ~2x the live count.
const COMPACT_SLACK = 64;

// KindRing stores its live items in the half-open span [head, buf.length) of a
// plain array. Removal nulls a slot and advances an index instead of splicing
// or rebuilding the array, so both eviction and ack cost stay independent of
// ring depth — the default ops budget holds ~2600 items, and an ack that
// rebuilds the array pays (and allocates) 2600 slots four times a second.
// The dead prefix and any holes left by a partial ack are reclaimed by an
// amortized repack.
class KindRing<K extends RingKind> {
	private readonly budget: number;
	private buf: (RingItem<K> | undefined)[] = [];
	private head = 0;
	private liveCount = 0;
	private usedBytes = 0;
	dropCount = 0;
	readonly kind: K;

	constructor(kind: K, budget: number) {
		this.kind = kind;
		this.budget = budget;
	}

	push(message: RingMessage[K]): void {
		const bytes = estimateSize(this.kind, message);
		if (bytes > this.budget) {
			// Single item exceeds budget — drop immediately.
			this.dropCount++;
			return;
		}
		while (this.usedBytes + bytes > this.budget && this.liveCount > 0) {
			this.dropOldest();
		}
		this.buf.push({ id: _nextId++, kind: this.kind, message, bytes });
		this.liveCount++;
		this.usedBytes += bytes;
	}

	// peek returns up to maxItems from the head WITHOUT removing them. Items stay
	// in the ring until commit() releases them — this is the at-least-once
	// contract: a peeked batch stays in the ring (oldest-first) and is re-peeked
	// on the next stream if the runtime never acks it.
	peek(maxItems: number): RingItem<K>[] {
		const out: RingItem<K>[] = [];
		if (maxItems <= 0) return out;
		for (let i = this.head; i < this.buf.length; i++) {
			const item = this.buf[i];
			if (item === undefined) continue;
			out.push(item);
			if (out.length === maxItems) break;
		}
		return out;
	}

	// commit removes items whose ids are in the acked set, releasing the bytes.
	commit(ackedIds: Set<number>): void {
		if (ackedIds.size === 0 || this.liveCount === 0) return;
		let removed = 0;
		// peek hands out oldest-first, so an ack almost always covers a head
		// prefix. Walking the head first and stopping once the whole set is
		// accounted for keeps the common case O(|ack|) instead of O(ring).
		while (this.head < this.buf.length && removed < ackedIds.size) {
			const item = this.buf[this.head];
			if (item === undefined) {
				this.head++;
				continue;
			}
			if (!ackedIds.has(item.id)) break;
			this.release(this.head);
			removed++;
		}
		if (removed < ackedIds.size) {
			for (let i = this.head; i < this.buf.length; i++) {
				const item = this.buf[i];
				if (item === undefined || !ackedIds.has(item.id)) continue;
				this.release(i);
				removed++;
				if (removed === ackedIds.size) break;
			}
		}
		this.advanceHead();
		this.maybeCompact();
	}

	get size(): number {
		return this.liveCount;
	}

	get bytes(): number {
		return this.usedBytes;
	}

	private dropOldest(): void {
		this.advanceHead();
		this.release(this.head);
		this.dropCount++;
		this.advanceHead();
		this.maybeCompact();
	}

	private release(index: number): void {
		const item = this.buf[index] as RingItem<K>;
		this.buf[index] = undefined;
		this.liveCount--;
		this.usedBytes -= item.bytes;
	}

	private advanceHead(): void {
		while (this.head < this.buf.length && this.buf[this.head] === undefined) {
			this.head++;
		}
	}

	// maybeCompact repacks the live items to the front once dead slots outnumber
	// live ones. Each repack costs O(buf) but needs at least `liveCount` removals
	// to become due again, so removal stays amortized O(1).
	private maybeCompact(): void {
		const dead = this.buf.length - this.liveCount;
		if (dead <= COMPACT_SLACK || dead <= this.liveCount) return;
		const packed: (RingItem<K> | undefined)[] = [];
		for (let i = this.head; i < this.buf.length; i++) {
			const item = this.buf[i];
			if (item !== undefined) packed.push(item);
		}
		this.buf = packed;
		this.head = 0;
	}
}

// TelemetryRing holds one KindRing per kind and exposes a unified peek/commit
// interface. The transport peeks a batch, writes it, and commits on ack.
// @public — см. ./README.md
export class TelemetryRing {
	private readonly rings: {
		ops: KindRing<"ops">;
		logs: KindRing<"logs">;
		metrics: KindRing<"metrics">;
		payloads: KindRing<"payloads">;
	};

	// Metric state is accumulated per series here and materialised into the
	// metrics ring one point per series per aggregation window. The ring owns the
	// aggregator but never drains it: the window boundary belongs to whoever
	// drives the flush cycle (the transport calls `metrics.flush()` once per
	// cycle, so a cycle's points land in one batch).
	readonly metrics = new MetricsAggregator(this);

	constructor(budgets?: RingBudgets) {
		this.rings = {
			ops: new KindRing("ops", budgets?.ops ?? DEFAULT_KIND_BUDGETS.ops),
			logs: new KindRing("logs", budgets?.logs ?? DEFAULT_KIND_BUDGETS.logs),
			metrics: new KindRing(
				"metrics",
				budgets?.metrics ?? DEFAULT_KIND_BUDGETS.metrics,
			),
			payloads: new KindRing(
				"payloads",
				budgets?.payloads ?? DEFAULT_KIND_BUDGETS.payloads,
			),
		};
	}

	push<K extends RingKind>(kind: K, message: RingMessage[K]): void {
		(this.rings[kind] as KindRing<K>).push(message);
	}

	// peek returns up to maxPerKind items from each kind without removing them.
	peek(maxPerKind = 100): RingItem[] {
		const out: RingItem[] = [];
		for (const kind of ["ops", "logs", "metrics", "payloads"] as RingKind[]) {
			out.push(...(this.rings[kind] as KindRing<RingKind>).peek(maxPerKind));
		}
		return out;
	}

	// commit releases the acked items from each kind ring.
	commit(items: RingItem[]): void {
		if (items.length === 0) return;
		const byKind: Record<RingKind, Set<number>> = {
			ops: new Set(),
			logs: new Set(),
			metrics: new Set(),
			payloads: new Set(),
		};
		for (const it of items) byKind[it.kind].add(it.id);
		for (const kind of ["ops", "logs", "metrics", "payloads"] as RingKind[]) {
			(this.rings[kind] as KindRing<RingKind>).commit(byKind[kind]);
		}
	}

	dropCount(kind: RingKind): number {
		return this.rings[kind].dropCount;
	}

	totalDropCount(): number {
		return (
			this.rings.ops.dropCount +
			this.rings.logs.dropCount +
			this.rings.metrics.dropCount +
			this.rings.payloads.dropCount
		);
	}

	size(kind: RingKind): number {
		return this.rings[kind].size;
	}

	bytes(kind: RingKind): number {
		return this.rings[kind].bytes;
	}
}
