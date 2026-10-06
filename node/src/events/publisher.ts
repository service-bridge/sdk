import { Metadata } from "@grpc/grpc-js";
import {
	AccessDeniedError,
	ServiceBridgeError,
	StateError,
	TimeoutError,
	toServiceBridgeError,
	ValidationError,
} from "../errors";
import type { Logger } from "../logger";
import type {
	EventEnvelope,
	EventsClient,
	PublishResponse,
} from "../pb/servicebridge/v1/events";
import { PublishStatus } from "../pb/servicebridge/v1/events";
import type { SchemaPair } from "../serde/serializer";
import { InvalidEventNameError } from "./errors";
import { uuidv7 } from "./ids";

// EVENT_NAME_RE: dot-separated lowercase segments, no wildcards in a publish.
// @internal
const EVENT_NAME_RE = /^[a-z0-9_-]+(\.[a-z0-9_-]+)*$/;

/** Defaults; the same numbers in the Go SDK (WithPublishTimeout, WithMaxPendingPublishes). */
export const DEFAULT_PUBLISH_TIMEOUT_MS = 30_000;
export const DEFAULT_MAX_PENDING_PUBLISHES = 10_000;

// MAX_BATCH bounds one Publish request.
const MAX_BATCH = 100;
// ATTEMPT_DEADLINE_MS bounds one Publish request on the wire.
const ATTEMPT_DEADLINE_MS = 10_000;
// BACKOFF_MS is the retry ladder for transient failures; the last rung repeats.
const BACKOFF_MS = [100, 250, 500, 1000, 2000, 5000] as const;

/** @public — см. ./README.md */
export interface PublishOpts {
	/** Runtime-side dedup key: a repeat with the same content is a success. */
	idempotencyKey?: string;
	/** FIFO lane: subscribers see events sharing a key in publication order. */
	partitionKey?: string;
	headers?: Record<string, string>;
	/** When the event happened, unix ms. Default: now. */
	occurredAtMs?: number;
	/**
	 * Resolve as soon as the event is queued in memory instead of after the
	 * runtime's acknowledgement. The event is still sent with retries, but it
	 * is lost if the process dies first, and a terminal rejection is only
	 * logged. Use it for events whose loss is acceptable.
	 */
	fireAndForget?: boolean;
}

// SchemaIndex maps a published event name to its contract hash and schema.
// @internal
export interface SchemaIndex {
	get(name: string): { contractHash: string; pair: SchemaPair } | undefined;
}

// @internal
export interface PublisherDeps {
	// The events channel; null before start().
	client: () => EventsClient | null;
	schemaIndex: SchemaIndex;
	logger: Logger;
	timeoutMs: number;
	maxPending: number;
	// X-SB-Trace of the publishing scope; "" outside of one.
	xSbTraceFn: () => string;
	onPolicyViolation: (v: {
		declaration: string;
		value: string;
		denySide: string;
		reason: string;
	}) => void;
	// Test hooks.
	now?: () => number;
}

interface Pending {
	envelope: EventEnvelope;
	deadlineAt: number;
	// Whether a request carrying this event ever left the process: after that
	// a timeout cannot say whether the runtime stored it.
	sent: boolean;
	inFlight: boolean;
	fireAndForget: boolean;
	resolve: (r: { eventId: string }) => void;
	reject: (err: ServiceBridgeError) => void;
}

// Publisher sends events to the runtime and resolves each publish on the
// runtime's acknowledgement (the event is in Postgres). While the runtime is
// unreachable events wait in a bounded in-memory queue and are retried with
// backoff until their deadline. Ordering: one request in flight, at most one
// event per partition key in a request — events sharing a key reach the
// runtime in publication order whatever fails in between.
//
// @internal — см. ./README.md
export class Publisher {
	private readonly queue: Pending[] = [];
	private readonly now: () => number;
	private running = false;
	private closed = false;
	private attempt = 0;
	private wake: (() => void) | null = null;
	private idle: Promise<void> = Promise.resolve();

	constructor(private readonly d: PublisherDeps) {
		this.now = d.now ?? Date.now;
	}

	pending(): number {
		return this.queue.length;
	}

	publish(
		name: string,
		payload: unknown,
		opts: PublishOpts = {},
	): Promise<{ eventId: string }> {
		if (this.closed)
			return Promise.reject(new StateError("events: publisher is stopped"));
		if (!EVENT_NAME_RE.test(name))
			return Promise.reject(new InvalidEventNameError(name));
		const entry = this.d.schemaIndex.get(name);
		if (!entry)
			return Promise.reject(
				new StateError(
					`events: no schema for event "${name}" — declare it with sb.event.define("${name}", schema) before start()`,
				),
			);
		if (this.queue.length >= this.d.maxPending)
			return Promise.reject(
				new ServiceBridgeError(
					"QUEUE_FULL",
					`events: ${this.queue.length} publishes are waiting for the runtime (maxPendingPublishes=${this.d.maxPending})`,
				),
			);

		let encoded: Uint8Array;
		try {
			encoded = entry.pair.input.encode(payload);
		} catch (err) {
			return Promise.reject(
				new ValidationError(
					`events: payload of "${name}" does not match its schema — ${(err as Error).message}`,
					{ cause: err },
				),
			);
		}
		// JSON view of the same payload: subscription filters and workflow
		// wait_event conditions are evaluated on it.
		let payloadJson: Buffer;
		try {
			payloadJson = Buffer.from(JSON.stringify(payload ?? null));
		} catch {
			payloadJson = Buffer.alloc(0);
		}
		const envelope: EventEnvelope = {
			id: uuidv7(),
			name,
			payload: Buffer.from(encoded),
			payloadJson,
			contractHash: entry.contractHash,
			partitionKey: opts.partitionKey ?? "",
			idempotencyKey: opts.idempotencyKey ?? "",
			headers: opts.headers ?? {},
			occurredAtUnixMs: opts.occurredAtMs ?? this.now(),
			xSbTrace: this.d.xSbTraceFn(),
		};

		const result = new Promise<{ eventId: string }>((resolve, reject) => {
			this.queue.push({
				envelope,
				deadlineAt: this.now() + this.d.timeoutMs,
				sent: false,
				inFlight: false,
				fireAndForget: opts.fireAndForget === true,
				resolve,
				reject,
			});
		});
		this.run();
		if (opts.fireAndForget) {
			result.catch(() => {});
			return Promise.resolve({ eventId: envelope.id });
		}
		return result;
	}

	/** Retry now: the connection came back. */
	kick(): void {
		this.attempt = 0;
		const wake = this.wake;
		this.wake = null;
		wake?.();
		this.run();
	}

	/**
	 * Sends what is queued until the queue is empty or `deadlineMs` passes,
	 * then rejects the rest with CONNECTION "client stopped".
	 */
	async close(deadlineMs: number): Promise<void> {
		this.closed = true;
		this.kick();
		await Promise.race([
			this.idle,
			new Promise<void>((r) => setTimeout(r, deadlineMs).unref?.()),
		]);
		for (const p of this.queue.splice(0)) {
			this.settle(
				p,
				new ServiceBridgeError(
					"CONNECTION",
					`events: publish of "${p.envelope.name}" abandoned — client stopped before the runtime acknowledged it`,
				),
			);
		}
		this.wake?.();
	}

	private run(): void {
		if (this.running) return;
		this.running = true;
		// Start on the next microtask so publishes issued in the same tick share
		// one request.
		this.idle = Promise.resolve().then(() => this.loop());
	}

	private async loop(): Promise<void> {
		try {
			while (this.queue.length > 0) {
				this.expire();
				const batch = this.nextBatch();
				if (batch.length === 0) break;
				const client = this.d.client();
				if (!client) {
					await this.backoff();
					continue;
				}
				for (const p of batch) {
					p.inFlight = true;
					p.sent = true;
				}
				let response: PublishResponse | null = null;
				let failure: unknown = null;
				try {
					response = await send(client, batch);
				} catch (err) {
					failure = err;
				}
				for (const p of batch) p.inFlight = false;
				if (failure !== null) {
					this.d.logger.warn("events: publish failed, retrying", {
						events: batch.length,
						error: failure instanceof Error ? failure.message : String(failure),
					});
					await this.backoff();
					continue;
				}
				const transient = this.apply(batch, response);
				if (transient) await this.backoff();
				else this.attempt = 0;
			}
		} finally {
			// Cleared in the same turn as the last queue check: a publish whose
			// caller resumed on an acknowledgement above must find the loop
			// stopped and start it again, never a loop about to exit.
			this.running = false;
		}
	}

	// nextBatch takes queued events in order, skipping an event whose partition
	// key already has an earlier event in this batch (and therefore every later
	// event of that key too).
	private nextBatch(): Pending[] {
		const batch: Pending[] = [];
		const keys = new Set<string>();
		for (const p of this.queue) {
			if (batch.length >= MAX_BATCH) break;
			const key = p.envelope.partitionKey;
			if (key) {
				if (keys.has(key)) continue;
				keys.add(key);
			}
			batch.push(p);
		}
		return batch;
	}

	// apply settles every event the runtime answered for. results[i] answers
	// events[i] (a duplicate reports the ORIGINAL event's id, so the id cannot
	// be the correlation key). Returns true when some event got a transient
	// answer and stays queued.
	private apply(batch: Pending[], response: PublishResponse | null): boolean {
		const results = response?.results ?? [];
		let transient = false;
		for (const [i, p] of batch.entries()) {
			const r = results.length === batch.length ? results[i] : undefined;
			switch (r?.status) {
				case PublishStatus.PUBLISH_STATUS_ACCEPTED:
					this.settle(p, null, p.envelope.id);
					break;
				case PublishStatus.PUBLISH_STATUS_REJECTED_DUPLICATE:
					// Already stored with the same content — by an earlier attempt of
					// this very publish or under the same idempotency key.
					this.settle(p, null, r.eventId || p.envelope.id);
					break;
				case PublishStatus.PUBLISH_STATUS_REJECTED_CONFLICT:
					this.settle(
						p,
						new ServiceBridgeError(
							"CONFLICT",
							`events: "${p.envelope.name}" conflicts with an event already stored under the same id or idempotency key${r.message ? ` — ${r.message}` : ""}`,
						),
					);
					break;
				case PublishStatus.PUBLISH_STATUS_REJECTED_INVALID_NAME:
					this.settle(p, new InvalidEventNameError(p.envelope.name));
					break;
				case PublishStatus.PUBLISH_STATUS_REJECTED_FORBIDDEN: {
					const reason = r.message || "event.publish denied by policy";
					this.d.onPolicyViolation({
						declaration: "event.publish",
						value: p.envelope.name,
						denySide: "self_egress",
						reason,
					});
					this.settle(
						p,
						new AccessDeniedError(
							`events: publish of "${p.envelope.name}" denied — ${reason}`,
						),
					);
					break;
				}
				default:
					// UNSPECIFIED or no answer: the runtime could not decide (database
					// unavailable, rate limit, a concurrent publish holding the key).
					// The same envelope is retried; a repeat after an actual write is
					// answered DUPLICATE.
					transient = true;
			}
		}
		return transient;
	}

	// expire rejects every queued event past its deadline that is not on the
	// wire right now.
	private expire(): void {
		const now = this.now();
		for (let i = 0; i < this.queue.length; ) {
			const p = this.queue[i];
			if (p && !p.inFlight && p.deadlineAt <= now) {
				this.settle(
					p,
					new TimeoutError(
						p.sent
							? `events: publish of "${p.envelope.name}" timed out — outcome unknown; a repeat with the same idempotency key is safe`
							: `events: publish of "${p.envelope.name}" timed out — the runtime was unreachable, the event was not sent`,
					),
				);
				continue;
			}
			i++;
		}
	}

	private settle(
		p: Pending,
		err: ServiceBridgeError | null,
		eventId = p.envelope.id,
	): void {
		const index = this.queue.indexOf(p);
		if (index >= 0) this.queue.splice(index, 1);
		if (err) {
			if (p.fireAndForget)
				this.d.logger.warn("events: fire-and-forget publish lost", {
					event: p.envelope.name,
					eventId: p.envelope.id,
					error: err.message,
				});
			p.reject(err);
			return;
		}
		p.resolve({ eventId });
	}

	// backoff waits one rung of the ladder, cut short by kick() or by the
	// earliest queued deadline.
	private async backoff(): Promise<void> {
		const rung =
			BACKOFF_MS[Math.min(this.attempt, BACKOFF_MS.length - 1)] ?? 5000;
		this.attempt++;
		let wait: number = rung;
		for (const p of this.queue)
			wait = Math.min(wait, Math.max(0, p.deadlineAt - this.now()));
		if (this.closed) wait = Math.min(wait, 100);
		await new Promise<void>((resolve) => {
			const timer = setTimeout(done, wait);
			const self = this;
			function done() {
				clearTimeout(timer);
				if (self.wake === done) self.wake = null;
				resolve();
			}
			this.wake = done;
		});
	}
}

function send(
	client: EventsClient,
	batch: Pending[],
): Promise<PublishResponse> {
	return new Promise((resolve, reject) => {
		client.publish(
			{ events: batch.map((p) => p.envelope) },
			new Metadata(),
			{ deadline: new Date(Date.now() + ATTEMPT_DEADLINE_MS) },
			(err, res) => {
				if (err) reject(toServiceBridgeError("events: publish", err));
				else resolve(res);
			},
		);
	});
}
