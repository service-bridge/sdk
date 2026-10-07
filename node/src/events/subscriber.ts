import type { Logger } from "../logger";
import type {
	EventDelivery,
	EventsClient,
	SubscribeClientMessage,
	SubscribeServerMessage,
} from "../pb/servicebridge/v1/events";
import { Ack, Nack, SubscribeInit } from "../pb/servicebridge/v1/events";
import type { SubscriptionEntry } from "../registry/registry";
import { StreamSupervisor } from "../registry/stream-supervisor";
import type { ReconnectDelayOptions } from "../utils/reconnect-ladder";
import { Semaphore } from "../utils/semaphore";

// EventStream is the bidi Subscribe call.
type EventStream = ReturnType<EventsClient["subscribe"]>;

/** Default concurrently handled deliveries; same in the Go SDK. */
export const DEFAULT_EVENTS_MAX_IN_FLIGHT = 32;

// @internal
export interface SubscriberDeps {
	// The events channel; null until the bridge has one.
	client: () => EventsClient | null;
	identity: () => { serviceId: string; instanceId: string } | null;
	// This process's handler for one matched pattern.
	subscription: (pattern: string) => SubscriptionEntry | undefined;
	maxInFlight: number;
	logger: Logger;
	// Runs the handler inside the publisher's trace context so nested calls
	// join the same trace.
	runWithTrace: (xSbTrace: string, fn: () => Promise<void>) => Promise<void>;
	// Test hooks: pin the reconnect ladder / observe scheduled delays.
	reconnectOpts?: ReconnectDelayOptions;
	onSchedule?: (delayMs: number) => void;
}

// Subscriber holds the Subscribe stream open and runs the handlers of every
// pattern a delivery matched (EventDelivery.matched_patterns). Nothing is
// matched locally: routing and filters are the runtime's (ADR-0002, decision
// 7). Handlers must be idempotent: delivery is at-least-once.
//
// @internal — см. ./README.md
export class Subscriber {
	private readonly supervisor: StreamSupervisor<
		EventStream,
		SubscribeServerMessage
	>;
	// Per-partition serial queues: deliveries sharing a key reach handlers in
	// order even though stream frames are dispatched asynchronously.
	private partitionQueues = new Map<string, Promise<void>>();
	private readonly slots: Semaphore;
	private streamController = new AbortController();
	private draining = false;
	private inflight = 0;
	private idleWaiters: (() => void)[] = [];

	constructor(private readonly d: SubscriberDeps) {
		this.slots = new Semaphore(d.maxInFlight, 0);
		this.supervisor = new StreamSupervisor({
			open: () => this.openStream(),
			onData: (msg, stream) => this.handleFrame(msg, stream),
			onDisconnect: () => this.streamController.abort(),
			onError: (err) =>
				d.logger.warn("events: subscribe stream failed", {
					error: err.message,
				}),
			reconnectOpts: d.reconnectOpts,
			onSchedule: d.onSchedule,
		});
	}

	start(): void {
		this.supervisor.start();
	}

	/** Reopens the stream at once (new session identity, recovered channel). */
	restart(): void {
		this.supervisor.restart();
	}

	/**
	 * Stops taking new deliveries (left unanswered: the runtime redelivers them
	 * once this instance disconnects) and waits for the handlers already running, up to
	 * timeoutMs. The stream stays open meanwhile so their acks still reach the
	 * runtime.
	 */
	async drain(timeoutMs: number): Promise<void> {
		this.draining = true;
		if (this.inflight === 0) return;
		await new Promise<void>((resolve) => {
			const timer = setTimeout(done, timeoutMs);
			const self = this;
			function done() {
				clearTimeout(timer);
				self.idleWaiters = self.idleWaiters.filter((w) => w !== done);
				resolve();
			}
			this.idleWaiters.push(done);
		});
	}

	stop(): void {
		this.draining = true;
		this.streamController.abort();
		this.supervisor.stop();
	}

	private openStream(): EventStream | null {
		const client = this.d.client();
		const id = this.d.identity();
		if (!client || !id) return null;
		this.streamController = new AbortController();
		const stream = client.subscribe();
		stream.write({
			init: SubscribeInit.create({
				subscriberServiceId: id.serviceId,
				subscriberInstanceId: id.instanceId,
				maxInFlight: this.d.maxInFlight,
			}),
		});
		return stream;
	}

	private handleFrame(msg: SubscribeServerMessage, stream: EventStream): void {
		const delivery = msg.delivery;
		if (!delivery) return;
		// A draining instance leaves new deliveries unanswered: they return to
		// the runtime when it disconnects, without spending an attempt.
		if (this.draining) return;
		const signal = this.streamController.signal;
		const key = delivery.envelope?.partitionKey ?? "";
		const admitted = this.slots.acquire(signal);
		void admitted.catch(() => {});
		this.inflight++;
		const work = async () => {
			try {
				try {
					await admitted;
				} catch {
					this.nack(stream, delivery, "local_overload");
					return;
				}
				try {
					if (!signal.aborted)
						await this.d.runWithTrace(delivery.envelope?.xSbTrace ?? "", () =>
							this.handleDelivery(stream, delivery, signal),
						);
				} finally {
					this.slots.release();
				}
			} finally {
				this.done();
			}
		};
		if (key === "") {
			void work();
			return;
		}
		const prev = this.partitionQueues.get(key) ?? Promise.resolve();
		const next = prev.then(work, work);
		this.partitionQueues.set(key, next);
		void next.finally(() => {
			if (this.partitionQueues.get(key) === next)
				this.partitionQueues.delete(key);
		});
	}

	private done(): void {
		this.inflight--;
		if (this.inflight > 0) return;
		const waiters = this.idleWaiters;
		this.idleWaiters = [];
		for (const w of waiters) w();
	}

	private async handleDelivery(
		stream: EventStream,
		delivery: EventDelivery,
		signal: AbortSignal,
	): Promise<void> {
		const envelope = delivery.envelope;
		if (!envelope) {
			this.nack(stream, delivery, "missing envelope");
			return;
		}
		// During a rolling deploy another instance of this service may subscribe
		// to a pattern this one does not have: run what this instance has; with
		// nothing to run, nack so the runtime redelivers — likely elsewhere.
		const entries = delivery.matchedPatterns
			.map((p) => this.d.subscription(p))
			.filter((e): e is SubscriptionEntry => e !== undefined);
		if (entries.length === 0) {
			this.nack(
				stream,
				delivery,
				`no handler for matched patterns [${delivery.matchedPatterns.join(", ")}]`,
			);
			return;
		}
		const ctx = {
			eventId: envelope.id,
			eventName: envelope.name,
			attempt: delivery.attempt,
			deliveryId: delivery.deliveryId,
			leaseToken: delivery.leaseToken,
			partitionKey: envelope.partitionKey,
			headers: envelope.headers,
			occurredAtMs: Number(envelope.occurredAtUnixMs),
			signal,
		};
		for (const entry of entries) {
			if (signal.aborted) return;
			let payload: unknown = envelope.payload;
			if (entry.schemaPair) {
				try {
					payload = entry.schemaPair.input.decode(envelope.payload);
				} catch (err) {
					this.nack(
						stream,
						delivery,
						`decode for pattern ${entry.pattern}: ${(err as Error).message}`,
					);
					return;
				}
			}
			try {
				await entry.fn(payload, ctx);
			} catch (err) {
				this.nack(
					stream,
					delivery,
					err instanceof Error ? err.message : String(err),
				);
				return;
			}
		}
		if (!signal.aborted) this.ack(stream, delivery);
	}

	private ack(stream: EventStream, delivery: EventDelivery): void {
		this.write(stream, {
			ack: Ack.create({
				deliveryId: delivery.deliveryId,
				leaseToken: delivery.leaseToken,
				eventId: Buffer.from(delivery.envelope?.id ?? ""),
			}),
		});
	}

	private nack(
		stream: EventStream,
		delivery: EventDelivery,
		reason: string,
	): void {
		this.write(stream, {
			nack: Nack.create({
				deliveryId: delivery.deliveryId,
				leaseToken: delivery.leaseToken,
				errorMessage: reason,
				eventId: Buffer.from(delivery.envelope?.id ?? ""),
			}),
		});
	}

	private write(stream: EventStream, msg: SubscribeClientMessage): void {
		try {
			stream.write(msg);
		} catch (err) {
			this.d.logger.warn("events: ack/nack write failed", {
				error: (err as Error).message,
			});
		}
	}
}
