import { randomUUID } from "node:crypto";
import type { PublishOpts } from "../events/publisher";

// PublishedEventRecord captures one outbound `event.publish()` made through
// the harness, in the order it happened. `opts` is present only when the
// caller passed it.
// @public — см. ./README.md
export interface PublishedEventRecord {
	name: string;
	payload: unknown;
	opts?: PublishOpts;
}

// EventDeliveryResult mirrors the Ack/Nack outcome src/events/subscriber.ts
// sends back to the runtime for one delivery.
// @public — см. ./README.md
export type EventDeliveryResult =
	| { outcome: "ack" }
	| { outcome: "nack"; reason: string };

// EventHandlerFn is the function shape accepted by `sb.event.handle()`
// (src/events/domain.ts) — a decoded payload in, no return value used.
// @public — см. ./README.md
export type EventHandlerFn = (payload: unknown) => Promise<void> | void;

interface RegisteredHandler {
	pattern: string;
	fn: EventHandlerFn;
}

// TestEventDomain doubles `sb.event` for unit tests. `handle()` registers
// subscriptions with the exact same signature as production
// (src/events/domain.ts `EventDomain.handle`). `deliver()` simulates one
// inbound delivery attempt against those registrations and returns the same
// ack/nack outcome the real Subscriber sends back to the runtime
// (src/events/subscriber.ts `handleDelivery`): no handler registered for the
// exact name → ack (routing/matching is server-side, ADR-0002); a handler
// throws → nack with `String(error)`, remaining handlers for that delivery
// are skipped; every handler succeeds → ack. `publish()` doubles the
// outbound direction: it records the call instead of writing to the local
// outbox and returns a freshly generated eventId.
// @public — см. ./README.md
export class TestEventDomain {
	private readonly handlers: RegisteredHandler[] = [];
	private readonly _published: PublishedEventRecord[] = [];

	handle(pattern: string, fn: EventHandlerFn): void {
		this.handlers.push({ pattern, fn });
	}

	async deliver(name: string, payload: unknown): Promise<EventDeliveryResult> {
		const matched = this.handlers.filter((h) => h.pattern === name);
		for (const handler of matched) {
			try {
				await handler.fn(payload);
			} catch (err) {
				return { outcome: "nack", reason: String(err) };
			}
		}
		return { outcome: "ack" };
	}

	async publish<T = unknown>(
		name: string,
		payload: T,
		opts?: PublishOpts,
	): Promise<{ eventId: string }> {
		const record: PublishedEventRecord = { name, payload };
		if (opts !== undefined) record.opts = opts;
		this._published.push(record);
		return { eventId: randomUUID() };
	}

	published(): readonly PublishedEventRecord[] {
		return this._published;
	}

	reset(): void {
		this.handlers.length = 0;
		this._published.length = 0;
	}
}
