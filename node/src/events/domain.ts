// @public — см. ./README.md

import { StateError } from "../errors";
import type {
	EventHandlerFn,
	EventHandlerOpts,
	Registry,
} from "../registry/registry";
import type { SchemaSpec } from "../serde/serializer";
import type { Publisher, PublishOpts } from "./publisher";

// EventDomain — namespace `sb.event`: declare published events, subscribe to
// event patterns, publish.
//
// A publisher declares its events with define(name, spec); a subscriber never
// does — it passes its own schema to handle() and is not listed as a
// publisher of anything.
export class EventDomain {
	constructor(
		private readonly registry: Registry,
		private readonly getPublisher: () => Publisher | null,
	) {}

	/** Declares an event this service publishes and its payload schema. */
	define(name: string, spec: SchemaSpec): void {
		this.registry._handle.publishEvent(name, spec);
	}

	/**
	 * Subscribes to an event name or AMQP pattern. The runtime routes (ADR-0002)
	 * and evaluates `opts.filter`; the handler gets the payload decoded with
	 * `opts.schema`, or the raw bytes when no schema is given. Handlers must be
	 * idempotent: delivery is at-least-once.
	 */
	handle(
		pattern: string,
		fn: EventHandlerFn,
		opts: EventHandlerOpts = {},
	): void {
		this.registry._handle.event(pattern, fn, opts);
	}

	/**
	 * Publishes one event and resolves once the runtime has stored it. While
	 * the runtime is unreachable the event waits in memory and is retried until
	 * `publishTimeoutMs`; see PublishOpts.fireAndForget for the variant that
	 * does not wait.
	 */
	publish<T = unknown>(
		name: string,
		payload: T,
		opts?: PublishOpts,
	): Promise<{ eventId: string }> {
		const publisher = this.getPublisher();
		if (!publisher)
			return Promise.reject(
				new StateError(
					"events: publish before start() — call sb.start() first",
				),
			);
		return publisher.publish(name, payload, opts);
	}
}
