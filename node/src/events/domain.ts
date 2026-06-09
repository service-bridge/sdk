// @public — см. ./README.md

import type { Registry } from "../registry/registry";
import type { SchemaSpec } from "../serde/serializer";
import type { Publisher, PublishOpts } from "./publisher";

// EventDomain — namespace `sb.event` for declaring published events,
// subscribing to event patterns, and emitting events.
//
// Schema source is the same `SchemaSpec` used by `sb.rpc.handle` — either a
// `.proto` file (ProtoFileSpec) or a `.schema.json` file with explicit
// `fieldNumber` per property (JsonSchemaFileSpec). Inline JSON Schema is no
// longer accepted (ADR-0002). The Protobuf encoder also runs `type.verify()`
// on publish, so payload errors throw before reaching the outbox.
export class EventDomain {
	constructor(
		private readonly registry: Registry,
		private readonly getPublisher: () => Publisher | null,
	) {}

	define(name: string, spec?: SchemaSpec): void {
		this.registry._handle.publishEvent(name, spec);
	}

	handle(
		pattern: string,
		fn: (payload: unknown) => Promise<void> | void,
	): void {
		this.registry._handle.event(pattern, fn);
	}

	async publish<T = unknown>(
		name: string,
		payload: T,
		opts?: PublishOpts,
	): Promise<{ eventId: string }> {
		const publisher = this.getPublisher();
		if (!publisher) {
			throw new Error(
				"ServiceBridge: events publisher not ready — call start() before sb.event.publish()",
			);
		}
		return publisher.publish(name, payload, opts);
	}
}
