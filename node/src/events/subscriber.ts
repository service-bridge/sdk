import type { ServiceBridge } from "../connection/service-bridge";
import type {
	EventsClient,
	SubscribeClientMessage,
} from "../pb/servicebridge/v1/events";
import {
	Ack,
	Nack,
	SubscribeClientMessage as SubscribeClientMessageFns,
	SubscribeInit,
} from "../pb/servicebridge/v1/events";
import type { EventHandlerFn } from "../registry/registry";
import { StreamSupervisor } from "../registry/stream-supervisor";
import type { SchemaPair } from "../serde/serializer";
import type { ReconnectDelayOptions } from "../utils/reconnect-ladder";
import type { Logger } from "./publisher";

// EventStream is the bidi Subscribe call. Typed from the generated client so
// the ack/nack writes stay on the same object the supervisor owns.
type EventStream = ReturnType<EventsClient["subscribe"]>;

// InboundDelivery is the only server frame the subscriber acts on.
interface InboundDelivery {
	deliveryId: string;
	envelope?: {
		id: string;
		name: string;
		payload: Uint8Array;
		partitionKey?: string;
		xSbTrace?: string;
	};
	attempt: number;
}

// @internal
interface SubscriberIdentity {
	serviceId: string;
	instanceId: string;
}

// SchemaIndex for subscriber — maps event name to its schema pair.
// @internal
export interface SubscriberSchemaIndex {
	get(name: string): { contractHash: string; pair: SchemaPair } | undefined;
}

const DEFAULT_MAX_IN_FLIGHT = 32;

// @public — см. ./README.md
export interface SubscriberDeps {
	rpcClient: EventsClient;
	schemaIndex: SubscriberSchemaIndex;
	identity: () => SubscriberIdentity | null;
	// handlers returns the in-process fan-out set for one exact event name.
	// Lookup, not a scan: the registry keeps the bucket indexed by pattern, so a
	// delivery costs a Map hit instead of rebuilding the whole handler list.
	handlers: (pattern: string) => readonly EventHandlerFn[];
	maxInFlight?: number;
	logger?: Logger;
	sb?: ServiceBridge;
	// reconnectOpts pins the backoff ladder/jitter; tests inject a short
	// deterministic ladder so reconnect behaviour is observable in milliseconds.
	// @internal
	reconnectOpts?: ReconnectDelayOptions;
	// runWithTrace runs the handler inside an AsyncLocalStorage trace context
	// derived from envelope.xSbTrace so nested RPC/event calls inherit the trace.
	// Mandatory: a missing hook would silently drop trace propagation into the
	// delivered handler.
	// @internal
	runWithTrace: (xSbTrace: string, fn: () => Promise<void>) => Promise<void>;
}

// Subscriber opens a long-lived bidi Subscribe stream and dispatches inbound
// EventDelivery messages to registered handlers by exact event name. Routing
// + dedup live on the server (ADR-0002); handlers must be idempotent.
// @public — см. ./README.md
export class Subscriber {
	private readonly deps: SubscriberDeps;
	private readonly logger: Logger;
	private readonly maxInFlight: number;
	private readonly runWithTrace: (
		xSbTrace: string,
		fn: () => Promise<void>,
	) => Promise<void>;

	private readonly supervisor: StreamSupervisor<
		EventStream,
		{ delivery?: InboundDelivery }
	>;

	// Per-partition serial queues. Events that share a partition_key must reach
	// their handlers in publication order — the server enforces «one in_flight
	// per partition» but the gRPC stream "data" listener fires async, so two
	// deliveries can arrive in quick succession and race on handler scheduling.
	// We serialize by chaining a Promise per partition key. Empty partition_key
	// stays parallel (no FIFO requirement). Keys with no pending work are
	// dropped from the map after their chain drains.
	private partitionQueues = new Map<string, Promise<void>>();

	constructor(deps: SubscriberDeps) {
		this.deps = deps;
		this.maxInFlight = deps.maxInFlight ?? DEFAULT_MAX_IN_FLIGHT;
		this.logger = deps.logger ?? { warn: console.warn, error: console.error };
		this.runWithTrace = deps.runWithTrace;
		this.supervisor = new StreamSupervisor({
			open: () => this.openStream(),
			onData: (msg, stream) => this.handleFrame(msg, stream),
			onError: (err) =>
				this.logger.warn("events: subscriber: stream error", err.message),
			reconnectOpts: deps.reconnectOpts,
		});
	}

	start(): void {
		this.supervisor.start();
	}

	async stop(): Promise<void> {
		this.supervisor.stop();
	}

	// openStream opens the bidi Subscribe call and sends SubscribeInit as its
	// first frame. Null while identity is missing — the supervisor retries on
	// the ladder.
	private openStream(): EventStream | null {
		const id = this.deps.identity();
		if (!id) return null;

		const stream = this.deps.rpcClient.subscribe();
		stream.write(
			SubscribeClientMessageFns.create({
				init: SubscribeInit.create({
					subscriberServiceId: id.serviceId,
					subscriberInstanceId: id.instanceId,
					maxInFlight: this.maxInFlight,
				}),
			}),
		);
		return stream;
	}

	private handleFrame(
		msg: { delivery?: InboundDelivery },
		stream: EventStream,
	): void {
		const delivery = msg.delivery;
		if (!delivery) return;
		const key = delivery.envelope?.partitionKey ?? "";
		const xSbTrace = delivery.envelope?.xSbTrace ?? "";
		const work = () =>
			this.runWithTrace(xSbTrace, () => this.handleDelivery(stream, delivery));
		if (key === "") {
			// No partition → parallel processing OK.
			void work();
			return;
		}
		// Chain onto the partition's queue so the next handler waits for the
		// previous one's full handler→ack cycle to complete.
		const prev = this.partitionQueues.get(key) ?? Promise.resolve();
		const next = prev.then(work, work);
		this.partitionQueues.set(key, next);
		void next.finally(() => {
			if (this.partitionQueues.get(key) === next) {
				this.partitionQueues.delete(key);
			}
		});
	}

	private async handleDelivery(
		stream: EventStream,
		delivery: InboundDelivery,
	): Promise<void> {
		const { deliveryId, envelope } = delivery;
		if (!envelope) {
			this.sendNack(stream, deliveryId, "missing envelope");
			return;
		}

		const { id: eventId, name, payload } = envelope;
		const schemaEntry = this.deps.schemaIndex.get(name);
		if (!schemaEntry) {
			this.sendNack(stream, deliveryId, "no_schema", eventId);
			return;
		}

		// Dispatch by exact name — server is the single source of truth for
		// routing (ADR-0002). Handlers must be idempotent.
		const handlers = this.deps.handlers(name);
		if (handlers.length === 0) {
			this.sendAck(stream, deliveryId, eventId);
			return;
		}

		// Decode payload once.
		let decoded: unknown;
		try {
			decoded = schemaEntry.pair.input.decode(payload);
		} catch (decodeErr) {
			this.sendNack(
				stream,
				deliveryId,
				`decode_error: ${String(decodeErr)}`,
				eventId,
			);
			return;
		}

		for (const handler of handlers) {
			try {
				await handler(decoded);
			} catch (err) {
				this.sendNack(stream, deliveryId, String(err), eventId);
				return;
			}
		}

		this.sendAck(stream, deliveryId, eventId);
	}

	private sendAck(
		stream: EventStream,
		deliveryId: string,
		eventId?: string,
	): void {
		try {
			const msg: SubscribeClientMessage = {
				ack: Ack.create({
					deliveryId,
					eventId: eventId ? Buffer.from(eventId) : Buffer.alloc(0),
				}),
			};
			stream.write(msg);
		} catch (err) {
			this.logger.warn("events: subscriber: ack write failed", String(err));
		}
	}

	private sendNack(
		stream: EventStream,
		deliveryId: string,
		errorMessage: string,
		eventId?: string,
	): void {
		try {
			const msg: SubscribeClientMessage = {
				nack: Nack.create({
					deliveryId,
					errorMessage,
					eventId: eventId ? Buffer.from(eventId) : Buffer.alloc(0),
				}),
			};
			stream.write(msg);
		} catch (err) {
			this.logger.warn("events: subscriber: nack write failed", String(err));
		}
	}
}
