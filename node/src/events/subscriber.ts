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
import type { SchemaPair } from "../serde/serializer";
import type { Logger } from "./publisher";

// Fixed reconnect ladder — deterministic for tests; saturates at the last
// rung. Wildcards and rare-event hooks do not belong here, only timing.
const RECONNECT_LADDER_MS = [1000, 5000, 15000, 30000, 60000] as const;

function nextDelay(attempt: number): number {
	const idx = Math.min(Math.max(attempt, 0), RECONNECT_LADDER_MS.length - 1);
	return RECONNECT_LADDER_MS[idx]!;
}

// @internal
interface SubscriberIdentity {
	serviceId: string;
	instanceId: string;
}

// EventHandler is the registered handler for one event name. Dispatch is by
// exact name match — wildcards are server-side only (ADR-0002).
// @internal
export interface EventHandler {
	pattern: string;
	fn: (payload: unknown) => Promise<void> | void;
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
	handlers: () => EventHandler[];
	maxInFlight?: number;
	logger?: Logger;
	sb?: ServiceBridge;
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

	private _stopped = false;
	private _attempt = 0;
	private _timer: ReturnType<typeof setTimeout> | null = null;
	// biome-ignore lint/suspicious/noExplicitAny: grpc stream type is opaque
	private _stream: any | null = null;

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
	}

	start(): void {
		if (this._stopped) return;
		this.connect();
	}

	async stop(): Promise<void> {
		this._stopped = true;
		if (this._timer !== null) {
			clearTimeout(this._timer);
			this._timer = null;
		}
		if (this._stream) {
			try {
				this._stream.cancel?.();
				this._stream.end?.();
			} catch {
				// ignore close errors
			}
			this._stream = null;
		}
	}

	private connect(): void {
		if (this._stopped) return;

		const id = this.deps.identity();
		if (!id) {
			// Identity not yet available — schedule reconnect.
			const delay = nextDelay(++this._attempt);
			this.scheduleReconnect(delay);
			return;
		}

		const stream = this.deps.rpcClient.subscribe();
		this._stream = stream;

		// Send SubscribeInit as the first message.
		stream.write(
			SubscribeClientMessageFns.create({
				init: SubscribeInit.create({
					subscriberServiceId: id.serviceId,
					subscriberInstanceId: id.instanceId,
					maxInFlight: this.maxInFlight,
				}),
			}),
		);

		stream.on(
			"data",
			(msg: {
				delivery?: {
					deliveryId: string;
					envelope?: {
						id: string;
						name: string;
						payload: Uint8Array;
						partitionKey?: string;
						xSbTrace?: string;
					};
					attempt: number;
				};
			}) => {
				if (msg.delivery) {
					const delivery = msg.delivery;
					const key = delivery.envelope?.partitionKey ?? "";
					const xSbTrace = delivery.envelope?.xSbTrace ?? "";
					const work = () =>
						this.runWithTrace(xSbTrace, () =>
							this.handleDelivery(stream, delivery),
						);
					if (key === "") {
						// No partition → parallel processing OK.
						void work();
						return;
					}
					// Chain onto the partition's queue so the next handler waits
					// for the previous one's full handler→ack cycle to complete.
					const prev = this.partitionQueues.get(key) ?? Promise.resolve();
					const next = prev.then(work, work);
					this.partitionQueues.set(key, next);
					void next.finally(() => {
						if (this.partitionQueues.get(key) === next) {
							this.partitionQueues.delete(key);
						}
					});
				}
			},
		);

		stream.on("error", (err: Error) => {
			this.logger.warn("events: subscriber: stream error", err.message);
			this._stream = null;
			if (!this._stopped) {
				const delay = nextDelay(++this._attempt);
				this.scheduleReconnect(delay);
			}
		});

		stream.on("end", () => {
			this._stream = null;
			if (!this._stopped) {
				const delay = nextDelay(++this._attempt);
				this.scheduleReconnect(delay);
			}
		});
	}

	private scheduleReconnect(delayMs: number): void {
		if (this._stopped) return;
		this._timer = setTimeout(() => {
			this._timer = null;
			this.connect();
		}, delayMs);
	}

	private async handleDelivery(
		// biome-ignore lint/suspicious/noExplicitAny: grpc stream write
		stream: any,
		delivery: {
			deliveryId: string;
			envelope?: { id: string; name: string; payload: Uint8Array };
			attempt: number;
		},
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
		const handlers = this.deps.handlers().filter((h) => h.pattern === name);
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
				await handler.fn(decoded);
			} catch (err) {
				this.sendNack(stream, deliveryId, String(err), eventId);
				return;
			}
		}

		this.sendAck(stream, deliveryId, eventId);

		// Successful delivery — reset attempt counter.
		this._attempt = 0;
	}

	// biome-ignore lint/suspicious/noExplicitAny: grpc stream write
	private sendAck(stream: any, deliveryId: string, eventId?: string): void {
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
		// biome-ignore lint/suspicious/noExplicitAny: grpc stream write
		stream: any,
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
