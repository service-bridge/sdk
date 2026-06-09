import type { Identity, ServiceBridge } from "../connection/service-bridge";
import type { EventsClient } from "../pb/servicebridge/v1/events";
import type { SchemaPair } from "../serde/serializer";
import type { Storage } from "../sqlite/storage";
import { InvalidEventNameError, OutboxFullError } from "./errors";
import { uuidv7 } from "./ids";

// EVENT_NAME_RE validates that an event name consists of dot-separated
// lowercase alphanumeric/underscore/hyphen segments with no empty segments.
// @internal
const EVENT_NAME_RE = /^[a-z0-9_-]+(\.[a-z0-9_-]+)*$/;

// @public — см. ./README.md
export interface PublishOpts {
	idempotencyKey?: string;
	partitionKey?: string;
	fireAndForget?: boolean;
	headers?: Record<string, string>;
	occurredAtMs?: number;
}

// SchemaIndex maps event name to its contract hash and schema pair.
// @internal
export interface SchemaIndex {
	get(name: string): { contractHash: string; pair: SchemaPair } | undefined;
}

// DrainerHandle exposes the kick signal for edge-triggered wakeup.
// @internal
export interface DrainerHandle {
	kick(): void;
}

// Logger abstraction — compatible with a subset of the slog API shape.
// @internal
export interface Logger {
	warn(msg: string, ...args: unknown[]): void;
	error(msg: string, ...args: unknown[]): void;
}

// @internal
export interface PublisherDeps {
	storage: Storage;
	rpcClient: EventsClient;
	schemaIndex: SchemaIndex;
	drainer: DrainerHandle;
	identity: () => Identity | null;
	maxOutboxRows: number;
	logger: Logger;
	sb?: ServiceBridge;
	// xSbTraceFn returns the current X-SB-Trace header to attach to outbound
	// envelopes, read from the active AsyncLocalStorage trace scope. Returns ""
	// when no trace context is active, in which case the runtime mints a fresh
	// root trace on ingest (T-017). Mandatory: a missing hook would silently
	// break trace propagation across the publish boundary.
	// @internal
	xSbTraceFn: () => string;
}

// Publisher enqueues events into the local SQLite outbox (durable path) or
// sends them directly to the runtime (fireAndForget path).
// @public — см. ./README.md
export class Publisher {
	private readonly deps: PublisherDeps;
	private readonly xSbTraceFn: () => string;

	constructor(deps: PublisherDeps) {
		this.deps = deps;
		this.xSbTraceFn = deps.xSbTraceFn;
	}

	// publish enqueues or directly sends an event.
	// Throws InvalidEventNameError if name is malformed.
	// Throws Error if schema not registered.
	// Throws OutboxFullError if outbox cap exceeded.
	async publish(
		name: string,
		payload: unknown,
		opts?: PublishOpts,
	): Promise<{ eventId: string }> {
		if (!EVENT_NAME_RE.test(name)) {
			throw new InvalidEventNameError(name);
		}

		const entry = this.deps.schemaIndex.get(name);
		if (entry === undefined) {
			throw new Error(
				`events: no schema registered for event "${name}" — call sb.publish("${name}", schema) before start()`,
			);
		}

		const encoded = entry.pair.input.encode(payload);
		// JSON view of the same payload — runtime treats it as opaque bytes
		// (ADR-0013) but uses it for workflow wait_event JSON-path filters.
		// Falls back to empty bytes when payload is not JSON-serializable.
		let payloadJsonBytes: Buffer;
		try {
			payloadJsonBytes = Buffer.from(JSON.stringify(payload ?? null));
		} catch {
			payloadJsonBytes = Buffer.alloc(0);
		}
		const eventId = uuidv7();
		const occurredAtMs = opts?.occurredAtMs ?? Date.now();
		const xSbTrace = this.xSbTraceFn();

		if (opts?.fireAndForget) {
			const ident = this.deps.identity();
			await new Promise<void>((resolve, reject) => {
				this.deps.rpcClient.publish(
					{
						publisherServiceId: ident?.serviceId ?? "",
						publisherInstanceId: ident?.instanceId ?? "",
						events: [
							{
								id: eventId,
								name,
								payload: Buffer.from(encoded),
								payloadJson: payloadJsonBytes,
								contractHash: entry.contractHash,
								partitionKey: opts?.partitionKey ?? "",
								idempotencyKey: opts?.idempotencyKey ?? "",
								fireAndForget: true,
								headers: opts?.headers ?? {},
								occurredAtUnixMs: occurredAtMs,
								xSbTrace,
							},
						],
					},
					(err) => {
						if (err) reject(err);
						else resolve();
					},
				);
			});
			return { eventId };
		}

		// Durable path: insert into outbox inside a transaction.
		// Cap check + INSERT are atomic to prevent races.
		const { storage, maxOutboxRows } = this.deps;
		const now = Date.now();

		storage.transaction(() => {
			const row = storage
				.prepare("SELECT COUNT(*) AS count FROM event_outbox")
				.get() as { count: number } | null;
			const count = row?.count ?? 0;
			if (count >= maxOutboxRows) {
				throw new OutboxFullError(maxOutboxRows);
			}

			storage
				.prepare(
					`INSERT INTO event_outbox
          (id, name, payload, payload_json, contract_hash, partition_key, idempotency_key,
           fire_and_forget, headers, occurred_at_ms, enqueued_at_ms, status,
           attempts, last_error, next_attempt_at_ms, x_sb_trace)
          VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, 'pending', 0, '', 0, ?)`,
				)
				.run(
					eventId,
					name,
					Buffer.from(encoded),
					payloadJsonBytes,
					entry.contractHash,
					opts?.partitionKey ?? "",
					opts?.idempotencyKey ?? "",
					JSON.stringify(opts?.headers ?? {}),
					occurredAtMs,
					now,
					xSbTrace,
				);
		});

		this.deps.drainer.kick();
		return { eventId };
	}
}
