// harness.ts — unit-test a service's handlers without a runtime.
//
// The harness owns a real ServiceBridge started against an in-memory runtime.
// Handlers are registered through the ordinary API (sb.rpc.handle,
// sb.event.handle, sb.event.define, sb.client/useSchema) and every path goes
// through production code: requests and responses are encoded and decoded
// with the declared schemas, handler errors come back in the caller's form
// (HandlerError with the business code), publishes go through the real
// Publisher, deliveries through the real Subscriber. Same scenarios as the Go
// SDK's sbtest (see ./README.md, parity table).
//
// @public — см. ./README.md

import { EventEmitter } from "node:events";
import type { Identity } from "../connection/service-bridge";
import {
	ServiceBridge,
	type ServiceBridgeOptions,
} from "../connection/service-bridge";
import {
	ConfigurationError,
	codeForGrpcStatus,
	HandlerError,
	ServiceBridgeError,
	ValidationError,
} from "../errors";
import { Subscriber } from "../events/subscriber";
import { silentLogger } from "../logger";
import type {
	EventEnvelope,
	EventsClient,
	PublishRequest,
	PublishResponse,
	SubscribeClientMessage,
} from "../pb/servicebridge/v1/events";
import { PublishStatus } from "../pb/servicebridge/v1/events";
import type { Handle } from "../registry/registry";
import type { CallOpts, RpcCaller, SchemaRegistry } from "../rpc/client";
import type { RpcHandlerContext, UnaryResult } from "../rpc/dispatch-port";

/** Identity the harness's in-memory session reports. */
export const TEST_IDENTITY: Identity = {
	sessionId: "test-session",
	serviceId: "00000000-0000-7000-8000-000000000001",
	serviceName: "test-service",
	instanceId: "test-instance",
};

/** Options of one invoke(). */
export interface InvokeOpts {
	caller?: { serviceId: string; instanceId: string };
	requestId?: string;
	idempotencyKey?: string;
	signal?: AbortSignal;
	/** Absolute deadline, unix ms. */
	deadline?: number;
}

/**
 * Answers an outbound call. Receives the decoded request; throw a
 * HandlerError to answer with a business code, any other error answers
 * "INTERNAL".
 */
export type Responder<Req = unknown, Res = unknown> = (
	req: Req,
	call: CallRecord,
) => Res | Promise<Res>;

/** One outbound call the code under test made. */
export interface CallRecord {
	service: string;
	method: string;
	payload: unknown;
	opts: CallOpts;
}

/** One event the code under test published (acknowledged by the in-memory runtime). */
export interface PublishedRecord {
	id: string;
	name: string;
	payload: unknown;
	payloadJson: unknown;
	partitionKey: string;
	idempotencyKey: string;
	headers: Record<string, string>;
	occurredAtMs: number;
}

/** Options of one deliver(). */
export interface DeliverOpts {
	/** Overrides the runtime's pattern matching. */
	matchedPatterns?: string[];
	attempt?: number;
	partitionKey?: string;
	headers?: Record<string, string>;
}

/** Ack or nack of one delivery, and what it matched. */
export interface DeliveryResult {
	acked: boolean;
	reason: string;
	matchedPatterns: string[];
}

/** @public — см. ./README.md */
export interface TestHarness {
	/** The bridge under test: register handlers on it before start(). */
	sb: ServiceBridge;
	/** Seals declarations and loads schemas. */
	start(): Promise<void>;
	invoke<Req = unknown, Res = unknown>(
		method: string,
		req: Req,
		opts?: InvokeOpts,
	): Promise<Res>;
	invokeStream<Req = unknown, Chunk = unknown>(
		method: string,
		req: Req,
		opts?: InvokeOpts,
	): Promise<Chunk[]>;
	respond<Req = unknown, Res = unknown>(
		service: string,
		method: string,
		fn: Responder<Req, Res>,
	): void;
	respondStream<Req = unknown, Chunk = unknown>(
		service: string,
		method: string,
		fn: (req: Req, call: CallRecord) => AsyncIterable<Chunk> | Iterable<Chunk>,
	): void;
	calls(): readonly CallRecord[];
	published(): readonly PublishedRecord[];
	deliver(
		name: string,
		payload: unknown,
		opts?: DeliverOpts,
	): Promise<DeliveryResult>;
	/** Forgets recorded calls and publishes; registrations and responders stay. */
	reset(): void;
	stop(): Promise<void>;
}

/**
 * matchPattern applies the runtime's routing rules: "*" matches exactly one
 * segment, "#" zero or more.
 */
export function matchPattern(pattern: string, name: string): boolean {
	const p = pattern.split(".");
	const n = name.split(".");
	const go = (i: number, j: number): boolean => {
		if (i === p.length) return j === n.length;
		if (p[i] === "#") return go(i + 1, j) || (j < n.length && go(i, j + 1));
		return j < n.length && (p[i] === "*" || p[i] === n[j]) && go(i + 1, j + 1);
	};
	return go(0, 0);
}

function handlerError(err: unknown): HandlerError {
	if (err instanceof HandlerError) return err;
	return new HandlerError(
		"INTERNAL",
		err instanceof Error ? err.message : String(err),
		{ cause: err },
	);
}

function refusal(result: UnaryResult): ServiceBridgeError {
	const code = codeForGrpcStatus(result.status ?? 13);
	return new ServiceBridgeError(code, result.errorMessage ?? "refused");
}

/** Builds a harness around a fresh, never-connected ServiceBridge. */
export function createTestHarness(
	options: Pick<ServiceBridgeOptions, "callDefaults" | "publishTimeoutMs"> = {},
): TestHarness {
	const sb = new ServiceBridge("in-memory:0", "in-memory", {
		...options,
		advertise: false,
		logger: silentLogger,
	});
	const calls: CallRecord[] = [];
	const published: PublishedRecord[] = [];
	const responders = new Map<string, Responder>();
	const streamResponders = new Map<
		string,
		(
			req: unknown,
			call: CallRecord,
		) => AsyncIterable<unknown> | Iterable<unknown>
	>();
	let internals: { handle: Handle; schemas: SchemaRegistry } | null = null;
	let subscriber: Subscriber | null = null;
	const deliveryStream = new EventEmitter();
	const outcomes = new Map<
		string,
		(r: { acked: boolean; reason: string }) => void
	>();
	let deliveries = 0;

	const ready = () => {
		if (!internals)
			throw new ConfigurationError("testing: call harness.start() first");
		return internals;
	};

	const schemaFor = (service: string, method: string) => {
		const schema = ready().schemas.get(service, method);
		if (!schema)
			throw new ConfigurationError(
				`rpc: no schema for ${service}/${method} — declare it with sb.client() or sb.useSchema() before start()`,
			);
		return schema;
	};

	const rpc: RpcCaller = {
		async call<Req, Res>(
			service: string,
			method: string,
			payload: Req,
			opts?: CallOpts,
		): Promise<Res> {
			const schema = schemaFor(service, method);
			// Round-trip through the schema both ways, as the wire would.
			const request = schema.pair.input.decode(
				schema.pair.input.encode(payload),
			);
			const record: CallRecord = {
				service,
				method,
				payload: request,
				opts: opts ?? {},
			};
			calls.push(record);
			const fn = responders.get(`${service}/${method}`);
			if (!fn)
				throw new ServiceBridgeError(
					"NO_LIVE_INSTANCE",
					`testing: no responder for ${service}/${method} — call harness.respond("${service}", "${method}", fn)`,
				);
			let response: unknown;
			try {
				response = await fn(request, record);
			} catch (err) {
				throw handlerError(err);
			}
			return schema.pair.output.decode(
				schema.pair.output.encode(response),
			) as Res;
		},
		async *stream<Req, Chunk>(
			service: string,
			method: string,
			payload: Req,
			opts?: CallOpts,
		): AsyncIterable<Chunk> {
			const schema = schemaFor(service, method);
			const request = schema.pair.input.decode(
				schema.pair.input.encode(payload),
			);
			const record: CallRecord = {
				service,
				method,
				payload: request,
				opts: opts ?? {},
			};
			calls.push(record);
			const fn = streamResponders.get(`${service}/${method}`);
			if (!fn)
				throw new ServiceBridgeError(
					"NO_LIVE_INSTANCE",
					`testing: no stream responder for ${service}/${method}`,
				);
			try {
				for await (const chunk of fn(request, record))
					yield schema.pair.output.decode(
						schema.pair.output.encode(chunk),
					) as Chunk;
			} catch (err) {
				throw handlerError(err);
			}
		},
	};

	const events = {
		publish(
			req: PublishRequest,
			_md: unknown,
			_opts: unknown,
			cb: (err: Error | null, res?: PublishResponse) => void,
		) {
			for (const env of req.events) published.push(record(env));
			queueMicrotask(() =>
				cb(null, {
					results: req.events.map((e) => ({
						eventId: e.id,
						status: PublishStatus.PUBLISH_STATUS_ACCEPTED,
						message: "",
					})),
				}),
			);
		},
		subscribe() {
			const stream = Object.assign(deliveryStream, {
				write(msg: SubscribeClientMessage) {
					const id = msg.ack?.deliveryId ?? msg.nack?.deliveryId;
					if (!id) return true;
					outcomes.get(id)?.({
						acked: msg.ack !== undefined,
						reason: msg.nack?.errorMessage ?? "",
					});
					return true;
				},
				cancel() {},
				end() {},
			});
			return stream;
		},
	} as unknown as EventsClient;

	const record = (env: EventEnvelope): PublishedRecord => {
		const schema = ready().handle.getPublishedEvent(env.name);
		let payloadJson: unknown = null;
		try {
			payloadJson = JSON.parse(
				Buffer.from(env.payloadJson).toString() || "null",
			);
		} catch {
			payloadJson = null;
		}
		return {
			id: env.id,
			name: env.name,
			payload: schema ? schema.pair.input.decode(env.payload) : env.payload,
			payloadJson,
			partitionKey: env.partitionKey,
			idempotencyKey: env.idempotencyKey,
			headers: env.headers,
			occurredAtMs: Number(env.occurredAtUnixMs),
		};
	};

	const ctxFor = (opts: InvokeOpts): RpcHandlerContext => ({
		signal: opts.signal ?? new AbortController().signal,
		deadline: opts.deadline ?? null,
		requestId: opts.requestId ?? crypto.randomUUID(),
		idempotencyKey: opts.idempotencyKey ?? "",
		caller: opts.caller ?? null,
	});

	const handlerSchema = (method: string) => {
		const entry = ready().handle._entries.find(
			(e) => e.name === method && e.schemaPair,
		);
		return entry?.schemaPair;
	};

	return {
		sb,
		async start() {
			internals = await sb._startInMemory({
				rpc,
				events,
				identity: TEST_IDENTITY,
			});
			if (internals.handle.subscriptionCount() > 0) {
				subscriber = new Subscriber({
					client: () => events,
					identity: () => TEST_IDENTITY,
					subscription: (p) => internals?.handle.subscription(p),
					maxInFlight: 32,
					logger: silentLogger,
					runWithTrace: (_x, fn) => fn(),
				});
				subscriber.start();
			}
		},
		async invoke<Req, Res>(
			method: string,
			req: Req,
			opts: InvokeOpts = {},
		): Promise<Res> {
			const port = ready().handle.asDispatchPort();
			const pair = handlerSchema(method);
			const bytes = pair ? pair.input.encode(req) : new Uint8Array();
			const result = await port.dispatchUnary(method, bytes, ctxFor(opts));
			if (result.status !== undefined) throw refusal(result);
			if (result.errorCode)
				throw new HandlerError(result.errorCode, result.errorMessage ?? "");
			return (pair as NonNullable<typeof pair>).output.decode(
				result.payload ?? new Uint8Array(),
			) as Res;
		},
		async invokeStream<Req, Chunk>(
			method: string,
			req: Req,
			opts: InvokeOpts = {},
		): Promise<Chunk[]> {
			const port = ready().handle.asDispatchPort();
			const pair = handlerSchema(method);
			const bytes = pair ? pair.input.encode(req) : new Uint8Array();
			const out: Chunk[] = [];
			for await (const item of port.dispatchStream(
				method,
				bytes,
				ctxFor(opts),
			)) {
				if (item.status !== undefined) throw refusal(item);
				if (item.errorCode)
					throw new HandlerError(item.errorCode, item.errorMessage ?? "");
				out.push(
					(pair as NonNullable<typeof pair>).output.decode(
						item.payload ?? new Uint8Array(),
					) as Chunk,
				);
			}
			return out;
		},
		respond(service, method, fn) {
			responders.set(`${service}/${method}`, fn as Responder);
		},
		respondStream(service, method, fn) {
			streamResponders.set(
				`${service}/${method}`,
				fn as (req: unknown, call: CallRecord) => AsyncIterable<unknown>,
			);
		},
		calls: () => calls,
		published: () => published,
		async deliver(name, payload, opts = {}) {
			const { handle } = ready();
			if (!subscriber)
				throw new ConfigurationError(
					"testing: no event handler registered — sb.event.handle() before start()",
				);
			const patterns = handle.eventSubscriptions().map((s) => s.pattern);
			const matched =
				opts.matchedPatterns ?? patterns.filter((p) => matchPattern(p, name));
			// The publisher's encoding: the first matched subscription's schema,
			// or raw bytes when the payload already is bytes.
			const pair = matched
				.map((p) => handle.subscription(p)?.schemaPair)
				.find((s) => s !== undefined);
			let bytes: Uint8Array;
			if (payload instanceof Uint8Array) bytes = payload;
			else if (pair) bytes = pair.input.encode(payload);
			else
				throw new ValidationError(
					"testing: deliver() needs a schema on a matched subscription or a Uint8Array payload",
				);
			const deliveryId = `delivery-${++deliveries}`;
			const outcome = new Promise<{ acked: boolean; reason: string }>(
				(resolve) => outcomes.set(deliveryId, resolve),
			);
			deliveryStream.emit("data", {
				delivery: {
					deliveryId,
					attempt: opts.attempt ?? 1,
					leaseToken: `lease-${deliveries}`,
					matchedPatterns: matched,
					envelope: {
						id: crypto.randomUUID(),
						name,
						payload: Buffer.from(bytes),
						payloadJson: Buffer.alloc(0),
						contractHash: "",
						partitionKey: opts.partitionKey ?? "",
						idempotencyKey: "",
						headers: opts.headers ?? {},
						occurredAtUnixMs: Date.now(),
						xSbTrace: "",
					},
				},
			});
			const result = await outcome;
			outcomes.delete(deliveryId);
			return { ...result, matchedPatterns: matched };
		},
		reset() {
			calls.length = 0;
			published.length = 0;
		},
		async stop() {
			subscriber?.stop();
			await sb.stop();
		},
	};
}
