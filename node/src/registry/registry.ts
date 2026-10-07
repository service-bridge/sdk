import {
	PROTOCOL_VERSION,
	SDK_LANGUAGE,
	SDK_VERSION,
} from "../connection/handshake";
import { HandlerError, ValidationError } from "../errors";
import { RouteCollector } from "../http/route";
import type {
	IncomingMethod as PbIncomingMethod,
	OutgoingDep as PbOutgoingDep,
	PublishedEvent as PbPublishedEvent,
	RegisterRequest,
} from "../pb/servicebridge/v1/registry";
import { MethodType } from "../pb/servicebridge/v1/registry";
import type { WorkflowDefinition as PbWorkflowDefinition } from "../pb/servicebridge/v1/workflows";
import type {
	DispatchPort,
	RpcHandlerContext,
	StreamItem,
	UnaryResult,
} from "../rpc/dispatch-port";
import { computeContractHash } from "../serde/contract-hash";
import type { SchemaPair, SchemaSpec } from "../serde/serializer";
import type { CaptureMode } from "../telemetry/payload-capture";

export type { MethodDescriptor } from "../pb/servicebridge/v1/registry";
export { MethodType } from "../pb/servicebridge/v1/registry";

// RpcHandlerOpts accepts a SchemaSpec (.proto file + message names) for both
// input and output. `schema` is required — every RPC handler must declare a
// schema so the dispatcher can decode payloads and the LB can filter by
// contract hash (ADR 0005 / 0012). Declaration-only registry tests use the
// internal `_declareForTests()` path.
export interface RpcHandlerOpts {
	schema: SchemaSpec;
	// captureMode — per-handler override for payload capture ("all"|"errors"|
	// "none"). May only NARROW the runtime-pushed effective mode (privacy
	// ordering none < errors < all), never widen it.
	captureMode?: CaptureMode;
}

export interface ServiceDeps {
	rpc?: string[];
	workflows?: string[];
	http?: string[];
}

// RpcHandlerFn is the function shape accepted by sb.rpc.handle(). Throw a
// HandlerError to answer with a business code; any other error reaches the
// caller as handler code "INTERNAL".
export type RpcHandlerFn<Req = unknown, Res = unknown> = (
	req: Req,
	ctx: RpcHandlerContext,
) => Promise<Res> | Res;

// RpcStreamHandlerFn is the function shape accepted by sb.rpc.handleStream():
// an async generator yielding one chunk at a time. ctx.signal aborts when the
// caller goes away; stop producing then.
export type RpcStreamHandlerFn<Req = unknown, Chunk = unknown> = (
	req: Req,
	ctx: RpcHandlerContext,
) => AsyncIterable<Chunk>;

/** What an event handler knows about the delivery it serves. @public */
export interface EventHandlerContext {
	eventId: string;
	/** Concrete event name the publisher used. */
	eventName: string;
	attempt: number;
	deliveryId: string;
	leaseToken: string;
	partitionKey: string;
	headers: Record<string, string>;
	occurredAtMs: number;
	/** Aborts when the delivery stream breaks or the bridge stops. */
	signal: AbortSignal;
}

export type EventHandlerFn = (
	payload: unknown,
	context: EventHandlerContext,
) => Promise<void> | void;

/**
 * Options of sb.event.handle(). `schema` decodes the payload (without it the
 * handler receives the raw protobuf bytes); `filter` is a Filter Expression
 * the runtime evaluates against the event before delivering it.
 *
 * @public — см. ../events/README.md
 */
export interface EventHandlerOpts {
	schema?: SchemaSpec;
	/** All-equalities filter, e.g. `{ "$.region": "eu", "$.amount": 100 }`. */
	filter?: Record<string, unknown>;
}

/** One subscription of this process: pattern → handler. @internal */
export interface SubscriptionEntry {
	pattern: string;
	filter: string;
	fn: EventHandlerFn;
	schemaPair?: SchemaPair;
}

interface HandlerEntry {
	type: MethodType;
	name: string;
	inputSchemaJson: Buffer | null;
	outputSchemaJson: Buffer | null;
	fn: unknown;
	streaming?: boolean;
	schemaPair?: SchemaPair;
	// contractHashOverride — used by workflow entries (ADR-W-002): the graph
	// fingerprint is computed by WorkflowDomain over the canonical JSON, NOT
	// derived from a SchemaPair. When present, incomingMethods() emits it
	// verbatim into IncomingMethod.contract_hash.
	contractHashOverride?: string;
	captureMode?: CaptureMode;
	// workflow — the structured definition of a METHOD_TYPE_WORKFLOW entry.
	workflow?: PbWorkflowDefinition;
}

// PublishedEntry stores a published event declaration plus its async-loaded
// schema. `schemaPair` and `contractHash` populate after buildSchemaPair
// resolves in finalize(); they remain undefined / "" forever when the event
// was declared without a spec.
interface PublishedEntry {
	name: string;
	inputSchemaJson: Buffer | null;
	contractHash: string;
	schemaPair?: SchemaPair;
	// Reference identity for "same spec" no-op detection on repeated define().
	// Two define(name, spec) calls with the same SchemaSpec object are no-op;
	// distinct SchemaSpec objects throw. Schema equivalence by structure is
	// out of scope — users pass the same `import`ed spec or get the error.
	spec?: SchemaSpec;
}

interface OutgoingEntry {
	serviceName: string;
	methodName: string;
	type: MethodType;
}

// Handle — internal storage for incoming handler entries AND published-event
// declarations. Accessed only through domain classes (RpcDomain, EventDomain,
// WorkflowDomain, JobDomain).
// @internal — см. ./README.md
export class Handle {
	readonly _entries: HandlerEntry[] = [];
	// Published events declared via sb.event.define. Symmetric to _entries —
	// schema loading is async, finalized() must await before serialization.
	readonly _published: PublishedEntry[] = [];

	// Lookup indexes maintained on every registration. `_entries` mixes RPC,
	// EVENT, WORKFLOW, JOB and HTTP rows, and dispatch/publish/delivery are per
	// message: scanning the array there costs O(handlers) on every inbound call.
	// Every mutation funnels through addEntry / publishEvent so the indexes
	// cannot drift from the arrays.
	private readonly rpcByName = new Map<string, HandlerEntry>();
	private readonly publishedByName = new Map<string, PublishedEntry>();
	// Event subscriptions are not published events: a subscriber never lands in
	// `published` (NSDK-08) and carries its own decoding schema.
	private readonly subscriptions = new Map<string, SubscriptionEntry>();

	// Pending registrations awaiting their async SchemaPair to load. Covers
	// rpc / stream handlers AND publishEvent declarations.
	// finalize() resolves all of them before incomingMethods() /
	// publishedEvents() are called.
	private pending: Promise<void>[] = [];

	private addEntry(entry: HandlerEntry): void {
		this._entries.push(entry);
		if (entry.type === MethodType.METHOD_TYPE_RPC) {
			this.rpcByName.set(entry.name, entry);
		}
	}

	// trackPending registers an async schema load. The no-op catch is what keeps
	// a bad .proto from surfacing as an unhandled rejection between handler
	// registration and start(): the real error is re-thrown from finalize(),
	// which is the only place that can report it to the caller.
	private trackPending(load: Promise<void>): void {
		this.pending.push(load);
		load.catch(() => {});
	}

	rpc<Req = unknown, Res = unknown>(
		name: string,
		fn: RpcHandlerFn<Req, Res>,
		opts: RpcHandlerOpts,
	): void {
		this.registerRpc(name, fn, false, opts);
	}

	// stream registers a server-side streaming RPC. The handler is an async
	// generator (or any AsyncIterable). Each yielded value is encoded as a
	// StreamChunk and pushed to the caller. Thrown errors terminate the stream
	// with a final chunk carrying error_code/error_message.
	stream<Req = unknown, Chunk = unknown>(
		name: string,
		fn: RpcStreamHandlerFn<Req, Chunk>,
		opts: RpcHandlerOpts,
	): void {
		this.registerRpc(name, fn, true, opts);
	}

	private registerRpc(
		name: string,
		fn: unknown,
		streaming: boolean,
		opts: RpcHandlerOpts,
	): void {
		if (!name) throw new ValidationError("rpc.handle: method name is empty");
		if (this.rpcByName.has(name))
			throw new ValidationError(
				`rpc.handle: method "${name}" already has a handler`,
			);
		const entry: HandlerEntry = {
			type: MethodType.METHOD_TYPE_RPC,
			name,
			inputSchemaJson: null,
			outputSchemaJson: null,
			fn,
			streaming,
			captureMode: opts.captureMode,
		};
		this.addEntry(entry);

		// For ProtoFileSpec without explicit input/output, propagate the method
		// name so buildSchemaPair can look it up in the .proto service block.
		const spec: SchemaSpec =
			"protoFile" in opts.schema && !opts.schema.method
				? { ...opts.schema, method: name }
				: opts.schema;
		const load = import("../serde/serializer").then(({ buildSchemaPair }) =>
			buildSchemaPair(spec).then((pair) => {
				entry.schemaPair = pair;
				entry.inputSchemaJson = Buffer.from(
					JSON.stringify(pair.input.toJsonSchema()),
				);
				entry.outputSchemaJson = Buffer.from(
					JSON.stringify(pair.output.toJsonSchema()),
				);
			}),
		);
		this.trackPending(load);
	}

	// publishEvent declares a published event (publisher-side). `spec` is the
	// Protobuf schema source — either a .proto file or a .schema.json file with
	// explicit fieldNumber per property. Symmetric to rpc(): the SchemaPair is
	// loaded asynchronously and finalize() awaits it before
	// publishedEvents() reflects the contract hash.
	//
	// Re-declaration with the same SchemaSpec object is no-op. Re-declaration
	// with a different SchemaSpec throws — there must be one canonical schema
	// per (process, event-name).
	publishEvent(name: string, spec: SchemaSpec): void {
		const existing = this.publishedByName.get(name);
		if (existing) {
			if (existing.spec === spec) return; // idempotent re-define with same spec
			throw new ValidationError(
				`event.define: event "${name}" already declared with a different schema spec`,
			);
		}

		const entry: PublishedEntry = {
			name,
			inputSchemaJson: null,
			contractHash: "",
			spec,
		};
		this._published.push(entry);
		this.publishedByName.set(name, entry);

		// For ProtoFileSpec without explicit input/output, propagate the event
		// name so buildSchemaPair can look it up in the .proto service block.
		const resolvedSpec: SchemaSpec =
			"protoFile" in spec && !spec.method ? { ...spec, method: name } : spec;

		const load = Promise.all([
			import("../serde/serializer"),
			import("../serde/contract-hash"),
		]).then(async ([{ buildSchemaPair }, { computeEventContractHash }]) => {
			const pair = await buildSchemaPair(resolvedSpec);
			entry.schemaPair = pair;
			entry.inputSchemaJson = Buffer.from(
				JSON.stringify(pair.input.toJsonSchema()),
			);
			entry.contractHash = computeEventContractHash(pair.input);
		});
		this.trackPending(load);
	}

	// getPublishedEvent — schema lookup for Publisher / Subscriber. Returns the
	// loaded SchemaPair plus contractHash, or undefined when the event was not
	// declared (or declared without a spec, or finalize() not yet called).
	getPublishedEvent(
		name: string,
	): { contractHash: string; pair: SchemaPair } | undefined {
		const entry = this.publishedByName.get(name);
		if (!entry?.schemaPair) return undefined;
		return { contractHash: entry.contractHash, pair: entry.schemaPair };
	}

	// event registers this process's handler for an event pattern (exact name
	// or AMQP wildcard). Routing is the runtime's (ADR-0002): the delivery says
	// which patterns it matched, nothing is matched locally. One handler per
	// pattern: the runtime keys a subscription (and its filter) by pattern.
	event(
		pattern: string,
		fn: EventHandlerFn,
		opts: EventHandlerOpts = {},
	): void {
		if (!EVENT_PATTERN_RE.test(pattern))
			throw new ValidationError(
				`event.handle: invalid pattern "${pattern}" — dot-separated segments of [a-z0-9_-], "*" or "#"`,
			);
		if (this.subscriptions.has(pattern))
			throw new ValidationError(
				`event.handle: pattern "${pattern}" already has a handler`,
			);
		const entry: SubscriptionEntry = {
			pattern,
			filter: opts.filter ? JSON.stringify(opts.filter) : "",
			fn,
		};
		this.subscriptions.set(pattern, entry);
		if (!opts.schema) return;
		const spec: SchemaSpec =
			"protoFile" in opts.schema && !opts.schema.method
				? { ...opts.schema, method: pattern }
				: opts.schema;
		const load = import("../serde/serializer").then(({ buildSchemaPair }) =>
			buildSchemaPair(spec).then((pair) => {
				entry.schemaPair = pair;
			}),
		);
		this.trackPending(load);
	}

	// subscription returns this process's handler for one matched pattern.
	subscription(pattern: string): SubscriptionEntry | undefined {
		return this.subscriptions.get(pattern);
	}

	// subscriptionCount — the subscriber stream opens only when non-zero.
	subscriptionCount(): number {
		return this.subscriptions.size;
	}

	// eventSubscriptions emits RegisterRequest.event_subscriptions.
	eventSubscriptions(): { pattern: string; filter: string }[] {
		return [...this.subscriptions.values()].map((s) => ({
			pattern: s.pattern,
			filter: s.filter,
		}));
	}

	// workflow registers a workflow definition; the runtime validates it and
	// computes its fingerprint.
	// @internal — used by WorkflowDomain.handle.
	workflow(name: string, definition: PbWorkflowDefinition): void {
		this.addEntry({
			type: MethodType.METHOD_TYPE_WORKFLOW,
			name,
			inputSchemaJson: null,
			outputSchemaJson: null,
			fn: null,
			workflow: definition,
		});
	}

	// job registers a scheduled job. `contractHash` is the SDK-computed
	// SHA-256 of the canonical-spec JSON; `specJson` is the canonical spec
	// itself (CanonicalJobSpec, see runtime/internal/jobs/canonical.go).
	// fn is the handler — it is stored locally and not sent over the wire.
	// @internal — used by JobDomain.handle.
	job(name: string, contractHash: string, specJson: string, fn: unknown): void {
		this.addEntry({
			type: MethodType.METHOD_TYPE_JOB,
			name,
			inputSchemaJson: Buffer.from(specJson, "utf8"),
			outputSchemaJson: null,
			fn,
			contractHashOverride: contractHash,
		});
	}

	// finalize resolves any pending schema loads. Called by ServiceBridge.start()
	// before buildRegisterRequest() so IncomingMethod.input_schema_json reflects
	// the real protobuf descriptors.
	async finalize(): Promise<void> {
		if (this.pending.length === 0) return;
		const pending = this.pending;
		this.pending = [];
		await Promise.all(pending);
	}

	// incomingMethods emits ВСЕ типы кроме EVENT. Event subscriptions едут только
	// через RegisterRequest.event_subscriptions — единственный канал для них.
	// См. ADR 0006 + registry README.
	incomingMethods(): PbIncomingMethod[] {
		return this._entries.map((e) => ({
			type: e.type,
			name: e.name,
			inputSchemaJson: e.inputSchemaJson ?? Buffer.alloc(0),
			outputSchemaJson: e.outputSchemaJson ?? Buffer.alloc(0),
			streaming: e.streaming ?? false,
			// SDK computes the contract hash; runtime stores opaque (ADR 0005).
			// Workflow entries carry a graph fingerprint (ADR-W-002) via
			// contractHashOverride. Empty when neither source is set.
			contractHash: e.contractHashOverride
				? e.contractHashOverride
				: e.schemaPair
					? computeContractHash(e.schemaPair)
					: "",
			workflow: e.workflow,
		}));
	}

	// publishedEvents emits PublishedEvent rows for RegisterRequest. ADR-0002:
	// publishers carry contract_hash so the runtime keep-history per schema
	// version (different hashes coexist for the same name) works correctly.
	publishedEvents(): PbPublishedEvent[] {
		return this._published.map((e) => ({
			name: e.name,
			schemaJson: e.inputSchemaJson ?? Buffer.alloc(0),
			contractHash: e.contractHash,
		}));
	}

	// asDispatchPort exposes a DispatchPort over the registered handlers.
	// Refusals before the handler (unknown method, wrong kind, undecodable
	// request) are gRPC statuses; a handler failure is error_code under OK.
	asDispatchPort(): DispatchPort {
		const lookup = (
			method: string,
			streaming: boolean,
		):
			| { entry: HandlerEntry & { schemaPair: SchemaPair } }
			| { refusal: UnaryResult } => {
			const entry = this.rpcByName.get(method);
			if (!entry)
				return {
					refusal: {
						status: GRPC_NOT_FOUND,
						errorMessage: `rpc: no handler for method ${method}`,
					},
				};
			if (!!entry.streaming !== streaming)
				return {
					refusal: {
						status: GRPC_FAILED_PRECONDITION,
						errorMessage: streaming
							? `rpc: method ${method} is unary — call it with Unary`
							: `rpc: method ${method} is streaming — call it with Stream`,
					},
				};
			if (!entry.schemaPair)
				return {
					refusal: {
						status: GRPC_FAILED_PRECONDITION,
						errorMessage: `rpc: schema not loaded for method ${method}`,
					},
				};
			return { entry: entry as HandlerEntry & { schemaPair: SchemaPair } };
		};
		const decode = (
			entry: HandlerEntry & { schemaPair: SchemaPair },
			payload: Uint8Array,
		): { request: unknown } | { refusal: UnaryResult } => {
			try {
				return { request: entry.schemaPair.input.decode(payload) };
			} catch (err) {
				return {
					refusal: {
						status: GRPC_INVALID_ARGUMENT,
						errorMessage: `rpc: decode request: ${(err as Error).message}`,
					},
				};
			}
		};
		return {
			dispatchUnary: async (method, payload, ctx): Promise<UnaryResult> => {
				const found = lookup(method, false);
				if ("refusal" in found) return found.refusal;
				const decoded = decode(found.entry, payload);
				if ("refusal" in decoded) return decoded.refusal;
				try {
					const fn = found.entry.fn as RpcHandlerFn;
					const result = await fn(decoded.request, ctx);
					return { payload: found.entry.schemaPair.output.encode(result) };
				} catch (err) {
					return handlerFailure(err);
				}
			},
			captureMode: (method: string): CaptureMode | undefined =>
				this.rpcByName.get(method)?.captureMode,
			dispatchStream: async function* (
				method: string,
				payload: Uint8Array,
				ctx: RpcHandlerContext,
			): AsyncIterable<StreamItem> {
				const found = lookup(method, true);
				if ("refusal" in found) {
					yield found.refusal;
					return;
				}
				const decoded = decode(found.entry, payload);
				if ("refusal" in decoded) {
					yield decoded.refusal;
					return;
				}
				const { entry } = found;
				try {
					const fn = entry.fn as RpcStreamHandlerFn;
					for await (const chunk of fn(decoded.request, ctx)) {
						yield { payload: entry.schemaPair.output.encode(chunk) };
					}
				} catch (err) {
					yield handlerFailure(err);
				}
			},
		};
	}
}

// gRPC status codes of the refusals above (numeric: no grpc-js import here).
const GRPC_INVALID_ARGUMENT = 3;
const GRPC_NOT_FOUND = 5;
const GRPC_FAILED_PRECONDITION = 9;

// EVENT_PATTERN_RE accepts an event name or an AMQP pattern ("*" one segment,
// "#" zero or more).
const EVENT_PATTERN_RE = /^([a-z0-9_-]+|\*|#)(\.([a-z0-9_-]+|\*|#))*$/;

// handlerFailure turns a thrown error into the handler's answer: a
// HandlerError keeps its business code, anything else is "INTERNAL".
function handlerFailure(err: unknown): UnaryResult {
	if (err instanceof HandlerError && !err.remote)
		return { errorCode: err.handlerCode, errorMessage: err.message };
	return {
		errorCode: "INTERNAL",
		errorMessage: err instanceof Error ? err.message : String(err),
	};
}

export class Registry {
	// Internal storage for incoming handlers and published events — accessed
	// via domain classes.
	// @internal
	readonly _handle = new Handle();
	/**
	 * Public-but-undocumented route collector. Используется только интеграциями
	 * `servicebridge/{express,fastify,hono}`. Прикладной код пишет роуты в свой
	 * фреймворк, не сюда. См. ADR 0001 и userDocs/integrations.md.
	 */
	readonly routes: RouteCollector;
	private readonly _outgoing: OutgoingEntry[] = [];

	/**
	 * @param onRestart — вызывается из `routes.publishHttp(...)` после записи
	 * нового endpoint'а. ServiceBridge подставляет туда рестарт Registry-watch
	 * стрима. До `sb.start()` callback ожидает no-op.
	 */
	constructor(onRestart: () => void = () => {}) {
		this.routes = new RouteCollector({
			setEndpoint: (ep: string) => {
				this._httpEndpoint = ep;
			},
			triggerRestart: onRestart,
		});
	}

	service(serviceName: string, deps: ServiceDeps): void {
		for (const method of deps.rpc ?? []) {
			this._outgoing.push({
				serviceName,
				methodName: method,
				type: MethodType.METHOD_TYPE_RPC,
			});
		}
		for (const method of deps.workflows ?? []) {
			this._outgoing.push({
				serviceName,
				methodName: method,
				type: MethodType.METHOD_TYPE_WORKFLOW,
			});
		}
		for (const method of deps.http ?? []) {
			this._outgoing.push({
				serviceName,
				methodName: method,
				type: MethodType.METHOD_TYPE_HTTP,
			});
		}
	}

	buildRegisterRequest(): RegisterRequest {
		const incoming: PbIncomingMethod[] = this._handle.incomingMethods();

		// HTTP routes collected by an integration are emitted as METHOD_TYPE_HTTP
		// IncomingMethod entries (ADR 0001). No schema, no contract hash —
		// HTTP routes are declared, not transported through the runtime.
		for (const r of this.routes.snapshot()) {
			incoming.push({
				type: MethodType.METHOD_TYPE_HTTP,
				name: `${r.method} ${r.pattern}`,
				inputSchemaJson: Buffer.alloc(0),
				outputSchemaJson: Buffer.alloc(0),
				streaming: false,
				contractHash: "",
			});
		}

		const published: PbPublishedEvent[] = this._handle.publishedEvents();

		// Dedup outgoing deps. `sb.client("svc", proto)` and an explicit
		// `service("svc", {...})` for the same target both append a row, and the
		// runtime's outgoing_calls upsert drops the duplicate anyway — sending it
		// only inflates the register frame.
		const seenOutgoing = new Set<string>();
		const outgoing: PbOutgoingDep[] = [];
		for (const o of this._outgoing) {
			const key = `${o.serviceName}|${o.methodName}|${o.type}`;
			if (seenOutgoing.has(key)) continue;
			seenOutgoing.add(key);
			outgoing.push({
				serviceName: o.serviceName,
				methodName: o.methodName,
				type: o.type,
			});
		}

		const eventSubscriptions = this._handle.eventSubscriptions();

		return {
			incoming,
			published,
			outgoing,
			callEndpoint: this._callEndpoint,
			eventSubscriptions,
			httpEndpoint: this._httpEndpoint,
			protocolVersion: PROTOCOL_VERSION,
			sdkLanguage: SDK_LANGUAGE,
			sdkVersion: SDK_VERSION,
		};
	}

	setCallEndpoint(endpoint: string): void {
		this._callEndpoint = endpoint;
	}

	private _callEndpoint = "";
	private _httpEndpoint = "";
}
