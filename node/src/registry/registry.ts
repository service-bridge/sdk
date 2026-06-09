import { RouteCollector } from "../http/route";
import type {
	IncomingMethod as PbIncomingMethod,
	OutgoingDep as PbOutgoingDep,
	PublishedEvent as PbPublishedEvent,
	RegisterRequest,
} from "../pb/servicebridge/v1/registry";
import { MethodType } from "../pb/servicebridge/v1/registry";
import type {
	DispatchPort,
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

// WorkflowHandlerOpts — input-only schema. Workflow output never exists at the
// transport level: каждый step возвращает обновлённый state (см.
// `userDocs/workflows.md`). Финального output у workflow как сущности нет.
export interface WorkflowHandlerOpts {
	input?: Record<string, unknown>;
}

export interface ServiceDeps {
	rpc?: string[];
	workflows?: string[];
	http?: string[];
}

// RpcHandlerFn is the function shape accepted by Handle.rpc().
export type RpcHandlerFn<Req = unknown, Res = unknown> = (
	req: Req,
) => Promise<Res> | Res;

// RpcStreamHandlerFn is the function shape accepted by Handle.stream().
// Implementations return an AsyncIterable / async generator that yields one
// chunk at a time. Termination (return) ends the stream; thrown errors are
// caught by the CallServer and mapped to the final StreamChunk.error_code.
export type RpcStreamHandlerFn<Req = unknown, Chunk = unknown> = (
	req: Req,
) => AsyncIterable<Chunk>;

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

function schemaToBuffer(schema?: Record<string, unknown>): Buffer | null {
	if (!schema) return null;
	return Buffer.from(JSON.stringify(schema));
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

	// Pending registrations awaiting their async SchemaPair to load. Covers
	// rpc / stream handlers AND publishEvent declarations.
	// finalize() resolves all of them before incomingMethods() /
	// publishedEvents() are called.
	private pending: Promise<void>[] = [];

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

	// _declareForTests registers an RPC entry without a schema. Tests use this
	// to assemble Handle / Registry fixtures that exercise registration paths
	// without loading real .proto files. Production code MUST go through `rpc`
	// or `stream` with an explicit `schema`.
	//
	// @internal — см. ./README.md
	_declareForTests(name: string, streaming = false): void {
		this._entries.push({
			type: MethodType.METHOD_TYPE_RPC,
			name,
			inputSchemaJson: null,
			outputSchemaJson: null,
			fn: () => undefined,
			streaming,
		});
	}

	private registerRpc(
		name: string,
		fn: unknown,
		streaming: boolean,
		opts: RpcHandlerOpts,
	): void {
		const entry: HandlerEntry = {
			type: MethodType.METHOD_TYPE_RPC,
			name,
			inputSchemaJson: null,
			outputSchemaJson: null,
			fn,
			streaming,
			captureMode: opts.captureMode,
		};
		this._entries.push(entry);

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
		this.pending.push(load);
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
	publishEvent(name: string, spec?: SchemaSpec): void {
		const existing = this._published.find((p) => p.name === name);
		if (existing) {
			if (existing.spec === spec) return; // idempotent re-define with same spec
			if (existing.spec === undefined && spec === undefined) return;
			throw new Error(
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

		if (!spec) return; // schema-less event — registered name only

		// For ProtoFileSpec without explicit input/output, propagate the event
		// name so buildSchemaPair can look it up in the .proto service block.
		const resolvedSpec: SchemaSpec =
			"protoFile" in spec && !spec.method ? { ...spec, method: name } : spec;

		const load = Promise.all([
			import("../serde/serializer"),
			import("../serde/contract-hash"),
		]).then(async ([{ buildSchemaPair }, { computeContractHash }]) => {
			const pair = await buildSchemaPair(resolvedSpec);
			entry.schemaPair = pair;
			entry.inputSchemaJson = Buffer.from(
				JSON.stringify(pair.input.toJsonSchema()),
			);
			entry.contractHash = computeContractHash(pair);
		});
		this.pending.push(load);
	}

	// getPublishedEvent — schema lookup for Publisher / Subscriber. Returns the
	// loaded SchemaPair plus contractHash, or undefined when the event was not
	// declared (or declared without a spec, or finalize() not yet called).
	getPublishedEvent(
		name: string,
	): { contractHash: string; pair: SchemaPair } | undefined {
		const entry = this._published.find((e) => e.name === name);
		if (!entry || !entry.schemaPair) return undefined;
		return { contractHash: entry.contractHash, pair: entry.schemaPair };
	}

	// _declarePublishedEventForTests — registers a published event without a
	// real schema spec. Bypasses async load; tests use this to exercise
	// registration paths without .proto files. Production code MUST go through
	// publishEvent(name, spec) with an explicit schema.
	// @internal
	_declarePublishedEventForTests(name: string): void {
		if (this._published.find((p) => p.name === name)) return;
		this._published.push({
			name,
			inputSchemaJson: null,
			contractHash: "",
		});
	}

	// event registers a durable event subscription. Pattern может быть точным
	// именем или AMQP wildcard. Схема payload-а живёт у publisher'а (event.define)
	// и используется и для encode, и для decode — handler-side schema смысла не
	// имеет (один pattern матчит много событий с разными схемами).
	event(name: string, fn: unknown): void {
		this._entries.push({
			type: MethodType.METHOD_TYPE_EVENT,
			name,
			inputSchemaJson: null,
			outputSchemaJson: null,
			fn,
		});
	}

	workflow(
		name: string,
		steps: unknown,
		opts?: WorkflowHandlerOpts,
		// Workflow-graph payload (ADR-W-002). When present, `graphJson`
		// overrides the schema-derived input_schema_json (the graph IS the
		// declaration), and `contractHash` is the canonical-graph fingerprint
		// that travels as IncomingMethod.contract_hash.
		graphJson?: Buffer,
		contractHash?: string,
	): void {
		this._entries.push({
			type: MethodType.METHOD_TYPE_WORKFLOW,
			name,
			inputSchemaJson: graphJson ?? schemaToBuffer(opts?.input),
			outputSchemaJson: null,
			fn: steps,
			contractHashOverride: contractHash,
		});
	}

	// job registers a scheduled job. `contractHash` is the SDK-computed
	// SHA-256 of the canonical-spec JSON; `specJson` is the canonical spec
	// itself (CanonicalJobSpec, see runtime/internal/jobs/canonical.go).
	// fn is the handler — it is stored locally and not sent over the wire.
	// @internal — used by JobDomain.handle.
	job(name: string, contractHash: string, specJson: string, fn: unknown): void {
		this._entries.push({
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
		return this._entries
			.filter((e) => e.type !== MethodType.METHOD_TYPE_EVENT)
			.map((e) => ({
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
			}));
	}

	// publishedEvents emits PublishedEvent rows for RegisterRequest. ADR-0013:
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
	// CallServer uses this instead of reaching into private _entries.
	asDispatchPort(): DispatchPort {
		const findRpc = (method: string): HandlerEntry | undefined =>
			this._entries.find(
				(e) => e.type === MethodType.METHOD_TYPE_RPC && e.name === method,
			);

		const handle = this;

		return {
			dispatchUnary: async (
				method: string,
				payload: Uint8Array,
			): Promise<UnaryResult> => {
				const entry = findRpc(method);
				if (!entry) {
					return {
						payload: new Uint8Array(0),
						errorCode: "NOT_FOUND",
						errorMessage: `no RPC handler for method ${method}`,
					};
				}
				if (entry.streaming) {
					return {
						payload: new Uint8Array(0),
						errorCode: "FAILED_PRECONDITION",
						errorMessage: `method ${method} is streaming — call via Stream`,
					};
				}
				if (!entry.schemaPair) {
					return {
						payload: new Uint8Array(0),
						errorCode: "INTERNAL",
						errorMessage: `schema not loaded for method ${method}`,
					};
				}
				let request: unknown;
				try {
					request = entry.schemaPair.input.decode(payload);
				} catch (err) {
					return {
						payload: new Uint8Array(0),
						errorCode: "INVALID_ARGUMENT",
						errorMessage: `decode: ${(err as Error).message}`,
					};
				}
				try {
					const fn = entry.fn as RpcHandlerFn;
					const result = await fn(request);
					const bytes = entry.schemaPair.output.encode(
						result as Record<string, unknown>,
					);
					return { payload: bytes };
				} catch (err) {
					return {
						payload: new Uint8Array(0),
						errorCode: "INTERNAL",
						errorMessage: (err as Error).message,
					};
				}
			},
			captureMode: (method: string): CaptureMode | undefined => {
				return findRpc(method)?.captureMode;
			},
			dispatchStream: async function* (
				method: string,
				payload: Uint8Array,
			): AsyncIterable<StreamItem> {
				void handle;
				const entry = findRpc(method);
				if (!entry || !entry.streaming) {
					yield {
						errorCode: "NOT_FOUND",
						errorMessage: `no streaming handler for method ${method}`,
					};
					return;
				}
				if (!entry.schemaPair) {
					yield {
						errorCode: "INTERNAL",
						errorMessage: `schema not loaded for method ${method}`,
					};
					return;
				}
				let request: unknown;
				try {
					request = entry.schemaPair.input.decode(payload);
				} catch (err) {
					yield {
						errorCode: "INVALID_ARGUMENT",
						errorMessage: `decode: ${(err as Error).message}`,
					};
					return;
				}
				try {
					const fn = entry.fn as RpcStreamHandlerFn;
					for await (const chunk of fn(request)) {
						const bytes = entry.schemaPair.output.encode(
							chunk as Record<string, unknown>,
						);
						yield { payload: bytes };
					}
				} catch (err) {
					yield {
						errorCode: "INTERNAL",
						errorMessage: (err as Error).message,
					};
				}
			},
		};
	}
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

		const outgoing: PbOutgoingDep[] = this._outgoing.map((o) => ({
			serviceName: o.serviceName,
			methodName: o.methodName,
			type: o.type,
		}));

		// Dedup event subscriptions by pattern. Multiple `event.handle(name, fn)`
		// calls with the same pattern produce multiple HandlerEntry rows (so the
		// subscriber dispatches to all matching handlers locally — that's the
		// fan-out-within-SDK semantics tests rely on), but the runtime's
		// `event_subscriptions` table has PRIMARY KEY (subscriber_id, pattern),
		// so sending duplicates would trip the unique constraint inside
		// `ReplaceEventSubs` and roll back the entire registration. One row per
		// distinct pattern is enough — the SDK side handles in-process fan-out.
		const seenPatterns = new Set<string>();
		const eventSubscriptions: { pattern: string; durable: boolean }[] = [];
		for (const e of this._handle._entries) {
			if (e.type !== MethodType.METHOD_TYPE_EVENT) continue;
			if (seenPatterns.has(e.name)) continue;
			seenPatterns.add(e.name);
			eventSubscriptions.push({ pattern: e.name, durable: true });
		}

		return {
			incoming,
			published,
			outgoing,
			callEndpoint: this._callEndpoint,
			eventSubscriptions,
			httpEndpoint: this._httpEndpoint,
		};
	}

	setCallEndpoint(endpoint: string): void {
		this._callEndpoint = endpoint;
	}

	private _callEndpoint = "";
	private _httpEndpoint = "";
}
