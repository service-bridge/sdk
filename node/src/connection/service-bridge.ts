import "reflect-metadata";
import * as grpc from "@grpc/grpc-js";
import { EventDomain } from "../events/domain";
import { Drainer } from "../events/drainer";
import type { SchemaIndex } from "../events/publisher";
import { Publisher } from "../events/publisher";
import type { EventHandler } from "../events/subscriber";
import { Subscriber } from "../events/subscriber";
import { JobDomain } from "../job/domain";
import { JobSubscriber } from "../job/subscriber";
import { ControlClient } from "../pb/servicebridge/v1/control";
import { EventsClient } from "../pb/servicebridge/v1/events";
import { JobsClient } from "../pb/servicebridge/v1/jobs";
import type {
	EventSubscriptionDescriptor,
	OutgoingCallDescriptor,
	PolicyEvaluation,
	RegistryClient,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import { RegistryClient as RegistryClientImpl } from "../pb/servicebridge/v1/registry";
import { TelemetryClient } from "../pb/servicebridge/v1/telemetry";
import { WorkflowsClient } from "../pb/servicebridge/v1/workflows";
import type { MethodDescriptor, ServiceDeps } from "../registry/registry";
import { MethodType, Registry } from "../registry/registry";
import { WatchStream } from "../registry/watch";
import { CircuitBreakerRegistry } from "../rpc/circuit-breaker";
import type { CallOpts } from "../rpc/client";
import { RpcClient, SchemaRegistry } from "../rpc/client";
import { DirectTransport } from "../rpc/direct-transport";
import { RpcDomain } from "../rpc/domain";
import { InstanceCache } from "../rpc/instance-cache";
import { LoadBalancer } from "../rpc/lb";
import { ProxyTransport } from "../rpc/proxy-transport";
import { type AdvertiseConfig, CallServer } from "../rpc/server";
import { extractServiceMethods, type TypedClient } from "../rpc/typed-client";
import { buildSchemaPair, type SchemaSpec } from "../serde/serializer";
import { Storage } from "../sqlite/storage";
import { currentTraceContext, runWithTrace } from "../telemetry/context";
import { type LogFields, makeLogger } from "../telemetry/logs";
import {
	type Labels,
	makeCounter,
	makeGauge,
	makeHistogram,
} from "../telemetry/metrics";
import {
	Channel,
	OpHandle,
	type StartOpParams,
	Status,
	UserSubOp,
} from "../telemetry/ops";
import {
	type CaptureMode,
	DEFAULT_PAYLOAD_MAX_BYTES,
} from "../telemetry/payload-capture";
import { ProcessSampler } from "../telemetry/process-sampler";
import { TelemetryRing } from "../telemetry/ring";
import { childContext } from "../telemetry/trace-context";
import {
	adaptTelemetryClient,
	TelemetryTransport,
} from "../telemetry/transport";
import { formatXSbTrace, parseXSbTrace } from "../telemetry/wire-trace";
import { WorkflowDomain } from "../workflow/domain";
import { makeRuntimeOps } from "../workflow/runtime-ops";
import { WorkflowSubscriber } from "../workflow/subscriber";
import type { Step } from "../workflow/types";
import { parseBootstrapKey } from "./key";
import { derToPem } from "./pem";
import type { ProvisionResult } from "./provision";
import { provision as defaultProvision, refresh } from "./provision";
import { isRetryable, ServiceBridgeError } from "./service-bridge-error";
import type { SessionCallbacks } from "./session";
import { openControlStream, Session } from "./session";

export type { ServiceInstanceInfo } from "../pb/servicebridge/v1/registry";
export type {
	MethodDescriptor,
	MethodType,
	RpcHandlerOpts,
	ServiceDeps,
	WorkflowHandlerOpts,
} from "../registry/registry";

/**
 * Entry в карте `sb.serviceMap()`. Группирует видимые caller'у методы сервиса
 * (`methods`) и его текущих живых инстансов с их endpoint'ами (`instances`).
 * `httpEndpoint` поле каждого инстанса — публичный HTTP host:port пользователя
 * (ADR 0001), пустая строка если у инстанса нет HTTP-интеграции.
 */
export interface ServiceMapEntry {
	methods: MethodDescriptor[];
	instances: ServiceInstanceInfo[];
	// ADR-0014 service-map enrichment (populated for caller's own service +
	// services in caller's outgoing-dep scope). Empty arrays when the runtime
	// doesn't carry that info for this service in the current snapshot.
	eventSubscriptions: EventSubscriptionDescriptor[];
	outgoingCalls: OutgoingCallDescriptor[];
}
export type { CallOpts } from "../rpc/client";
export type { AdvertiseConfig } from "../rpc/server";
export type { SchemaSpec } from "../serde/serializer";

// Reconnect configuration.
const RECONNECT_INTERVAL_MS = 3_000;
const RECONNECT_ATTEMPTS = 3; // 0 = unlimited

// Events defaults (overridden via ServiceBridgeOptions in ensureRpcReady).
const DEFAULT_MAX_OUTBOX_ROWS = 100_000;
const DEFAULT_DRAINER_BATCH = 50;
const DEFAULT_EVENTS_MAX_IN_FLIGHT = 32;
// Default telemetry ops-ring byte budget. Override via
// ServiceBridgeOptions.telemetryRingSize. Sized for the dense USER.SUBOP
// step-span emission of a workflow run between flush ticks (≈800 op frames).
const DEFAULT_TELEMETRY_RING_SIZE = 256 * 1024;
// Refresh cert 30 minutes before expiry.
const CERT_REFRESH_LEAD_MS = 30 * 60 * 1000;
// Random offset added to the refresh delay to spread herd of N clients across a
// window. With 10k clients started in lockstep this turns a 10k-RPS spike at
// T+30min into ~8 RPS smeared across a 5-minute window.
const CERT_REFRESH_JITTER_MS = 5 * 60 * 1000;
// During overlap rotation, wait at most this long for the new session to
// produce a Welcome before closing the old session. With heartbeats removed
// from Control, Welcome on the new stream is the only liveness signal.
const ROTATION_HANDSHAKE_TIMEOUT_MS = 10_000;

// randomJitter returns an integer in [0, maxMs); 0 if maxMs <= 0.
// Math.random is sufficient for load-spreading; not for security timing.
function randomJitter(maxMs: number): number {
	if (maxMs <= 0) return 0;
	return Math.floor(Math.random() * maxMs);
}

/**
 * Build the TelemetryAPI surface. Identity is resolved lazily so user code
 * may emit before start() — instance_id will be the empty string until the
 * first Welcome.
 * @internal
 */
function makeTelemetryAPI(
	ring: TelemetryRing,
	getInstanceId: () => string,
	_getServiceId: () => string,
	getCaptureModeForChannel: (channel: Channel) => CaptureMode,
	payloadMaxBytes: number,
): TelemetryAPI {
	const log = makeLazyLogger(ring, getInstanceId);
	return {
		startOp(params) {
			return OpHandle.start(ring, {
				...params,
				effectiveCaptureMode: getCaptureModeForChannel(params.channel),
				payloadMaxBytes,
			});
		},
		captureModeForChannel: getCaptureModeForChannel,
		log,
		counter(name, labels) {
			return makeCounter(ring, getInstanceId(), name, labels);
		},
		gauge(name, labels) {
			return makeGauge(ring, getInstanceId(), name, labels);
		},
		histogram(name, unit, labels) {
			return makeHistogram(ring, getInstanceId(), name, unit, labels);
		},
	};
}

/**
 * Lazy logger that resolves instanceId at emission time so logs published
 * after Welcome carry the correct identity even if the logger handle was
 * captured during construction.
 * @internal
 */
function makeLazyLogger(
	ring: TelemetryRing,
	getInstanceId: () => string,
): ReturnType<typeof makeLogger> {
	const emit = (level: "debug" | "info" | "warn" | "error") => {
		return (message: string, fields?: LogFields) => {
			makeLogger(ring, getInstanceId())[level](message, fields);
		};
	};
	return {
		debug: emit("debug"),
		info: emit("info"),
		warn: emit("warn"),
		error: emit("error"),
	};
}

// mergeOpts combines two CallOpts where the per-call override wins. Used by
// the typed client to layer { client-default ← per-call } on top of the
// global ServiceBridge.callDefaults already applied by sb.rpc.call / sb.stream.
function mergeOpts(base?: CallOpts, override?: CallOpts): CallOpts | undefined {
	if (!base && !override) return undefined;
	return { ...(base ?? {}), ...(override ?? {}) };
}

// resolveAdvertise turns the user-provided advertise option into the
// effective AdvertiseConfig the SDK will use, or null for caller-only.
//
// Resolution order:
//   1. `false`                 → null (caller-only, no inbound server)
//   2. explicit { host, port } → used as-is
//   3. fallback                → { host: "127.0.0.1", port: 0 } + console warn
//      (suitable for local dev; the loopback address is not reachable from
//      other hosts — pass { advertise: { host, port } } in containers / k8s).
function resolveAdvertise(
	provided: AdvertiseConfig | false | undefined,
): AdvertiseConfig | null {
	if (provided === false) return null;
	if (provided) return provided;
	console.warn(
		"[ServiceBridge] advertise not configured — falling back to 127.0.0.1. " +
			"Pass { advertise: { host, port } } for cross-host reachability.",
	);
	return { host: "127.0.0.1", port: 0 };
}

/**
 * TelemetryAPI is the public surface for emitting telemetry from user code
 * and from internal subsystems (RPC, HTTP plugins, events, workflow, jobs).
 *
 * @public — см. ../telemetry/README.md
 */
export interface TelemetryAPI {
	/** Start an op; returns a handle to `.end(status, message?)`. */
	startOp(params: StartOpParams): OpHandle;
	/**
	 * Runtime-pushed effective payload capture mode for the given op channel.
	 * "none" until the first registry snapshot arrives (fail-safe). Read-only
	 * introspection — capture itself is applied automatically inside startOp.
	 */
	captureModeForChannel(channel: Channel): CaptureMode;
	log: ReturnType<typeof makeLogger>;
	counter(name: string, labels?: Labels): ReturnType<typeof makeCounter>;
	gauge(name: string, labels?: Labels): ReturnType<typeof makeGauge>;
	histogram(
		name: string,
		unit?: string,
		labels?: Labels,
	): ReturnType<typeof makeHistogram>;
}

export interface ConnectedEvent {
	sessionId: string;
	serviceId: string;
	serviceName: string;
}

/**
 * Identity of the current live session. Populated on Welcome (start + every
 * successful overlap rotation), cleared on stop / stream end. Read via
 * `ServiceBridge.identity()`.
 */
export interface Identity {
	sessionId: string;
	serviceId: string;
	serviceName: string;
	instanceId: string;
}

export interface ReconnectingEvent {
	attempt: number;
	delayMs: number;
	reason: string;
}

export interface DisconnectedEvent {
	reason: string;
	error?: ServiceBridgeError;
}

/**
 * One violation reported by the runtime in `RegistrySnapshot.policy.warnings`.
 * Emitted via `sb.on("policy_violation", ...)` and logged via console.warn.
 *
 * Fields mirror `pb.PolicyViolation`:
 *
 * - `declaration` — what was declared, e.g. `rpc.call`, `event.handle`,
 *   `event.publish`, `rpc.handle` (capability).
 * - `value` — concrete value, e.g. `payments/charge`, `orders.*`.
 * - `denySide` — `capability` | `self_egress` | `self_acceptance` | `peer_acceptance`.
 * - `reason` — human-readable explanation from the runtime.
 */
export interface PolicyViolationEvent {
	declaration: string;
	value: string;
	denySide: string;
	reason: string;
}

type EventMap = {
	connected: ConnectedEvent;
	reconnecting: ReconnectingEvent;
	disconnected: DisconnectedEvent;
	/**
	 * Fired once per warning when the runtime sends a PolicyEvaluation in the
	 * registry snapshot (after `start()` or after operator changes policy).
	 */
	policy_violation: PolicyViolationEvent;
};

type Handler<K extends keyof EventMap> = (event: EventMap[K]) => void;

/** Public configuration. Documented in `./README.md` (Public contract). */
export interface ServiceBridgeOptions {
	reconnectIntervalMs?: number;
	reconnectAttempts?: number;
	/**
	 * Advertise config for the inbound Call RPC server.
	 *
	 * - `{ host, port }` — explicit (recommended for prod, required in k8s with POD_IP)
	 * - `undefined` (default) — bind "127.0.0.1" on a free port. Local dev
	 *   friendly; logs a warning when falling back to loopback so cross-host
	 *   limitations are visible.
	 * - `false` — explicit caller-only mode: do not bind any inbound server.
	 *   Set this when you know the instance never serves RPC.
	 */
	advertise?: AdvertiseConfig | false;
	/**
	 * Default options applied to every `sb.rpc.call()` unless overridden.
	 */
	callDefaults?: CallOpts;
	/**
	 * ADR-0014: when `true`, any policy violation reported by the runtime in
	 * the registry snapshot's `PolicyEvaluation.warnings` makes `start()`
	 * surface a `disconnected` event with reason='policy' and the SDK stops.
	 * Default `false` — warnings only (logged via console.warn + emitted as
	 * `policy_violation` events).
	 */
	failOnPolicyViolation?: boolean;
	/**
	 * Emit telemetry (ops, logs, metrics) to the runtime. `false` fully disables
	 * the telemetry transport — the ring still buffers but nothing is drained.
	 * Default `true`.
	 */
	telemetry?: boolean;
	/** Ops-ring byte budget. Default 262144 (256 KiB). */
	telemetryRingSize?: number;
	/** Local SQLite outbox directory. Default "./.servicebridge". */
	dataDir?: string;
	/** Max rows kept in the event outbox before publish back-pressures. Default 100000. */
	maxOutboxRows?: number;
	/** Rows the events drainer pulls per tick. Default 50. */
	eventsDrainerBatch?: number;
	/** Max in-flight inbound events the subscriber processes concurrently. Default 32. */
	eventsMaxInFlight?: number;
	/** Per-direction captured-payload byte cap. Default 65536. */
	payloadMaxBytes?: number;
}

/**
 * @internal см. ./README.md
 */
interface ServiceBridgeInternalHooks extends ServiceBridgeOptions {
	certRefreshLeadMs?: number;
	certRefreshJitterMs?: number;
	rotationHandshakeTimeoutMs?: number;
	provisionFn?: typeof defaultProvision;
	refreshFn?: typeof refresh;
	clientFactory?: (
		url: string,
		creds: grpc.ChannelCredentials,
	) => ControlClient;
	registryClientFactory?: (
		url: string,
		creds: grpc.ChannelCredentials,
	) => RegistryClient;
}

interface ResolvedOptions {
	reconnectIntervalMs: number;
	reconnectAttempts: number;
	certRefreshLeadMs: number;
	certRefreshJitterMs: number;
	rotationHandshakeTimeoutMs: number;
	provisionFn: typeof defaultProvision;
	refreshFn: typeof refresh;
	clientFactory: (url: string, creds: grpc.ChannelCredentials) => ControlClient;
	registryClientFactory: (
		url: string,
		creds: grpc.ChannelCredentials,
	) => RegistryClient;
	advertise: AdvertiseConfig | null;
	callDefaults: CallOpts;
	failOnPolicyViolation: boolean;
	telemetry: boolean;
	telemetryRingSize: number;
	dataDir: string;
	maxOutboxRows: number;
	eventsDrainerBatch: number;
	eventsMaxInFlight: number;
	payloadMaxBytes: number;
}

/**
 * ServiceBridge manages the full connection lifecycle:
 * 1. Parse the bootstrap key.
 * 2. Provision (Bootstrap.Provision) to get a leaf cert.
 * 3. Open a mTLS gRPC channel with the cert.
 * 4. Open (Control.Open) for the server-streamed Welcome / Drain signals.
 * 5. Auto-reconnect with configurable attempts.
 * 6. Cert rotation: overlap — new channel up AND Welcome received on the new
 *    stream before closing the old stream.
 */
export class ServiceBridge {
	private readonly url: string;
	private readonly rawKey: string;
	private readonly opts: ResolvedOptions;
	private readonly handlers = new Map<
		keyof EventMap,
		Handler<keyof EventMap>[]
	>();
	private stopped = false;
	private session: Session | null = null;
	private controlClient: ControlClient | null = null;
	private lastProvision: ProvisionResult | null = null;
	private certRefreshTimer: ReturnType<typeof setTimeout> | null = null;
	private currentIdentity: Identity | null = null;

	private readonly _registry: Registry = new Registry(() =>
		this.onRegistrationChanged(),
	);
	private readonly _watchStream = new WatchStream();
	private readonly _instanceCache = new InstanceCache();
	private readonly _schemaRegistry = new SchemaRegistry();
	private _started = false;
	private _callServer: CallServer | null = null;
	private _proxyTransport: ProxyTransport | null = null;
	private _directTransport: DirectTransport | null = null;
	private _rpcClient: RpcClient | null = null;
	private readonly _cb = new CircuitBreakerRegistry();
	private readonly _lb = new LoadBalancer(this._cb);

	// Events infrastructure — lazy init in ensureRpcReady after storage is open.
	private _eventsClient: EventsClient | null = null;
	private _storage: Storage | null = null;
	private _publisher: Publisher | null = null;
	private _drainer: Drainer | null = null;
	private _subscriber: Subscriber | null = null;

	// Workflows infrastructure — lazy init in ensureRpcReady. Subscriber only
	// started when service has at least one workflow handler registered.
	private _workflowsClient: WorkflowsClient | null = null;
	private _workflowSubscriber: WorkflowSubscriber | null = null;

	// Jobs infrastructure — lazy init in ensureRpcReady. Subscriber started
	// on first Welcome when at least one job is registered.
	private _jobsClient: JobsClient | null = null;
	private _jobSubscriber: JobSubscriber | null = null;

	// Telemetry infrastructure — лежит на самом ServiceBridge, потому что её
	// lifecycle совпадает с lifecycle сессии (Welcome → start, stop → close).
	// Ring создаётся в конструкторе, чтобы prod-код мог писать логи / ops до
	// подключения; они буферятся в ring до старта transport'а. SDK всегда
	// flushes (ADR-0008); единственный off-switch — option telemetry: false,
	// который полностью отключает создание transport.
	private readonly _telemetryEnabled: boolean;
	private readonly _telemetryRing: TelemetryRing;
	private readonly _telemetryApi: TelemetryAPI;
	private _telemetryClient: TelemetryClient | null = null;
	private _telemetryTransport: TelemetryTransport | null = null;
	private _processSampler: ProcessSampler | null = null;
	private _telemetryInstanceId = "";
	// SchemaIndex adapter — single source of truth lives on Handle._published.
	// Publisher and Subscriber both read through this adapter; entries appear
	// after event.define(name, spec) resolves its async schema load in
	// Registry._handle.finalize() (called by start()).
	private readonly _schemaIndexAdapter: SchemaIndex = {
		get: (name: string) =>
			this._registry._handle.getPublishedEvent(name) as ReturnType<
				SchemaIndex["get"]
			>,
	};

	/** RPC domain — incoming handlers and outgoing calls. */
	readonly rpc: RpcDomain;

	/** Event domain — define published events, subscribe, publish. */
	readonly event: EventDomain;

	/** Workflow domain — register workflow handlers (stub; full impl in workflows.md). */
	readonly workflow: WorkflowDomain;

	/** Job domain — register scheduled job handlers via `.handle(name, opts, fn)`. */
	readonly job: JobDomain;

	/** Declares outgoing dependencies (rpc/workflows/http). Call before start(). */
	service(serviceName: string, deps: ServiceDeps): void {
		this._registry.service(serviceName, deps);
	}

	constructor(
		url: string,
		key: string,
		options: ServiceBridgeOptions | ServiceBridgeInternalHooks = {},
	) {
		this.url = url;
		this.rawKey = key;
		const hooks = options as ServiceBridgeInternalHooks;
		this.opts = {
			reconnectIntervalMs: options.reconnectIntervalMs ?? RECONNECT_INTERVAL_MS,
			reconnectAttempts: options.reconnectAttempts ?? RECONNECT_ATTEMPTS,
			certRefreshLeadMs: hooks.certRefreshLeadMs ?? CERT_REFRESH_LEAD_MS,
			certRefreshJitterMs: hooks.certRefreshJitterMs ?? CERT_REFRESH_JITTER_MS,
			rotationHandshakeTimeoutMs:
				hooks.rotationHandshakeTimeoutMs ?? ROTATION_HANDSHAKE_TIMEOUT_MS,
			provisionFn: hooks.provisionFn ?? defaultProvision,
			refreshFn: hooks.refreshFn ?? refresh,
			clientFactory: hooks.clientFactory ?? ((u, c) => new ControlClient(u, c)),
			registryClientFactory:
				hooks.registryClientFactory ?? ((u, c) => new RegistryClientImpl(u, c)),
			advertise: resolveAdvertise(options.advertise),
			callDefaults: options.callDefaults ?? {},
			failOnPolicyViolation: options.failOnPolicyViolation ?? false,
			telemetry: options.telemetry ?? true,
			telemetryRingSize:
				options.telemetryRingSize ?? DEFAULT_TELEMETRY_RING_SIZE,
			dataDir: options.dataDir ?? "./.servicebridge",
			maxOutboxRows: options.maxOutboxRows ?? DEFAULT_MAX_OUTBOX_ROWS,
			eventsDrainerBatch: options.eventsDrainerBatch ?? DEFAULT_DRAINER_BATCH,
			eventsMaxInFlight:
				options.eventsMaxInFlight ?? DEFAULT_EVENTS_MAX_IN_FLIGHT,
			payloadMaxBytes: options.payloadMaxBytes ?? DEFAULT_PAYLOAD_MAX_BYTES,
		};

		// Telemetry wiring (ADR-0008). Ring is owned by the bridge so user code
		// may emit logs/ops/metrics before start(); they buffer in the ring and
		// the transport drains once it's up.
		this._telemetryEnabled = this.opts.telemetry;
		// telemetryRingSize sets the ops ring byte budget (the kind under burst
		// pressure from dense workflow step-span emission). The ring's own per-kind
		// defaults apply when the value is non-finite or non-positive.
		const opsBudget = this.opts.telemetryRingSize;
		this._telemetryRing = new TelemetryRing(
			Number.isFinite(opsBudget) && opsBudget > 0
				? { ops: opsBudget }
				: undefined,
		);

		// Telemetry API surface. Identity fields are resolved lazily — user code
		// may call sb.telemetry.log() before start(), in which case instance_id
		// will be empty (best-effort). After Welcome, all subsequent emits carry
		// the real instanceId from currentIdentity.
		this._telemetryApi = makeTelemetryAPI(
			this._telemetryRing,
			() => this._telemetryInstanceId,
			() => this.currentIdentity?.serviceId ?? "",
			(channel) => this._watchStream.captureModeForChannel(channel),
			this.opts.payloadMaxBytes,
		);

		// Wire domain namespaces with lazy transport access. Call-time policy
		// denials (rpc.call / workflow.run / event.publish gates) are surfaced
		// through the same `policy_violation` channel as registration warnings.
		const onPolicyViolation = (v: PolicyViolationEvent) =>
			this.emitPolicyViolation(v);
		this.rpc = new RpcDomain(
			this._registry,
			() => this._rpcClient,
			onPolicyViolation,
		);
		this.event = new EventDomain(this._registry, () => this._publisher);
		this.workflow = new WorkflowDomain(this._registry, onPolicyViolation);
		this.job = new JobDomain(this._registry);
	}

	/**
	 * Surface a policy denial uniformly: log via console.warn and emit
	 * `policy_violation`. Shared by the registration evaluation (gate #0
	 * warnings) and call-time denials (rpc.call / workflow.run / event.publish).
	 */
	private emitPolicyViolation(v: PolicyViolationEvent): void {
		console.warn(
			`[ServiceBridge] policy violation: ${v.declaration} ${v.value} ` +
				`(deny side: ${v.denySide}) — ${v.reason}`,
		);
		this.emit("policy_violation", v);
	}

	/**
	 * Route collector — для использования ТОЛЬКО HTTP-интеграциями
	 * (`servicebridge/express`, `servicebridge/fastify`, `servicebridge/hono`).
	 * Прикладной код не вызывает это напрямую: роуты живут в Express/Fastify/Hono.
	 * См. ADR 0001 и userDocs/integrations.md.
	 */
	get routes() {
		return this._registry.routes;
	}

	/**
	 * Telemetry API surface — emit ops, logs, metrics from user code.
	 * Resources buffer in the ring before start(); the transport drains once
	 * Welcome arrives. parent_op_id/trace_id are inherited from the active
	 * ALS TraceContext.
	 *
	 * @public — см. ../telemetry/README.md
	 */
	get telemetry(): TelemetryAPI {
		return this._telemetryApi;
	}

	/**
	 * Structured logger — auto-injects instance_id and trace_id/op_id from the
	 * active context. Kept as a top-level convenience surface; equivalent to
	 * `sb.telemetry.log`.
	 */
	get logger() {
		return this._telemetryApi.log;
	}

	/**
	 * Instance ID текущей сессии (`telemetry_spans.instance_id`) — 12-символьная
	 * Crockford-base32 строка. До `start()` / до первого RegisterResponse
	 * возвращает пустую строку. Используется HTTP-плагинами для автотрейсинга.
	 * @public
	 */
	instanceIdString(): string {
		return this._telemetryInstanceId;
	}

	/**
	 * Дёргается `RouteCollector.publishHttp(...)` после `sb.start()`, когда
	 * интеграция узнала фактический host:port HTTP-сервера и хочет, чтобы
	 * runtime увидел его моментально (а не ждал natural reconnect).
	 *
	 * Безопасно если `start()` ещё не вызван — endpoint просто оседает в
	 * Registry и попадёт в первый RegisterRequest.
	 */
	private onRegistrationChanged(): void {
		if (!this._started) return;
		if (this.stopped) return;
		this.session?.updateRegistration(this._registry.buildRegisterRequest());
	}

	on<K extends keyof EventMap>(event: K, handler: Handler<K>): this {
		const list = this.handlers.get(event) ?? [];
		list.push(handler as Handler<keyof EventMap>);
		this.handlers.set(event, list);
		return this;
	}

	async start(): Promise<void> {
		this.stopped = false;
		this._started = true;
		// Finalize any pending RPC handler schema loads before sending RegisterRequest.
		await this._registry._handle.finalize();

		// Open local SQLite outbox before connecting — drainer needs storage ready.
		if (!this._storage) {
			this._storage = Storage.open({ dataDir: this.opts.dataDir });
		}

		// Start the inbound Call server if advertise is configured. We need the
		// runtime-issued leaf cert + CA chain — those arrive from Bootstrap.Provision
		// during connect(). Defer the actual bind until we have a ProvisionResult.
		this._instanceCache.bind(this._watchStream);
		this._proxyTransport = null;
		this._directTransport = null;
		this._rpcClient = null;

		await this.connect(1);
	}

	async stop(): Promise<void> {
		this.stopped = true;
		this._started = false;
		this.clearTimers();
		this._watchStream.stop();
		this.session?.close();
		this.session = null;
		this.currentIdentity = null;
		this._instanceCache.dispose();
		if (this._callServer) {
			await this._callServer.stop();
			this._callServer = null;
		}
		this._proxyTransport?.close();
		this._proxyTransport = null;
		this._directTransport?.close();
		this._directTransport = null;
		this._rpcClient = null;

		// Telemetry teardown — before events teardown so any final logs from
		// drainer/subscriber/storage close paths still have a sink (best-effort).
		if (this._processSampler) {
			this._processSampler.close();
			this._processSampler = null;
		}
		if (this._telemetryTransport) {
			await this._telemetryTransport.stop();
			this._telemetryTransport = null;
		}
		if (this._telemetryClient) {
			this._telemetryClient.close();
			this._telemetryClient = null;
		}

		// Workflows teardown: subscriber → client. Subscriber owns the heartbeat
		// timers and the long-lived Subscribe stream; close() stops both.
		if (this._workflowSubscriber) {
			this._workflowSubscriber.close();
			this._workflowSubscriber = null;
		}
		if (this._workflowsClient) {
			this._workflowsClient.close();
			this._workflowsClient = null;
		}

		// Jobs teardown: subscriber → client.
		if (this._jobSubscriber) {
			await this._jobSubscriber.stop();
			this._jobSubscriber = null;
		}
		if (this._jobsClient) {
			this._jobsClient.close();
			this._jobsClient = null;
		}

		// Events teardown: subscriber → drainer → events client → storage.
		if (this._subscriber) {
			await this._subscriber.stop();
			this._subscriber = null;
		}
		if (this._drainer) {
			await this._drainer.stop();
			this._drainer = null;
		}
		if (this._eventsClient) {
			this._eventsClient.close();
			this._eventsClient = null;
		}
		if (this._storage) {
			this._storage.close();
			this._storage = null;
		}
		this._publisher = null;
	}

	/**
	 * Registers a SchemaPair on the caller side for the given (service, method).
	 * Must be called before `sb.rpc.call()` is invoked for that method. The schema
	 * MUST match the one used by the target service (same .proto file).
	 *
	 * For an ergonomic alternative that handles dependency declaration, schema
	 * loading and typed method calls in one shot — see `client()`.
	 */
	async useSchema(
		serviceName: string,
		methodName: string,
		spec: SchemaSpec,
	): Promise<void> {
		const pair = await buildSchemaPair({
			...spec,
			// Propagate method into ProtoFileSpec so service-block auto-resolution
			// can find the right request/response types.
			...("protoFile" in spec && !spec.method ? { method: methodName } : {}),
		} as SchemaSpec);
		this._schemaRegistry.set(serviceName, methodName, pair);
	}

	/**
	 * High-level typed caller: reads the `.proto` file once, registers all
	 * methods in its `service` block as outgoing dependencies, loads schemas,
	 * and returns a proxy object with typed method calls.
	 *
	 * ```ts
	 * const payment = await sb.client("payment-svc", "./payment.proto");
	 * await sb.start();
	 * const res = await payment.Charge({ userId: "u", amount: 100 });
	 *
	 * // streaming method auto-detected from `rpc Generate(...) returns (stream Chunk)`
	 * for await (const chunk of payment.Generate({ prompt: "..." })) { ... }
	 * ```
	 *
	 * Must be called BEFORE `start()` — declared dependencies are sent in the
	 * initial RegisterRequest. Pass `methods` to bind only a subset; otherwise
	 * every method in any service block is exposed.
	 */
	async client(
		serviceName: string,
		protoFile: string,
		opts?: { methods?: string[]; callDefaults?: CallOpts },
	): Promise<TypedClient> {
		const all = await extractServiceMethods(protoFile);
		const allowed = opts?.methods ? new Set(opts.methods) : null;
		const selected = allowed ? all.filter((m) => allowed.has(m.name)) : all;
		if (selected.length === 0) {
			throw new Error(
				`rpc: client(${serviceName}, ${protoFile}): no methods to bind`,
			);
		}

		// Register outgoing dependency for each method.
		this._registry.service(serviceName, {
			rpc: selected.map((m) => m.name),
		});

		// Load schema for every method up front.
		for (const m of selected) {
			const pair = await buildSchemaPair({
				protoFile,
				input: m.requestType,
				output: m.responseType,
			});
			this._schemaRegistry.set(serviceName, m.name, pair);
		}

		const proxy = {} as Record<string, unknown>;
		for (const m of selected) {
			if (m.responseStream) {
				proxy[m.name] = (req: unknown, callOpts?: CallOpts) =>
					this.stream(
						serviceName,
						m.name,
						req,
						mergeOpts(opts?.callDefaults, callOpts),
					);
			} else {
				proxy[m.name] = (req: unknown, callOpts?: CallOpts) =>
					this.rpc.call(
						serviceName,
						m.name,
						req,
						mergeOpts(opts?.callDefaults, callOpts),
					);
			}
		}
		return proxy as TypedClient;
	}

	/**
	 * Server-side streaming RPC. Returns an AsyncIterable of decoded chunks.
	 * Cancelling the for-await loop (break/return) closes the underlying gRPC
	 * stream which propagates to the callee.
	 */
	stream<Req = unknown, Chunk = unknown>(
		serviceName: string,
		methodName: string,
		payload: Req,
		opts?: CallOpts,
	): AsyncIterable<Chunk> {
		if (!this._rpcClient) {
			throw new Error(
				"ServiceBridge: rpc client not ready — call start() and wait for 'connected' event before calling sb.stream()",
			);
		}
		const merged: CallOpts = { ...this.opts.callDefaults, ...(opts ?? {}) };
		return this._rpcClient.stream<Req, Chunk>(
			serviceName,
			methodName,
			payload,
			merged,
		);
	}

	/**
	 * Returns the identity of the current live session, or `null` if not
	 * connected (before first Welcome, during reconnect, or after stop()).
	 */
	identity(): Identity | null {
		return this.currentIdentity;
	}

	/**
	 * Live snapshot of the service registry grouped by serviceName. Each entry
	 * carries both the method descriptors visible to this caller (subject to
	 * outgoing-dep subscriptions) и список текущих живых инстансов с их
	 * endpoint'ами (`callEndpoint` для gRPC, `httpEndpoint` для HTTP-сервера
	 * пользователя по ADR 0001).
	 */
	serviceMap(): ReadonlyMap<string, ServiceMapEntry> {
		const blank = (): ServiceMapEntry => ({
			methods: [],
			instances: [],
			eventSubscriptions: [],
			outgoingCalls: [],
		});
		const result = new Map<string, ServiceMapEntry>();
		for (const m of this._watchStream.snapshot().values()) {
			const entry = result.get(m.serviceName) ?? blank();
			entry.methods.push(m);
			result.set(m.serviceName, entry);
		}
		for (const i of this._watchStream.instancesSnapshot().values()) {
			const entry = result.get(i.serviceName) ?? blank();
			entry.instances.push(i);
			result.set(i.serviceName, entry);
		}
		// ADR-0014 enrichment. Subscriptions are keyed by serviceId — find the
		// matching ServiceMapEntry by walking known instances/methods.
		const byServiceId = new Map<string, ServiceMapEntry>();
		for (const [name, entry] of result) {
			void name;
			for (const m of entry.methods) byServiceId.set(m.serviceId, entry);
			for (const i of entry.instances) byServiceId.set(i.serviceId, entry);
		}
		for (const es of this._watchStream.eventSubscriptionsSnapshot().values()) {
			const entry = byServiceId.get(es.serviceId);
			if (entry) entry.eventSubscriptions.push(es);
		}
		for (const oc of this._watchStream.outgoingCallsSnapshot().values()) {
			const entry = byServiceId.get(oc.callerServiceId);
			if (entry) entry.outgoingCalls.push(oc);
		}
		return result;
	}

	/**
	 * ADR-0014: the caller's last-known PolicyEvaluation as pushed by the
	 * runtime via `RegistrySnapshot.policy`. `null` until the first snapshot
	 * frame arrives. Updated automatically when the operator edits policy and
	 * Postgres NOTIFY fires → runtime re-emits.
	 */
	policyEvaluation(): PolicyEvaluation | null {
		return this._watchStream.policyEvaluation();
	}

	// ── internals ──────────────────────────────────────────────────────────────

	private emit<K extends keyof EventMap>(event: K, data: EventMap[K]): void {
		const list = this.handlers.get(event) ?? [];
		for (const h of list) {
			(h as Handler<K>)(data);
		}
	}

	private async connect(attempt: number): Promise<void> {
		if (this.stopped) return;
		try {
			const key = parseBootstrapKey(this.rawKey);
			const result = await this.opts.provisionFn(this.url, key);
			await this.openSession(result, attempt);
		} catch (err) {
			const sbErr = new ServiceBridgeError("provision", err);
			if (!isRetryable(sbErr.code)) {
				this.emit("disconnected", { reason: sbErr.message, error: sbErr });
				void this.stop();
				return;
			}
			this.scheduleReconnect(attempt + 1, sbErr.message);
		}
	}

	private async openSession(
		prov: ProvisionResult,
		attempt: number,
	): Promise<void> {
		const creds = this.buildMTLSCredentials(prov);
		const client = this.opts.clientFactory(this.url, creds);
		const stream = openControlStream(client);
		this.controlClient = client;
		this.lastProvision = prov;

		// Wire up RPC infrastructure BEFORE building RegisterRequest — call_endpoint
		// must be present in the very first registration so callers see it.
		await this.ensureRpcReady(prov, creds);

		const registryClient = this.opts.registryClientFactory(this.url, creds);
		// ADR-0014: subscribe to PolicyEvaluation BEFORE start() so the first
		// snapshot frame's `policy.warnings` triggers `console.warn` + the
		// `policy_violation` event for each violation. Listener is idempotent
		// across reconnects; restart() preserves it.
		this._watchStream.onPolicyEvaluation((policy) => {
			for (const w of policy.warnings) {
				this.emitPolicyViolation({
					declaration: w.declaration,
					value: w.value,
					denySide: w.denySide,
					reason: w.reason,
				});
			}
			if (this.opts.failOnPolicyViolation && policy.warnings.length > 0) {
				// Surface as a clear ServiceBridgeError; the upcoming connect
				// loop catches and emits 'disconnected'.
				const message = policy.warnings
					.map((w) => `${w.declaration} ${w.value}: ${w.reason}`)
					.join("; ");
				const err = new ServiceBridgeError(
					"policy",
					new Error(`policy violations on start: ${message}`),
				);
				this.emit("disconnected", { reason: err.message, error: err });
				void this.stop();
			}
		});
		this._watchStream.start(
			this._registry.buildRegisterRequest(),
			registryClient,
			(_err) => {},
		);

		const callbacks: SessionCallbacks = {
			onWelcome: (welcome) => {
				this.scheduleCertRefresh(prov);
				this.currentIdentity = {
					sessionId: welcome.sessionId,
					serviceId: welcome.serviceId,
					serviceName: welcome.serviceName,
					instanceId: prov.instanceId,
				};
				this._telemetryInstanceId = prov.instanceId;
				this.maybeStartWorkflowSubscriber();
				this.maybeStartJobSubscriber();
				this.emit("connected", {
					sessionId: welcome.sessionId,
					serviceId: welcome.serviceId,
					serviceName: welcome.serviceName,
				});
			},
			onDrain: (reason) => {
				if (!this.stopped) {
					this.emit("disconnected", { reason: `drain: ${reason}` });
				}
			},
			onError: (err) => {
				if (!this.stopped) {
					this.scheduleReconnect(attempt + 1, err.message);
				}
			},
			onEnd: () => {
				this.clearTimers();
				if (!this.stopped) {
					this.scheduleReconnect(attempt + 1, "stream ended");
				}
			},
		};

		this.session = new Session(
			stream,
			callbacks,
			this._watchStream,
			registryClient,
		);
	}

	// ensureRpcReady starts the inbound CallServer (if advertise is set) and
	// wires up the outbound ProxyTransport + DirectTransport + RpcClient.
	// Called from openSession once a ProvisionResult is in hand. Idempotent
	// across reconnects; refreshes DirectTransport creds on cert rotation.
	private async ensureRpcReady(
		prov: ProvisionResult,
		creds: grpc.ChannelCredentials,
	): Promise<void> {
		// Outbound side: proxy + direct transports.
		if (!this._proxyTransport) {
			this._proxyTransport = new ProxyTransport(this.url, creds);
		}

		const directCreds = {
			caChainDer: Buffer.from(prov.caChainDer),
			leafCertDer: Buffer.from(prov.certDer),
			privateKeyDer: Buffer.from(prov.privateKeyDer),
			notAfterUnix: prov.notAfterUnix,
		};
		if (this._directTransport) {
			this._directTransport.updateCredentials(directCreds);
		} else {
			this._directTransport = new DirectTransport(directCreds);
		}

		if (!this._rpcClient) {
			this._rpcClient = new RpcClient(
				this._proxyTransport,
				this._directTransport,
				this._instanceCache,
				this._schemaRegistry.asResolver(),
				this.currentIdentity?.serviceId ?? "",
				this._cb,
				this._lb,
				this,
			);
		}

		// Events infrastructure: EventsClient → Drainer → Publisher (idempotent).
		if (!this._eventsClient) {
			this._eventsClient = new EventsClient(this.url, creds);
		}
		if (this._storage && !this._publisher) {
			const maxOutboxRows = this.opts.maxOutboxRows;
			const batchSize = this.opts.eventsDrainerBatch;
			const drainer = new Drainer({
				storage: this._storage,
				rpcClient: this._eventsClient,
				identity: () => this.currentIdentity,
				batchSize,
				logger: { warn: console.warn, error: console.error },
				onPolicyViolation: (v) => this.emitPolicyViolation(v),
			});
			drainer.start();
			this._drainer = drainer;
			this._publisher = new Publisher({
				storage: this._storage,
				rpcClient: this._eventsClient,
				schemaIndex: this._schemaIndexAdapter,
				drainer,
				identity: () => this.currentIdentity,
				maxOutboxRows,
				logger: { warn: console.warn, error: console.error },
				sb: this,
				// Propagate ALS trace context so events published inside a
				// workflow run (or any wrapped scope) inherit the parent trace.
				xSbTraceFn: () => {
					const ctx = currentTraceContext();
					if (!ctx) return "";
					return formatXSbTrace(ctx.traceId, ctx.parentOpId);
				},
			});
		}

		// Telemetry transport: idempotent across reconnects. Creds change on cert
		// rotation but the gRPC channel underneath the TelemetryClient already
		// re-resolves; we keep the same client + transport instance.
		if (this._telemetryEnabled && !this._telemetryClient) {
			this._telemetryClient = new TelemetryClient(this.url, creds);
			this._telemetryTransport = new TelemetryTransport({
				client: adaptTelemetryClient(this._telemetryClient),
				ring: this._telemetryRing,
				// No default onDrop: a library must not spam the host's console on
				// every drop-count tick. Drop observability is host opt-in via the
				// TelemetryTransportOptions.onDrop hook.
			});
			await this._telemetryTransport.start();
			this._processSampler = new ProcessSampler(
				this._telemetryRing,
				() => this._telemetryInstanceId,
			);
			this._processSampler.start();
		}

		// Workflows: WorkflowsClient is the single channel used by both caller-side
		// ops (WorkflowDomain.start/signal/cancel/...) and the owner-side subscriber.
		// Attached on every (re)connect — creds change on cert rotation, but we
		// reuse the same client; gRPC re-resolves underneath. Subscriber starts
		// only when this service has at least one workflow handler registered.
		if (!this._workflowsClient) {
			this._workflowsClient = new WorkflowsClient(this.url, creds);
			this.workflow._attachRpc(this._workflowsClient);
		}

		if (!this._jobsClient) {
			this._jobsClient = new JobsClient(this.url, creds);
		}

		// Events subscriber: long-lived bidi Subscribe stream (idempotent).
		if (this._storage && this._eventsClient && !this._subscriber) {
			this._subscriber = new Subscriber({
				rpcClient: this._eventsClient,
				schemaIndex: this._schemaIndexAdapter,
				identity: () => {
					const id = this.currentIdentity;
					return id
						? { serviceId: id.serviceId, instanceId: id.instanceId }
						: null;
				},
				handlers: () => this.handlersForSubscriber(),
				maxInFlight: this.opts.eventsMaxInFlight,
				logger: { warn: console.warn, error: console.error },
				runWithTrace: this.runHandlerWithTrace,
				sb: this,
			});
			this._subscriber.start();
		}

		// Inbound side — only when advertise is configured.
		if (!this.opts.advertise || this._callServer) return;
		const cs = new CallServer(
			this._registry._handle.asDispatchPort(),
			{
				caChainDer: Buffer.from(prov.caChainDer),
				leafCertDer: Buffer.from(prov.certDer),
				privateKeyDer: Buffer.from(prov.privateKeyDer),
			},
			() => this._watchStream.policyEvaluation(),
		);
		try {
			const endpoint = await cs.start(this.opts.advertise);
			this._callServer = cs;
			this._registry.setCallEndpoint(endpoint);
		} catch (err) {
			// CallServer bind failed — treat as fatal for this attempt, surface as
			// disconnected and let the reconnect cycle retry.
			this.emit("disconnected", {
				reason: `call-server bind: ${(err as Error).message}`,
			});
		}
	}

	private buildMTLSCredentials(prov: ProvisionResult): grpc.ChannelCredentials {
		const caChainPem = derToPem(prov.caChainDer, "CERTIFICATE");
		const certPem = derToPem(prov.certDer, "CERTIFICATE");
		const keyPem = derToPem(prov.privateKeyDer, "PRIVATE KEY");

		// Server cert has no SAN; chain validates against the embedded CA cert.
		const verifyOptions: Parameters<typeof grpc.credentials.createSsl>[3] = {
			checkServerIdentity: () => undefined,
		};

		return grpc.credentials.createSsl(
			caChainPem,
			keyPem,
			certPem,
			verifyOptions,
		);
	}

	// Owner-side workflow subscriber: started on first Welcome after start(),
	// only when at least one workflow handler is registered. Idempotent across
	// reconnects/rotations — the subscriber's reconnect ladder owns its own
	// stream lifecycle and the WorkflowsClient gRPC channel re-resolves under
	// the hood on cert rotation.
	private maybeStartWorkflowSubscriber(): void {
		if (this._workflowSubscriber) return;
		if (!this._workflowsClient) return;
		const id = this.currentIdentity;
		if (!id) return;
		const hasWorkflowHandlers = this._registry._handle._entries.some(
			(e) => e.type === MethodType.METHOD_TYPE_WORKFLOW,
		);
		if (!hasWorkflowHandlers) return;
		const rpc = this._workflowsClient;
		const telemetryApi = this._telemetryApi;
		const sub = new WorkflowSubscriber({
			rpc,
			serviceId: id.serviceId,
			instanceId: id.instanceId,
			deps: {
				sb: { rpc: this.rpc, event: this.event, workflow: this.workflow },
				ops: makeRuntimeOps(rpc, () => this.currentIdentity?.instanceId ?? ""),
				// Step span emission (T-022 / ADR-0026 nesting): the runner calls
				// this around every executed unit — each step, each fanout group,
				// each fanout branch, each compensation. We open one USER.SUBOP
				// op (parent = the current trace context, i.e. the run root or an
				// enclosing group/branch span) and run the unit inside a child
				// context rooted at that span's op_id. That child context is what
				// makes the unit's nested sb.rpc.call / sb.event.publish /
				// sub-workflow.start emit X-SB-Trace with parent = the step span,
				// turning a flat trace into the run → step → op tree. Compensation
				// units carry is_compensation + compensates_for_step_id so the UI
				// correlates the compensation op back to the forward step that
				// shares its step_id.
				wrapStep: async (info, fn) => {
					const meta: Record<string, unknown> = {
						step_id: info.stepId,
						step_name: info.stepName,
						workflow_run_id: info.runId,
					};
					if (info.isCompensation) {
						meta.is_compensation = true;
						meta.compensates_for_step_id = info.compensatesForStepId ?? "";
					}
					const subject =
						info.role === "compensation"
							? `compensate:${info.compensatesForStepId ?? info.stepId}`
							: `${info.role}:${info.stepId}`;
					const handle = telemetryApi.startOp({
						channel: Channel.USER,
						kind: UserSubOp,
						subject,
						businessKey: info.runId,
						metaJson: Buffer.from(JSON.stringify(meta)),
					});
					const parent = currentTraceContext();
					const scoped = parent
						? () => runWithTrace(childContext(parent, handle.opId), fn)
						: fn;
					try {
						const out = await scoped();
						handle.end(Status.SUCCESS);
						return out;
					} catch (err) {
						handle.end(
							Status.ERROR,
							err instanceof Error ? err.message : String(err),
						);
						throw err;
					}
				},
			},
			logger: { warn: console.warn, error: console.error },
			lookupLocalGraph: (name) => {
				const entry = this._registry._handle._entries.find(
					(e) => e.type === MethodType.METHOD_TYPE_WORKFLOW && e.name === name,
				);
				return entry ? (entry.fn as unknown as Step[]) : null;
			},
			sb: this,
		});
		sub.start();
		this._workflowSubscriber = sub;
	}

	// Wrap a handler invocation in the inbound trace context so nested
	// sb.rpc.call / sb.event.publish / sb.workflow.start inherit the job /
	// event op as their parent. Without this, work spawned inside a handler
	// detaches into its own root trace. Mirrors the WorkflowSubscriber's own
	// runWithTrace wrapping.
	private readonly runHandlerWithTrace = (
		xSbTrace: string,
		fn: () => Promise<void>,
	): Promise<void> => {
		const parsed = parseXSbTrace(xSbTrace);
		return parsed ? runWithTrace(parsed, fn) : fn();
	};

	private maybeStartJobSubscriber(): void {
		if (this._jobSubscriber) return;
		if (!this._jobsClient) return;
		const id = this.currentIdentity;
		if (!id) return;
		if (this.job.size() === 0) return;

		// Job definitions were already sent to the runtime via
		// Registry.RegisterAndWatch as IncomingMethod{type=JOB,...}; here we
		// only open the dispatch subscriber stream.
		const sub = new JobSubscriber({
			rpcClient: this._jobsClient,
			identity: () => this.currentIdentity,
			domain: this.job,
			logger: { warn: console.warn, error: console.error },
			runWithTrace: this.runHandlerWithTrace,
		});
		sub.start();
		this._jobSubscriber = sub;
	}

	private handlersForSubscriber(): EventHandler[] {
		return this._registry._handle._entries
			.filter((e) => e.type === MethodType.METHOD_TYPE_EVENT)
			.map((e) => ({
				pattern: e.name,
				fn: e.fn as (payload: unknown) => Promise<void> | void,
			}));
	}

	private scheduleCertRefresh(prov: ProvisionResult): void {
		if (this.certRefreshTimer) {
			clearTimeout(this.certRefreshTimer);
			this.certRefreshTimer = null;
		}
		const now = BigInt(Math.floor(Date.now() / 1000));
		const remainingMs =
			Number(prov.notAfterUnix - now) * 1000 -
			this.opts.certRefreshLeadMs +
			randomJitter(this.opts.certRefreshJitterMs);
		if (remainingMs <= 0) {
			void this.rotateCert();
			return;
		}
		this.certRefreshTimer = setTimeout(() => {
			void this.rotateCert();
		}, remainingMs);
	}

	/**
	 * Overlap rotation:
	 *   1. Re-provision (new cert).
	 *   2. Open new mTLS channel + Control.Open stream.
	 *   3. Wait for Welcome on the new stream.
	 *   4. Only then close the old session.
	 *
	 * On failure: schedule a reconnect so the application sees a `reconnecting`
	 * event; the next reconnect cycle will re-provision and proceed.
	 */
	private async rotateCert(): Promise<void> {
		if (this.stopped) return;
		if (!this.controlClient || !this.lastProvision) {
			// No live session — nothing to refresh from. Let reconnect handle it.
			this.scheduleReconnect(1, "rotation: no live session");
			return;
		}

		let newProv: ProvisionResult;
		try {
			newProv = await this.opts.refreshFn(
				this.controlClient,
				this.lastProvision,
			);
		} catch (err) {
			const sbErr = new ServiceBridgeError("rotation refresh", err);
			if (!isRetryable(sbErr.code)) {
				this.emit("disconnected", { reason: sbErr.message, error: sbErr });
				void this.stop();
				return;
			}
			this.scheduleReconnect(1, sbErr.message);
			return;
		}

		const newCreds = this.buildMTLSCredentials(newProv);
		const newRegistryClient = this.opts.registryClientFactory(
			this.url,
			newCreds,
		);
		const newClient = this.opts.clientFactory(this.url, newCreds);
		const newStream = openControlStream(newClient);
		const oldSession = this.session;

		const swapPromise = new Promise<void>((resolve, reject) => {
			const timer = setTimeout(() => {
				reject(new Error("rotation: handshake timeout"));
			}, this.opts.rotationHandshakeTimeoutMs);

			let welcomed = false;
			const cbs: SessionCallbacks = {
				onWelcome: (welcome) => {
					welcomed = true;
					this.session = newSession;
					this.scheduleCertRefresh(newProv);
					this.currentIdentity = {
						sessionId: welcome.sessionId,
						serviceId: welcome.serviceId,
						serviceName: welcome.serviceName,
						instanceId: newProv.instanceId,
					};
					this._telemetryInstanceId = newProv.instanceId;
					this.maybeStartWorkflowSubscriber();
					this.maybeStartJobSubscriber();
					this.emit("connected", {
						sessionId: welcome.sessionId,
						serviceId: welcome.serviceId,
						serviceName: welcome.serviceName,
					});
					clearTimeout(timer);
					resolve();
				},
				onDrain: (reason) => {
					if (!this.stopped)
						this.emit("disconnected", { reason: `drain: ${reason}` });
				},
				onError: (err) => {
					clearTimeout(timer);
					reject(err);
				},
				onEnd: () => {
					if (!welcomed) {
						clearTimeout(timer);
						reject(new Error("rotation: new stream ended before welcome"));
					}
				},
			};
			const newSession = new Session(
				newStream,
				cbs,
				this._watchStream,
				newRegistryClient,
			);
		});

		try {
			await swapPromise;
			// Close the old session only after the new one is fully usable.
			oldSession?.close();
			this._watchStream.restart(
				this._registry.buildRegisterRequest(),
				newRegistryClient,
				(_err) => {},
			);
		} catch (err) {
			// Roll back to the previous session: keep oldSession alive and tear
			// down the partially-built new one. Schedule a reconnect to retry.
			if (this.session !== oldSession) {
				this.session?.close();
				this.session = oldSession;
			}
			this.scheduleReconnect(
				1,
				`rotation: ${err instanceof Error ? err.message : String(err)}`,
			);
		}
	}

	private scheduleReconnect(attempt: number, reason: string): void {
		if (this.stopped) return;
		if (
			this.opts.reconnectAttempts > 0 &&
			attempt > this.opts.reconnectAttempts
		) {
			this.emit("disconnected", { reason: "exhausted" });
			void this.stop();
			return;
		}
		this.emit("reconnecting", {
			attempt,
			delayMs: this.opts.reconnectIntervalMs,
			reason,
		});
		setTimeout(() => {
			void this.connect(attempt);
		}, this.opts.reconnectIntervalMs);
	}

	private clearTimers(): void {
		if (this.certRefreshTimer) {
			clearTimeout(this.certRefreshTimer);
			this.certRefreshTimer = null;
		}
	}
}
