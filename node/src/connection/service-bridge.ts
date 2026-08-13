import "reflect-metadata";
import * as grpc from "@grpc/grpc-js";
import { EventDomain } from "../events/domain";
import { Drainer } from "../events/drainer";
import type { SchemaIndex } from "../events/publisher";
import { Publisher } from "../events/publisher";
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
import type { CaptureMode } from "../telemetry/payload-capture";
import { ProcessSampler } from "../telemetry/process-sampler";
import { TelemetryRing } from "../telemetry/ring";
import { childContext } from "../telemetry/trace-context";
import {
	adaptTelemetryClient,
	TelemetryTransport,
} from "../telemetry/transport";
import { formatXSbTrace, parseXSbTrace } from "../telemetry/wire-trace";
import { reconnectDelay } from "../utils/reconnect-ladder";
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
	// ADR-0004 service-map enrichment (populated for caller's own service +
	// services in caller's outgoing-dep scope). Empty arrays when the runtime
	// doesn't carry that info for this service in the current snapshot.
	eventSubscriptions: EventSubscriptionDescriptor[];
	outgoingCalls: OutgoingCallDescriptor[];
}
export type { CallOpts } from "../rpc/client";
export type { AdvertiseConfig } from "../rpc/server";
export type { SchemaSpec } from "../serde/serializer";

// Reconnect configuration. When reconnectIntervalMs is left unset the SDK uses
// the shared jittered reconnect-ladder so a fleet that loses the runtime at once
// does not hammer it back in lockstep on a fixed 3s tick (the production OOM
// reconnect storm). An explicit reconnectIntervalMs overrides the ladder with a
// flat delay for callers that tune it deliberately.
const RECONNECT_ATTEMPTS = 3; // 0 = unlimited

// Events defaults (overridden via ServiceBridgeOptions in ensureRpcReady).
const DEFAULT_MAX_OUTBOX_ROWS = 100_000;
const DEFAULT_DRAINER_BATCH = 50;
const DEFAULT_EVENTS_MAX_IN_FLIGHT = 32;
// Default telemetry ops-ring byte budget. Sized for the dense USER.SUBOP
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
 * Build the TelemetryAPI surface. Identity is read at emission time, never
 * captured at handle-construction time — user code may take a logger or a
 * counter before start(), and those handles must follow the identity once it
 * appears. Emissions made before the first Welcome carry an empty instance_id.
 * @internal
 */
function makeTelemetryAPI(
	ring: TelemetryRing,
	getInstanceId: () => string,
	_getServiceId: () => string,
	getEnabled: () => boolean,
	getCaptureModeForChannel: (channel: Channel) => CaptureMode,
	getPayloadMaxBytes: () => number,
): TelemetryAPI {
	const log = makeLazyLogger(ring, getInstanceId);
	return {
		startOp(params) {
			return OpHandle.start(ring, {
				...params,
				effectiveCaptureMode: getCaptureModeForChannel(params.channel),
				payloadMaxBytes: getPayloadMaxBytes(),
			});
		},
		enabled: getEnabled,
		captureModeForChannel: getCaptureModeForChannel,
		log,
		counter(name, labels) {
			const series = lazySeries(getInstanceId, (id) =>
				makeCounter(ring, id, name, labels),
			);
			return {
				inc(amount) {
					series().inc(amount);
				},
			};
		},
		gauge(name, labels) {
			const series = lazySeries(getInstanceId, (id) =>
				makeGauge(ring, id, name, labels),
			);
			return {
				set(value) {
					series().set(value);
				},
			};
		},
		histogram(name, unit, labels) {
			const series = lazySeries(getInstanceId, (id) =>
				makeHistogram(ring, id, name, unit, labels),
			);
			return {
				observe(value) {
					series().observe(value);
				},
			};
		},
	};
}

/**
 * Binds a metric handle to the series of the *current* instance_id and rebinds
 * whenever that identity changes (first Welcome, every cert rotation). The
 * aggregator keys a series on (kind, name, instance_id, labels) and the handle
 * it returns mutates that series object directly, so resolving once at
 * construction pins a counter taken before start() to the empty instance_id
 * forever — it would grow in a row parallel to the one the same counter gets
 * after connecting, and the user would see two series for one counter. The
 * resolved handle is cached until the identity actually changes, so the steady
 * state costs one string comparison per emit rather than a series lookup.
 * @internal
 */
function lazySeries<T>(
	getInstanceId: () => string,
	build: (instanceId: string) => T,
): () => T {
	let boundId: string | null = null;
	let handle: T | null = null;
	return () => {
		const id = getInstanceId();
		if (handle === null || boundId !== id) {
			boundId = id;
			handle = build(id);
		}
		return handle;
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
	 * Whether the runtime currently wants telemetry at all
	 * (`CaptureModes.telemetry_enabled`). `true` until the first registry
	 * snapshot arrives (fail-safe), and it flips on a live connection whenever
	 * the operator changes the setting — so callers must invoke it per emission
	 * rather than caching the result.
	 *
	 * Emission gate for hot paths: `startOp` builds a START frame and pushes it
	 * into the ring regardless of this flag, so deferring the work inside
	 * `startOp` saves nothing. Check this first and skip the whole op block —
	 * including the meta the caller would otherwise serialize for it.
	 */
	enabled(): boolean;
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
	 * ADR-0004: when `true`, any policy violation reported by the runtime in
	 * the registry snapshot's `PolicyEvaluation.warnings` makes `start()`
	 * surface a `disconnected` event with reason='policy' and the SDK stops.
	 * Default `false` — warnings only (logged via console.warn + emitted as
	 * `policy_violation` events).
	 */
	failOnPolicyViolation?: boolean;
	/** Local SQLite outbox directory. Default "./.servicebridge". */
	dataDir?: string;
	/** Max rows kept in the event outbox before publish back-pressures. Default 100000. */
	maxOutboxRows?: number;
	/** Rows the events drainer pulls per tick. Default 50. */
	eventsDrainerBatch?: number;
	/** Max in-flight inbound events the subscriber processes concurrently. Default 32. */
	eventsMaxInFlight?: number;
	/**
	 * Max inbound RPC handlers running concurrently on the Call server. Default
	 * 256. Bounds callee-side memory: every admitted call holds a decoded request
	 * plus whatever the handler allocates.
	 */
	rpcMaxConcurrentCalls?: number;
	/**
	 * Max inbound calls allowed to wait for a free execution slot. Defaults to
	 * `rpcMaxConcurrentCalls`. Past this depth the caller gets
	 * `RESOURCE_EXHAUSTED` — load is shed, not queued. `0` rejects any call that
	 * cannot start immediately.
	 */
	rpcMaxQueuedCalls?: number;
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
	/** Test hook: skip TelemetryClient + TelemetryTransport startup. */
	_disableTelemetryTransport?: boolean;
}

interface ResolvedOptions {
	// undefined → use the jittered reconnect-ladder; a number → flat override.
	reconnectIntervalMs: number | undefined;
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
	dataDir: string;
	maxOutboxRows: number;
	eventsDrainerBatch: number;
	eventsMaxInFlight: number;
	// undefined → CallServer applies its own defaults (256 / equal to
	// concurrency). Kept unresolved so the bound lives in one place, next to the
	// semaphore that enforces it, instead of being restated here.
	rpcMaxConcurrentCalls: number | undefined;
	rpcMaxQueuedCalls: number | undefined;
	_disableTelemetryTransport: boolean;
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
	// Bumped by stop(). Every async lifecycle path (connect → openSession →
	// ensureRpcReady, rotateCert) captures it on entry and re-checks after each
	// await: a stop() landing mid-flight must not let the resumed continuation
	// rebuild the stack behind it.
	private generation = 0;
	private session: Session | null = null;
	private controlClient: ControlClient | null = null;
	// Registry channel of the current live session. Owned here (not by Session or
	// WatchStream — those only drive the stream on top of it) so every (re)connect
	// and rotation can close the predecessor channel and stop() closes the last.
	private registryClient: RegistryClient | null = null;
	private lastProvision: ProvisionResult | null = null;
	private certRefreshTimer: ReturnType<typeof setTimeout> | null = null;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	private currentIdentity: Identity | null = null;

	private readonly _registry: Registry = new Registry(() =>
		this.onRegistrationChanged(),
	);
	private readonly _watchStream = new WatchStream();
	private readonly _instanceCache = new InstanceCache();
	private readonly _schemaRegistry = new SchemaRegistry();
	private _started = false;
	private _watchListenersRegistered = false;
	private _callServer: CallServer | null = null;
	// Address the inbound CallServer is actually bound to. Reused when the server
	// is rebuilt on cert rotation so an advertise port of 0 does not move the
	// endpoint callers already cached.
	private _callServerAdvertise: AdvertiseConfig | null = null;
	// ProvisionResult the cert-bound side channels were built from. Compared by
	// identity: a transport reconnect reuses the cached ProvisionResult object, a
	// fresh Provision or a rotation yields a new one.
	private _rpcProvision: ProvisionResult | null = null;
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
	// подключения; они буферятся в ring до старта transport'а.
	// Whether telemetry is wanted at all is not mirrored here: the runtime-pushed
	// value lives in WatchStream and is read on demand, so the transport gate and
	// `sb.telemetry.enabled()` can never drift apart.
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

	/** Workflow domain — define workflows, register step handlers, start/execute runs. */
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
			reconnectIntervalMs: options.reconnectIntervalMs,
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
			dataDir: options.dataDir ?? "./.servicebridge",
			maxOutboxRows: options.maxOutboxRows ?? DEFAULT_MAX_OUTBOX_ROWS,
			eventsDrainerBatch: options.eventsDrainerBatch ?? DEFAULT_DRAINER_BATCH,
			eventsMaxInFlight:
				options.eventsMaxInFlight ?? DEFAULT_EVENTS_MAX_IN_FLIGHT,
			rpcMaxConcurrentCalls: options.rpcMaxConcurrentCalls,
			rpcMaxQueuedCalls: options.rpcMaxQueuedCalls,
			_disableTelemetryTransport: hooks._disableTelemetryTransport ?? false,
		};

		// Telemetry wiring (ADR-0007). Ring is owned by the bridge so user code
		// may emit logs/ops/metrics before start(); they buffer in the ring and
		// the transport drains once it's up. The ops ring byte budget is the
		// internal DEFAULT_TELEMETRY_RING_SIZE — sized for dense workflow step-span
		// emission bursts (≈800 op frames). Not user-configurable.
		this._telemetryRing = new TelemetryRing({
			ops: DEFAULT_TELEMETRY_RING_SIZE,
		});

		// Telemetry API surface. Every runtime-pushed input is a getter, never a
		// constructor-time copy: identity appears at the first Welcome and changes
		// on cert rotation, while `enabled` and `payloadMaxBytes` change whenever
		// the operator edits telemetry settings on a live connection.
		this._telemetryApi = makeTelemetryAPI(
			this._telemetryRing,
			() => this._telemetryInstanceId,
			() => this.currentIdentity?.serviceId ?? "",
			() => this._watchStream.pushedTelemetryConfig().enabled,
			(channel) => this._watchStream.captureModeForChannel(channel),
			() => this._watchStream.pushedTelemetryConfig().payloadMaxBytes,
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
		this._instanceCache.bind(this._watchStream, this._cb);
		this._proxyTransport = null;
		this._directTransport = null;
		this._rpcClient = null;

		this.registerWatchListeners();

		await this.connect(1);
	}

	// registerWatchListeners subscribes the policy/telemetry-config listeners
	// onto the shared _watchStream exactly once per bridge. The _watchStream
	// instance outlives reconnects (openSession only restarts the underlying
	// gRPC stream, not the listener set), so registering here — not in
	// openSession — avoids re-adding a fresh closure on every reconnect, which
	// would otherwise fire each PolicyEvaluation N times after N reconnects.
	private registerWatchListeners(): void {
		if (this._watchListenersRegistered) return;
		this._watchListenersRegistered = true;
		// ADR-0004: PolicyEvaluation must be observed before the first snapshot
		// frame so `policy.warnings` triggers the `policy_violation` event for
		// each violation on initial connect.
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
				// Surface as a clear ServiceBridgeError; the connect loop catches
				// and emits 'disconnected'.
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
	}

	async stop(): Promise<void> {
		this.stopped = true;
		this._started = false;
		this.generation++;
		this.clearTimers();
		this._watchStream.stop();
		this.session?.close();
		this.session = null;
		// Close the live session's control + registry channels. Session.close()
		// only cancels the streams on top of them; the channels (and their backoff
		// timers) are owned here and must be released explicitly.
		this.controlClient?.close();
		this.controlClient = null;
		this.registryClient?.close();
		this.registryClient = null;
		this.currentIdentity = null;
		this._instanceCache.dispose();

		await this.closeCertBoundResources();

		this._directTransport?.close();
		this._directTransport = null;
		this._rpcProvision = null;
		this._callServerAdvertise = null;

		if (this._storage) {
			this._storage.close();
			this._storage = null;
		}
	}

	/**
	 * Tears down every resource whose TLS material is pinned at construction
	 * time. `grpc.credentials.createSsl` / `ServerCredentials.createSsl` copy the
	 * PEM into the channel, so a rotated leaf cert reaches them only through a
	 * rebuild. Subsystems that hold a client reference (drainer / publisher /
	 * subscriber, workflow and job subscribers, telemetry transport) go down with
	 * their channel and are rebuilt by `ensureRpcReady`.
	 *
	 * Order matters: CallServer first (stops accepting inbound work), telemetry
	 * before events so late logs from the events close paths still have a sink.
	 * DirectTransport is absent on purpose — it rotates in place via
	 * `updateCredentials`.
	 *
	 * Every field is detached before the first await so a stop() racing a
	 * rotation teardown cannot close the same resource twice.
	 */
	private async closeCertBoundResources(): Promise<void> {
		const callServer = this._callServer;
		const sampler = this._processSampler;
		const telemetryTransport = this._telemetryTransport;
		const telemetryClient = this._telemetryClient;
		const workflowSubscriber = this._workflowSubscriber;
		const workflowsClient = this._workflowsClient;
		const jobSubscriber = this._jobSubscriber;
		const jobsClient = this._jobsClient;
		const subscriber = this._subscriber;
		const drainer = this._drainer;
		const eventsClient = this._eventsClient;
		const proxyTransport = this._proxyTransport;

		this._callServer = null;
		this._processSampler = null;
		this._telemetryTransport = null;
		this._telemetryClient = null;
		this._workflowSubscriber = null;
		this._workflowsClient = null;
		this._jobSubscriber = null;
		this._jobsClient = null;
		this._subscriber = null;
		this._drainer = null;
		this._eventsClient = null;
		this._publisher = null;
		this._proxyTransport = null;
		this._rpcClient = null;

		if (callServer) await callServer.stop();

		sampler?.close();
		if (telemetryTransport) await telemetryTransport.stop();
		telemetryClient?.close();

		// Workflows teardown: subscriber → client. Subscriber owns the heartbeat
		// timers and the long-lived Subscribe stream; close() stops both.
		workflowSubscriber?.close();
		workflowsClient?.close();

		// Jobs teardown: subscriber → client.
		if (jobSubscriber) await jobSubscriber.stop();
		jobsClient?.close();

		// Events teardown: subscriber → drainer → events client.
		if (subscriber) await subscriber.stop();
		if (drainer) await drainer.stop();
		eventsClient?.close();

		proxyTransport?.close();
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
		// ADR-0004 enrichment. Subscriptions are keyed by serviceId — find the
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
	 * ADR-0004: the caller's last-known PolicyEvaluation as pushed by the
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

	// stale reports whether the lifecycle generation captured before an await is
	// no longer current — stop() ran while the caller was suspended, so anything
	// the continuation would build belongs to a torn-down bridge.
	private stale(gen: number): boolean {
		return this.stopped || gen !== this.generation;
	}

	private async connect(attempt: number): Promise<void> {
		if (this.stopped) return;
		const gen = this.generation;
		try {
			// Reuse the cached leaf cert on a transport-level reconnect instead of
			// re-running the expensive Bootstrap.Provision (a 64 MiB argon2 hash on
			// the runtime) every time the stream drops. A fresh Provision happens
			// only on the first connect or once the cached cert nears expiry; the
			// cert-refresh timer normally renews earlier via the lighter RefreshCert.
			const result =
				this.reusableProvision() ??
				(await this.opts.provisionFn(this.url, parseBootstrapKey(this.rawKey)));
			if (this.stale(gen)) return;
			await this.openSession(result, attempt, gen);
		} catch (err) {
			if (this.stale(gen)) return;
			const sbErr = new ServiceBridgeError("provision", err);
			if (!isRetryable(sbErr.code)) {
				this.emit("disconnected", { reason: sbErr.message, error: sbErr });
				void this.stop();
				return;
			}
			this.scheduleReconnect(attempt + 1, sbErr.message);
		}
	}

	/**
	 * Returns the cached provision when its leaf cert is still comfortably valid
	 * (more than certRefreshLeadMs remaining), so a reconnect reuses the existing
	 * identity instead of re-running the Bootstrap argon2 handshake. Returns null
	 * on the first connect or when the cert is near expiry — the caller then does
	 * a fresh Provision.
	 */
	private reusableProvision(): ProvisionResult | null {
		const prov = this.lastProvision;
		if (!prov) return null;
		const now = BigInt(Math.floor(Date.now() / 1000));
		const remainingMs = Number(prov.notAfterUnix - now) * 1000;
		if (remainingMs <= this.opts.certRefreshLeadMs) return null;
		return prov;
	}

	private async openSession(
		prov: ProvisionResult,
		attempt: number,
		gen: number,
	): Promise<void> {
		const creds = this.buildMTLSCredentials(prov);
		const client = this.opts.clientFactory(this.url, creds);
		// A transport-level reconnect lands here with the previous session's
		// control/registry channels still referenced. grpc-js channels are not
		// GC'd while their internal backoff timers live, so we must close the
		// predecessors explicitly before overwriting the fields — otherwise each
		// reconnect permanently leaks two TLS channels (the production OOM).
		this.controlClient?.close();
		this.registryClient?.close();
		this.registryClient = null;
		this.controlClient = client;
		this.lastProvision = prov;

		// Wire up RPC infrastructure BEFORE building RegisterRequest — call_endpoint
		// must be present in the very first registration so callers see it.
		await this.ensureRpcReady(prov, creds, gen);
		if (this.stale(gen)) return;

		const registryClient = this.opts.registryClientFactory(this.url, creds);
		this.registryClient = registryClient;
		this._watchStream.start(
			this._registry.buildRegisterRequest(),
			registryClient,
			(err) => {
				console.warn(
					`[ServiceBridge] registry watch stream error (auto-restarts): ${err.message}`,
				);
			},
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

		// Opened last and handed to Session in the same synchronous run: a
		// ClientReadableStream is an EventEmitter, and an 'error' arriving while no
		// listener is attached is an uncaught exception that kills the process
		// instead of a `reconnecting` event. Nothing may await between these two
		// statements.
		const stream = openControlStream(client);
		this.session = new Session(
			stream,
			callbacks,
			this._watchStream,
			registryClient,
		);
	}

	// ensureRpcReady starts the inbound CallServer (if advertise is set) and
	// wires up the outbound ProxyTransport + DirectTransport + RpcClient plus the
	// events / workflows / jobs / telemetry channels. Called from openSession and
	// from rotateCert once a ProvisionResult is in hand. Reuses everything while
	// the ProvisionResult is unchanged; rebuilds the cert-bound half when it is.
	private async ensureRpcReady(
		prov: ProvisionResult,
		creds: grpc.ChannelCredentials,
		gen: number,
	): Promise<void> {
		// grpc.credentials.createSsl copies the PEM material into the channel, so
		// a rotated leaf cert reaches these channels only through a rebuild —
		// otherwise they keep handshaking with the expired cert past notAfter while
		// Control and Registry look healthy.
		if (this._rpcProvision !== prov) {
			this._rpcProvision = prov;
			await this.closeCertBoundResources();
			if (this.stale(gen)) return;
		}

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
				() => this.currentIdentity?.serviceId ?? "",
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

		// Telemetry transport. Built into locals and adopted only after start()
		// resolves: a stop() racing the bind must not leave a started transport
		// behind with no owner to close it.
		// _disableTelemetryTransport is a test hook; the enable flag is the
		// runtime-pushed telemetry.enable value (default true until first snapshot),
		// read from the same place `sb.telemetry.enabled()` reads it.
		if (
			this._telemetryApi.enabled() &&
			!this._telemetryClient &&
			!this.opts._disableTelemetryTransport
		) {
			const telemetryClient = new TelemetryClient(this.url, creds);
			const transport = new TelemetryTransport({
				client: adaptTelemetryClient(telemetryClient),
				ring: this._telemetryRing,
				// No default onDrop: a library must not spam the host's console on
				// every drop-count tick. Drop observability is host opt-in via the
				// TelemetryTransportOptions.onDrop hook.
			});
			await transport.start();
			if (this.stale(gen)) {
				await transport.stop();
				telemetryClient.close();
				return;
			}
			this._telemetryClient = telemetryClient;
			this._telemetryTransport = transport;
			const sampler = new ProcessSampler(
				this._telemetryRing,
				() => this._telemetryInstanceId,
			);
			sampler.start();
			this._processSampler = sampler;
		}

		// Workflows: WorkflowsClient is the single channel used by both caller-side
		// ops (WorkflowDomain.start/signal/cancel/...) and the owner-side subscriber.
		// Subscriber starts only when this service has at least one workflow
		// handler registered.
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
				handlers: (name) => this._registry._handle.eventHandlers(name),
				maxInFlight: this.opts.eventsMaxInFlight,
				logger: { warn: console.warn, error: console.error },
				runWithTrace: this.runHandlerWithTrace,
				sb: this,
			});
			this._subscriber.start();
		}

		// Inbound side — only when advertise is configured. On a rotation rebuild
		// we re-bind the port the previous server actually got, so an advertise
		// port of 0 does not move the endpoint callers already resolved.
		if (!this.opts.advertise || this._callServer) return;
		const advertise = this._callServerAdvertise ?? this.opts.advertise;
		const cs = new CallServer(
			this._registry._handle.asDispatchPort(),
			{
				caChainDer: Buffer.from(prov.caChainDer),
				leafCertDer: Buffer.from(prov.certDer),
				privateKeyDer: Buffer.from(prov.privateKeyDer),
			},
			() => this._watchStream.policyEvaluation(),
			{
				maxConcurrentCalls: this.opts.rpcMaxConcurrentCalls,
				maxQueuedCalls: this.opts.rpcMaxQueuedCalls,
			},
		);
		try {
			const endpoint = await cs.start(advertise);
			if (this.stale(gen)) {
				await cs.stop();
				return;
			}
			this._callServer = cs;
			this._callServerAdvertise = {
				host: advertise.host,
				port: Number(endpoint.slice(endpoint.lastIndexOf(":") + 1)),
			};
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
			identity: () => this.currentIdentity,
			deps: {
				sb: { rpc: this.rpc, event: this.event, workflow: this.workflow },
				ops: makeRuntimeOps(rpc, () => this.currentIdentity?.instanceId ?? ""),
				// Step span emission (ADR-0003 nesting): the runner calls
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
		const gen = this.generation;
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
			if (this.stale(gen)) return;
			const sbErr = new ServiceBridgeError("rotation refresh", err);
			if (!isRetryable(sbErr.code)) {
				this.emit("disconnected", { reason: sbErr.message, error: sbErr });
				void this.stop();
				return;
			}
			this.scheduleReconnect(1, sbErr.message);
			return;
		}

		if (this.stale(gen)) return;

		const newCreds = this.buildMTLSCredentials(newProv);
		const newRegistryClient = this.opts.registryClientFactory(
			this.url,
			newCreds,
		);
		const newClient = this.opts.clientFactory(this.url, newCreds);
		const oldSession = this.session;
		const oldControlClient = this.controlClient;
		const oldRegistryClient = this.registryClient;

		const swapPromise = new Promise<void>((resolve, reject) => {
			const timer = setTimeout(() => {
				reject(new Error("rotation: handshake timeout"));
			}, this.opts.rotationHandshakeTimeoutMs);

			let welcomed = false;
			const cbs: SessionCallbacks = {
				onWelcome: (welcome) => {
					welcomed = true;
					this.session = newSession;
					// Adopt the new channels as the live pair. Ownership transfers
					// here so a later reconnect / stop() closes them; the old pair is
					// closed by the success block once the swap completes.
					this.controlClient = newClient;
					this.registryClient = newRegistryClient;
					// The rotated cert becomes the cache the next transport-level
					// reconnect reuses; without this, reusableProvision() keeps judging
					// freshness by the evicted cert and forces a full argon2 Provision.
					this.lastProvision = newProv;
					this.scheduleCertRefresh(newProv);
					this.currentIdentity = {
						sessionId: welcome.sessionId,
						serviceId: welcome.serviceId,
						serviceName: welcome.serviceName,
						instanceId: newProv.instanceId,
					};
					this._telemetryInstanceId = newProv.instanceId;
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
				// Both callbacks branch on `welcomed`: before Welcome they settle the
				// swap promise (rollback path), after it the promise is already
				// resolved and they are the only supervision the live session has —
				// без этого ротация оставляла сессию без надзора и обрыв стрима уходил
				// в тишину.
				onError: (err) => {
					if (!welcomed) {
						clearTimeout(timer);
						reject(err);
						return;
					}
					if (!this.stopped) this.scheduleReconnect(1, err.message);
				},
				onEnd: () => {
					if (!welcomed) {
						clearTimeout(timer);
						reject(new Error("rotation: new stream ended before welcome"));
						return;
					}
					this.clearTimers();
					if (!this.stopped) this.scheduleReconnect(1, "stream ended");
				},
			};
			// Opened and adopted in one synchronous run — see openSession for why an
			// unlistened 'error' on this stream is fatal.
			const newStream = openControlStream(newClient);
			const newSession = new Session(
				newStream,
				cbs,
				this._watchStream,
				newRegistryClient,
			);
		});

		try {
			await swapPromise;
		} catch (err) {
			if (this.stale(gen)) return;
			// Roll back to the previous session: keep oldSession alive and tear
			// down the partially-built new one. The new channels were never adopted
			// (no Welcome) so close them here, then schedule a reconnect to retry.
			if (this.session !== oldSession) {
				this.session?.close();
				this.session = oldSession;
			}
			this.controlClient = oldControlClient;
			this.registryClient = oldRegistryClient;
			newClient.close();
			newRegistryClient.close();
			this.scheduleReconnect(
				1,
				`rotation: ${err instanceof Error ? err.message : String(err)}`,
			);
			return;
		}

		if (this.stale(gen)) return;

		// Close the old session only after the new one is fully usable, then
		// release the old session's channels (Session.close cancels the stream
		// but not the underlying channel).
		oldSession?.close();
		oldControlClient?.close();
		oldRegistryClient?.close();
		this._watchStream.restart(
			this._registry.buildRegisterRequest(),
			newRegistryClient,
			(err) => {
				console.warn(
					`[ServiceBridge] registry watch stream error (auto-restarts): ${err.message}`,
				);
			},
		);

		// Rebuild every channel that pinned the previous leaf cert. Control and
		// Registry alone are not the connection: RPC, events, workflows, jobs,
		// telemetry and the inbound CallServer each hold their own TLS material.
		try {
			await this.ensureRpcReady(newProv, newCreds, gen);
		} catch (err) {
			this.scheduleReconnect(
				1,
				`rotation: rpc rewire: ${err instanceof Error ? err.message : String(err)}`,
			);
			return;
		}
		if (this.stale(gen)) return;
		this.maybeStartWorkflowSubscriber();
		this.maybeStartJobSubscriber();
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
		const delayMs = this.computeReconnectDelay(attempt);
		this.emit("reconnecting", {
			attempt,
			delayMs,
			reason,
		});
		if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = null;
			void this.connect(attempt);
		}, delayMs);
	}

	// computeReconnectDelay returns the backoff before the next reconnect. An
	// explicit reconnectIntervalMs is honoured as a flat delay (deliberate caller
	// tuning, deterministic for tests). Otherwise the shared jittered ladder is
	// used so a fleet reconnecting at once spreads its load across a window
	// instead of spiking the runtime on a fixed tick. `attempt` is the 1-based
	// next-attempt number; the ladder is 0-based, so attempt 1 → first rung.
	private computeReconnectDelay(attempt: number): number {
		if (this.opts.reconnectIntervalMs !== undefined) {
			return this.opts.reconnectIntervalMs;
		}
		return reconnectDelay(attempt - 1);
	}

	private clearTimers(): void {
		if (this.certRefreshTimer) {
			clearTimeout(this.certRefreshTimer);
			this.certRefreshTimer = null;
		}
		if (this.reconnectTimer) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = null;
		}
	}
}
