import type {
	EventSubscriptionDescriptor,
	MethodDescriptor,
	OutgoingCallDescriptor,
	PolicyEvaluation,
	CaptureModes as ProtoCaptureModes,
	RegisterRequest,
	RegistryClient,
	RegistryEvent,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import { CaptureMode as ProtoCaptureMode } from "../pb/servicebridge/v1/registry";
import { Channel } from "../pb/servicebridge/v1/telemetry";
import type { CaptureMode } from "../telemetry/payload-capture";
import {
	type ReconnectDelayOptions,
	reconnectDelay,
} from "../utils/reconnect-ladder";

// captureModeFromProto maps the runtime-pushed registry enum to the SDK string
// mode. UNSPECIFIED and unknown values fail safe to "none".
function captureModeFromProto(m: ProtoCaptureMode): CaptureMode {
	switch (m) {
		case ProtoCaptureMode.CAPTURE_MODE_ALL:
			return "all";
		case ProtoCaptureMode.CAPTURE_MODE_ERRORS:
			return "errors";
		default:
			return "none";
	}
}

// ChannelCaptureModes is the SDK-side per-channel effective capture mode set,
// keyed by op Channel. Every channel defaults to "none" (fail-safe) until the
// runtime pushes a snapshot.
type ChannelCaptureModes = Record<Channel, CaptureMode>;

// DEFAULT_PAYLOAD_MAX_BYTES_PUSHED is the fallback used before the runtime
// pushes a payload_max_bytes value via the registry snapshot.
const DEFAULT_PAYLOAD_MAX_BYTES_PUSHED = 65536;

function defaultChannelCaptureModes(): ChannelCaptureModes {
	return {
		[Channel.CHANNEL_UNSPECIFIED]: "none",
		[Channel.HTTP]: "none",
		[Channel.RPC]: "none",
		[Channel.EVENT]: "none",
		[Channel.WORKFLOW]: "none",
		[Channel.JOB]: "none",
		[Channel.USER]: "none",
		[Channel.UNRECOGNIZED]: "none",
	};
}

// channelCaptureModesFromProto maps the runtime-pushed CaptureModes message to
// the SDK per-channel record. A missing message leaves every channel at "none".
function channelCaptureModesFromProto(
	m: ProtoCaptureModes | undefined,
): ChannelCaptureModes {
	const out = defaultChannelCaptureModes();
	if (!m) return out;
	out[Channel.RPC] = captureModeFromProto(m.rpc);
	out[Channel.HTTP] = captureModeFromProto(m.http);
	out[Channel.EVENT] = captureModeFromProto(m.event);
	out[Channel.WORKFLOW] = captureModeFromProto(m.workflow);
	return out;
}

// PushedTelemetryConfig carries the runtime-pushed telemetry global settings.
// @internal — см. ./README.md
export interface PushedTelemetryConfig {
	enabled: boolean;
	payloadMaxBytes: number;
}

function channelModesEqual(
	a: ChannelCaptureModes,
	b: ChannelCaptureModes,
): boolean {
	return (
		a[Channel.RPC] === b[Channel.RPC] &&
		a[Channel.HTTP] === b[Channel.HTTP] &&
		a[Channel.EVENT] === b[Channel.EVENT] &&
		a[Channel.WORKFLOW] === b[Channel.WORKFLOW]
	);
}

// cacheKey builds a unique key for a MethodDescriptor.
function cacheKey(d: MethodDescriptor): string {
	return `${d.instanceId}:${d.type}:${d.name}:${d.published}`;
}

// eventSubKey: subscriptions are keyed by (service_id, pattern) — matches the
// PRIMARY KEY of event_subscriptions on the server.
function eventSubKey(d: EventSubscriptionDescriptor): string {
	return `${d.serviceId}|${d.pattern}`;
}

// outgoingKey: outgoing decls are keyed by (caller, target, method, type).
function outgoingKey(d: OutgoingCallDescriptor): string {
	return `${d.callerServiceId}|${d.targetServiceId}|${d.targetMethod}|${d.targetType}`;
}

// WatchStream manages the gRPC Registry.RegisterAndWatch stream.
// @internal — см. ./README.md
export class WatchStream {
	private stream: ReturnType<RegistryClient["registerAndWatch"]> | null = null;
	private cache = new Map<string, MethodDescriptor>();
	private instances = new Map<string, ServiceInstanceInfo>();
	// ADR-0004 enrichment: caller's own event subscriptions + outgoing calls
	// plus its current PolicyEvaluation. Updated from snapshot + update frames.
	private eventSubs = new Map<string, EventSubscriptionDescriptor>();
	private outgoing = new Map<string, OutgoingCallDescriptor>();
	private policy: PolicyEvaluation | null = null;
	// Runtime-pushed effective capture mode per op channel. Every channel is
	// "none" until the first snapshot arrives (fail-safe — never capture before
	// the runtime authorises it).
	private _captureModes: ChannelCaptureModes = defaultChannelCaptureModes();
	private captureModeListeners = new Set<
		(modes: ChannelCaptureModes) => void
	>();
	// Runtime-pushed telemetry global config. Default fail-safe: enabled=true,
	// payloadMaxBytes=65536. Applied once the first snapshot arrives.
	private _telemetryConfig: PushedTelemetryConfig = {
		enabled: true,
		payloadMaxBytes: DEFAULT_PAYLOAD_MAX_BYTES_PUSHED,
	};
	private telemetryConfigListeners = new Set<
		(config: PushedTelemetryConfig) => void
	>();
	private onError: (err: Error) => void = () => {};
	private instanceListeners = new Set<
		(added: ServiceInstanceInfo[], removed: ServiceInstanceInfo[]) => void
	>();
	private methodListeners = new Set<
		(added: MethodDescriptor[], removed: MethodDescriptor[]) => void
	>();
	private policyListeners = new Set<(policy: PolicyEvaluation) => void>();
	private peersChangeListeners = new Set<
		(added: string[], removed: string[]) => void
	>();
	// Retry state. A broken watch self-restarts on the shared jittered ladder
	// using the (req, client) from the latest start()/restart(); session
	// rotation refreshes both via restart(). One pending timer at a time —
	// "error" and "end" can both fire for a single broken stream, and a second
	// timer would multiply live restart loops on every break.
	private _stopped = false;
	private _attempt = 0;
	private _timer: ReturnType<typeof setTimeout> | null = null;
	private lastReq: RegisterRequest | null = null;
	private lastClient: RegistryClient | null = null;

	constructor(private readonly reconnectOpts?: ReconnectDelayOptions) {}

	start(
		req: RegisterRequest,
		client: RegistryClient,
		onError?: (err: Error) => void,
	): void {
		if (onError) this.onError = onError;
		this.lastReq = req;
		this.lastClient = client;
		this._stopped = false;
		this._attempt = 0;
		this.clearRetryTimer();
		this.open();
	}

	stop(): void {
		this._stopped = true;
		this.clearRetryTimer();
		const old = this.stream;
		this.stream = null;
		old?.cancel();
	}

	restart(
		req: RegisterRequest,
		client: RegistryClient,
		onError?: (err: Error) => void,
	): void {
		this.stop();
		this.start(req, client, onError);
	}

	private open(): void {
		if (this.lastReq === null || this.lastClient === null) {
			throw new Error("registry: watch: open before start");
		}
		const old = this.stream;
		this.stream = null;
		old?.cancel();

		const stream = this.lastClient.registerAndWatch(this.lastReq);
		this.stream = stream;
		// Identity guards: a cancelled/replaced stream still emits error/end
		// asynchronously; only the current stream may act.
		stream.on("data", (evt: RegistryEvent) => {
			if (this.stream !== stream) return;
			this._attempt = 0;
			this.handleEvent(evt);
		});
		stream.on("error", (err: Error) => {
			if (this.stream !== stream) return;
			this.stream = null;
			this.onError(err);
			this.scheduleRestart();
		});
		stream.on("end", () => {
			if (this.stream !== stream) return;
			this.stream = null;
			this.scheduleRestart();
		});
	}

	private scheduleRestart(): void {
		if (this._stopped || this._timer !== null) return;
		const delay = reconnectDelay(++this._attempt, this.reconnectOpts);
		this._timer = setTimeout(() => {
			this._timer = null;
			if (this._stopped) return;
			this.open();
		}, delay);
	}

	private clearRetryTimer(): void {
		if (this._timer !== null) {
			clearTimeout(this._timer);
			this._timer = null;
		}
	}

	// snapshot exposes the live descriptor cache read-only. Copying it here
	// allocated a full map of every mesh descriptor on every RegistryUpdate —
	// i.e. on every scaling event and every rolling-deploy step — for consumers
	// that only iterate it. Consumers that need a stable view copy it themselves.
	snapshot(): ReadonlyMap<string, MethodDescriptor> {
		return this.cache;
	}

	instancesSnapshot(): Map<string, ServiceInstanceInfo> {
		return this.instances;
	}

	// ADR-0004: snapshot accessors for service-map enrichment.
	eventSubscriptionsSnapshot(): Map<string, EventSubscriptionDescriptor> {
		return new Map(this.eventSubs);
	}

	outgoingCallsSnapshot(): Map<string, OutgoingCallDescriptor> {
		return new Map(this.outgoing);
	}

	policyEvaluation(): PolicyEvaluation | null {
		return this.policy;
	}

	// captureModeForChannel returns the runtime-pushed effective payload capture
	// mode for the given op channel. "none" until the first registry snapshot
	// arrives, and for any channel the runtime did not push a mode for.
	captureModeForChannel(channel: Channel): CaptureMode {
		return this._captureModes[channel] ?? "none";
	}

	// onCaptureModes fires whenever the runtime pushes a (changed) per-channel
	// capture mode set on the snapshot or an update.
	onCaptureModes(fn: (modes: ChannelCaptureModes) => void): () => void {
		this.captureModeListeners.add(fn);
		return () => this.captureModeListeners.delete(fn);
	}

	private applyCaptureModes(next: ChannelCaptureModes): void {
		if (channelModesEqual(next, this._captureModes)) return;
		this._captureModes = next;
		for (const fn of this.captureModeListeners) fn(next);
	}

	// pushedTelemetryConfig returns the last runtime-pushed telemetry global
	// config. Default fail-safe before the first snapshot: enabled=true, cap=65536.
	pushedTelemetryConfig(): PushedTelemetryConfig {
		return this._telemetryConfig;
	}

	// onTelemetryConfig fires whenever the runtime pushes a (changed) telemetry
	// config on the snapshot or an update.
	onTelemetryConfig(fn: (config: PushedTelemetryConfig) => void): () => void {
		this.telemetryConfigListeners.add(fn);
		return () => this.telemetryConfigListeners.delete(fn);
	}

	private applyTelemetryConfig(m: ProtoCaptureModes | undefined): void {
		if (!m) return;
		// payload_max_bytes=0 on the wire means "not set" (proto3 default); fall
		// back to the default cap so an older runtime that doesn't push the field
		// doesn't silently truncate everything to 0.
		const payloadMaxBytes =
			m.payloadMaxBytes > 0
				? m.payloadMaxBytes
				: DEFAULT_PAYLOAD_MAX_BYTES_PUSHED;
		// telemetry_enabled defaults to false in proto3; the runtime always
		// explicitly sets it from telemetry.enable (default true), so a missing
		// field means the runtime didn't push it — keep the current value.
		const enabled =
			m.payloadMaxBytes !== 0 || m.telemetryEnabled
				? m.telemetryEnabled
				: this._telemetryConfig.enabled;
		const next: PushedTelemetryConfig = { enabled, payloadMaxBytes };
		if (
			next.enabled === this._telemetryConfig.enabled &&
			next.payloadMaxBytes === this._telemetryConfig.payloadMaxBytes
		) {
			return;
		}
		this._telemetryConfig = next;
		for (const fn of this.telemetryConfigListeners) fn(next);
	}

	// onInstancesChange fires with the instances to upsert and the instances that
	// are gone. `added` carries every instance the frame described (a snapshot
	// re-describes all of them), `removed` only the ones no longer present — so a
	// listener can apply the delta without rebuilding its whole index.
	onInstancesChange(
		fn: (added: ServiceInstanceInfo[], removed: ServiceInstanceInfo[]) => void,
	): () => void {
		this.instanceListeners.add(fn);
		return () => this.instanceListeners.delete(fn);
	}

	// onMethodsChange is the descriptor-side twin of onInstancesChange: `added`
	// are descriptors to upsert, `removed` are the ones evicted (by an update,
	// by a snapshot that no longer lists them, or by a policy peer removal).
	onMethodsChange(
		fn: (added: MethodDescriptor[], removed: MethodDescriptor[]) => void,
	): () => void {
		this.methodListeners.add(fn);
		return () => this.methodListeners.delete(fn);
	}

	// onPolicyEvaluation fires every time a fresh PolicyEvaluation lands on the
	// stream (initial snapshot, plus future RegistryUpdate.policy when policy
	// rules change live for this caller).
	onPolicyEvaluation(fn: (policy: PolicyEvaluation) => void): () => void {
		this.policyListeners.add(fn);
		return () => this.policyListeners.delete(fn);
	}

	// onPeersChange fires when added_peers or removed_peers arrive in a
	// RegistryUpdate. SDK uses this to update serviceMap and close stale
	// direct connections.
	onPeersChange(fn: (added: string[], removed: string[]) => void): () => void {
		this.peersChangeListeners.add(fn);
		return () => this.peersChangeListeners.delete(fn);
	}

	private emitInstances(
		added: ServiceInstanceInfo[],
		removed: ServiceInstanceInfo[],
	): void {
		if (added.length === 0 && removed.length === 0) return;
		for (const fn of this.instanceListeners) fn(added, removed);
	}

	private emitMethods(
		added: MethodDescriptor[],
		removed: MethodDescriptor[],
	): void {
		if (added.length === 0 && removed.length === 0) return;
		for (const fn of this.methodListeners) fn(added, removed);
	}

	private emitPolicy(policy: PolicyEvaluation): void {
		for (const fn of this.policyListeners) fn(policy);
	}

	private handleEvent(evt: RegistryEvent): void {
		if (evt.snapshot) {
			// A snapshot replaces the world. `added` re-describes everything it
			// lists (an upsert covers both new and changed rows), `removed` holds
			// only what the new world no longer has — a listener that applied the
			// previous frame stays consistent without a full rebuild.
			const prevMethods = new Map(this.cache);
			this.cache.clear();
			for (const m of evt.snapshot.methods) {
				this.cache.set(cacheKey(m), m);
			}
			const removedMethods: MethodDescriptor[] = [];
			for (const [k, m] of prevMethods) {
				if (!this.cache.has(k)) removedMethods.push(m);
			}
			const prevInstances = new Map(this.instances);
			this.instances.clear();
			for (const i of evt.snapshot.instances) {
				this.instances.set(i.instanceId, i);
			}
			const removedInstances: ServiceInstanceInfo[] = [];
			for (const [id, inst] of prevInstances) {
				if (!this.instances.has(id)) removedInstances.push(inst);
			}
			this.eventSubs.clear();
			for (const es of evt.snapshot.eventSubscriptions ?? []) {
				this.eventSubs.set(eventSubKey(es), es);
			}
			this.outgoing.clear();
			for (const oc of evt.snapshot.outgoingCalls ?? []) {
				this.outgoing.set(outgoingKey(oc), oc);
			}
			if (evt.snapshot.policy) {
				this.policy = evt.snapshot.policy;
				this.emitPolicy(evt.snapshot.policy);
			}
			this.applyCaptureModes(
				channelCaptureModesFromProto(evt.snapshot.captureModes),
			);
			this.applyTelemetryConfig(evt.snapshot.captureModes);
			this.emitMethods(evt.snapshot.methods, removedMethods);
			this.emitInstances(evt.snapshot.instances, removedInstances);
		} else if (evt.update) {
			for (const m of evt.update.added) {
				this.cache.set(cacheKey(m), m);
			}
			for (const m of evt.update.removed) {
				this.cache.delete(cacheKey(m));
			}
			for (const i of evt.update.addedInstances) {
				this.instances.set(i.instanceId, i);
			}
			for (const i of evt.update.removedInstances) {
				this.instances.delete(i.instanceId);
			}
			for (const es of evt.update.addedEventSubscriptions ?? []) {
				this.eventSubs.set(eventSubKey(es), es);
			}
			for (const es of evt.update.removedEventSubscriptions ?? []) {
				this.eventSubs.delete(eventSubKey(es));
			}
			for (const oc of evt.update.addedOutgoingCalls ?? []) {
				this.outgoing.set(outgoingKey(oc), oc);
			}
			for (const oc of evt.update.removedOutgoingCalls ?? []) {
				this.outgoing.delete(outgoingKey(oc));
			}
			// removedPeers: when a peer falls out of the caller's policy scope
			// (e.g. rule revoked), runtime emits the peer's serviceId in
			// removedPeers. SDK must drop every cached entry tied to that peer
			// so serviceMap loses the entry within the same propagation window.
			const removedMethods: MethodDescriptor[] = [...evt.update.removed];
			const removedPeers = evt.update.removedPeers ?? [];
			if (removedPeers.length > 0) {
				const removedSet = new Set(removedPeers);
				const removedInstancesByPeer: ServiceInstanceInfo[] = [];
				for (const [k, m] of this.cache) {
					if (removedSet.has(m.serviceId)) {
						removedMethods.push(m);
						this.cache.delete(k);
					}
				}
				for (const [k, inst] of this.instances) {
					if (removedSet.has(inst.serviceId)) {
						removedInstancesByPeer.push(inst);
						this.instances.delete(k);
					}
				}
				for (const [k, es] of this.eventSubs) {
					if (removedSet.has(es.serviceId)) this.eventSubs.delete(k);
				}
				for (const [k, oc] of this.outgoing) {
					if (removedSet.has(oc.targetServiceId)) this.outgoing.delete(k);
				}
				if (removedInstancesByPeer.length > 0) {
					this.emitInstances([], removedInstancesByPeer);
				}
			}
			if (evt.update.policy) {
				this.policy = evt.update.policy;
				this.emitPolicy(evt.update.policy);
			}
			this.applyCaptureModes(
				channelCaptureModesFromProto(evt.update.captureModes),
			);
			this.applyTelemetryConfig(evt.update.captureModes);
			const addedPeers = evt.update.addedPeers ?? [];
			if (addedPeers.length > 0 || removedPeers.length > 0) {
				for (const fn of this.peersChangeListeners) {
					fn(addedPeers, removedPeers);
				}
			}
			this.emitMethods(evt.update.added, removedMethods);
			this.emitInstances(
				evt.update.addedInstances,
				evt.update.removedInstances,
			);
		}
	}
}
