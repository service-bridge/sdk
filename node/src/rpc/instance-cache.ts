import type {
	MethodDescriptor,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import type { WatchStream } from "../registry/watch";
import { type Candidate, cbKey } from "./lb";

// InstanceCache joins MethodDescriptor (per-method contract) with
// ServiceInstanceInfo (per-instance endpoint/status) using data observed via
// WatchStream events, and materialises the lookup the call path needs.
//
// The registry keys descriptors by (instance, type, method, published), so the
// descriptor set grows with fleet size × method count, not with method count.
// A linear scan over it made every call slower the wider the callee scaled —
// the opposite of what scaling out is for. Candidates are therefore indexed by
// (service, method, contractHash) at refresh time: a call costs one Map lookup,
// and the rebuild is paid on registry churn instead.
//
// @internal — см. ./README.md

// Instance bundles the registry-supplied ServiceInstanceInfo with the
// runtime-supplied health hint and the derived circuit-breaker key.
// `isUnhealthyAt` is the wall-clock the runtime first marked this instance
// unhealthy via HealthTracker (ADR-0007); null means "healthy or unknown". The
// LB filter treats hints older than HEALTH_HINT_TTL_MS as stale (ADR-0001).
export interface Instance extends ServiceInstanceInfo {
	isUnhealthyAt: Date | null;
	// cbKey is the circuit-breaker key resolved once per refresh. The LB reads it
	// for every candidate on every pick and the breaker reads it again on every
	// call outcome, so deriving it per read was pure per-call garbage.
	cbKey: string;
}

// InstanceRetainer is per-instance state keyed by cbKey that must drop entries
// for pods the registry no longer lists — without it a rolling deploy leaks one
// entry per retired pod. Declared here rather than imported so InstanceCache
// does not depend on the circuit-breaker implementation.
export interface InstanceRetainer {
	retain(liveKeys: ReadonlySet<string>): void;
}

interface Binding {
	watch: WatchStream;
	breakers: InstanceRetainer;
}

const NO_CANDIDATES: Candidate[] = [];
const EMPTY_KEYS: ReadonlySet<string> = new Set();

export class InstanceCache {
	// byMethod: `${serviceName}/${methodName}/${contractHash}` → candidates.
	private byMethod = new Map<string, Candidate[]>();
	// byMethodAny: `${serviceName}/${methodName}` → any descriptor for the pair.
	// Contract-independent: used only for existence and the streaming flag.
	private byMethodAny = new Map<string, MethodDescriptor>();
	private instances = new Map<string, Instance>();
	private binding: Binding | null = null;
	private unsubscribe: (() => void) | null = null;

	bind(watch: WatchStream, breakers: InstanceRetainer): void {
		this.binding = { watch, breakers };
		this.refresh();
		this.unsubscribe = watch.onInstancesChange(() => this.refresh());
	}

	dispose(): void {
		this.unsubscribe?.();
		this.unsubscribe = null;
		this.binding?.breakers.retain(EMPTY_KEYS);
		this.binding = null;
		this.byMethod.clear();
		this.byMethodAny.clear();
		this.instances.clear();
	}

	// candidatesFor returns every connected (descriptor, instance) candidate for
	// the given (serviceName, methodName) pair whose descriptor was published
	// with `contractHash` (ADR 0005 — version routing). The returned array is the
	// live index entry: callers read it, never mutate it.
	candidatesFor(
		serviceName: string,
		methodName: string,
		contractHash: string,
	): Candidate[] {
		return (
			this.byMethod.get(`${serviceName}/${methodName}/${contractHash}`) ??
			NO_CANDIDATES
		);
	}

	// descriptorFor returns the contract for one (service, method) pair without
	// resolving an endpoint. Used to access schema during encoding when no LB
	// pick has yet happened.
	descriptorFor(
		serviceName: string,
		methodName: string,
	): MethodDescriptor | null {
		return this.byMethodAny.get(`${serviceName}/${methodName}`) ?? null;
	}

	private refresh(): void {
		if (!this.binding) return;
		const { watch, breakers } = this.binding;

		this.instances.clear();
		const liveKeys = new Set<string>();
		for (const [id, info] of watch.instancesSnapshot()) {
			const instance: Instance = {
				...info,
				isUnhealthyAt: info.isUnhealthySinceUnixMs
					? new Date(info.isUnhealthySinceUnixMs)
					: null,
				cbKey: cbKey(info),
			};
			this.instances.set(id, instance);
			liveKeys.add(instance.cbKey);
		}

		this.byMethod.clear();
		this.byMethodAny.clear();
		for (const descriptor of watch.snapshot().values()) {
			const pair = `${descriptor.serviceName}/${descriptor.name}`;
			if (!this.byMethodAny.has(pair)) this.byMethodAny.set(pair, descriptor);
			const instance = this.instances.get(descriptor.instanceId);
			if (!instance) continue;
			const key = `${pair}/${descriptor.contractHash}`;
			const candidate: Candidate = {
				descriptor,
				instance,
				isUnhealthyAt: instance.isUnhealthyAt,
			};
			const bucket = this.byMethod.get(key);
			if (bucket) bucket.push(candidate);
			else this.byMethod.set(key, [candidate]);
		}

		breakers.retain(liveKeys);
	}
}
