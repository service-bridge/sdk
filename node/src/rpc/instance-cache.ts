import type {
	MethodDescriptor,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import type { WatchStream } from "../registry/watch";
import type { Candidate } from "./lb";

// InstanceCache joins MethodDescriptor (per-method contract) with
// ServiceInstanceInfo (per-instance endpoint/status) using data observed via
// WatchStream events. Used by the call client to look up target instances for
// a given (serviceName, methodName) pair.
//
// Only the primary `byService` index is kept (ADR 0009 §4): at ≤1000 services
// the per-call linear scan over methods is single-digit microseconds, whereas
// a secondary `byServiceMethod` index needs O(methods) rebuild on every
// snapshot tick.
//
// @internal — см. ./README.md

// Instance bundles the registry-supplied ServiceInstanceInfo with the
// runtime-supplied health hint. `isUnhealthyAt` is the wall-clock the runtime
// first marked this instance unhealthy via HealthTracker (ADR-0008); null
// means "healthy or unknown". The LB filter treats hints older than
// HEALTH_HINT_TTL_MS as stale (ADR-0009 §3).
export interface Instance extends ServiceInstanceInfo {
	isUnhealthyAt: Date | null;
}

export class InstanceCache {
	private byService = new Map<string, MethodDescriptor[]>();
	private instances = new Map<string, Instance>();
	private unsubscribe: (() => void) | null = null;
	private watch: WatchStream | null = null;

	bind(watch: WatchStream): void {
		this.watch = watch;
		this.refresh();
		this.unsubscribe = watch.onInstancesChange(() => this.refresh());
	}

	dispose(): void {
		this.unsubscribe?.();
		this.unsubscribe = null;
		this.watch = null;
		this.byService.clear();
		this.instances.clear();
	}

	// pickAll returns every (descriptor, instance) candidate for the given
	// (serviceName, methodName) pair where the instance is connected. Hash
	// filtering for contract-version routing is the caller's job — pickAll
	// returns all descriptors so the caller can compare hashes once after
	// reading the schema. Used by the LB for P2C selection.
	pickAll(serviceName: string, methodName: string): Candidate[] {
		const descs = this.byService.get(serviceName);
		if (!descs || descs.length === 0) return [];
		const out: Candidate[] = [];
		for (const d of descs) {
			if (d.name !== methodName) continue;
			const inst = this.instances.get(d.instanceId);
			if (!inst) continue;
			out.push({
				descriptor: d,
				instance: inst,
				isUnhealthyAt: inst.isUnhealthyAt,
			});
		}
		return out;
	}

	// descriptorFor returns the contract for one (service, method) pair without
	// resolving an endpoint. Used to access schema during encoding when no LB
	// pick has yet happened.
	descriptorFor(
		serviceName: string,
		methodName: string,
	): MethodDescriptor | null {
		const descs = this.byService.get(serviceName);
		if (!descs) return null;
		return descs.find((d) => d.name === methodName) ?? null;
	}

	private refresh(): void {
		if (!this.watch) return;
		this.instances.clear();
		for (const [id, info] of this.watch.instancesSnapshot()) {
			this.instances.set(id, {
				...info,
				isUnhealthyAt: info.isUnhealthySinceUnixMs
					? new Date(info.isUnhealthySinceUnixMs)
					: null,
			});
		}

		this.byService.clear();
		for (const desc of this.watch.snapshot().values()) {
			const arr = this.byService.get(desc.serviceName);
			if (arr) arr.push(desc);
			else this.byService.set(desc.serviceName, [desc]);
		}
	}
}
