import type {
	MethodDescriptor,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import type { CircuitBreakerRegistry } from "./circuit-breaker";

// Power-of-Two-Choices load balancer with per-pod inflight tracking (ADR 0009).
// Local CB filter excludes OPEN instances; runtime `is_unhealthy_since_unix_ms` hint
// excludes instances marked unhealthy by the runtime within the last
// HEALTH_HINT_TTL_MS. After the TTL the hint is treated as stale (runtime may
// itself be broken) and the local CB carries the routing decision.
//
// @internal — см. ./README.md

// HEALTH_HINT_TTL_MS bounds how long a runtime-supplied unhealthy hint is
// trusted. Bound = 2× the runtime HealthTracker healing window (ADR 0009 §3).
export const HEALTH_HINT_TTL_MS = 60_000;

export interface Candidate {
	descriptor: MethodDescriptor;
	instance: ServiceInstanceInfo;
	// isUnhealthyAt is the wall-clock time the runtime first marked this
	// instance unhealthy through the watch snapshot (ADR 0009 §3). Null when
	// the runtime has no opinion or has not yet wired the hint.
	isUnhealthyAt: Date | null;
}

export class NoLiveInstanceError extends Error {
	constructor(message?: string) {
		super(message ?? "rpc: no live instance");
		this.name = "NoLiveInstanceError";
	}
}

export class LoadBalancer {
	private inflight = new Map<string, number>();
	private now: () => number;
	private random: () => number;

	constructor(
		private readonly cb: CircuitBreakerRegistry,
		opts?: { now?: () => number; random?: () => number },
	) {
		this.now = opts?.now ?? Date.now;
		this.random = opts?.random ?? Math.random;
	}

	// pick selects an eligible candidate via Power of Two Choices. Eligibility:
	//   - candidate has a non-empty call_endpoint
	//   - the runtime health hint is stale or absent
	//   - the local circuit breaker is not OPEN for the instance
	// Throws NoLiveInstanceError when no candidate is eligible. Picking is
	// random when fewer than two eligible candidates exist or when inflight
	// counters tie. Caller MUST pair every successful pick with a `release`
	// call in a finally block so inflight stays balanced.
	pick(candidates: Candidate[]): Candidate {
		const now = this.now();
		const live = candidates.filter((c) => {
			if (!c.instance.callEndpoint) return false;
			const hintActive =
				c.isUnhealthyAt !== null &&
				now - c.isUnhealthyAt.getTime() < HEALTH_HINT_TTL_MS;
			if (hintActive) return false;
			return this.cb.state(cbKey(c.instance)) !== "OPEN";
		});
		if (live.length === 0) throw new NoLiveInstanceError();
		if (live.length === 1) return live[0]!;

		const i = Math.floor(this.random() * live.length);
		let j = Math.floor(this.random() * (live.length - 1));
		if (j >= i) j++;
		const a = live[i]!;
		const b = live[j]!;
		const ai = this.inflight.get(a.instance.instanceId) ?? 0;
		const bi = this.inflight.get(b.instance.instanceId) ?? 0;
		return ai <= bi ? a : b;
	}

	// acquire increments the inflight counter for an instance before the call
	// is dispatched. Always returns a matching release closure that decrements
	// the counter exactly once.
	acquire(instanceId: string): () => void {
		this.inflight.set(instanceId, (this.inflight.get(instanceId) ?? 0) + 1);
		let released = false;
		return () => {
			if (released) return;
			released = true;
			const cur = this.inflight.get(instanceId) ?? 0;
			if (cur <= 1) {
				this.inflight.delete(instanceId);
			} else {
				this.inflight.set(instanceId, cur - 1);
			}
		};
	}

	// inflightOf returns the current inflight count — for tests / metrics.
	inflightOf(instanceId: string): number {
		return this.inflight.get(instanceId) ?? 0;
	}
}

export function cbKey(instance: ServiceInstanceInfo): string {
	return `${instance.serviceId}:${instance.instanceId}`;
}
