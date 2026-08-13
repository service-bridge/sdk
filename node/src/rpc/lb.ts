import type {
	MethodDescriptor,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import type { CircuitBreakerRegistry } from "./circuit-breaker";

// Power-of-Two-Choices load balancer with per-pod inflight tracking (ADR 0001).
// Local CB filter excludes OPEN instances; runtime `is_unhealthy_since_unix_ms` hint
// excludes instances marked unhealthy by the runtime within the last
// HEALTH_HINT_TTL_MS. After the TTL the hint is treated as stale (runtime may
// itself be broken) and the local CB carries the routing decision.
//
// @internal — см. ./README.md

// HEALTH_HINT_TTL_MS bounds how long a runtime-supplied unhealthy hint is
// trusted. Bound = 2× the runtime HealthTracker healing window (ADR 0001).
export const HEALTH_HINT_TTL_MS = 60_000;

export interface Candidate {
	descriptor: MethodDescriptor;
	instance: ServiceInstanceInfo;
	// isUnhealthyAt is the wall-clock time the runtime first marked this
	// instance unhealthy through the watch snapshot (ADR 0001). Null when
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
	//   - the local circuit breaker is not OPEN, and — when HALF_OPEN — its single
	//     probe slot is free
	// Throws NoLiveInstanceError when no candidate is eligible. A tie on inflight
	// counters resolves to the first sampled candidate. Caller MUST pair every
	// successful pick with a `release` call in a finally block so inflight stays
	// balanced.
	//
	// HALF_OPEN single-probe: a HALF_OPEN instance is eligible only while its
	// probe slot is free (read-only `probeAvailable`). Once a winner is chosen,
	// pick CLAIMS the probe on that instance via `canCall`; the claim is released
	// by the matching cb.recordSuccess/recordFailure the caller runs after the
	// dispatch. pick runs to completion synchronously, so a concurrent caller's
	// later pick sees the claimed slot and routes elsewhere (or fails over) —
	// exactly one probe in flight per HALF_OPEN instance.
	//
	// P2C needs exactly two eligible candidates, so eligibility and sampling run
	// in a single pass with a two-slot reservoir. Materialising the eligible
	// subset would allocate an array on every call — with a few hundred callee
	// pods that is the dominant garbage the RPC path produces.
	pick(candidates: Candidate[]): Candidate {
		const now = this.now();
		let eligible = 0;
		let a: Candidate | undefined;
		let b: Candidate | undefined;
		for (const c of candidates) {
			if (!c.instance.callEndpoint) continue;
			const hintActive =
				c.isUnhealthyAt !== null &&
				now - c.isUnhealthyAt.getTime() < HEALTH_HINT_TTL_MS;
			if (hintActive) continue;
			if (!this.cb.probeAvailable(cbKey(c.instance))) continue;

			eligible++;
			if (eligible === 1) {
				a = c;
			} else if (eligible === 2) {
				b = c;
			} else {
				// Reservoir sampling, k=2: the n-th eligible candidate takes a
				// uniformly chosen slot with probability 2/n, which leaves every
				// eligible pair equally likely without holding the whole subset.
				const slot = Math.floor(this.random() * eligible);
				if (slot === 0) a = c;
				else if (slot === 1) b = c;
			}
		}
		if (a === undefined) throw new NoLiveInstanceError();
		let winner = a;
		if (b !== undefined) {
			// Reservoir sampling picks the pair uniformly but not its order: the
			// second eligible candidate can only ever land in slot B. Since a tie
			// on inflight resolves to slot A, an unshuffled reservoir would starve
			// that candidate on an idle fleet. One coin flip restores uniformity.
			winner =
				this.random() < 0.5 ? this.leastLoaded(a, b) : this.leastLoaded(b, a);
		}
		// Claim the HALF_OPEN probe slot on the winner only. CLOSED instances
		// always return true here without side effects.
		this.cb.canCall(cbKey(winner.instance));
		return winner;
	}

	private leastLoaded(a: Candidate, b: Candidate): Candidate {
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

// keyCache memoises the derived breaker key per instance object. cbKey runs once
// per candidate per pick plus twice per completed call, and the template literal
// was allocating a short-lived string every time. Keying by the instance object
// keeps the cache self-clearing: InstanceCache rebuilds instance objects on each
// registry snapshot, so retired pods drop out with their objects and the cache
// cannot grow without bound the way a Map<instanceId, string> would.
const keyCache = new WeakMap<ServiceInstanceInfo, string>();

export function cbKey(instance: ServiceInstanceInfo): string {
	let key = keyCache.get(instance);
	if (key === undefined) {
		key = `${instance.serviceId}:${instance.instanceId}`;
		keyCache.set(instance, key);
	}
	return key;
}
