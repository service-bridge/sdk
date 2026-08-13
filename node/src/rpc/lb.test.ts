import { describe, expect, it } from "bun:test";
import type {
	MethodDescriptor,
	ServiceInstanceInfo,
} from "../pb/servicebridge/v1/registry";
import { MethodType } from "../pb/servicebridge/v1/registry";
import {
	CircuitBreakerRegistry,
	MIN_REQUESTS,
	OPEN_DURATION_MS,
} from "./circuit-breaker";
import {
	type Candidate,
	cbKey,
	HEALTH_HINT_TTL_MS,
	LoadBalancer,
	NoLiveInstanceError,
} from "./lb";

function inst(id: string, endpoint: string): ServiceInstanceInfo {
	return {
		instanceId: id,
		serviceId: "svc",
		serviceName: "svc",
		callEndpoint: endpoint,
		status: "connected",
		httpEndpoint: "",
		isUnhealthySinceUnixMs: 0,
	};
}

function desc(): MethodDescriptor {
	return {
		instanceId: "i",
		serviceId: "svc",
		serviceName: "svc",
		type: MethodType.METHOD_TYPE_RPC,
		name: "charge",
		published: false,
		contractHash: "h",
		inputSchema: Buffer.alloc(0),
		outputSchema: Buffer.alloc(0),
		streaming: false,
	};
}

function cand(
	id: string,
	endpoint: string,
	unhealthyAt: Date | null = null,
): Candidate {
	return {
		descriptor: desc(),
		instance: inst(id, endpoint),
		isUnhealthyAt: unhealthyAt,
	};
}

// scriptedRandom returns the next value from a fixed sequence, looping.
function scriptedRandom(values: number[]): () => number {
	let i = 0;
	return () => {
		const v = values[i % values.length]!;
		i++;
		return v;
	};
}

describe("LoadBalancer P2C", () => {
	it("returns the only live candidate when one matches", () => {
		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const picked = lb.pick([cand("a", "h:1"), cand("b", "")]);
		expect(picked.instance.instanceId).toBe("a");
	});

	it("throws NoLiveInstanceError when all are unhealthy or CB-open", () => {
		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const a = inst("a", "h:1");
		for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure(cbKey(a));
		expect(cb.state(cbKey(a))).toBe("OPEN");
		expect(() =>
			lb.pick([{ descriptor: desc(), instance: a, isUnhealthyAt: null }]),
		).toThrow(NoLiveInstanceError);
	});

	it("excludes instances with non-stale runtime unhealthy hint", () => {
		const now = 10_000_000;
		const cb = new CircuitBreakerRegistry(() => now);
		const lb = new LoadBalancer(cb, { now: () => now });
		const unhealthy = cand("a", "h:1", new Date(now - HEALTH_HINT_TTL_MS / 2));
		const healthy = cand("b", "h:2");
		const picked = lb.pick([unhealthy, healthy]);
		expect(picked.instance.instanceId).toBe("b");
	});

	it("ignores stale runtime unhealthy hint after TTL", () => {
		const now = 10_000_000;
		const cb = new CircuitBreakerRegistry(() => now);
		const lb = new LoadBalancer(cb, { now: () => now });
		const stale = cand("a", "h:1", new Date(now - HEALTH_HINT_TTL_MS - 1));
		const picked = lb.pick([stale]);
		expect(picked.instance.instanceId).toBe("a");
	});

	it("filters out candidates with empty callEndpoint", () => {
		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const picked = lb.pick([cand("a", ""), cand("b", "h:2")]);
		expect(picked.instance.instanceId).toBe("b");
	});

	it("P2C picks the candidate with fewer inflight", () => {
		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const a = cand("a", "h:1");
		const b = cand("b", "h:2");

		const release = lb.acquire("a"); // a has 1 inflight; b has 0
		try {
			// Exactly two eligible candidates → both fill the reservoir, no sampling.
			expect(lb.pick([a, b]).instance.instanceId).toBe("b");
		} finally {
			release();
		}
	});

	it("ties on inflight resolve by coin flip, not by candidate order", () => {
		const cb = new CircuitBreakerRegistry();
		const a = cand("a", "h:1");
		const b = cand("b", "h:2");
		// Both at inflight=0. The final draw orders the sampled pair, so neither
		// position is privileged.
		const heads = new LoadBalancer(cb, { random: scriptedRandom([0.1]) });
		expect(heads.pick([a, b]).instance.instanceId).toBe("a");
		const tails = new LoadBalancer(cb, { random: scriptedRandom([0.9]) });
		expect(tails.pick([a, b]).instance.instanceId).toBe("b");
	});

	describe("two-slot reservoir sampling", () => {
		const a = cand("a", "h:1");
		const b = cand("b", "h:2");
		const c = cand("c", "h:3");

		// With three eligible candidates the draws are [slot, coin]: the third
		// candidate replaces reservoir slot floor(slot * 3) when that lands on 0
		// or 1, then the coin orders the pair.
		it("third candidate takes the first slot when the draw is 0", () => {
			const cb = new CircuitBreakerRegistry();
			const lb = new LoadBalancer(cb, { random: scriptedRandom([0.0, 0.9]) });
			const release = lb.acquire("b"); // reservoir becomes {c, b}; b is loaded
			try {
				expect(lb.pick([a, b, c]).instance.instanceId).toBe("c");
			} finally {
				release();
			}
		});

		it("third candidate takes the second slot when the draw is 1", () => {
			const cb = new CircuitBreakerRegistry();
			const lb = new LoadBalancer(cb, { random: scriptedRandom([0.5, 0.9]) });
			const release = lb.acquire("a"); // reservoir becomes {a, c}; a is loaded
			try {
				expect(lb.pick([a, b, c]).instance.instanceId).toBe("c");
			} finally {
				release();
			}
		});

		it("reservoir is untouched when the draw falls outside both slots", () => {
			const cb = new CircuitBreakerRegistry();
			const lb = new LoadBalancer(cb, { random: scriptedRandom([0.9, 0.9]) });
			const release = lb.acquire("a"); // reservoir stays {a, b}; a is loaded
			try {
				expect(lb.pick([a, b, c]).instance.instanceId).toBe("b");
			} finally {
				release();
			}
		});

		it("every candidate in a large fleet stays reachable", () => {
			const cb = new CircuitBreakerRegistry();
			// Deterministic LCG so the distribution assertion cannot flake.
			let seed = 0x2545f491;
			const lb = new LoadBalancer(cb, {
				random: () => {
					seed = (seed * 1664525 + 1013904223) >>> 0;
					return seed / 0x1_0000_0000;
				},
			});
			const fleet = Array.from({ length: 50 }, (_, i) =>
				cand(`i-${i}`, `h:${i}`),
			);
			const wins = new Map<string, number>();
			for (let n = 0; n < 20_000; n++) {
				const id = lb.pick(fleet).instance.instanceId;
				wins.set(id, (wins.get(id) ?? 0) + 1);
			}
			expect(wins.size).toBe(fleet.length);
			// All inflight counters stay at 0, so selection is pure sampling —
			// no candidate may dominate the fleet.
			for (const count of wins.values()) {
				expect(count).toBeGreaterThan(20_000 / fleet.length / 4);
				expect(count).toBeLessThan((20_000 / fleet.length) * 4);
			}
		});
	});

	it("pick does not materialise an intermediate array", () => {
		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const fleet = Array.from({ length: 200 }, (_, i) =>
			cand(`i-${i}`, `h:${i}`),
		);
		// Any array-producing traversal on the candidate list is a per-call
		// allocation on the RPC hot path — trap them all.
		for (const method of ["filter", "map", "slice", "concat", "flatMap"]) {
			Object.defineProperty(fleet, method, {
				configurable: true,
				value: () => {
					throw new Error(`pick allocated via Array#${method}`);
				},
			});
		}
		expect(lb.pick(fleet).instance.instanceId).toMatch(/^i-\d+$/);
	});

	it("acquire / release symmetry", () => {
		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb);
		const r1 = lb.acquire("a");
		const r2 = lb.acquire("a");
		expect(lb.inflightOf("a")).toBe(2);
		r1();
		r1(); // idempotent
		expect(lb.inflightOf("a")).toBe(1);
		r2();
		expect(lb.inflightOf("a")).toBe(0);
	});
});

describe("LoadBalancer HALF_OPEN single-probe", () => {
	// driveHalfOpen opens the breaker for `inst` then advances the shared clock
	// past OPEN_DURATION_MS so the next state read lazily transitions to HALF_OPEN.
	function driveHalfOpen(
		cb: CircuitBreakerRegistry,
		clock: { t: number },
		key: string,
	): void {
		for (let i = 0; i < MIN_REQUESTS; i++) cb.recordFailure(key);
		expect(cb.state(key)).toBe("OPEN");
		clock.t += OPEN_DURATION_MS + 1;
		expect(cb.state(key)).toBe("HALF_OPEN");
	}

	it("admits exactly one probe through concurrent picks in HALF_OPEN", () => {
		const clock = { t: 1_000_000 };
		const cb = new CircuitBreakerRegistry(() => clock.t);
		const lb = new LoadBalancer(cb, { now: () => clock.t });
		const a = cand("a", "h:1");
		driveHalfOpen(cb, clock, cbKey(a.instance));

		// First pick claims the single probe slot.
		const first = lb.pick([a]);
		expect(first.instance.instanceId).toBe("a");

		// While the probe is in flight (no record* yet), the instance is no longer
		// eligible — a concurrent pick fails over to nothing → NoLiveInstance.
		expect(() => lb.pick([a])).toThrow(NoLiveInstanceError);
	});

	it("routes a concurrent caller to a healthy peer instead of the probing instance", () => {
		const clock = { t: 1_000_000 };
		const cb = new CircuitBreakerRegistry(() => clock.t);
		const lb = new LoadBalancer(cb, { now: () => clock.t });
		const probing = cand("a", "h:1");
		const healthy = cand("b", "h:2");
		driveHalfOpen(cb, clock, cbKey(probing.instance));

		// First pick may land on the probing instance (claims probe) or the
		// healthy one; force it onto the probing instance via inflight skew is not
		// needed — claim it directly to make the test deterministic.
		cb.canCall(cbKey(probing.instance)); // claim probe explicitly
		const picked = lb.pick([probing, healthy]);
		expect(picked.instance.instanceId).toBe("b");
	});

	it("re-admits a probe after the prior probe fails and the window reopens then re-halfopens", () => {
		const clock = { t: 1_000_000 };
		const cb = new CircuitBreakerRegistry(() => clock.t);
		const lb = new LoadBalancer(cb, { now: () => clock.t });
		const a = cand("a", "h:1");
		const key = cbKey(a.instance);
		driveHalfOpen(cb, clock, key);

		lb.pick([a]); // claims probe
		cb.recordFailure(key); // probe fails → OPEN, probe released
		expect(cb.state(key)).toBe("OPEN");
		expect(() => lb.pick([a])).toThrow(NoLiveInstanceError);

		// After another OPEN_DURATION_MS it half-opens again and admits one probe.
		clock.t += OPEN_DURATION_MS + 1;
		const second = lb.pick([a]);
		expect(second.instance.instanceId).toBe("a");
	});

	it("admits exactly one probe across many picks over a fleet", () => {
		const clock = { t: 1_000_000 };
		const cb = new CircuitBreakerRegistry(() => clock.t);
		const lb = new LoadBalancer(cb, { now: () => clock.t });
		const probing = cand("p", "h:0");
		const peers = Array.from({ length: 5 }, (_, i) =>
			cand(`peer-${i}`, `h:${i + 1}`),
		);
		driveHalfOpen(cb, clock, cbKey(probing.instance));

		// More than two eligible candidates, so the reservoir sampling path runs.
		// The probe slot must still be claimed at most once — nothing releases it
		// because no caller records an outcome.
		let probeWins = 0;
		for (let n = 0; n < 200; n++) {
			if (lb.pick([probing, ...peers]).instance.instanceId === "p") {
				probeWins++;
			}
		}
		expect(probeWins).toBeLessThanOrEqual(1);
	});

	it("closes the breaker on probe success and admits all callers again", () => {
		const clock = { t: 1_000_000 };
		const cb = new CircuitBreakerRegistry(() => clock.t);
		const lb = new LoadBalancer(cb, { now: () => clock.t });
		const a = cand("a", "h:1");
		const key = cbKey(a.instance);
		driveHalfOpen(cb, clock, key);

		lb.pick([a]); // claims probe
		cb.recordSuccess(key); // probe succeeds → CLOSED
		expect(cb.state(key)).toBe("CLOSED");
		// CLOSED instance has no probe gate — repeated picks all succeed.
		expect(lb.pick([a]).instance.instanceId).toBe("a");
		expect(lb.pick([a]).instance.instanceId).toBe("a");
	});
});
