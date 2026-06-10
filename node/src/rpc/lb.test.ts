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
		// Random sequence drives p2c sampling: pick i=0, j=1 (no shift, since 1>=0 → j=2; we want a vs b)
		// random() called twice: floor(0.0 * 3)=0 → i=0; floor(0.0 * 2)=0 → j=0, j>=i so j=1.
		const lb = new LoadBalancer(cb, { random: scriptedRandom([0.0, 0.0]) });
		const a = cand("a", "h:1");
		const b = cand("b", "h:2");
		const c = cand("c", "h:3");

		const release = lb.acquire("a"); // a has 1 inflight; b has 0
		try {
			const picked = lb.pick([a, b, c]);
			// random pick samples a (i=0) and b (j=1); b has fewer inflight → b wins.
			expect(picked.instance.instanceId).toBe("b");
		} finally {
			release();
		}
	});

	it("ties break toward the first sampled candidate", () => {
		const cb = new CircuitBreakerRegistry();
		const lb = new LoadBalancer(cb, { random: scriptedRandom([0.0, 0.0]) });
		const a = cand("a", "h:1");
		const b = cand("b", "h:2");
		const picked = lb.pick([a, b]);
		// Both at inflight=0 → ai <= bi true → a wins.
		expect(picked.instance.instanceId).toBe("a");
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
