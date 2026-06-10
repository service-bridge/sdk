import { describe, expect, it } from "bun:test";
import { RECONNECT_LADDER_MS, reconnectDelay } from "./reconnect-ladder";

describe("reconnectDelay", () => {
	it("returns the exact rung when jitter is disabled", () => {
		const opts = { jitterRatio: 0 };
		expect(reconnectDelay(0, opts)).toBe(1000);
		expect(reconnectDelay(1, opts)).toBe(5000);
		expect(reconnectDelay(2, opts)).toBe(15000);
		expect(reconnectDelay(3, opts)).toBe(30000);
		expect(reconnectDelay(4, opts)).toBe(60000);
	});

	it("saturates at the last rung beyond the ladder length", () => {
		const opts = { jitterRatio: 0 };
		expect(reconnectDelay(5, opts)).toBe(60000);
		expect(reconnectDelay(100, opts)).toBe(60000);
		expect(reconnectDelay(RECONNECT_LADDER_MS.length, opts)).toBe(60000);
	});

	it("clamps negative attempts to the first rung", () => {
		expect(reconnectDelay(-1, { jitterRatio: 0 })).toBe(1000);
		expect(reconnectDelay(-100, { jitterRatio: 0 })).toBe(1000);
	});

	it("applies symmetric jitter around the rung", () => {
		// random()=0 → offset = -ratio (lower bound).
		expect(reconnectDelay(0, { random: () => 0 })).toBe(800);
		// random()=0.5 → offset = 0 (exact rung).
		expect(reconnectDelay(0, { random: () => 0.5 })).toBe(1000);
		// random()≈1 → offset ≈ +ratio (upper bound).
		expect(reconnectDelay(0, { random: () => 0.999999 })).toBeCloseTo(1200, -1);
	});

	it("keeps the jittered delay within [base*(1-ratio), base*(1+ratio)]", () => {
		const ratio = 0.2;
		for (let i = 0; i < 1000; i++) {
			const d = reconnectDelay(2, { jitterRatio: ratio });
			expect(d).toBeGreaterThanOrEqual(15000 * (1 - ratio) - 1);
			expect(d).toBeLessThanOrEqual(15000 * (1 + ratio) + 1);
		}
	});

	it("respects a custom ladder", () => {
		const ladder = [100, 200, 300];
		expect(reconnectDelay(0, { ladder, jitterRatio: 0 })).toBe(100);
		expect(reconnectDelay(2, { ladder, jitterRatio: 0 })).toBe(300);
		expect(reconnectDelay(99, { ladder, jitterRatio: 0 })).toBe(300);
	});

	it("never returns negative delays even with extreme jitter", () => {
		expect(
			reconnectDelay(0, { jitterRatio: 5, random: () => 0 }),
		).toBeGreaterThanOrEqual(0);
	});

	it("throws on an empty ladder", () => {
		expect(() => reconnectDelay(0, { ladder: [] })).toThrow(/empty ladder/);
	});
});
