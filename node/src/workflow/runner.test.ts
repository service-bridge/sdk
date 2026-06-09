import { describe, expect, it } from "bun:test";
import { AsyncLocalStorage } from "node:async_hooks";
import {
	type RunnerDeps,
	RunnerParkedError,
	type RuntimeOps,
	run,
	type SbDomains,
	type StepSpanInfo,
} from "./runner";
import type { Step } from "./types";

function makeOps(): {
	ops: RuntimeOps;
	begins: unknown[];
	completes: unknown[];
	fails: unknown[];
	parks: unknown[];
} {
	const begins: unknown[] = [];
	const completes: unknown[] = [];
	const fails: unknown[] = [];
	const parks: unknown[] = [];
	const ops: RuntimeOps = {
		async beginStep(args) {
			begins.push(args);
			return { alreadyDone: false };
		},
		async completeStep(args) {
			completes.push(args);
		},
		async failStep(args) {
			fails.push(args);
			return { nextAction: "fail" };
		},
		async park(args) {
			parks.push(args);
		},
		async completeRun() {
			// no-op in unit tests — run completion is a subscriber concern
		},
	};
	return { ops, begins, completes, fails, parks };
}

function makeSb(): {
	sb: SbDomains;
	calls: Array<[string, string, unknown, unknown]>;
	publishes: Array<[string, unknown, unknown]>;
	starts: Array<[string, unknown, unknown]>;
} {
	const calls: Array<[string, string, unknown, unknown]> = [];
	const publishes: Array<[string, unknown, unknown]> = [];
	const starts: Array<[string, unknown, unknown]> = [];
	const sb: SbDomains = {
		rpc: {
			async call(service, method, payload, opts) {
				calls.push([service, method, payload, opts]);
				return { service, method, payload };
			},
		},
		event: {
			async publish(name, payload, opts) {
				publishes.push([name, payload, opts]);
				return { eventId: `ev-${name}` };
			},
		},
		workflow: {
			async start(name, input, opts) {
				starts.push([name, input, opts]);
				return { runId: `sub-${name}` };
			},
			async await(runId) {
				return { ok: runId };
			},
		},
	};
	return { sb, calls, publishes, starts };
}

describe("runner — thin call dispatch (ADR-W-018)", () => {
	it("call step → exactly one sb.rpc.call, args evaluated, output checkpointed", async () => {
		const { ops, begins, completes } = makeOps();
		const { sb, calls } = makeSb();
		const deps: RunnerDeps = { sb, ops };
		const steps: Step[] = [
			{
				id: "a",
				type: "call",
				service: "billing",
				method: "$.input.method",
				input: { amount: "$.input.amount" },
				opts: { timeout: "$.input.timeout" },
			},
		];
		const state = await run(
			steps,
			{
				runId: "r1",
				leaseEpoch: 1,
				state: {
					input: { method: "card.charge", amount: 100, timeout: "10s" },
				},
				compensating: false,
				maxParallelism: 0,
			},
			deps,
		);

		expect(calls).toHaveLength(1);
		expect(calls[0]).toEqual([
			"billing",
			"card.charge",
			{ amount: 100 },
			{ timeout: "10s" },
		]);
		expect(begins).toHaveLength(1);
		expect(completes).toHaveLength(1);
		expect(state.a).toEqual({
			service: "billing",
			method: "card.charge",
			payload: { amount: 100 },
		});
	});

	it("call step makes EXACTLY one sb.rpc.call (runner does not re-call)", async () => {
		const { ops } = makeOps();
		const { sb, calls } = makeSb();
		const steps: Step[] = [
			{
				id: "a",
				type: "call",
				service: "s",
				method: "m",
				input: {},
			},
		];
		await run(
			steps,
			{
				runId: "r1",
				leaseEpoch: 1,
				state: { input: {} },
				compensating: false,
				maxParallelism: 0,
			},
			{ sb, ops },
		);
		expect(calls).toHaveLength(1);
	});
});

describe("runner — sequential / parallel scheduling via waitFor", () => {
	it("sequential chain runs in order", async () => {
		const { ops } = makeOps();
		const { sb, calls } = makeSb();
		const steps: Step[] = [
			{ id: "a", type: "call", service: "s", method: "m", input: {} },
			{
				id: "b",
				type: "call",
				service: "s",
				method: "m",
				input: {},
				waitFor: ["a"],
			},
			{
				id: "c",
				type: "call",
				service: "s",
				method: "m",
				input: {},
				waitFor: ["b"],
			},
		];
		await run(
			steps,
			{
				runId: "r",
				leaseEpoch: 1,
				state: { input: {} },
				compensating: false,
				maxParallelism: 0,
			},
			{ sb, ops },
		);
		expect(calls).toHaveLength(3);
	});

	it("parallel via empty waitFor — maxParallelism respected", async () => {
		const { ops } = makeOps();
		const calls: number[] = [];
		const inFlight = { v: 0, peak: 0 };
		const sb: SbDomains = {
			rpc: {
				async call() {
					inFlight.v += 1;
					inFlight.peak = Math.max(inFlight.peak, inFlight.v);
					await new Promise((r) => setTimeout(r, 5));
					inFlight.v -= 1;
					calls.push(1);
					return null;
				},
			},
			event: {
				async publish() {
					return null;
				},
			},
			workflow: {
				async start() {
					return { runId: "" };
				},
				async await() {
					return null;
				},
			},
		};
		const steps: Step[] = ["a", "b", "c", "d"].map((id) => ({
			id,
			type: "call",
			service: "s",
			method: "m",
			input: {},
		}));
		await run(
			steps,
			{
				runId: "r",
				leaseEpoch: 1,
				state: { input: {} },
				compensating: false,
				maxParallelism: 2,
			},
			{ sb, ops },
		);
		expect(calls).toHaveLength(4);
		expect(inFlight.peak).toBeLessThanOrEqual(2);
	});
});

describe("runner — when skips step", () => {
	it("when=false → state.<id>=null, descendant runs", async () => {
		const { ops } = makeOps();
		const { sb, calls } = makeSb();
		const steps: Step[] = [
			{
				id: "skipped",
				type: "call",
				service: "s",
				method: "m",
				input: {},
				when: { equals: ["$.input.go", true] },
			},
			{
				id: "after",
				type: "call",
				service: "s",
				method: "m",
				input: {},
				waitFor: ["skipped"],
			},
		];
		const state = await run(
			steps,
			{
				runId: "r",
				leaseEpoch: 1,
				state: { input: { go: false } },
				compensating: false,
				maxParallelism: 0,
			},
			{ sb, ops },
		);
		expect(state.skipped).toBeNull();
		expect(calls.map((c) => c[0])).toEqual(["s"]); // only 'after' ran
	});
});

describe("runner — sleep parks", () => {
	it("sleep step calls Park and throws RunnerParkedError", async () => {
		const { ops, parks } = makeOps();
		const { sb } = makeSb();
		const steps: Step[] = [{ id: "wait", type: "sleep", durationSec: 5 }];
		await expect(
			run(
				steps,
				{
					runId: "r",
					leaseEpoch: 1,
					state: { input: {} },
					compensating: false,
					maxParallelism: 0,
				},
				{ sb, ops },
			),
		).rejects.toBeInstanceOf(RunnerParkedError);
		expect(parks).toHaveLength(1);
		expect((parks[0] as { sleep: unknown }).sleep).toEqual({ durationSec: 5 });
	});
});

describe("runner — wait_event / wait_signal", () => {
	it("wait_event parks with filterJson", async () => {
		const { ops, parks } = makeOps();
		const { sb } = makeSb();
		const steps: Step[] = [
			{
				id: "w",
				type: "wait_event",
				event: "order.paid",
				filter: { "$.userId": "$.input.userId" },
				timeoutSec: 30,
			},
		];
		await expect(
			run(
				steps,
				{
					runId: "r",
					leaseEpoch: 1,
					state: { input: { userId: "u-1" } },
					compensating: false,
					maxParallelism: 0,
				},
				{ sb, ops },
			),
		).rejects.toBeInstanceOf(RunnerParkedError);
		expect(
			(parks[0] as { eventWait: { filterJson: string } }).eventWait.filterJson,
		).toContain("u-1");
	});

	it("wait_signal parks with signal name", async () => {
		const { ops, parks } = makeOps();
		const { sb } = makeSb();
		const steps: Step[] = [
			{ id: "s", type: "wait_signal", signal: "admin-approve" },
		];
		await expect(
			run(
				steps,
				{
					runId: "r",
					leaseEpoch: 1,
					state: { input: {} },
					compensating: false,
					maxParallelism: 0,
				},
				{ sb, ops },
			),
		).rejects.toBeInstanceOf(RunnerParkedError);
		expect(
			(parks[0] as { signalWait: { signal: string } }).signalWait.signal,
		).toBe("admin-approve");
	});
});

describe("runner — beginStep cached output is honored (idempotency)", () => {
	it("alreadyDone=true → no sb.rpc.call, cached output reused", async () => {
		const begins: unknown[] = [];
		const completes: unknown[] = [];
		const ops: RuntimeOps = {
			async beginStep(args) {
				begins.push(args);
				return { alreadyDone: true, cachedOutput: { cached: true } };
			},
			async completeStep(args) {
				completes.push(args);
			},
			async failStep() {
				return { nextAction: "fail" };
			},
			async park() {},
			async completeRun() {},
		};
		const { sb, calls } = makeSb();
		const steps: Step[] = [
			{
				id: "a",
				type: "call",
				service: "s",
				method: "m",
				input: {},
			},
		];
		const state = await run(
			steps,
			{
				runId: "r",
				leaseEpoch: 1,
				state: { input: {} },
				compensating: false,
				maxParallelism: 0,
			},
			{ sb, ops },
		);
		expect(calls).toHaveLength(0);
		expect(completes).toHaveLength(0);
		expect(state.a).toEqual({ cached: true });
	});
});

describe("runner — forEach fan-out", () => {
	it("expands template per item with state.<as> set", async () => {
		const { ops } = makeOps();
		const { sb, calls } = makeSb();
		const steps: Step[] = [
			{
				id: "g",
				type: "parallel",
				forEach: { from: "$.input.items", as: "item" },
				steps: [
					{
						id: "track",
						type: "call",
						service: "tracking",
						method: "track.start",
						input: { itemId: "$.item" },
					},
				],
			},
		];
		await run(
			steps,
			{
				runId: "r",
				leaseEpoch: 1,
				state: { input: { items: ["a", "b", "c"] } },
				compensating: false,
				maxParallelism: 0,
			},
			{ sb, ops },
		);
		expect(calls).toHaveLength(3);
		expect(calls.map((c) => (c[2] as { itemId: string }).itemId)).toEqual([
			"a",
			"b",
			"c",
		]);
	});

	it("empty forEach.from → group completes, descendant runs", async () => {
		const { ops } = makeOps();
		const { sb, calls } = makeSb();
		const steps: Step[] = [
			{
				id: "g",
				type: "parallel",
				forEach: { from: "$.input.items", as: "item" },
				steps: [
					{
						id: "track",
						type: "call",
						service: "s",
						method: "m",
						input: {},
					},
				],
			},
			{
				id: "after",
				type: "call",
				service: "s",
				method: "m",
				input: {},
				waitFor: ["g"],
			},
		];
		await run(
			steps,
			{
				runId: "r",
				leaseEpoch: 1,
				state: { input: { items: [] } },
				compensating: false,
				maxParallelism: 0,
			},
			{ sb, ops },
		);
		expect(calls).toHaveLength(1); // only 'after'
	});
});

describe("runner — step span emission + ALS nesting (Task 1/2)", () => {
	// A wrapStep stub that simulates the real telemetry hook: it mints a span
	// opId, records the span info + the parent context observed at call time,
	// then runs fn inside a child ALS scope so nested steps/calls parent to it.
	function makeStepSpy() {
		const als = new AsyncLocalStorage<{ parentOpId: string }>();
		const spans: Array<{
			info: StepSpanInfo;
			parentOpId: string;
			opId: string;
		}> = [];
		let seq = 0;
		const wrapStep = async <T>(
			info: StepSpanInfo,
			fn: () => Promise<T>,
		): Promise<T> => {
			const parentOpId = als.getStore()?.parentOpId ?? "ROOT";
			const opId = `span-${seq++}`;
			spans.push({ info, parentOpId, opId });
			return als.run({ parentOpId: opId }, fn);
		};
		// Reading the context the call would observe.
		const currentParent = () => als.getStore()?.parentOpId ?? "ROOT";
		return { wrapStep, spans, currentParent };
	}

	it("emits one step span per executed step; inner call observes the span as parent", async () => {
		const { ops } = makeOps();
		const { sb, calls } = makeSb();
		const spy = makeStepSpy();
		// Capture the parent context observed inside the rpc call.
		let observedParent = "";
		const sbWithObserve: SbDomains = {
			...sb,
			rpc: {
				async call(service, method, payload, opts) {
					observedParent = spy.currentParent();
					return sb.rpc.call(service, method, payload, opts);
				},
			},
		};
		const deps: RunnerDeps = { sb: sbWithObserve, ops, wrapStep: spy.wrapStep };
		const steps: Step[] = [
			{ id: "reserve", type: "call", service: "inv", method: "m", input: {} },
		];
		await run(
			steps,
			{
				runId: "r1",
				leaseEpoch: 1,
				state: { input: {} },
				compensating: false,
				maxParallelism: 0,
			},
			deps,
		);
		expect(calls).toHaveLength(1);
		// Exactly one step span emitted for the single step.
		expect(spy.spans).toHaveLength(1);
		expect(spy.spans[0]!.info.stepId).toBe("reserve");
		expect(spy.spans[0]!.info.role).toBe("step");
		// The rpc call ran inside the step span scope.
		expect(observedParent).toBe(spy.spans[0]!.opId);
	});

	it("fanout: group span parents branch spans; branch steps parent to branch span", async () => {
		const { ops } = makeOps();
		const { sb, calls } = makeSb();
		const spy = makeStepSpy();
		const steps: Step[] = [
			{
				id: "ship-all",
				type: "parallel",
				forEach: { from: "$.input.items", as: "item" },
				steps: [
					{
						id: "ship",
						type: "call",
						service: "s",
						method: "m",
						input: { i: "$.item" },
					},
				],
			},
		];
		await run(
			steps,
			{
				runId: "r",
				leaseEpoch: 1,
				state: { input: { items: ["a", "b"] } },
				compensating: false,
				maxParallelism: 0,
			},
			{ sb, ops, wrapStep: spy.wrapStep },
		);
		expect(calls).toHaveLength(2);
		const group = spy.spans.find((s) => s.info.role === "group");
		expect(group).toBeDefined();
		expect(group!.parentOpId).toBe("ROOT");
		const branches = spy.spans.filter((s) => s.info.role === "branch");
		expect(branches).toHaveLength(2);
		// Each branch parents to the group span.
		for (const b of branches) {
			expect(b.parentOpId).toBe(group!.opId);
		}
		// Each branch step parents to its OWN branch span (R1: no sibling leak).
		const branchSteps = spy.spans.filter((s) => s.info.role === "step");
		expect(branchSteps).toHaveLength(2);
		const branchOpIds = new Set(branches.map((b) => b.opId));
		for (const st of branchSteps) {
			expect(branchOpIds.has(st.parentOpId)).toBe(true);
		}
		// The two branch steps must parent to DIFFERENT branch spans.
		expect(branchSteps[0]!.parentOpId).not.toBe(branchSteps[1]!.parentOpId);
	});
});

describe("runner — compensation flow", () => {
	it("compensating=true runs reverse-order compensate for completed call steps", async () => {
		const { ops } = makeOps();
		const { sb, calls } = makeSb();
		const steps: Step[] = [
			{
				id: "reserve",
				type: "call",
				service: "inventory",
				method: "items.reserve",
				input: {},
				compensate: {
					service: "inventory",
					method: "items.release",
					input: { resId: "$.reserve.id" },
				},
			},
			{
				id: "charge",
				type: "call",
				service: "billing",
				method: "card.charge",
				input: {},
				compensate: {
					service: "billing",
					method: "card.refund",
					input: { txId: "$.charge.txId" },
				},
			},
		];
		await run(
			steps,
			{
				runId: "r",
				leaseEpoch: 1,
				// state already contains completed outputs (resume after fail/cancel)
				state: {
					input: {},
					reserve: { id: "res-1" },
					charge: { txId: "tx-7" },
				},
				compensating: true,
				maxParallelism: 0,
			},
			{ sb, ops },
		);
		// Reverse order: charge.refund, then reserve.release.
		expect(calls.map((c) => `${c[0]}/${c[1]}`)).toEqual([
			"billing/card.refund",
			"inventory/items.release",
		]);
	});
});
