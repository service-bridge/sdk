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
			return { nextAction: "fail_run", retryDelaySec: 0 };
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
				return { nextAction: "fail_run", retryDelaySec: 0 };
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
	// reserve → charge saga; both steps declare a compensating call.
	const sagaSteps = (): Step[] => [
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

	it("re-assigned compensation does not replay already-done compensations", async () => {
		// The runtime re-assigns a compensating run whose lease expired; the
		// reverse walk starts over. Compensations already checkpointed must not
		// run a second time (a saga refund would be paid twice).
		const done = new Set(["charge.compensate"]);
		const begins: string[] = [];
		const completes: string[] = [];
		const ops: RuntimeOps = {
			async beginStep(args) {
				begins.push(args.stepId);
				return { alreadyDone: done.has(args.stepId) };
			},
			async completeStep(args) {
				completes.push(args.stepId);
			},
			async failStep() {
				return { nextAction: "fail_run", retryDelaySec: 0 };
			},
			async park() {},
			async completeRun() {},
		};
		const { sb, calls } = makeSb();
		await run(
			sagaSteps(),
			{
				runId: "r",
				leaseEpoch: 2,
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
		expect(begins).toEqual(["charge.compensate", "reserve.compensate"]);
		// Only the outstanding compensation reached the outside world.
		expect(calls.map((c) => `${c[0]}/${c[1]}`)).toEqual([
			"inventory/items.release",
		]);
		expect(completes).toEqual(["reserve.compensate"]);
	});

	it("failed compensation is reported, walk continues, run is not completed", async () => {
		const { ops, fails, completes } = makeOps();
		const calls: string[] = [];
		const sb: SbDomains = {
			rpc: {
				async call(service, method) {
					calls.push(`${service}/${method}`);
					if (method === "card.refund") throw new Error("refund gateway down");
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
		await expect(
			run(
				sagaSteps(),
				{
					runId: "r",
					leaseEpoch: 1,
					state: {
						input: {},
						reserve: { id: "res-1" },
						charge: { txId: "tx-7" },
					},
					compensating: true,
					maxParallelism: 0,
				},
				{ sb, ops },
			),
		).rejects.toThrow("refund gateway down");
		// The earlier step was still compensated.
		expect(calls).toEqual(["billing/card.refund", "inventory/items.release"]);
		expect(fails).toHaveLength(1);
		expect((fails[0] as { stepId: string }).stepId).toBe("charge.compensate");
		expect((fails[0] as { retriable: boolean }).retriable).toBe(false);
		expect(completes).toHaveLength(1);
	});
});

describe("runner — FailStep decision is executed (retry / fail_run)", () => {
	it("nextAction=retry re-executes the step; runtime decides when to stop", async () => {
		const fails: Array<{ retriable: boolean }> = [];
		let attempts = 0;
		const ops: RuntimeOps = {
			async beginStep() {
				return { alreadyDone: false };
			},
			async completeStep() {},
			async failStep(args) {
				fails.push(args);
				// Budget of 2 attempts: retry once, then hand the run over.
				return fails.length < 2
					? { nextAction: "retry", retryDelaySec: 0 }
					: { nextAction: "compensate", retryDelaySec: 0 };
			},
			async park() {},
			async completeRun() {},
		};
		const sb: SbDomains = {
			rpc: {
				async call() {
					attempts += 1;
					throw new Error("flaky");
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
		const plan: Step[] = [
			{
				id: "a",
				type: "call",
				service: "s",
				method: "m",
				input: {},
				retry: { maxAttempts: 2 },
			},
		];
		await expect(
			run(
				plan,
				{
					runId: "r",
					leaseEpoch: 1,
					state: { input: {} },
					compensating: false,
					maxParallelism: 0,
				},
				{ sb, ops },
			),
		).rejects.toThrow("flaky");
		expect(attempts).toBe(2);
		expect(fails).toHaveLength(2);
		// A declared retry policy is what makes the step retriable for the runtime.
		expect(fails.every((f) => f.retriable)).toBe(true);
	});

	it("step without a retry policy reports retriable=false and is not re-executed", async () => {
		const { ops, fails } = makeOps();
		let attempts = 0;
		const sb: SbDomains = {
			rpc: {
				async call() {
					attempts += 1;
					throw new Error("boom");
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
		await expect(
			run(
				[{ id: "a", type: "call", service: "s", method: "m", input: {} }],
				{
					runId: "r",
					leaseEpoch: 1,
					state: { input: {} },
					compensating: false,
					maxParallelism: 0,
				},
				{ sb, ops },
			),
		).rejects.toThrow("boom");
		expect(attempts).toBe(1);
		expect((fails[0] as { retriable: boolean }).retriable).toBe(false);
	});
});

describe("runner — a failing step stops its level", () => {
	it("sibling does not dispatch or checkpoint once another step failed", async () => {
		const completes: string[] = [];
		const ops: RuntimeOps = {
			async beginStep(args) {
				// 'slow' is still checkpointing when 'fast' fails.
				if (args.stepId === "slow") {
					await new Promise((r) => setTimeout(r, 20));
				}
				return { alreadyDone: false };
			},
			async completeStep(args) {
				completes.push(args.stepId);
			},
			async failStep() {
				return { nextAction: "compensate", retryDelaySec: 0 };
			},
			async park() {},
			async completeRun() {},
		};
		const calls: string[] = [];
		const sb: SbDomains = {
			rpc: {
				async call(_service, method) {
					calls.push(method);
					if (method === "fails") throw new Error("nope");
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
		const plan: Step[] = [
			{ id: "fast", type: "call", service: "s", method: "fails", input: {} },
			{ id: "slow", type: "call", service: "s", method: "ok", input: {} },
		];
		await expect(
			run(
				plan,
				{
					runId: "r",
					leaseEpoch: 1,
					state: { input: {} },
					compensating: false,
					maxParallelism: 0,
				},
				{ sb, ops },
			),
		).rejects.toThrow("nope");
		expect(calls).toEqual(["fails"]);
		expect(completes).toEqual([]);
	});
});

describe("scoped fanout and cancellation regressions", () => {
	it("honors sibling dependencies inside fanout and resolves template aliases", async () => {
		const { ops } = makeOps();
		const { sb } = makeSb();
		const order: string[] = [];
		const steps: Step[] = [
			{
				id: "group",
				type: "parallel",
				forEach: { from: "$.input.items", as: "item" },
				steps: [
					{
						id: "b",
						type: "local",
						waitFor: ["a"],
						fn: async (state) => {
							order.push(`b:${state.item}`);
							return state.a;
						},
					},
					{
						id: "a",
						type: "local",
						fn: async (state) => {
							order.push(`a:${state.item}`);
							return state.item;
						},
					},
				],
			},
		];
		const state = await run(
			steps,
			{
				runId: "r",
				leaseEpoch: 1,
				state: { input: { items: [1, 2] } },
				compensating: false,
				maxParallelism: 2,
			},
			{ sb, ops },
		);
		expect(order.indexOf("a:1")).toBeLessThan(order.indexOf("b:1"));
		expect(order.indexOf("a:2")).toBeLessThan(order.indexOf("b:2"));
		expect(state.group).toEqual({ "a:0": 1, "b:0": 1, "a:1": 2, "b:1": 2 });
	});
	it("compensates null concrete outputs with restored fanout bindings in reverse order", async () => {
		const { ops } = makeOps();
		const { sb, calls } = makeSb();
		const steps: Step[] = [
			{
				id: "group",
				type: "parallel",
				forEach: { from: "$.input.items", as: "item" },
				steps: [
					{
						id: "charge",
						type: "call",
						service: "billing",
						method: "charge",
						input: "$.item",
						compensate: {
							service: "billing",
							method: "refund",
							input: "$.item",
						},
					},
				],
			},
		];
		await run(
			steps,
			{
				runId: "r",
				leaseEpoch: 2,
				state: {
					input: { items: ["first", "second"] },
					"charge:0": null,
					"charge:1": null,
				},
				compensating: true,
				maxParallelism: 2,
			},
			{ sb, ops },
		);
		expect(calls.map((call) => call[2])).toEqual(["second", "first"]);
	});
	it("bounds atomic operations across nested groups with one shared semaphore", async () => {
		const { ops } = makeOps();
		const { sb } = makeSb();
		let running = 0;
		let peak = 0;
		const steps: Step[] = [0, 1, 2].map((group) => ({
			id: `g${group}`,
			type: "parallel",
			steps: [0, 1, 2, 3].map((item) => ({
				id: `s${group}_${item}`,
				type: "local",
				fn: async () => {
					running++;
					peak = Math.max(peak, running);
					await new Promise((resolve) => setTimeout(resolve, 2));
					running--;
					return null;
				},
			})),
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
		expect(peak).toBe(2);
	});
	it("aborts cooperative local work without checkpointing its stale output", async () => {
		const { ops, completes } = makeOps();
		const { sb } = makeSb();
		const controller = new AbortController();
		let entered!: () => void;
		const ready = new Promise<void>((resolve) => {
			entered = resolve;
		});
		const promise = run(
			[
				{
					id: "local",
					type: "local",
					fn: async (_state, context) => {
						entered();
						await new Promise((resolve) =>
							context.signal!.addEventListener("abort", resolve, {
								once: true,
							}),
						);
						return "stale";
					},
				},
			],
			{
				runId: "r",
				leaseEpoch: 1,
				state: { input: {} },
				compensating: false,
				maxParallelism: 1,
				signal: controller.signal,
			},
			{ sb, ops },
		);
		await ready;
		controller.abort();
		await expect(promise).rejects.toThrow(/aborted/);
		expect(completes).toEqual([]);
	});
});

it("large fanout with zero/default cap completes every item with bounded concurrency", async () => {
	const { ops, completes } = makeOps();
	const { sb } = makeSb();
	let running = 0;
	let peak = 0;
	const seen = new Set<number>();
	const items = Array.from({ length: 1200 }, (_, idx) => idx);
	await run(
		[
			{
				id: "large",
				type: "parallel",
				forEach: { from: "$.input.items", as: "item" },
				steps: [
					{
						id: "work",
						type: "local",
						fn: async (state) => {
							running++;
							peak = Math.max(peak, running);
							await new Promise((resolve) => setTimeout(resolve, 1));
							seen.add(state.item as number);
							running--;
							return state.item;
						},
					},
				],
			},
		],
		{
			runId: "large",
			leaseEpoch: 1,
			state: { input: { items } },
			compensating: false,
			maxParallelism: 0,
		},
		{ sb, ops },
	);
	expect(seen.size).toBe(items.length);
	expect(peak).toBeLessThanOrEqual(64);
	expect(peak).toBeGreaterThan(1);
	expect(completes).toHaveLength(items.length + 1);
});

it("fanout budget counts every statically nested child before business dispatch", async () => {
	const { ops } = makeOps();
	const { sb } = makeSb();
	let calls = 0;
	const nested: Step = {
		id: "nested",
		type: "sequence",
		steps: Array.from({ length: 100 }, (_, i) => ({
			id: `work_${i}`,
			type: "local",
			fn: async () => {
				calls++;
				return null;
			},
		})),
	};
	await expect(
		run(
			[
				{
					id: "fanout",
					type: "parallel",
					forEach: { from: "$.input.items", as: "item" },
					steps: [nested],
				},
			],
			{
				runId: "r",
				leaseEpoch: 1,
				state: { input: { items: Array.from({ length: 100 }, (_, i) => i) } },
				compensating: false,
				maxParallelism: 0,
			},
			{ ops, sb },
		),
	).rejects.toThrow();
	expect(calls).toBe(0);
});

it("compensation reverses causal dependencies rather than declaration order", async () => {
	const { ops } = makeOps();
	const { sb } = makeSb();
	const undone: string[] = [];
	sb.rpc.call = async (_service, method) => {
		undone.push(method);
		return null;
	};
	await run(
		[
			{
				id: "a",
				type: "call",
				service: "s",
				method: "A",
				waitFor: ["b"],
				input: {},
				compensate: { method: "UndoA", input: {} },
			},
			{
				id: "b",
				type: "call",
				service: "s",
				method: "B",
				input: {},
				compensate: { method: "UndoB", input: {} },
			},
		],
		{
			runId: "r",
			leaseEpoch: 1,
			state: { a: null, b: null },
			compensating: true,
			maxParallelism: 0,
		},
		{ ops, sb },
	);
	expect(undone).toEqual(["UndoA", "UndoB"]);
});
