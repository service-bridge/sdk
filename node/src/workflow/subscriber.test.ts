// subscriber.test.ts — WorkflowSubscriber stream lifecycle, identity binding
// and lease heartbeats.
//
// The stream state machine itself lives in registry/StreamSupervisor and is
// covered there; these tests pin the wiring WorkflowSubscriber owns — which
// identity the Subscribe stream and each heartbeat carry, how a break walks the
// ladder, and which exec is allowed to retire a lease.

import { describe, expect, it, spyOn } from "bun:test";
import type { ReconnectDelayOptions } from "../utils/reconnect-ladder";
import type { SubscriberIdentity } from "./subscriber";
import { WorkflowSubscriber } from "./subscriber";

const realSetTimeout = globalThis.setTimeout;
const wait = (ms: number): Promise<void> =>
	new Promise((resolve) => {
		realSetTimeout(resolve, ms);
	});

interface FakeStream {
	on(event: string, cb: (...args: unknown[]) => void): void;
	emit(event: string, ...args: unknown[]): void;
	cancel(): void;
	cancels(): number;
}

function makeFakeStream(): FakeStream {
	const listeners = new Map<string, Array<(...a: unknown[]) => void>>();
	let cancels = 0;
	return {
		on(event, cb) {
			const list = listeners.get(event) ?? [];
			list.push(cb);
			listeners.set(event, list);
		},
		emit(event, ...args) {
			for (const cb of listeners.get(event) ?? []) cb(...args);
		},
		cancel() {
			cancels++;
		},
		cancels: () => cancels,
	};
}

interface HeartbeatCall {
	runId: string;
	instanceId: string;
	leaseEpoch: number;
}

interface Harness {
	sub: WorkflowSubscriber;
	streams: FakeStream[];
	requests: Array<{ serviceId: string; instanceId: string }>;
	heartbeats: HeartbeatCall[];
	warns: string[];
	last(): FakeStream;
	failHeartbeat(msg: string | null): void;
	throwHeartbeat(on: boolean): void;
}

function makeHarness(
	opts: {
		identity?: () => SubscriberIdentity | null;
		reconnectOpts?: ReconnectDelayOptions;
	} = {},
): Harness {
	const streams: FakeStream[] = [];
	const requests: Array<{ serviceId: string; instanceId: string }> = [];
	const heartbeats: HeartbeatCall[] = [];
	const warns: string[] = [];
	let heartbeatError: string | null = null;
	let heartbeatThrows = false;

	const sub = new WorkflowSubscriber({
		rpc: {
			subscribe: (req: { serviceId: string; instanceId: string }) => {
				requests.push(req);
				const s = makeFakeStream();
				streams.push(s);
				return s;
			},
			heartbeat: (
				req: HeartbeatCall,
				cb: (err: { message: string } | null) => void,
			) => {
				if (heartbeatThrows) throw new Error("channel closed");
				heartbeats.push(req);
				cb(heartbeatError === null ? null : { message: heartbeatError });
			},
			// biome-ignore lint/suspicious/noExplicitAny: minimal grpc stub
		} as any,
		identity:
			opts.identity ?? (() => ({ serviceId: "svc-1", instanceId: "inst-1" })),
		// biome-ignore lint/suspicious/noExplicitAny: these tests never run the runner
		deps: {} as any,
		logger: {
			warn: (...args: unknown[]) => warns.push(args.map(String).join(" ")),
			error: () => {},
		},
		lookupLocalGraph: () => null,
		reconnectOpts: opts.reconnectOpts ?? { ladder: [4], jitterRatio: 0 },
	});

	return {
		sub,
		streams,
		requests,
		heartbeats,
		warns,
		last: () => streams[streams.length - 1]!,
		failHeartbeat: (msg) => {
			heartbeatError = msg;
		},
		throwHeartbeat: (on) => {
			heartbeatThrows = on;
		},
	};
}

// Waiting on the reconnect actually happening beats waiting a fixed span: the
// ladder rungs are milliseconds apart, and a loaded machine can outrun any
// sleep long enough to make the next close land on an already-dead stream.
async function waitForReconnect(h: Harness, closed: unknown): Promise<void> {
	for (let i = 0; i < 400 && h.last() === closed; i++) {
		await wait(5);
	}
}

async function recordedDelays(body: () => Promise<void>): Promise<number[]> {
	const setSpy = spyOn(globalThis, "setTimeout");
	try {
		await body();
		return setSpy.mock.calls.map((c) => c[1] as number);
	} finally {
		setSpy.mockRestore();
	}
}

// tick drives one heartbeat sweep without waiting the 10s interval.
function tick(sub: WorkflowSubscriber): void {
	// biome-ignore lint/suspicious/noExplicitAny: private sweep hook
	(sub as any).tick();
}

function startHeartbeat(
	sub: WorkflowSubscriber,
	runId: string,
	leaseEpoch: number,
): void {
	// biome-ignore lint/suspicious/noExplicitAny: private lease hook
	(sub as any).startHeartbeat(runId, leaseEpoch);
}

function stopHeartbeat(
	sub: WorkflowSubscriber,
	runId: string,
	leaseEpoch: number,
): void {
	// biome-ignore lint/suspicious/noExplicitAny: private lease hook
	(sub as any).stopHeartbeat(runId, leaseEpoch);
}

describe("WorkflowSubscriber stream", () => {
	it("Start_SubscribesWithCurrentIdentity", () => {
		const h = makeHarness();
		h.sub.start();
		expect(h.requests).toEqual([{ serviceId: "svc-1", instanceId: "inst-1" }]);
		h.sub.close();
	});

	it("NoIdentity_RetriesUntilAvailable", async () => {
		let id: SubscriberIdentity | null = null;
		const h = makeHarness({ identity: () => id });
		h.sub.start();
		expect(h.streams).toHaveLength(0);

		id = { serviceId: "svc-1", instanceId: "inst-7" };
		await wait(25);
		expect(h.requests).toEqual([{ serviceId: "svc-1", instanceId: "inst-7" }]);
		h.sub.close();
	});

	it("RotatedInstanceId_RestartsSubscribeStream", () => {
		// Control.RefreshCert mints a fresh instance_id on every rotation. A
		// stream still bound to the retired id keeps the runtime assigning runs
		// to an instance it has already torn down.
		let id: SubscriberIdentity = { serviceId: "svc-1", instanceId: "inst-1" };
		const h = makeHarness({ identity: () => id });
		h.sub.start();
		expect(h.requests).toHaveLength(1);

		id = { serviceId: "svc-1", instanceId: "inst-2" };
		tick(h.sub);

		expect(h.requests).toEqual([
			{ serviceId: "svc-1", instanceId: "inst-1" },
			{ serviceId: "svc-1", instanceId: "inst-2" },
		]);
		expect(h.streams[0]!.cancels()).toBe(1);
		h.sub.close();
	});

	it("UnchangedIdentity_KeepsTheStream", () => {
		const h = makeHarness();
		h.sub.start();
		tick(h.sub);
		tick(h.sub);
		expect(h.requests).toHaveLength(1);
		h.sub.close();
	});

	it("ErrorPlusEnd_SchedulesSingleReconnectPerCycle", async () => {
		const h = makeHarness();
		h.sub.start();
		for (let cycle = 0; cycle < 3; cycle++) {
			const cur = h.last();
			cur.emit("error", new Error("broken"));
			cur.emit("end");
			await wait(25);
			expect(h.streams).toHaveLength(cycle + 2);
		}
		h.sub.close();
	});

	it("CleanCloses_ClimbTheLadder", async () => {
		// A runtime that drains streams gracefully used to pin this subscriber at
		// the 1s rung forever, because the old connect loop reset the attempt
		// counter whenever runOnce resolved.
		const h = makeHarness({
			reconnectOpts: { ladder: [4, 12, 24], jitterRatio: 0 },
		});
		const delays = await recordedDelays(async () => {
			h.sub.start();
			for (let i = 0; i < 3; i++) {
				const closed = h.last();
				closed.emit("end");
				await waitForReconnect(h, closed);
			}
		});
		h.sub.close();
		expect(delays.slice(0, 3)).toEqual([4, 12, 24]);
	});

	it("LateEndFromReplacedStream_DoesNotOpenAThirdStream", async () => {
		const h = makeHarness();
		h.sub.start();
		const streamA = h.streams[0]!;
		streamA.emit("error", new Error("broken"));
		await wait(25);
		expect(h.streams).toHaveLength(2);

		streamA.emit("end");
		await wait(25);
		expect(h.streams).toHaveLength(2);
		h.sub.close();
		expect(h.streams[1]!.cancels()).toBe(1);
	});

	it("Close_NoReconnect", async () => {
		const h = makeHarness();
		h.sub.start();
		h.sub.close();
		h.streams[0]!.emit("error", new Error("closed"));
		h.streams[0]!.emit("end");
		await wait(25);
		expect(h.streams).toHaveLength(1);
	});

	it("Close_ClearsPendingReconnectTimer", () => {
		const setSpy = spyOn(globalThis, "setTimeout");
		const clearSpy = spyOn(globalThis, "clearTimeout");
		try {
			const h = makeHarness({
				reconnectOpts: { ladder: [10_000], jitterRatio: 0 },
			});
			h.sub.start();
			h.last().emit("error", new Error("boom"));

			const timerHandle = setSpy.mock.results.at(-1)?.value;
			expect(timerHandle).toBeDefined();

			h.sub.close();
			expect(clearSpy.mock.calls.some(([t]) => t === timerHandle)).toBe(true);
		} finally {
			setSpy.mockRestore();
			clearSpy.mockRestore();
		}
	});
});

describe("WorkflowSubscriber heartbeat", () => {
	it("OneSweepTimerRegardlessOfRunCount", () => {
		const setSpy = spyOn(globalThis, "setInterval");
		try {
			const h = makeHarness();
			h.sub.start();
			const before = setSpy.mock.calls.length;
			for (let i = 0; i < 50; i++) startHeartbeat(h.sub, `run-${i}`, 1);
			expect(setSpy.mock.calls.length).toBe(before);
			h.sub.close();
		} finally {
			setSpy.mockRestore();
		}
	});

	it("SweepTimerIsUnrefd_DoesNotHoldTheProcess", () => {
		const setSpy = spyOn(globalThis, "setInterval");
		try {
			const h = makeHarness();
			h.sub.start();
			const timer = setSpy.mock.results.at(-1)?.value as { hasRef(): boolean };
			expect(timer.hasRef()).toBe(false);
			h.sub.close();
		} finally {
			setSpy.mockRestore();
		}
	});

	it("SweepSendsOneHeartbeatPerLeaseWithItsEpoch", () => {
		const h = makeHarness();
		h.sub.start();
		startHeartbeat(h.sub, "run-a", 3);
		startHeartbeat(h.sub, "run-b", 7);
		tick(h.sub);

		expect(h.heartbeats).toEqual([
			{ runId: "run-a", instanceId: "inst-1", leaseEpoch: 3 },
			{ runId: "run-b", instanceId: "inst-1", leaseEpoch: 7 },
		]);
		h.sub.close();
	});

	it("HeartbeatUsesCurrentInstanceIdAfterRotation", () => {
		let id: SubscriberIdentity = { serviceId: "svc-1", instanceId: "inst-1" };
		const h = makeHarness({ identity: () => id });
		h.sub.start();
		startHeartbeat(h.sub, "run-a", 1);

		id = { serviceId: "svc-1", instanceId: "inst-2" };
		tick(h.sub);

		expect(h.heartbeats).toEqual([
			{ runId: "run-a", instanceId: "inst-2", leaseEpoch: 1 },
		]);
		h.sub.close();
	});

	it("StaleEpochStop_DoesNotRetireTheNewLease", () => {
		// A run re-assigned while the previous exec is still unwinding bumps the
		// epoch. The old exec's `finally` must not kill the new assignment's
		// heartbeat: lease expiry → re-assignment → livelock.
		const h = makeHarness();
		h.sub.start();
		startHeartbeat(h.sub, "run-1", 1);
		startHeartbeat(h.sub, "run-1", 2);

		stopHeartbeat(h.sub, "run-1", 1);
		tick(h.sub);
		expect(h.heartbeats).toEqual([
			{ runId: "run-1", instanceId: "inst-1", leaseEpoch: 2 },
		]);

		stopHeartbeat(h.sub, "run-1", 2);
		tick(h.sub);
		expect(h.heartbeats).toHaveLength(1);
		h.sub.close();
	});

	it("RpcError_IsLoggedAndLeaseKept", () => {
		const h = makeHarness();
		h.sub.start();
		startHeartbeat(h.sub, "run-1", 1);
		h.failHeartbeat("unavailable");
		tick(h.sub);

		expect(
			h.warns.some((w) => w.includes("run-1") && w.includes("unavailable")),
		).toBe(true);
		h.failHeartbeat(null);
		tick(h.sub);
		expect(h.heartbeats).toHaveLength(2);
		h.sub.close();
	});

	it("SynchronousThrow_IsLoggedAndLeaseKept", () => {
		// A throw used to silently drop the lease: the run was reclaimed 15s
		// later with nothing in the logs to explain why.
		const h = makeHarness();
		h.sub.start();
		startHeartbeat(h.sub, "run-1", 1);
		h.throwHeartbeat(true);
		tick(h.sub);
		expect(
			h.warns.some((w) => w.includes("run-1") && w.includes("channel closed")),
		).toBe(true);

		h.throwHeartbeat(false);
		tick(h.sub);
		expect(h.heartbeats).toEqual([
			{ runId: "run-1", instanceId: "inst-1", leaseEpoch: 1 },
		]);
		h.sub.close();
	});

	it("Close_DropsEveryLeaseAndTheSweepTimer", () => {
		const clearSpy = spyOn(globalThis, "clearInterval");
		try {
			const h = makeHarness();
			h.sub.start();
			startHeartbeat(h.sub, "run-1", 1);
			h.sub.close();

			tick(h.sub);
			expect(h.heartbeats).toEqual([]);
			expect(clearSpy).toHaveBeenCalled();
		} finally {
			clearSpy.mockRestore();
		}
	});
});
