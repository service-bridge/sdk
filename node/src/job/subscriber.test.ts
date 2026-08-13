// subscriber.test.ts — JobSubscriber stream lifecycle and lease heartbeat.
//
// The stream state machine itself lives in registry/StreamSupervisor and is
// covered there; these tests pin the wiring JobSubscriber owns — what it sends
// on open, what a break does to the ladder, and how a failing heartbeat is
// escalated.

import { describe, expect, it, spyOn } from "bun:test";
import { Registry } from "../registry/registry";
import type { ReconnectDelayOptions } from "../utils/reconnect-ladder";
import { JobDomain } from "./domain";
import type { IdentityProvider, SubscriberDeps } from "./subscriber";
import { JobSubscriber } from "./subscriber";

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

interface Harness {
	sub: JobSubscriber;
	streams: FakeStream[];
	requests: Array<{ serviceId: string; instanceId: string }>;
	heartbeats: Array<{ serviceId: string; instanceId: string }>;
	warns: string[];
	domain: JobDomain;
	results: unknown[];
	last(): FakeStream;
	failHeartbeat(msg: string | null): void;
}

function makeHarness(
	opts: {
		identity?: () => IdentityProvider | null;
		reconnectOpts?: ReconnectDelayOptions;
	} = {},
): Harness {
	const streams: FakeStream[] = [];
	const requests: Array<{ serviceId: string; instanceId: string }> = [];
	const heartbeats: Array<{ serviceId: string; instanceId: string }> = [];
	const warns: string[] = [];
	const results: unknown[] = [];
	const domain = new JobDomain(new Registry());
	let heartbeatError: string | null = null;

	const deps: SubscriberDeps = {
		rpcClient: {
			subscribe: (req: { serviceId: string; instanceId: string }) => {
				requests.push(req);
				const s = makeFakeStream();
				streams.push(s);
				return s;
			},
			jobResult: (req: unknown, cb: (err: unknown) => void) => {
				results.push(req);
				cb(null);
			},
			heartbeat: (
				req: { serviceId: string; instanceId: string },
				cb: (err: { message: string } | null) => void,
			) => {
				heartbeats.push(req);
				cb(heartbeatError === null ? null : { message: heartbeatError });
			},
			// biome-ignore lint/suspicious/noExplicitAny: minimal grpc stub
		} as any,
		identity:
			opts.identity ?? (() => ({ serviceId: "svc-1", instanceId: "inst-1" })),
		domain,
		logger: {
			warn: (m) => warns.push(m),
			error: () => {},
		},
		runWithTrace: (_xSbTrace, fn) => fn(),
		reconnectOpts: opts.reconnectOpts ?? { ladder: [4], jitterRatio: 0 },
	};

	return {
		sub: new JobSubscriber(deps),
		streams,
		requests,
		heartbeats,
		warns,
		domain,
		results,
		last: () => streams[streams.length - 1]!,
		failHeartbeat: (msg) => {
			heartbeatError = msg;
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

describe("JobSubscriber stream", () => {
	it("Start_SubscribesWithCurrentIdentity", async () => {
		const h = makeHarness();
		h.sub.start();
		expect(h.requests).toEqual([{ serviceId: "svc-1", instanceId: "inst-1" }]);
		await h.sub.stop();
	});

	it("NoIdentity_RetriesUntilAvailable", async () => {
		let id: IdentityProvider | null = null;
		const h = makeHarness({ identity: () => id });
		h.sub.start();
		expect(h.streams).toHaveLength(0);

		id = { serviceId: "svc-1", instanceId: "inst-9" };
		await wait(25);
		expect(h.requests).toEqual([{ serviceId: "svc-1", instanceId: "inst-9" }]);
		await h.sub.stop();
	});

	it("StreamError_Reconnects", async () => {
		const h = makeHarness();
		h.sub.start();
		h.last().emit("error", new Error("boom"));
		await wait(25);
		expect(h.streams).toHaveLength(2);
		expect(h.warns.some((w) => w.includes("boom"))).toBe(true);
		await h.sub.stop();
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
		await h.sub.stop();
	});

	it("CleanCloses_ClimbTheLadder", async () => {
		// A runtime that drains streams gracefully (settings reload, graceful
		// drain) used to pin this subscriber at the 1s rung forever, because the
		// old connect loop reset the attempt counter whenever runOnce resolved.
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
		await h.sub.stop();
		expect(delays.slice(0, 3)).toEqual([4, 12, 24]);
	});

	it("DataFrame_ResetsLadder", async () => {
		const h = makeHarness({
			reconnectOpts: { ladder: [4, 12, 24], jitterRatio: 0 },
		});
		const delays = await recordedDelays(async () => {
			h.sub.start();
			const first = h.last();
			first.emit("end");
			await waitForReconnect(h, first);
			const second = h.last();
			second.emit("data", {
				executionId: "e-1",
				jobName: "unregistered",
				xSbTrace: "",
			});
			second.emit("end");
			await waitForReconnect(h, second);
		});
		await h.sub.stop();
		expect(delays.slice(0, 2)).toEqual([4, 4]);
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
		await h.sub.stop();
		expect(h.streams[1]!.cancels()).toBe(1);
	});

	it("Stop_NoReconnect", async () => {
		const h = makeHarness();
		h.sub.start();
		await h.sub.stop();
		h.streams[0]!.emit("error", new Error("closed"));
		h.streams[0]!.emit("end");
		await wait(25);
		expect(h.streams).toHaveLength(1);
	});

	it("Stop_ClearsPendingReconnectTimer", async () => {
		// An untracked wait timer keeps the event loop alive up to a full ladder
		// rung after the subscriber is closed.
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

			await h.sub.stop();
			expect(clearSpy.mock.calls.some(([t]) => t === timerHandle)).toBe(true);
		} finally {
			setSpy.mockRestore();
			clearSpy.mockRestore();
		}
	});
});

describe("JobSubscriber heartbeat", () => {
	it("TimerIsUnrefd_DoesNotHoldTheProcess", async () => {
		const setSpy = spyOn(globalThis, "setInterval");
		try {
			const h = makeHarness();
			h.sub.start();
			const timer = setSpy.mock.results.at(-1)?.value as {
				hasRef(): boolean;
			};
			expect(timer.hasRef()).toBe(false);
			await h.sub.stop();
		} finally {
			setSpy.mockRestore();
		}
	});

	it("ConsecutiveFailures_RestartTheStream", async () => {
		const h = makeHarness();
		h.sub.start();
		expect(h.streams).toHaveLength(1);

		for (let i = 0; i < 3; i++) {
			// biome-ignore lint/suspicious/noExplicitAny: private escalation hook
			(h.sub as any).onHeartbeatFailure("unavailable");
		}
		expect(h.streams).toHaveLength(2);
		expect(h.warns.filter((w) => w.includes("heartbeat failed"))).toHaveLength(
			3,
		);
		expect(h.warns.some((w) => w.includes("threshold reached"))).toBe(true);
		await h.sub.stop();
	});

	it("FailureBelowThreshold_LogsWithoutRestarting", async () => {
		const h = makeHarness();
		h.sub.start();
		// biome-ignore lint/suspicious/noExplicitAny: private escalation hook
		(h.sub as any).onHeartbeatFailure("transient");
		expect(h.streams).toHaveLength(1);
		expect(h.warns.some((w) => w.includes("(1/3): transient"))).toBe(true);
		await h.sub.stop();
	});
});

describe("JobSubscriber concurrency", () => {
	function makeExec(executionId: string) {
		return {
			executionId,
			jobName: "nightly",
			scheduledAtUnixMs: Date.now(),
			localScheduledAtUnixMs: Date.now(),
			attempt: 1,
			leaseEpoch: 1,
			idempotencyKey: `idem-${executionId}`,
			xSbTrace: "",
		};
	}

	it("MaxConcurrent_QueuesInsteadOfSheddingLoad", async () => {
		// Executions arrive holding a runtime-issued lease, so the wait queue is
		// deliberately unbounded: shedding here abandons work the runtime believes
		// this instance owns.
		const h = makeHarness();
		let running = 0;
		let peak = 0;
		const release: Array<() => void> = [];
		h.domain.handle(
			"nightly",
			{ trigger: { interval: 1000 }, maxConcurrent: 1 },
			async () => {
				running++;
				peak = Math.max(peak, running);
				await new Promise<void>((r) => release.push(r));
				running--;
			},
		);
		h.sub.start();

		for (let i = 0; i < 5; i++) h.last().emit("data", makeExec(`e-${i}`));
		await wait(10);

		expect(peak).toBe(1);
		expect(h.warns).toEqual([]);
		// Drain every queued execution: all five run, none was rejected.
		for (let i = 0; i < 5; i++) {
			release.shift()?.();
			await wait(5);
		}
		expect(h.results).toHaveLength(5);
		await h.sub.stop();
	});

	it("Stop_DropsExecutionsStillQueuedOnTheSemaphore", async () => {
		// Without an abort signal a waiter queued behind a running handler gets
		// its slot after stop() and starts a handler on a shut-down subscriber.
		const h = makeHarness();
		let started = 0;
		let releaseFirst: (() => void) | null = null;
		h.domain.handle(
			"nightly",
			{ trigger: { interval: 1000 }, maxConcurrent: 1 },
			async () => {
				started++;
				await new Promise<void>((r) => {
					releaseFirst = r;
				});
			},
		);
		h.sub.start();

		h.last().emit("data", makeExec("e-1"));
		h.last().emit("data", makeExec("e-2"));
		await wait(10);
		expect(started).toBe(1);

		await h.sub.stop();
		(releaseFirst as unknown as () => void)();
		await wait(10);

		expect(started).toBe(1);
		expect(
			h.warns.some(
				(w) => w.includes("stopped while queued") && w.includes("e-2"),
			),
		).toBe(true);
	});
});
