// stream-supervisor.test.ts — the reconnect state machine shared by the
// events / job / workflow subscribers.
//
// The behaviours pinned here are the ones each subscriber used to get wrong in
// its own way: a clean close must climb the ladder (not pin it at the first
// rung), a single break must schedule exactly one reconnect, and a late frame
// from a stream that has already been replaced must not touch the live one.

import { describe, expect, it, spyOn } from "bun:test";
import { StreamSupervisor } from "./stream-supervisor";

// Captured before any spy replaces the global, so test waits never show up in
// the recorded reconnect delays.
const realSetTimeout = globalThis.setTimeout;
const wait = (ms: number): Promise<void> =>
	new Promise((resolve) => {
		realSetTimeout(resolve, ms);
	});

interface FakeStream {
	// biome-ignore lint/suspicious/noExplicitAny: mirrors the grpc listener surface
	on(event: string, cb: (...args: any[]) => void): void;
	cancel(): void;
}

function makeFakeStream(): {
	stream: FakeStream;
	emit: (event: string, ...args: unknown[]) => void;
	cancels: () => number;
} {
	const listeners = new Map<string, Array<(...a: unknown[]) => void>>();
	let cancels = 0;
	return {
		stream: {
			on(event, cb) {
				const list = listeners.get(event) ?? [];
				list.push(cb);
				listeners.set(event, list);
			},
			cancel() {
				cancels++;
			},
		},
		emit(event, ...args) {
			for (const cb of listeners.get(event) ?? []) cb(...args);
		},
		cancels: () => cancels,
	};
}

function makeHarness(
	opts: {
		ladder?: readonly number[];
		openReturnsNull?: () => boolean;
		openThrows?: () => boolean;
	} = {},
) {
	const streams: Array<ReturnType<typeof makeFakeStream>> = [];
	const data: unknown[] = [];
	const errors: Error[] = [];
	const sup = new StreamSupervisor<FakeStream, unknown>({
		open: () => {
			if (opts.openThrows?.()) throw new Error("open blew up");
			if (opts.openReturnsNull?.()) return null;
			const f = makeFakeStream();
			streams.push(f);
			return f.stream;
		},
		onData: (msg) => {
			data.push(msg);
		},
		onError: (err) => {
			errors.push(err);
		},
		reconnectOpts: { ladder: opts.ladder ?? [5], jitterRatio: 0 },
	});
	return {
		sup,
		streams,
		data,
		errors,
		last: () => streams[streams.length - 1],
	};
}

// recordedDelays returns every delay the supervisor scheduled while `body` ran.
async function recordedDelays(body: () => Promise<void>): Promise<number[]> {
	const setSpy = spyOn(globalThis, "setTimeout");
	try {
		await body();
		return setSpy.mock.calls.map((c) => c[1] as number);
	} finally {
		setSpy.mockRestore();
	}
}

describe("StreamSupervisor", () => {
	it("Start_OpensStreamAndForwardsData", () => {
		const h = makeHarness();
		h.sup.start();
		expect(h.streams).toHaveLength(1);
		expect(h.sup.current()).toBe(h.streams[0]!.stream);

		h.last()!.emit("data", { n: 1 });
		expect(h.data).toEqual([{ n: 1 }]);
		h.sup.stop();
	});

	it("CleanCloses_ClimbTheLadder", async () => {
		// The bug this pins: resetting the attempt counter on a clean close made
		// every graceful stream shutdown reconnect at the first rung, forever —
		// a self-inflicted DDoS on a runtime that drains streams on purpose.
		const h = makeHarness({ ladder: [4, 12, 24, 40] });
		const delays = await recordedDelays(async () => {
			h.sup.start();
			for (let i = 0; i < 4; i++) {
				h.last()!.emit("end");
				await wait(30);
			}
		});
		h.sup.stop();

		expect(delays.slice(0, 4)).toEqual([4, 12, 24, 40]);
		// Three rungs elapsed inside the loop; the fourth was still pending.
		expect(h.streams).toHaveLength(4);
	});

	it("DataFrame_ResetsLadderToFirstRung", async () => {
		const h = makeHarness({ ladder: [4, 12, 24, 40] });
		const delays = await recordedDelays(async () => {
			h.sup.start();
			h.last()!.emit("end");
			await wait(20);
			h.last()!.emit("end");
			await wait(30);
			// Progress on the third stream — the next break starts over at rung 0.
			h.last()!.emit("data", { ok: true });
			h.last()!.emit("end");
			await wait(20);
		});
		h.sup.stop();

		expect(delays.slice(0, 3)).toEqual([4, 12, 4]);
	});

	it("ErrorPlusEnd_SchedulesSingleReconnect", async () => {
		// grpc-js emits both "error" and "end" for one broken stream. A second
		// timer would double the live reconnect loops on every cycle.
		const h = makeHarness({ ladder: [4] });
		h.sup.start();
		for (let cycle = 0; cycle < 3; cycle++) {
			const cur = h.last()!;
			cur.emit("error", new Error("broken"));
			cur.emit("end");
			await wait(25);
			expect(h.streams).toHaveLength(cycle + 2);
		}
		h.sup.stop();
		expect(h.errors).toHaveLength(3);
	});

	it("LateFrameFromReplacedStream_IsIgnored", async () => {
		// grpc-js flushes buffered frames before "end", so a dead stream can
		// outlive a whole ladder rung. Without the identity guard the late "end"
		// from stream A drops the live stream B (stop() can no longer cancel it)
		// and opens a third stream the runtime rejects with ALREADY_EXISTS.
		const h = makeHarness({ ladder: [4] });
		h.sup.start();
		const streamA = h.streams[0]!;

		streamA.emit("error", new Error("broken"));
		await wait(25);
		expect(h.streams).toHaveLength(2);
		const streamB = h.streams[1]!;

		streamA.emit("end");
		streamA.emit("data", { stale: true });
		await wait(25);

		expect(h.streams).toHaveLength(2);
		expect(h.sup.current()).toBe(streamB.stream);
		expect(h.data).toEqual([]);

		h.sup.stop();
		expect(streamB.cancels()).toBe(1);
	});

	it("Stop_CancelsStreamAndSuppressesReconnect", async () => {
		const h = makeHarness({ ladder: [4] });
		h.sup.start();
		const first = h.streams[0]!;
		h.sup.stop();

		expect(first.cancels()).toBe(1);
		expect(h.sup.current()).toBeNull();

		first.emit("error", new Error("closed"));
		first.emit("end");
		await wait(25);
		expect(h.streams).toHaveLength(1);
	});

	it("Stop_ClearsPendingReconnectTimer", async () => {
		const setSpy = spyOn(globalThis, "setTimeout");
		const clearSpy = spyOn(globalThis, "clearTimeout");
		try {
			const h = makeHarness({ ladder: [10_000] });
			h.sup.start();
			h.last()!.emit("end");
			const handle = setSpy.mock.results.at(-1)?.value;
			expect(handle).toBeDefined();

			h.sup.stop();
			expect(clearSpy.mock.calls.some(([t]) => t === handle)).toBe(true);
		} finally {
			setSpy.mockRestore();
			clearSpy.mockRestore();
		}
	});

	it("OpenReturnsNull_RetriesOnLadderWithoutOpening", async () => {
		let ready = false;
		const h = makeHarness({ ladder: [4], openReturnsNull: () => !ready });
		h.sup.start();
		expect(h.streams).toHaveLength(0);

		ready = true;
		await wait(25);
		expect(h.streams).toHaveLength(1);
		h.sup.stop();
	});

	it("OpenThrows_ReportsErrorAndRetries", async () => {
		let broken = true;
		const h = makeHarness({ ladder: [4], openThrows: () => broken });
		h.sup.start();
		expect(h.errors).toHaveLength(1);
		expect(h.errors[0]!.message).toBe("open blew up");
		expect(h.streams).toHaveLength(0);

		broken = false;
		await wait(25);
		expect(h.streams).toHaveLength(1);
		h.sup.stop();
	});

	it("Restart_ReplacesStreamImmediatelyOnFreshLadder", async () => {
		const h = makeHarness({ ladder: [4, 12, 24, 40] });
		h.sup.start();
		h.last()!.emit("end");
		await wait(20);
		h.last()!.emit("end");
		await wait(30);
		expect(h.streams).toHaveLength(3);

		const before = h.streams[2]!;
		const delays = await recordedDelays(async () => {
			h.sup.restart();
			expect(before.cancels()).toBe(1);
			expect(h.streams).toHaveLength(4);
			// Ladder is back at rung 0 after an explicit restart.
			h.last()!.emit("end");
			await wait(20);
		});
		h.sup.stop();
		expect(delays[0]).toBe(4);
	});

	it("RestartAfterStop_IsNoop", () => {
		const h = makeHarness();
		h.sup.start();
		h.sup.stop();
		h.sup.restart();
		expect(h.streams).toHaveLength(1);
	});
});
