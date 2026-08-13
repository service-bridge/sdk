import { describe, expect, it, spyOn } from "bun:test";
import { WorkflowSubscriber } from "./subscriber";

function makeSubscriber(): WorkflowSubscriber {
	return new WorkflowSubscriber({
		// biome-ignore lint/suspicious/noExplicitAny: heartbeat tests never touch rpc
		rpc: {} as any,
		serviceId: "svc-1",
		instanceId: "inst-1",
		// biome-ignore lint/suspicious/noExplicitAny: heartbeat tests never run the runner
		deps: {} as any,
		logger: { warn: () => {}, error: () => {} },
		lookupLocalGraph: () => null,
	});
}

describe("WorkflowSubscriber heartbeat", () => {
	it("ReDispatchedRun_ReplacesTimerInsteadOfOrphaningIt", () => {
		// A runId re-assigned after lease expiry must clear the previous
		// interval: overwriting the map entry alone leaks the timer and its
		// closure forever.
		const setSpy = spyOn(globalThis, "setInterval");
		const clearSpy = spyOn(globalThis, "clearInterval");
		const sub = makeSubscriber();
		try {
			// biome-ignore lint/suspicious/noExplicitAny: private hook, behaviour only observable via timers
			(sub as any).startHeartbeat("run-1", 1);
			const firstTimer = setSpy.mock.results.at(-1)?.value;
			expect(firstTimer).toBeDefined();

			// biome-ignore lint/suspicious/noExplicitAny: private hook
			(sub as any).startHeartbeat("run-1", 2);
			expect(clearSpy.mock.calls.some(([t]) => t === firstTimer)).toBe(true);

			// biome-ignore lint/suspicious/noExplicitAny: private hook
			(sub as any).stopHeartbeat("run-1");
		} finally {
			setSpy.mockRestore();
			clearSpy.mockRestore();
		}
	});
});

describe("WorkflowSubscriber reconnect timer", () => {
	it("Close_ClearsPendingReconnectTimer", async () => {
		// connectLoop() waits between reconnect attempts via setTimeout. If that
		// timer is never tracked, close() cannot cancel it — the process stays
		// alive up to the reconnect ladder's rung length after the subscriber is
		// closed (same class of bug as BUG-17 in connection/service-bridge.ts).
		const setSpy = spyOn(globalThis, "setTimeout");
		const clearSpy = spyOn(globalThis, "clearTimeout");
		try {
			const listeners = new Map<string, Array<(...a: unknown[]) => void>>();
			const stream = {
				on(event: string, cb: (...a: unknown[]) => void) {
					const list = listeners.get(event) ?? [];
					list.push(cb);
					listeners.set(event, list);
				},
				cancel() {},
			};
			const emit = (event: string, ...args: unknown[]) => {
				for (const cb of listeners.get(event) ?? []) cb(...args);
			};

			const sub = new WorkflowSubscriber({
				// biome-ignore lint/suspicious/noExplicitAny: minimal grpc stub
				rpc: { subscribe: () => stream } as any,
				serviceId: "svc-1",
				instanceId: "inst-1",
				// biome-ignore lint/suspicious/noExplicitAny: reconnect path never runs the runner
				deps: {} as any,
				logger: { warn: () => {}, error: () => {} },
				lookupLocalGraph: () => null,
			});
			sub.start();

			// Stream error → runOnce() rejects → connectLoop schedules a reconnect
			// wait via setTimeout.
			emit("error", new Error("boom"));
			await Bun.sleep(5);

			const timerHandle = setSpy.mock.results.at(-1)?.value;
			expect(timerHandle).toBeDefined();

			sub.close();

			expect(clearSpy.mock.calls.some(([t]) => t === timerHandle)).toBe(true);
		} finally {
			setSpy.mockRestore();
			clearSpy.mockRestore();
		}
	});
});
