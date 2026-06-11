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
