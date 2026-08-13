// subscriber.test.ts — JobSubscriber reconnect-timer lifecycle.
//
// connectLoop() waits between reconnect attempts via setTimeout. If that
// timer is never tracked, stop() cannot cancel it — the process stays alive
// up to the reconnect ladder's rung length after the subscriber is closed
// (same class of bug as BUG-17 in connection/service-bridge.ts).

import { describe, expect, it, spyOn } from "bun:test";
import { Registry } from "../registry/registry";
import { JobDomain } from "./domain";
import type { SubscriberDeps } from "./subscriber";
import { JobSubscriber } from "./subscriber";

interface FakeStream {
	on(event: string, cb: (...args: unknown[]) => void): void;
	emit(event: string, ...args: unknown[]): void;
	cancel(): void;
}

function makeFakeStream(): FakeStream {
	const listeners = new Map<string, Array<(...a: unknown[]) => void>>();
	return {
		on(event, cb) {
			const list = listeners.get(event) ?? [];
			list.push(cb);
			listeners.set(event, list);
		},
		emit(event, ...args) {
			for (const cb of listeners.get(event) ?? []) cb(...args);
		},
		cancel() {},
	};
}

function makeSubscriber(stream: FakeStream): JobSubscriber {
	const deps: SubscriberDeps = {
		rpcClient: {
			subscribe: () => stream,
			jobResult: () => {},
			heartbeat: () => {},
			// biome-ignore lint/suspicious/noExplicitAny: minimal grpc stub
		} as any,
		identity: () => ({ serviceId: "svc-1", instanceId: "inst-1" }),
		domain: new JobDomain(new Registry()),
		logger: { warn: () => {}, error: () => {} },
		runWithTrace: (_xSbTrace, fn) => fn(),
	};
	return new JobSubscriber(deps);
}

describe("JobSubscriber reconnect timer", () => {
	it("Stop_ClearsPendingReconnectTimer", async () => {
		const setSpy = spyOn(globalThis, "setTimeout");
		const clearSpy = spyOn(globalThis, "clearTimeout");
		try {
			const stream = makeFakeStream();
			const sub = makeSubscriber(stream);
			sub.start();

			// Stream error → runOnce() rejects → connectLoop schedules a reconnect
			// wait via setTimeout.
			stream.emit("error", new Error("boom"));
			await Bun.sleep(5);

			const timerHandle = setSpy.mock.results.at(-1)?.value;
			expect(timerHandle).toBeDefined();

			await sub.stop();

			expect(clearSpy.mock.calls.some(([t]) => t === timerHandle)).toBe(true);
		} finally {
			setSpy.mockRestore();
			clearSpy.mockRestore();
		}
	});
});
