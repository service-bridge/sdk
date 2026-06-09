import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import type {
	CaptureModes,
	RegisterRequest,
	RegistryClient,
	RegistryEvent,
} from "../pb/servicebridge/v1/registry";
import { CaptureMode, MethodType } from "../pb/servicebridge/v1/registry";
import { Channel } from "../pb/servicebridge/v1/telemetry";
import { WatchStream } from "./watch";

// allModes builds a CaptureModes message with every channel set to one mode,
// for tests that only care about a single channel's propagation.
function allModes(m: CaptureMode): CaptureModes {
	return { rpc: m, http: m, event: m, workflow: m };
}

// ── fake RegistryClient ──────────────────────────────────────────────────────

class FakeStream extends EventEmitter {
	cancelled = false;
	cancel() {
		this.cancelled = true;
	}
}

function makeClient(stream: FakeStream): RegistryClient {
	return {
		registerAndWatch: () =>
			stream as unknown as ReturnType<RegistryClient["registerAndWatch"]>,
	} as unknown as RegistryClient;
}

function makeDesc(
	instanceId: string,
	name: string,
	methodType = MethodType.METHOD_TYPE_RPC,
) {
	return {
		instanceId,
		type: methodType,
		name,
		published: false,
		serviceId: "svc",
		serviceName: "svc",
		contractHash: "hash",
		inputSchema: Buffer.alloc(0),
		outputSchema: Buffer.alloc(0),
		streaming: false,
	};
}

function snapshot(
	methods: ReturnType<typeof makeDesc>[],
	captureModes?: CaptureModes,
): RegistryEvent {
	return {
		snapshot: {
			methods,
			instances: [],
			eventSubscriptions: [],
			outgoingCalls: [],
			captureModes,
		},
	} as RegistryEvent;
}

function update(
	added: ReturnType<typeof makeDesc>[],
	removed: ReturnType<typeof makeDesc>[],
	captureModes?: CaptureModes,
): RegistryEvent {
	return {
		update: {
			added,
			removed,
			addedInstances: [],
			removedInstances: [],
			addedEventSubscriptions: [],
			removedEventSubscriptions: [],
			addedOutgoingCalls: [],
			removedOutgoingCalls: [],
			addedPeers: [],
			removedPeers: [],
			captureModes,
		},
	} as RegistryEvent;
}

const emptyReq: RegisterRequest = {
	incoming: [],
	published: [],
	outgoing: [],
	callEndpoint: "",
	eventSubscriptions: [],
	httpEndpoint: "",
};

// ── tests ────────────────────────────────────────────────────────────────────

describe("WatchStream snapshot", () => {
	it("replaces cache entirely on snapshot", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));

		stream.emit("data", snapshot([makeDesc("inst-1", "charge")]));

		expect(ws.snapshot().size).toBe(1);

		stream.emit(
			"data",
			snapshot([makeDesc("inst-2", "refund"), makeDesc("inst-3", "pay")]),
		);
		expect(ws.snapshot().size).toBe(2);
		expect(ws.snapshot().has("inst-1:1:charge:false")).toBe(false);
	});
});

describe("WatchStream per-channel capture modes (runtime authority)", () => {
	it("defaults every channel to none before any snapshot (fail-safe)", () => {
		const ws = new WatchStream();
		for (const ch of [
			Channel.RPC,
			Channel.HTTP,
			Channel.EVENT,
			Channel.WORKFLOW,
			Channel.JOB,
		]) {
			expect(ws.captureModeForChannel(ch)).toBe("none");
		}
	});

	it("applies the pushed per-channel modes from the snapshot", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));
		stream.emit(
			"data",
			snapshot([], {
				rpc: CaptureMode.CAPTURE_MODE_ALL,
				http: CaptureMode.CAPTURE_MODE_ERRORS,
				event: CaptureMode.CAPTURE_MODE_NONE,
				workflow: CaptureMode.CAPTURE_MODE_ALL,
			}),
		);
		expect(ws.captureModeForChannel(Channel.RPC)).toBe("all");
		expect(ws.captureModeForChannel(Channel.HTTP)).toBe("errors");
		expect(ws.captureModeForChannel(Channel.EVENT)).toBe("none");
		expect(ws.captureModeForChannel(Channel.WORKFLOW)).toBe("all");
		expect(ws.captureModeForChannel(Channel.JOB)).toBe("none");
	});

	it("channels resolve independently (RPC all does not widen EVENT)", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));
		stream.emit(
			"data",
			snapshot([], {
				rpc: CaptureMode.CAPTURE_MODE_ALL,
				http: CaptureMode.CAPTURE_MODE_NONE,
				event: CaptureMode.CAPTURE_MODE_NONE,
				workflow: CaptureMode.CAPTURE_MODE_NONE,
			}),
		);
		expect(ws.captureModeForChannel(Channel.RPC)).toBe("all");
		expect(ws.captureModeForChannel(Channel.EVENT)).toBe("none");
	});

	it("a missing CaptureModes message leaves every channel at none", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));
		stream.emit("data", snapshot([]));
		expect(ws.captureModeForChannel(Channel.RPC)).toBe("none");
		expect(ws.captureModeForChannel(Channel.EVENT)).toBe("none");
	});

	it("re-applies the pushed modes from an update", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));
		stream.emit("data", snapshot([], allModes(CaptureMode.CAPTURE_MODE_ALL)));
		stream.emit(
			"data",
			update([], [], allModes(CaptureMode.CAPTURE_MODE_ERRORS)),
		);
		expect(ws.captureModeForChannel(Channel.EVENT)).toBe("errors");
	});

	it("notifies onCaptureModes listeners on change", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		const seen: string[] = [];
		ws.onCaptureModes((modes) => seen.push(modes[Channel.EVENT]));
		ws.start(emptyReq, makeClient(stream));
		stream.emit(
			"data",
			snapshot([], allModes(CaptureMode.CAPTURE_MODE_ERRORS)),
		);
		expect(seen).toContain("errors");
	});
});

describe("WatchStream update", () => {
	it("added: inserts into cache", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));

		stream.emit("data", snapshot([]));
		stream.emit("data", update([makeDesc("inst-1", "charge")], []));

		expect(ws.snapshot().size).toBe(1);
	});

	it("removed: deletes from cache", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));

		stream.emit("data", snapshot([makeDesc("inst-1", "charge")]));
		expect(ws.snapshot().size).toBe(1);

		stream.emit("data", update([], [makeDesc("inst-1", "charge")]));
		expect(ws.snapshot().size).toBe(0);
	});

	it("removed for unknown key: no-op (no error)", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));

		stream.emit("data", snapshot([]));
		expect(() => {
			stream.emit("data", update([], [makeDesc("inst-X", "unknown")]));
		}).not.toThrow();
		expect(ws.snapshot().size).toBe(0);
	});
});

describe("WatchStream stop/restart", () => {
	it("stop cancels the stream", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));
		ws.stop();
		expect(stream.cancelled).toBe(true);
	});

	it("stop when stream is null: no error", () => {
		const ws = new WatchStream();
		// stop before start — must not throw
		expect(() => ws.stop()).not.toThrow();
	});

	it("error event invokes onError callback", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		const errors: Error[] = [];
		ws.start(emptyReq, makeClient(stream), (err) => errors.push(err));

		const testErr = new Error("stream broken");
		stream.emit("error", testErr);

		expect(errors).toHaveLength(1);
		expect(errors[0]).toBe(testErr);
	});

	it("restart replaces stream and clears old state via new snapshot", () => {
		const stream1 = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream1));

		stream1.emit("data", snapshot([makeDesc("inst-1", "charge")]));
		expect(ws.snapshot().size).toBe(1);

		const stream2 = new FakeStream();
		ws.restart(emptyReq, makeClient(stream2));

		// stream1 should be cancelled.
		expect(stream1.cancelled).toBe(true);

		// New snapshot from stream2 replaces old cache.
		stream2.emit("data", snapshot([]));
		expect(ws.snapshot().size).toBe(0);
	});
});
