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
	return {
		rpc: m,
		http: m,
		event: m,
		workflow: m,
		telemetryEnabled: true,
		payloadMaxBytes: 65536,
	};
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

describe("WatchStream change notifications are incrementally applicable", () => {
	function makeInstance(instanceId: string, serviceId = "svc") {
		return {
			instanceId,
			serviceId,
			serviceName: "svc",
			callEndpoint: `${instanceId}:1000`,
			status: "connected",
			httpEndpoint: "",
			isUnhealthySinceUnixMs: 0,
		};
	}

	function instanceSnapshot(
		instances: ReturnType<typeof makeInstance>[],
		methods: ReturnType<typeof makeDesc>[] = [],
	): RegistryEvent {
		return {
			snapshot: {
				methods,
				instances,
				eventSubscriptions: [],
				outgoingCalls: [],
			},
		} as RegistryEvent;
	}

	it("snapshot() hands out the live cache — no per-tick copy", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));
		stream.emit("data", snapshot([makeDesc("inst-1", "charge")]));

		const first = ws.snapshot();
		stream.emit("data", update([makeDesc("inst-2", "refund")], []));
		expect(ws.snapshot()).toBe(first);
		expect(first.size).toBe(2);
	});

	it("a re-sent instance is not reported as removed", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		const events: Array<{ added: string[]; removed: string[] }> = [];
		ws.onInstancesChange((added, removed) => {
			events.push({
				added: added.map((i) => i.instanceId),
				removed: removed.map((i) => i.instanceId),
			});
		});
		ws.start(emptyReq, makeClient(stream));

		stream.emit("data", instanceSnapshot([makeInstance("a")]));
		// Scale-out: 'a' stays, 'b' joins.
		stream.emit(
			"data",
			instanceSnapshot([makeInstance("a"), makeInstance("b")]),
		);
		// Rolling deploy: 'a' is gone.
		stream.emit("data", instanceSnapshot([makeInstance("b")]));

		expect(events[1]).toEqual({ added: ["a", "b"], removed: [] });
		expect(events[2]).toEqual({ added: ["b"], removed: ["a"] });
	});

	it("onMethodsChange reports descriptors to upsert and descriptors evicted", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		const events: Array<{ added: string[]; removed: string[] }> = [];
		ws.onMethodsChange((added, removed) => {
			events.push({
				added: added.map((m) => m.name),
				removed: removed.map((m) => m.name),
			});
		});
		ws.start(emptyReq, makeClient(stream));

		stream.emit("data", snapshot([makeDesc("inst-1", "charge")]));
		expect(events[0]).toEqual({ added: ["charge"], removed: [] });

		stream.emit(
			"data",
			update([makeDesc("inst-1", "refund")], [makeDesc("inst-1", "charge")]),
		);
		expect(events[1]).toEqual({ added: ["refund"], removed: ["charge"] });

		// A snapshot that no longer lists a descriptor evicts it.
		stream.emit("data", snapshot([makeDesc("inst-1", "pay")]));
		expect(events[2]).toEqual({ added: ["pay"], removed: ["refund"] });
	});

	it("removedPeers eviction is reported to method listeners", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		const removedNames: string[] = [];
		ws.onMethodsChange((_added, removed) => {
			for (const m of removed) removedNames.push(m.name);
		});
		ws.start(emptyReq, makeClient(stream));
		stream.emit("data", snapshot([makeDesc("inst-1", "charge")]));

		stream.emit("data", {
			update: {
				added: [],
				removed: [],
				addedInstances: [],
				removedInstances: [],
				addedEventSubscriptions: [],
				removedEventSubscriptions: [],
				addedOutgoingCalls: [],
				removedOutgoingCalls: [],
				addedPeers: [],
				removedPeers: ["svc"],
			},
		} as RegistryEvent);

		expect(removedNames).toEqual(["charge"]);
		expect(ws.snapshot().size).toBe(0);
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
				telemetryEnabled: true,
				payloadMaxBytes: 65536,
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
				telemetryEnabled: true,
				payloadMaxBytes: 65536,
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

describe("WatchStream pushed telemetry config", () => {
	it("defaults to enabled=true, payloadMaxBytes=65536 before any snapshot", () => {
		const ws = new WatchStream();
		const cfg = ws.pushedTelemetryConfig();
		expect(cfg.enabled).toBe(true);
		expect(cfg.payloadMaxBytes).toBe(65536);
	});

	it("applies telemetryEnabled=false from snapshot", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));
		stream.emit(
			"data",
			snapshot([], {
				rpc: CaptureMode.CAPTURE_MODE_NONE,
				http: CaptureMode.CAPTURE_MODE_NONE,
				event: CaptureMode.CAPTURE_MODE_NONE,
				workflow: CaptureMode.CAPTURE_MODE_NONE,
				telemetryEnabled: false,
				payloadMaxBytes: 65536,
			}),
		);
		expect(ws.pushedTelemetryConfig().enabled).toBe(false);
	});

	it("applies custom payloadMaxBytes from snapshot", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));
		stream.emit(
			"data",
			snapshot([], {
				rpc: CaptureMode.CAPTURE_MODE_NONE,
				http: CaptureMode.CAPTURE_MODE_NONE,
				event: CaptureMode.CAPTURE_MODE_NONE,
				workflow: CaptureMode.CAPTURE_MODE_NONE,
				telemetryEnabled: true,
				payloadMaxBytes: 1024,
			}),
		);
		expect(ws.pushedTelemetryConfig().payloadMaxBytes).toBe(1024);
	});

	it("fires onTelemetryConfig listener when config changes", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		const seen: Array<{ enabled: boolean; payloadMaxBytes: number }> = [];
		ws.onTelemetryConfig((cfg) => seen.push({ ...cfg }));
		ws.start(emptyReq, makeClient(stream));
		stream.emit(
			"data",
			snapshot([], {
				rpc: CaptureMode.CAPTURE_MODE_NONE,
				http: CaptureMode.CAPTURE_MODE_NONE,
				event: CaptureMode.CAPTURE_MODE_NONE,
				workflow: CaptureMode.CAPTURE_MODE_NONE,
				telemetryEnabled: false,
				payloadMaxBytes: 2048,
			}),
		);
		expect(seen).toHaveLength(1);
		const first = seen[0]!;
		expect(first.enabled).toBe(false);
		expect(first.payloadMaxBytes).toBe(2048);
	});

	it("does not fire listener when config is unchanged", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		let fireCount = 0;
		ws.onTelemetryConfig(() => fireCount++);
		ws.start(emptyReq, makeClient(stream));
		// Same as default: enabled=true, payloadMaxBytes=65536
		stream.emit(
			"data",
			snapshot([], {
				rpc: CaptureMode.CAPTURE_MODE_NONE,
				http: CaptureMode.CAPTURE_MODE_NONE,
				event: CaptureMode.CAPTURE_MODE_NONE,
				workflow: CaptureMode.CAPTURE_MODE_NONE,
				telemetryEnabled: true,
				payloadMaxBytes: 65536,
			}),
		);
		expect(fireCount).toBe(0);
	});

	it("applies config from an update frame", () => {
		const stream = new FakeStream();
		const ws = new WatchStream();
		ws.start(emptyReq, makeClient(stream));
		stream.emit("data", snapshot([], allModes(CaptureMode.CAPTURE_MODE_NONE)));
		stream.emit(
			"data",
			update([], [], {
				rpc: CaptureMode.CAPTURE_MODE_NONE,
				http: CaptureMode.CAPTURE_MODE_NONE,
				event: CaptureMode.CAPTURE_MODE_NONE,
				workflow: CaptureMode.CAPTURE_MODE_NONE,
				telemetryEnabled: false,
				payloadMaxBytes: 512,
			}),
		);
		const cfg = ws.pushedTelemetryConfig();
		expect(cfg.enabled).toBe(false);
		expect(cfg.payloadMaxBytes).toBe(512);
	});
});

describe("WatchStream retry", () => {
	function makeCountingClient(streams: FakeStream[]): {
		client: RegistryClient;
		calls: () => number;
	} {
		let n = 0;
		const client = {
			registerAndWatch: () => {
				const s = new FakeStream();
				streams.push(s);
				n++;
				return s as unknown as ReturnType<RegistryClient["registerAndWatch"]>;
			},
		} as unknown as RegistryClient;
		return { client, calls: () => n };
	}

	it("ErrorPlusEnd_SchedulesSingleRestartPerCycle", async () => {
		// grpc-js can emit both "error" and "end" for one broken stream. Each
		// break must schedule exactly one restart — a second timer would
		// multiply live restart loops on every cycle.
		const streams: FakeStream[] = [];
		const { client, calls } = makeCountingClient(streams);
		const ws = new WatchStream({ ladder: [1], jitterRatio: 0 });
		ws.start(emptyReq, client);
		expect(calls()).toBe(1);

		for (let cycle = 0; cycle < 3; cycle++) {
			const current = streams.at(-1);
			current?.emit("error", new Error("broken"));
			current?.emit("end");
			await Bun.sleep(20);
			expect(calls()).toBe(cycle + 2);
		}
		ws.stop();
	});

	it("StaleStreamEvents_Ignored", async () => {
		const streams: FakeStream[] = [];
		const { client, calls } = makeCountingClient(streams);
		const ws = new WatchStream({ ladder: [1], jitterRatio: 0 });
		ws.start(emptyReq, client);

		const first = streams[0];
		first?.emit("error", new Error("broken"));
		await Bun.sleep(20);
		expect(calls()).toBe(2);

		// The replaced stream keeps emitting (late cancel/error) — no effect.
		first?.emit("error", new Error("late"));
		first?.emit("end");
		await Bun.sleep(20);
		expect(calls()).toBe(2);
		ws.stop();
	});

	it("Stop_NoRestart", async () => {
		const streams: FakeStream[] = [];
		const { client, calls } = makeCountingClient(streams);
		const ws = new WatchStream({ ladder: [1], jitterRatio: 0 });
		ws.start(emptyReq, client);
		ws.stop();

		streams[0]?.emit("error", new Error("closed"));
		await Bun.sleep(20);
		expect(calls()).toBe(1);
	});

	it("DataResetsBackoffLadder", async () => {
		// After a delivered event the ladder restarts from the bottom: with
		// ladder [1, 1, 60000] a non-reset attempt counter would park the
		// second restart on the 60s rung and the count below would stay at 2.
		const streams: FakeStream[] = [];
		const { client, calls } = makeCountingClient(streams);
		const ws = new WatchStream({ ladder: [1, 1, 60_000], jitterRatio: 0 });
		ws.start(emptyReq, client);

		streams.at(-1)?.emit("error", new Error("first break"));
		await Bun.sleep(20);
		expect(calls()).toBe(2);

		streams
			.at(-1)
			?.emit("data", snapshot([], allModes(CaptureMode.CAPTURE_MODE_NONE)));
		streams.at(-1)?.emit("error", new Error("second break"));
		await Bun.sleep(20);
		expect(calls()).toBe(3);
		ws.stop();
	});
});
