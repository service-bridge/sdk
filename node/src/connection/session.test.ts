import { describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import type {
	ControlClient,
	OpenRequest,
} from "../pb/servicebridge/v1/control";
import { PROTOCOL_VERSION, SDK_LANGUAGE, SDK_VERSION } from "./handshake";
import {
	openControlStream,
	type ServerStream,
	Session,
	type SessionCallbacks,
} from "./session";

class FakeStream extends EventEmitter {
	cancelled = false;
	cancel(): void {
		this.cancelled = true;
		this.emit("error", new Error("cancelled"));
	}
}

function record(): { cb: SessionCallbacks; seen: string[] } {
	const seen: string[] = [];
	return {
		seen,
		cb: {
			onWelcome: (w) => seen.push(`welcome:${w.sessionId}`),
			onDrain: (r) => seen.push(`drain:${r}`),
			onError: (e) => seen.push(`error:${e.message}`),
			onEnd: () => seen.push("end"),
		},
	};
}

const welcome = {
	sessionId: "s1",
	serviceId: "svc",
	serviceName: "n",
	runtimeVersion: "v",
	protocolVersion: 1,
};

describe("Session", () => {
	test("forwards Welcome and Drain", () => {
		const stream = new FakeStream();
		const { cb, seen } = record();
		new Session(stream as unknown as ServerStream, cb);
		stream.emit("data", { welcome });
		stream.emit("data", { drain: { reason: "shutdown" } });
		expect(seen).toEqual(["welcome:s1", "drain:shutdown"]);
	});

	test("reports the end of the stream once, whichever of error/end comes first", () => {
		const stream = new FakeStream();
		const { cb, seen } = record();
		const session = new Session(stream as unknown as ServerStream, cb);
		stream.emit("error", new Error("boom"));
		stream.emit("end");
		expect(seen).toEqual(["error:boom"]);
		expect(session.isClosed()).toBe(true);
	});

	test("close() cancels the stream and reports nothing afterwards", () => {
		const stream = new FakeStream();
		const { cb, seen } = record();
		const session = new Session(stream as unknown as ServerStream, cb);
		session.close();
		stream.emit("data", { welcome });
		stream.emit("end");
		expect(stream.cancelled).toBe(true);
		expect(seen).toEqual([]);
		session.close();
	});
});

describe("openControlStream", () => {
	test("sends the handshake identity", () => {
		let sent: OpenRequest | undefined;
		const client = {
			open: (req: OpenRequest) => {
				sent = req;
				return new FakeStream();
			},
		} as unknown as ControlClient;
		openControlStream(client);
		expect(sent).toEqual({
			protocolVersion: PROTOCOL_VERSION,
			sdkLanguage: SDK_LANGUAGE,
			sdkVersion: SDK_VERSION,
		});
	});
});
