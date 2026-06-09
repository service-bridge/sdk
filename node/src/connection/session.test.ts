import { describe, expect, mock, test } from "bun:test";
import type { ServerControl } from "../pb/servicebridge/v1/control";
import type {
	RegisterRequest,
	RegistryClient,
} from "../pb/servicebridge/v1/registry";
import { WatchStream } from "../registry/watch";
import { type ServerStream, Session, type SessionCallbacks } from "./session";

// Minimal fake server-stream for testing.
function makeFakeStream() {
	let cancelled = false;
	const stream = {
		cancel() {
			cancelled = true;
			stream._endHandler?.();
		},
		on(event: string, handler: unknown) {
			if (event === "data") {
				stream._dataHandler = handler as (msg: ServerControl) => void;
			}
			if (event === "end") {
				stream._endHandler = handler as () => void;
			}
			if (event === "error") {
				stream._errorHandler = handler as (err: unknown) => void;
			}
			return stream;
		},
		_dataHandler: null as ((msg: ServerControl) => void) | null,
		_endHandler: null as (() => void) | null,
		_errorHandler: null as ((err: unknown) => void) | null,
		isCancelled() {
			return cancelled;
		},
	};
	return stream;
}

type FakeStream = ReturnType<typeof makeFakeStream>;

function makeRegistryStubs(): {
	watch: WatchStream;
	registryClient: RegistryClient;
	restarts: RegisterRequest[];
} {
	const watch = new WatchStream();
	const restarts: RegisterRequest[] = [];
	// Stub restart to capture invocations without touching real gRPC.
	(watch as unknown as { restart: (r: RegisterRequest) => void }).restart = (
		r,
	) => {
		restarts.push(r);
	};
	const registryClient = {} as RegistryClient;
	return { watch, registryClient, restarts };
}

function makeSession(stream: FakeStream): {
	session: Session;
	callbacks: SessionCallbacks;
	restarts: RegisterRequest[];
} {
	const callbacks: SessionCallbacks = {
		onWelcome: mock(() => {}),
		onDrain: mock(() => {}),
		onError: mock(() => {}),
		onEnd: mock(() => {}),
	};
	const { watch, registryClient, restarts } = makeRegistryStubs();
	const session = new Session(
		stream as unknown as ServerStream,
		callbacks,
		watch,
		registryClient,
	);
	return { session, callbacks, restarts };
}

describe("Session (Open server-stream)", () => {
	test("fires onWelcome when Welcome message arrives", () => {
		const stream = makeFakeStream();
		const { callbacks } = makeSession(stream);
		stream._dataHandler?.({
			welcome: {
				sessionId: "s1",
				serviceId: "svc1",
				serviceName: "my-svc",
			},
		});
		expect(callbacks.onWelcome).toHaveBeenCalledTimes(1);
	});

	test("fires onDrain with reason when Drain arrives", () => {
		const stream = makeFakeStream();
		const { callbacks } = makeSession(stream);
		stream._dataHandler?.({ drain: { reason: "maintenance" } });
		expect(callbacks.onDrain).toHaveBeenCalledTimes(1);
		expect(callbacks.onDrain).toHaveBeenCalledWith("maintenance");
	});

	test("fires onError when stream emits error", () => {
		const stream = makeFakeStream();
		const { callbacks } = makeSession(stream);
		const err = new Error("boom");
		stream._errorHandler?.(err);
		expect(callbacks.onError).toHaveBeenCalledTimes(1);
		expect(callbacks.onError).toHaveBeenCalledWith(err);
	});

	test("fires onEnd and marks closed on stream end", () => {
		const stream = makeFakeStream();
		const { session, callbacks } = makeSession(stream);
		stream._endHandler?.();
		expect(callbacks.onEnd).toHaveBeenCalledTimes(1);
		expect(session.isClosed()).toBe(true);
	});

	test("close() cancels the stream and suppresses onEnd", () => {
		const stream = makeFakeStream();
		const { session, callbacks } = makeSession(stream);
		session.close();
		expect(stream.isCancelled()).toBe(true);
		expect(session.isClosed()).toBe(true);
		expect(callbacks.onEnd).toHaveBeenCalledTimes(0);
	});

	test("close() is idempotent", () => {
		const stream = makeFakeStream();
		const { session } = makeSession(stream);
		session.close();
		const cancelledAfterFirst = stream.isCancelled();
		session.close();
		expect(cancelledAfterFirst).toBe(true);
	});

	test("ignores ServerControl with no oneof set", () => {
		const stream = makeFakeStream();
		const { callbacks } = makeSession(stream);
		stream._dataHandler?.({} as ServerControl);
		expect(callbacks.onWelcome).toHaveBeenCalledTimes(0);
		expect(callbacks.onDrain).toHaveBeenCalledTimes(0);
	});

	test("updateRegistration restarts the watch stream", () => {
		const stream = makeFakeStream();
		const { session, restarts } = makeSession(stream);
		const req = {
			incoming: [],
			published: [],
			outgoing: [],
			callEndpoint: "",
			eventSubscriptions: [],
			httpEndpoint: "127.0.0.1:3000",
		} as RegisterRequest;
		session.updateRegistration(req);
		expect(restarts).toHaveLength(1);
		expect(restarts[0]?.httpEndpoint).toBe("127.0.0.1:3000");
	});

	test("updateRegistration after close() is a no-op", () => {
		const stream = makeFakeStream();
		const { session, restarts } = makeSession(stream);
		session.close();
		const req = {
			incoming: [],
			published: [],
			outgoing: [],
			callEndpoint: "",
			eventSubscriptions: [],
			httpEndpoint: "h:1",
		} as RegisterRequest;
		session.updateRegistration(req);
		expect(restarts).toHaveLength(0);
	});
});
