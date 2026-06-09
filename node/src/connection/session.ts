import type {
	ControlClient,
	OpenRequest as OpenRequestType,
	ServerControl,
	Welcome,
} from "../pb/servicebridge/v1/control";
import { OpenRequest } from "../pb/servicebridge/v1/control";
import type {
	RegisterRequest,
	RegistryClient,
} from "../pb/servicebridge/v1/registry";
import type { WatchStream } from "../registry/watch";

export type ServerStream = ReturnType<ControlClient["open"]>;

export interface SessionCallbacks {
	onWelcome(welcome: Welcome): void;
	onDrain(reason: string): void;
	onError(err: Error): void;
	onEnd(): void;
}

/**
 * Wraps a single live SDK→runtime connection. Owns both server-streams used by
 * the connection lifecycle:
 *   - `Control.Open` — server-streamed Welcome/Drain signals (identity by mTLS).
 *   - `Registry.RegisterAndWatch` — push-based registry view; restartable via
 *     `updateRegistration(req)` when local state (e.g. HTTP endpoint) changes.
 *
 * Heartbeats moved out of Control entirely (now part of the Telemetry stream),
 * so the SDK side of `Control.Open` is read-only here.
 */
export class Session {
	private readonly stream: ServerStream;
	private readonly callbacks: SessionCallbacks;
	private readonly watch: WatchStream;
	private readonly registryClient: RegistryClient;
	private closed = false;
	private expectedClose = false;

	constructor(
		stream: ServerStream,
		callbacks: SessionCallbacks,
		watch: WatchStream,
		registryClient: RegistryClient,
	) {
		this.stream = stream;
		this.callbacks = callbacks;
		this.watch = watch;
		this.registryClient = registryClient;

		stream.on("data", (msg: ServerControl) => this.handleMessage(msg));
		stream.on("end", () => this.onEnd());
		stream.on("error", (err: Error) => {
			if (this.expectedClose) return;
			callbacks.onError(err);
		});
	}

	/**
	 * Restarts the Registry.RegisterAndWatch stream with a fresh RegisterRequest.
	 * Used when local registration state changes after `start()` (HTTP endpoint
	 * publishHttp from integrations). Safe to call after close() — becomes no-op.
	 */
	updateRegistration(req: RegisterRequest): void {
		if (this.closed) return;
		this.watch.restart(req, this.registryClient, (_err) => {});
	}

	/**
	 * close() cancels the server stream and suppresses subsequent onEnd /
	 * onError callbacks. The caller knows the close is expected and reconnect
	 * logic must NOT fire.
	 */
	close(): void {
		if (this.closed) return;
		this.closed = true;
		this.expectedClose = true;
		const cancellable = this.stream as unknown as { cancel?: () => void };
		if (typeof cancellable.cancel === "function") {
			try {
				cancellable.cancel();
			} catch {
				// stream may already be done — fine.
			}
		}
	}

	isClosed(): boolean {
		return this.closed;
	}

	private handleMessage(msg: ServerControl): void {
		if (msg.welcome) {
			this.callbacks.onWelcome(msg.welcome);
		} else if (msg.drain) {
			this.callbacks.onDrain(msg.drain.reason);
		}
	}

	private onEnd(): void {
		const expected = this.expectedClose;
		this.closed = true;
		if (expected) return;
		this.callbacks.onEnd();
	}
}

// openControlStream is the canonical way callers construct the server-stream.
// Kept separate so tests can stub it without binding to the proto-generated
// ControlClient interface.
export function openControlStream(client: ControlClient): ServerStream {
	const req: OpenRequestType = OpenRequest.create();
	return client.open(req);
}
