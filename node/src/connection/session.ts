import type {
	ControlClient,
	ServerControl,
	Welcome,
} from "../pb/servicebridge/v1/control";
import { OpenRequest } from "../pb/servicebridge/v1/control";
import { PROTOCOL_VERSION, SDK_LANGUAGE, SDK_VERSION } from "./handshake";

export type ServerStream = ReturnType<ControlClient["open"]>;

export interface SessionCallbacks {
	onWelcome(welcome: Welcome): void;
	onDrain(reason: string): void;
	onError(err: Error): void;
	onEnd(): void;
}

/**
 * One Control.Open stream: the runtime's Welcome (the session is live) and
 * Drain (the runtime is shutting down). The registry stream is separate and
 * owned by the bridge. A stream that the bridge closes on purpose reports
 * nothing.
 *
 * @internal — см. ./README.md
 */
export class Session {
	private closed = false;

	constructor(
		private readonly stream: ServerStream,
		callbacks: SessionCallbacks,
	) {
		stream.on("data", (msg: ServerControl) => {
			if (this.closed) return;
			if (msg.welcome) callbacks.onWelcome(msg.welcome);
			else if (msg.drain) callbacks.onDrain(msg.drain.reason);
		});
		stream.on("end", () => {
			if (this.closed) return;
			this.closed = true;
			callbacks.onEnd();
		});
		stream.on("error", (err: Error) => {
			if (this.closed) return;
			this.closed = true;
			callbacks.onError(err);
		});
	}

	/** Cancels the stream; no callback fires afterwards. */
	close(): void {
		if (this.closed) return;
		this.closed = true;
		try {
			this.stream.cancel();
		} catch {
			// Already finished.
		}
	}

	isClosed(): boolean {
		return this.closed;
	}
}

// openControlStream opens Control.Open with this SDK's handshake identity.
export function openControlStream(client: ControlClient): ServerStream {
	return client.open(
		OpenRequest.create({
			protocolVersion: PROTOCOL_VERSION,
			sdkLanguage: SDK_LANGUAGE,
			sdkVersion: SDK_VERSION,
		}),
	);
}
