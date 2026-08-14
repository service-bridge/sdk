// Lifecycle state machine for one long-lived gRPC stream: open → listen →
// break → wait on the shared reconnect ladder → reopen. Owns the stop flag,
// the single pending reconnect timer, the stream identity guard and the
// attempt counter. Domain code supplies only `open` (how to make the stream)
// and `onData` (what a frame means).
//
// @internal — см. ./README.md

import {
	type ReconnectDelayOptions,
	reconnectDelay,
} from "../utils/reconnect-ladder";

// SupervisedStream is the minimal grpc-js surface the supervisor drives.
// @internal
export interface SupervisedStream {
	// biome-ignore lint/suspicious/noExplicitAny: grpc stream listener surface is opaque
	on(event: string, listener: (...args: any[]) => void): unknown;
	cancel?(): void;
}

// @internal — см. ./README.md
export interface StreamSupervisorDeps<S extends SupervisedStream, M> {
	// open builds a fresh stream, or returns null when a precondition is not met
	// yet (no identity). Null is treated like a break: retry on the ladder.
	open: () => S | null;
	// onData receives every frame of the current stream, with the stream itself
	// so the handler can write back on the same call.
	onData: (msg: M, stream: S) => void;
	// onError reports a stream-level failure. Notification only — the supervisor
	// owns the reconnect decision.
	onError: (err: Error) => void;
	// reconnectOpts pins the backoff ladder/jitter; tests inject a short
	// deterministic ladder so reconnect behaviour is observable in milliseconds.
	reconnectOpts?: ReconnectDelayOptions;
	// onSchedule reports the delay this supervisor is about to wait. Tests read
	// the climbed ladder from here rather than from a spy on the global timer,
	// which in one test process also catches the timers of every other
	// supervisor still running from an earlier file.
	onSchedule?: (delayMs: number) => void;
}

// @internal — см. ./README.md
export class StreamSupervisor<S extends SupervisedStream, M> {
	private _stream: S | null = null;
	private _stopped = true;
	private _attempt = 0;
	private _timer: ReturnType<typeof setTimeout> | null = null;

	constructor(private readonly d: StreamSupervisorDeps<S, M>) {}

	start(): void {
		this._stopped = false;
		this._attempt = 0;
		this.clearTimer();
		this.open();
	}

	stop(): void {
		this._stopped = true;
		this.clearTimer();
		this.closeCurrent();
	}

	// restart drops the live stream and reopens immediately on a fresh ladder.
	// Used when the stream's own parameters went stale (rotated instance id) or
	// when an out-of-band health signal proves it is dead (missed heartbeats).
	restart(): void {
		if (this._stopped) return;
		this.clearTimer();
		this._attempt = 0;
		this.open();
	}

	// current exposes the live stream for writes. Null between a break and the
	// next successful open.
	current(): S | null {
		return this._stream;
	}

	private open(): void {
		this.closeCurrent();
		if (this._stopped) return;

		let stream: S | null;
		try {
			stream = this.d.open();
		} catch (err) {
			// Call creation fails synchronously while the channel is torn down
			// mid-rotation; an uncaught throw here lands in a timer callback and
			// kills the process.
			this.d.onError(err as Error);
			this.scheduleReconnect();
			return;
		}
		if (stream === null) {
			this.scheduleReconnect();
			return;
		}
		this._stream = stream;

		// Identity guards: a cancelled or replaced stream still emits data/error/
		// end asynchronously — grpc-js flushes buffered frames before "end", so a
		// dead stream can outlive a whole ladder rung. Without the guard a late
		// "end" from stream A nulls out the live stream B (stop() can no longer
		// cancel it) and opens a third stream the runtime rejects with
		// ALREADY_EXISTS, looping forever while deliveries go to the orphan.
		stream.on("data", (msg: M) => {
			if (this._stream !== stream) return;
			// Progress — and only progress — proves the stream is usable. A clean
			// close never resets the ladder: a runtime that keeps closing streams
			// gracefully would otherwise be hammered once per second forever.
			this._attempt = 0;
			this.d.onData(msg, stream);
		});
		stream.on("error", (err: Error) => {
			if (this._stream !== stream) return;
			this._stream = null;
			this.d.onError(err);
			this.scheduleReconnect();
		});
		stream.on("end", () => {
			if (this._stream !== stream) return;
			this._stream = null;
			this.scheduleReconnect();
		});
	}

	// One pending timer at a time: grpc-js emits both "error" and "end" for a
	// single broken stream, and a second timer would double the number of live
	// reconnect loops on every cycle — exponential runaway.
	private scheduleReconnect(): void {
		if (this._stopped || this._timer !== null) return;
		const delay = reconnectDelay(this._attempt++, this.d.reconnectOpts);
		this.d.onSchedule?.(delay);
		this._timer = setTimeout(() => {
			this._timer = null;
			this.open();
		}, delay);
	}

	private clearTimer(): void {
		if (this._timer !== null) {
			clearTimeout(this._timer);
			this._timer = null;
		}
	}

	private closeCurrent(): void {
		const old = this._stream;
		this._stream = null;
		old?.cancel?.();
	}
}
