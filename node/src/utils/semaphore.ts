import { ConfigurationError } from "../errors";
// Domain-neutral counting semaphore with a bounded wait queue. No I/O, no
// domain knowledge — see ./README.md.

// SemaphoreExhaustedError is thrown by acquire() when every slot is taken and
// the wait queue is already at its depth limit. Callers use it as the load-shed
// signal: reject the work now instead of queueing it without bound.
export class SemaphoreExhaustedError extends Error {
	constructor(
		readonly limit: number,
		readonly maxQueued: number,
	) {
		super(
			`semaphore: exhausted (limit=${limit}, maxQueued=${maxQueued}) — work rejected`,
		);
		this.name = "SemaphoreExhaustedError";
	}
}

// SemaphoreAbortedError is thrown when the AbortSignal passed to acquire()
// fires while the caller is still queued. A slot is never handed to an aborted
// waiter, so no release() is owed.
export class SemaphoreAbortedError extends Error {
	constructor() {
		super("semaphore: acquire aborted while waiting");
		this.name = "SemaphoreAbortedError";
	}
}

interface Waiter {
	resolve: () => void;
	reject: (err: unknown) => void;
	detach: () => void;
}

// Semaphore bounds how many operations run concurrently and how many may wait
// for a slot. Both bounds are required to shed load: a concurrency limit alone
// converts an overload into unbounded memory growth in the queue.
export class Semaphore {
	private _free: number;
	private readonly _queue: Waiter[] = [];

	// limit is the number of concurrently held slots. maxQueued caps the wait
	// queue; 0 means "never queue" — acquire() either takes a free slot or
	// rejects immediately.
	constructor(
		private readonly limit: number,
		private readonly maxQueued: number = limit,
	) {
		if (!Number.isInteger(limit) || limit < 1) {
			throw new ConfigurationError(`semaphore: limit must be an integer >= 1, got ${limit}`);
		}
		if (!Number.isInteger(maxQueued) || maxQueued < 0) {
			throw new ConfigurationError(
				`semaphore: maxQueued must be an integer >= 0, got ${maxQueued}`,
			);
		}
		this._free = limit;
	}

	// available is the number of slots that acquire() can take without queueing.
	get available(): number {
		return this._free;
	}

	// queued is the number of live waiters. Aborted waiters are removed eagerly,
	// so this never counts stale entries.
	get queued(): number {
		return this._queue.length;
	}

	// acquire takes a slot, queues for one, or rejects with
	// SemaphoreExhaustedError when the queue is full. Every resolved acquire()
	// owes exactly one release().
	acquire(signal?: AbortSignal): Promise<void> {
		if (signal?.aborted) {
			return Promise.reject(new SemaphoreAbortedError());
		}
		if (this._free > 0) {
			this._free--;
			return Promise.resolve();
		}
		if (this._queue.length >= this.maxQueued) {
			return Promise.reject(
				new SemaphoreExhaustedError(this.limit, this.maxQueued),
			);
		}
		return new Promise<void>((resolve, reject) => {
			const waiter: Waiter = {
				resolve,
				reject,
				detach: () => {},
			};
			if (signal) {
				const onAbort = () => {
					const idx = this._queue.indexOf(waiter);
					if (idx === -1) return;
					this._queue.splice(idx, 1);
					reject(new SemaphoreAbortedError());
				};
				signal.addEventListener("abort", onAbort, { once: true });
				waiter.detach = () => signal.removeEventListener("abort", onAbort);
			}
			this._queue.push(waiter);
		});
	}

	// release returns a slot. It hands the slot straight to the oldest waiter
	// rather than incrementing the counter, so a waiter can never be starved by
	// a fresh acquire() racing ahead of it.
	release(): void {
		const next = this._queue.shift();
		if (!next) {
			if (this._free < this.limit) this._free++;
			return;
		}
		next.detach();
		next.resolve();
	}
}
