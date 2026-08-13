import { describe, expect, it } from "bun:test";
import {
	Semaphore,
	SemaphoreAbortedError,
	SemaphoreExhaustedError,
} from "./semaphore";

describe("Semaphore construction", () => {
	it("rejects a non-positive limit", () => {
		expect(() => new Semaphore(0)).toThrow("limit must be an integer >= 1");
		expect(() => new Semaphore(-1)).toThrow("limit must be an integer >= 1");
	});

	it("rejects a fractional limit", () => {
		expect(() => new Semaphore(1.5)).toThrow("limit must be an integer >= 1");
	});

	it("rejects a negative queue depth", () => {
		expect(() => new Semaphore(1, -1)).toThrow(
			"maxQueued must be an integer >= 0",
		);
	});

	it("defaults maxQueued to limit", () => {
		const sem = new Semaphore(3);
		expect(sem.available).toBe(3);
		expect(sem.queued).toBe(0);
	});
});

describe("Semaphore acquire/release", () => {
	it("hands out slots up to the limit without queueing", async () => {
		const sem = new Semaphore(2);
		await sem.acquire();
		await sem.acquire();
		expect(sem.available).toBe(0);
		expect(sem.queued).toBe(0);
	});

	it("queues past the limit and resolves on release", async () => {
		const sem = new Semaphore(1, 4);
		await sem.acquire();

		let waiterResolved = false;
		const waiting = sem.acquire().then(() => {
			waiterResolved = true;
		});
		await Promise.resolve();
		expect(waiterResolved).toBe(false);
		expect(sem.queued).toBe(1);

		sem.release();
		await waiting;
		expect(waiterResolved).toBe(true);
		expect(sem.queued).toBe(0);
	});

	it("release hands the slot to the waiter, not back to the counter", async () => {
		const sem = new Semaphore(1, 2);
		await sem.acquire();
		const waiting = sem.acquire();
		sem.release();
		await waiting;
		// The waiter now holds the only slot — available stays 0.
		expect(sem.available).toBe(0);
	});

	it("release with no waiters restores a slot", async () => {
		const sem = new Semaphore(2);
		await sem.acquire();
		expect(sem.available).toBe(1);
		sem.release();
		expect(sem.available).toBe(2);
	});

	it("release never pushes available above the limit", () => {
		const sem = new Semaphore(1);
		sem.release();
		sem.release();
		expect(sem.available).toBe(1);
	});

	it("serves waiters in FIFO order", async () => {
		const sem = new Semaphore(1, 4);
		await sem.acquire();
		const order: number[] = [];
		const a = sem.acquire().then(() => order.push(1));
		const b = sem.acquire().then(() => order.push(2));
		const c = sem.acquire().then(() => order.push(3));

		sem.release();
		sem.release();
		sem.release();
		await Promise.all([a, b, c]);
		expect(order).toEqual([1, 2, 3]);
	});
});

describe("Semaphore load shedding", () => {
	it("rejects immediately when maxQueued is 0 and no slot is free", async () => {
		const sem = new Semaphore(1, 0);
		await sem.acquire();
		expect(sem.acquire()).rejects.toBeInstanceOf(SemaphoreExhaustedError);
	});

	it("rejects once the queue is at its depth limit", async () => {
		const sem = new Semaphore(1, 2);
		await sem.acquire();
		const q1 = sem.acquire();
		const q2 = sem.acquire();
		expect(sem.queued).toBe(2);

		await expect(sem.acquire()).rejects.toBeInstanceOf(SemaphoreExhaustedError);
		// The queue does not grow on rejection — this is the load-shed property.
		expect(sem.queued).toBe(2);

		sem.release();
		sem.release();
		await Promise.all([q1, q2]);
	});

	it("reports the limits it was configured with", async () => {
		const sem = new Semaphore(3, 5);
		await Promise.all([sem.acquire(), sem.acquire(), sem.acquire()]);
		const queued = [
			sem.acquire(),
			sem.acquire(),
			sem.acquire(),
			sem.acquire(),
			sem.acquire(),
		];
		const err = await sem.acquire().catch((e) => e);
		expect(err).toBeInstanceOf(SemaphoreExhaustedError);
		expect((err as SemaphoreExhaustedError).limit).toBe(3);
		expect((err as SemaphoreExhaustedError).maxQueued).toBe(5);

		for (let i = 0; i < 5; i++) sem.release();
		await Promise.all(queued);
	});

	it("accepts new work again after slots drain", async () => {
		const sem = new Semaphore(1, 0);
		await sem.acquire();
		await expect(sem.acquire()).rejects.toBeInstanceOf(SemaphoreExhaustedError);
		sem.release();
		await sem.acquire();
		expect(sem.available).toBe(0);
	});
});

describe("Semaphore cancellation", () => {
	it("rejects when the signal is already aborted", async () => {
		const sem = new Semaphore(1);
		const ctl = new AbortController();
		ctl.abort();
		await expect(sem.acquire(ctl.signal)).rejects.toBeInstanceOf(
			SemaphoreAbortedError,
		);
		// No slot was consumed.
		expect(sem.available).toBe(1);
	});

	it("removes the waiter from the queue when aborted while waiting", async () => {
		const sem = new Semaphore(1, 4);
		await sem.acquire();
		const ctl = new AbortController();
		const waiting = sem.acquire(ctl.signal);
		expect(sem.queued).toBe(1);

		ctl.abort();
		await expect(waiting).rejects.toBeInstanceOf(SemaphoreAbortedError);
		expect(sem.queued).toBe(0);
	});

	it("an aborted waiter never receives the released slot", async () => {
		const sem = new Semaphore(1, 4);
		await sem.acquire();
		const ctl = new AbortController();
		const aborted = sem.acquire(ctl.signal);
		const survivor = sem.acquire();

		ctl.abort();
		await expect(aborted).rejects.toBeInstanceOf(SemaphoreAbortedError);

		sem.release();
		await survivor;
		expect(sem.available).toBe(0);
		expect(sem.queued).toBe(0);
	});

	it("aborting after the slot was granted does not reject", async () => {
		const sem = new Semaphore(1, 4);
		await sem.acquire();
		const ctl = new AbortController();
		const waiting = sem.acquire(ctl.signal);
		sem.release();
		await waiting;

		ctl.abort();
		// The holder keeps its slot; abort past the grant is a no-op.
		expect(sem.available).toBe(0);
	});

	it("aborting frees the queue slot so new work is admitted", async () => {
		const sem = new Semaphore(1, 1);
		await sem.acquire();
		const ctl = new AbortController();
		const aborted = sem.acquire(ctl.signal);
		await expect(sem.acquire()).rejects.toBeInstanceOf(SemaphoreExhaustedError);

		ctl.abort();
		await expect(aborted).rejects.toBeInstanceOf(SemaphoreAbortedError);
		const admitted = sem.acquire();
		expect(sem.queued).toBe(1);
		sem.release();
		await admitted;
	});
});
