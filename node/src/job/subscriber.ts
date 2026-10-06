// @internal — см. ./README.md

import {
	type ClientReadableStream,
	type ClientUnaryCall,
	Metadata,
} from "@grpc/grpc-js";
import type { Logger } from "../logger";
import type { JobExecution, JobsClient } from "../pb/servicebridge/v1/jobs";
import { StreamSupervisor } from "../registry/stream-supervisor";
import type { ReconnectDelayOptions } from "../utils/reconnect-ladder";
import { Semaphore, SemaphoreAbortedError } from "../utils/semaphore";
import type { JobDomain } from "./domain";
import type { JobHandler, JobHandlerCtx, JobOpts } from "./types";

// First heartbeat cadence; afterwards the runtime's heartbeat_interval_ms.
// Every heartbeat extends the lease of all executions this instance holds.
const HEARTBEAT_INTERVAL_MS = 5_000;
const HEARTBEAT_FAIL_THRESHOLD = 3;

// Certificate/identity rotation replaces the subscriber while an application
// handler may still be finishing. Keep its slots with the owning JobDomain so
// the replacement cannot overlap a handler that has not honored cancellation.
const domainSemaphores = new WeakMap<JobDomain, Map<string, Semaphore>>();

export interface IdentityProvider {
	serviceId: string;
	instanceId: string;
}

// @public — см. ./README.md
export interface SubscriberDeps {
	// The jobs channel; null until the bridge has one.
	client: () => JobsClient | null;
	identity: () => IdentityProvider | null;
	domain: JobDomain;
	logger: Logger;
	// runWithTrace runs the handler inside an AsyncLocalStorage trace context
	// derived from JobExecution.xSbTrace so nested RPC/event calls inherit the
	// trace. Mandatory: a missing hook would silently drop trace propagation
	// into the job handler.
	runWithTrace: (xSbTrace: string, fn: () => Promise<void>) => Promise<void>;
	// reconnectOpts pins the backoff ladder/jitter; tests inject a short
	// deterministic ladder so reconnect behaviour is observable in milliseconds.
	// @internal
	reconnectOpts?: ReconnectDelayOptions;
	// onSchedule observes each reconnect delay. See StreamSupervisorDeps.
	onSchedule?: (delayMs: number) => void;
}

export class JobSubscriber {
	private _closed = false;
	private _draining = false;
	private readonly _running = new Set<Promise<void>>();
	// Released on stop() so executions still queued on a per-job semaphore are
	// dropped instead of starting a handler after shutdown.
	private _stopping = new AbortController();
	private readonly _active = new Map<
		string,
		{ epoch: number; controller: AbortController }
	>();
	private _heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
	private _heartbeatFailures = 0;
	private _heartbeatGeneration = 0;
	private _heartbeatCall: ClientUnaryCall | null = null;
	private readonly _semaphores: Map<string, Semaphore>;
	private readonly supervisor: StreamSupervisor<
		ClientReadableStream<JobExecution>,
		JobExecution
	>;
	private readonly runWithTrace: (
		xSbTrace: string,
		fn: () => Promise<void>,
	) => Promise<void>;

	constructor(private readonly d: SubscriberDeps) {
		let semaphores = domainSemaphores.get(d.domain);
		if (!semaphores) {
			semaphores = new Map();
			domainSemaphores.set(d.domain, semaphores);
		}
		this._semaphores = semaphores;
		this.runWithTrace = d.runWithTrace;
		this.supervisor = new StreamSupervisor({
			open: () => this.openStream(),
			onData: (exec) => {
				// A draining instance leaves new executions alone: the runtime
				// reassigns them once this instance disconnects.
				if (this._draining) return;
				const run = this.dispatch(exec).catch((err) => {
					this.d.logger.error("jobs: dispatch failed", {
						executionId: exec.executionId,
						error: (err as Error).message,
					});
				});
				this._running.add(run);
				void run.finally(() => this._running.delete(run));
			},
			onDisconnect: () => {
				this._stopping.abort();
				if (!this._closed) this._stopping = new AbortController();
			},
			onError: (err) =>
				this.d.logger.warn("jobs: subscribe stream failed", {
					error: err.message,
				}),
			reconnectOpts: d.reconnectOpts,
			onSchedule: d.onSchedule,
		});
	}

	/** Reopens the stream at once (recovered channel). */
	restart(): void {
		this.supervisor.restart();
	}

	/**
	 * Stops taking new executions and waits for the running ones (up to
	 * timeoutMs). Heartbeats continue meanwhile so their leases stay alive.
	 */
	async drain(timeoutMs: number): Promise<void> {
		this._draining = true;
		const running = [...this._running];
		if (running.length === 0) return;
		await Promise.race([
			Promise.allSettled(running),
			new Promise((r) => setTimeout(r, timeoutMs)),
		]);
	}

	start(): void {
		this._closed = false;
		this._stopping = new AbortController();
		this.supervisor.start();
		this.startHeartbeat();
	}

	async stop(): Promise<void> {
		this._closed = true;
		this._stopping.abort();
		this.stopHeartbeat();
		this.supervisor.stop();
	}

	private openStream(): ClientReadableStream<JobExecution> | null {
		const id = this.d.identity();
		const client = this.d.client();
		if (!id || !client) return null;
		return client.subscribe({
			serviceId: id.serviceId,
			instanceId: id.instanceId,
		});
	}

	private async dispatch(exec: JobExecution): Promise<void> {
		const id = this.d.identity();
		if (!id) {
			this.d.logger.warn("jobs: no identity, dropping execution", {
				executionId: exec.executionId,
			});
			return;
		}

		const reg = this.d.domain.lookup(exec.jobName, exec.fingerprint);
		if (!reg) {
			const message = `unsupported_version: ${exec.jobName}/${exec.fingerprint}`;
			this.d.logger.warn("jobs: execution for an unknown handler version", {
				job: exec.jobName,
				fingerprint: exec.fingerprint,
			});
			this.sendResult(exec, id.instanceId, false, {
				errorMessage: message,
				retryable: false,
			});
			return;
		}
		const previous = this._active.get(exec.executionId);
		if (previous && previous.epoch >= exec.leaseEpoch) return;
		previous?.controller.abort();
		const controller = new AbortController();
		this._active.set(exec.executionId, { epoch: exec.leaseEpoch, controller });
		const signal = AbortSignal.any([this._stopping.signal, controller.signal]);
		try {
			const maxConcurrent =
				(reg.opts.overlap ?? "skip") === "skip"
					? 1
					: (reg.opts.maxConcurrent ?? 32);
			const sem = this.getSemaphore(
				`${exec.jobName}:${exec.fingerprint}`,
				maxConcurrent,
			);

			try {
				await sem.acquire(signal);
			} catch (err) {
				if (!(err instanceof SemaphoreAbortedError)) throw err;
				// Without the signal a waiter queued behind a running handler would get
				// its slot after stop() and run the handler on a subscriber that is
				// already shut down. Dropping it is safe: the lease expires and the
				// runtime re-assigns the execution.
				this.d.logger.warn("jobs: stopped while queued, dropping execution", {
					executionId: exec.executionId,
				});
				return;
			}
			try {
				await this.run(exec, reg.fn, reg.opts, id.instanceId, signal);
			} finally {
				sem.release();
			}
		} finally {
			if (this._active.get(exec.executionId)?.controller === controller)
				this._active.delete(exec.executionId);
		}
	}

	private async run(
		exec: JobExecution,
		fn: JobHandler,
		_opts: JobOpts,
		instanceId: string,
		signal: AbortSignal,
	): Promise<void> {
		const ctx: JobHandlerCtx = {
			jobName: exec.jobName,
			executionId: exec.executionId,
			scheduledAt: new Date(exec.scheduledAtUnixMs),
			localScheduledAt: new Date(exec.localScheduledAtUnixMs),
			attempt: exec.attempt,
			idempotencyKey: exec.idempotencyKey,
			signal,
		};

		// xSbTrace is the canonical "<traceID>-<parentOpID>" header per
		// ADR 0006. The runtime always emits it for telemetry-enabled
		// executions; absent (empty string) means runtime telemetry was
		// disabled — handler still runs, just without trace propagation.
		const xSbTrace = exec.xSbTrace ?? "";

		try {
			await this.runWithTrace(xSbTrace, () => Promise.resolve(fn(ctx)));
			if (!signal.aborted) this.sendResult(exec, instanceId, true);
		} catch (err) {
			if (signal.aborted) return;
			const error = err as Error & { retryable?: boolean };
			const retryable = error.retryable !== false;
			this.sendResult(exec, instanceId, false, {
				errorMessage: error.message ?? "unknown error",
				retryable,
			});
		}
	}

	private sendResult(
		exec: JobExecution,
		instanceId: string,
		success: true,
		failure?: undefined,
	): void;
	private sendResult(
		exec: JobExecution,
		instanceId: string,
		success: false,
		failure: { errorMessage: string; retryable: boolean },
	): void;
	private sendResult(
		exec: JobExecution,
		instanceId: string,
		success: boolean,
		failure?: { errorMessage: string; retryable: boolean },
	): void {
		const request = success
			? {
					executionId: exec.executionId,
					instanceId,
					leaseEpoch: exec.leaseEpoch,
					success: {},
					failure: undefined,
				}
			: {
					executionId: exec.executionId,
					instanceId,
					leaseEpoch: exec.leaseEpoch,
					success: undefined,
					failure: {
						errorMessage: failure?.errorMessage ?? "unknown error",
						retryable: failure?.retryable ?? true,
					},
				};

		const client = this.d.client();
		if (this._closed || this._stopping.signal.aborted || !client) return;
		client.jobResult(request, (err) => {
			if (err) {
				this.d.logger.warn("jobs: result not delivered", {
					executionId: exec.executionId,
					error: err.message,
				});
			}
		});
	}

	private getSemaphore(jobName: string, maxConcurrent: number): Semaphore {
		const existing = this._semaphores.get(jobName);
		if (existing) return existing;
		const limit = maxConcurrent > 0 ? maxConcurrent : 32;
		// Unbounded wait queue, unlike the inbound RPC path which sheds load on a
		// full queue. An execution arriving here already holds a runtime-issued
		// lease and the runtime is the one rate-limiting dispatch; shedding it
		// client-side would not reject a request, it would abandon work the
		// runtime believes this instance owns until the lease expires.
		const sem = new Semaphore(limit, 1024);
		this._semaphores.set(jobName, sem);
		return sem;
	}

	private startHeartbeat(): void {
		this.stopHeartbeat();
		this._heartbeatFailures = 0;
		const generation = this._heartbeatGeneration;
		const schedule = (interval: number) => {
			if (this._closed || generation !== this._heartbeatGeneration) return;
			this._heartbeatTimer = setTimeout(() => beat(interval), interval);
			this._heartbeatTimer.unref();
		};
		const beat = (interval: number) => {
			if (this._closed || generation !== this._heartbeatGeneration) return;
			const id = this.d.identity();
			const client = this.d.client();
			if (!id || !client) {
				schedule(interval);
				return;
			}
			try {
				this._heartbeatCall = client.heartbeat(
					{ serviceId: id.serviceId, instanceId: id.instanceId },
					new Metadata(),
					{ deadline: Date.now() + interval },
					(err, response) => {
						if (this._closed || generation !== this._heartbeatGeneration)
							return;
						if (err) this.onHeartbeatFailure(err.message);
						else this._heartbeatFailures = 0;
						const hint = response?.heartbeatIntervalMs ?? 0;
						schedule(
							Number.isFinite(hint) && hint > 0
								? Math.max(100, hint)
								: interval,
						);
					},
				);
			} catch (err) {
				this.onHeartbeatFailure((err as Error).message);
				schedule(interval);
			}
		};
		beat(HEARTBEAT_INTERVAL_MS);
	}

	private onHeartbeatFailure(reason: string): void {
		this._heartbeatFailures++;
		this.d.logger.warn("jobs: heartbeat failed", {
			failures: this._heartbeatFailures,
			threshold: HEARTBEAT_FAIL_THRESHOLD,
			error: reason,
		});
		if (this._heartbeatFailures < HEARTBEAT_FAIL_THRESHOLD) return;
		this.d.logger.warn("jobs: heartbeat threshold reached, reconnecting");
		this._heartbeatFailures = 0;
		this.supervisor.restart();
	}

	private stopHeartbeat(): void {
		this._heartbeatGeneration++;
		this._heartbeatCall?.cancel();
		this._heartbeatCall = null;
		if (this._heartbeatTimer) {
			clearTimeout(this._heartbeatTimer);
			this._heartbeatTimer = null;
		}
	}
}
