// @internal — см. ./README.md

import type { ClientReadableStream } from "@grpc/grpc-js";
import type { JobExecution, JobsClient } from "../pb/servicebridge/v1/jobs";
import { StreamSupervisor } from "../registry/stream-supervisor";
import type { ReconnectDelayOptions } from "../utils/reconnect-ladder";
import { Semaphore, SemaphoreAbortedError } from "../utils/semaphore";
import type { JobDomain } from "./domain";
import type { JobHandler, JobHandlerCtx, JobOpts } from "./types";

const HEARTBEAT_INTERVAL_MS = 5_000;
const HEARTBEAT_FAIL_THRESHOLD = 3;

export interface Logger {
	warn(msg: string): void;
	error(msg: string): void;
}

export interface IdentityProvider {
	serviceId: string;
	instanceId: string;
}

// @public — см. ./README.md
export interface SubscriberDeps {
	rpcClient: JobsClient;
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
}

export class JobSubscriber {
	private _closed = false;
	// Released on stop() so executions still queued on a per-job semaphore are
	// dropped instead of starting a handler after shutdown.
	private _stopping = new AbortController();
	private _heartbeatTimer: ReturnType<typeof setInterval> | null = null;
	private _heartbeatFailures = 0;
	private readonly _semaphores = new Map<string, Semaphore>();
	private readonly supervisor: StreamSupervisor<
		ClientReadableStream<JobExecution>,
		JobExecution
	>;
	private readonly runWithTrace: (
		xSbTrace: string,
		fn: () => Promise<void>,
	) => Promise<void>;

	constructor(private readonly d: SubscriberDeps) {
		this.runWithTrace = d.runWithTrace;
		this.supervisor = new StreamSupervisor({
			open: () => this.openStream(),
			onData: (exec) => {
				void this.dispatch(exec).catch((err) => {
					this.d.logger.error(
						`jobs: dispatch error for execution ${exec.executionId}: ${(err as Error).message}`,
					);
				});
			},
			onError: (err) =>
				this.d.logger.warn(`jobs subscriber: stream error: ${err.message}`),
			reconnectOpts: d.reconnectOpts,
		});
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
		if (!id) return null;
		return this.d.rpcClient.subscribe({
			serviceId: id.serviceId,
			instanceId: id.instanceId,
		});
	}

	private async dispatch(exec: JobExecution): Promise<void> {
		const id = this.d.identity();
		if (!id) {
			this.d.logger.warn(
				`jobs: no identity, dropping execution ${exec.executionId}`,
			);
			return;
		}

		const reg = this.d.domain.lookup(exec.jobName);
		if (!reg) {
			this.d.logger.warn(
				`jobs: no handler for job "${exec.jobName}", dropping execution ${exec.executionId}`,
			);
			return;
		}

		const maxConcurrent = reg.opts.maxConcurrent ?? 0;
		const sem = this.getSemaphore(exec.jobName, maxConcurrent);

		try {
			await sem.acquire(this._stopping.signal);
		} catch (err) {
			if (!(err instanceof SemaphoreAbortedError)) throw err;
			// Without the signal a waiter queued behind a running handler would get
			// its slot after stop() and run the handler on a subscriber that is
			// already shut down. Dropping it is safe: the lease expires and the
			// runtime re-assigns the execution.
			this.d.logger.warn(
				`jobs: stopped while queued, dropping execution ${exec.executionId}`,
			);
			return;
		}
		try {
			await this.run(exec, reg.fn, reg.opts, id.instanceId);
		} finally {
			sem.release();
		}
	}

	private async run(
		exec: JobExecution,
		fn: JobHandler,
		_opts: JobOpts,
		instanceId: string,
	): Promise<void> {
		const abortCtrl = new AbortController();
		const ctx: JobHandlerCtx = {
			jobName: exec.jobName,
			executionId: exec.executionId,
			scheduledAt: new Date(exec.scheduledAtUnixMs),
			localScheduledAt: new Date(exec.localScheduledAtUnixMs),
			attempt: exec.attempt,
			idempotencyKey: exec.idempotencyKey,
			signal: abortCtrl.signal,
		};

		// xSbTrace is the canonical "<traceID>-<parentOpID>" header per
		// ADR 0006. The runtime always emits it for telemetry-enabled
		// executions; absent (empty string) means runtime telemetry was
		// disabled — handler still runs, just without trace propagation.
		const xSbTrace = exec.xSbTrace ?? "";

		try {
			await this.runWithTrace(xSbTrace, () => Promise.resolve(fn(ctx)));
			this.sendResult(exec, instanceId, true);
		} catch (err) {
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

		this.d.rpcClient.jobResult(request, (err) => {
			if (err) {
				this.d.logger.warn(
					`jobs: failed to send result for execution ${exec.executionId}: ${err.message}`,
				);
			}
		});
	}

	private getSemaphore(jobName: string, maxConcurrent: number): Semaphore {
		const existing = this._semaphores.get(jobName);
		if (existing) return existing;
		const limit = maxConcurrent > 0 ? maxConcurrent : Number.MAX_SAFE_INTEGER;
		// Unbounded wait queue, unlike the inbound RPC path which sheds load on a
		// full queue. An execution arriving here already holds a runtime-issued
		// lease and the runtime is the one rate-limiting dispatch; shedding it
		// client-side would not reject a request, it would abandon work the
		// runtime believes this instance owns until the lease expires.
		const sem = new Semaphore(limit, Number.MAX_SAFE_INTEGER);
		this._semaphores.set(jobName, sem);
		return sem;
	}

	private startHeartbeat(): void {
		this._heartbeatFailures = 0;
		const timer = setInterval(() => {
			if (this._closed) {
				this.stopHeartbeat();
				return;
			}
			const id = this.d.identity();
			if (!id) return;
			try {
				this.d.rpcClient.heartbeat(
					{ serviceId: id.serviceId, instanceId: id.instanceId },
					(err) => {
						if (err) {
							this.onHeartbeatFailure(err.message);
						} else {
							this._heartbeatFailures = 0;
						}
					},
				);
			} catch (err) {
				// A synchronous throw means the channel is mid-teardown. Transient —
				// killing the timer here would silently strand the lease until the
				// runtime reclaims it, with nothing in the logs to explain why.
				this.onHeartbeatFailure((err as Error).message);
			}
		}, HEARTBEAT_INTERVAL_MS);
		// Heartbeat must not be the reason a finished process stays alive; the
		// reconnect timer (inside the supervisor) is the one that holds the loop.
		timer.unref();
		this._heartbeatTimer = timer;
	}

	private onHeartbeatFailure(reason: string): void {
		this._heartbeatFailures++;
		this.d.logger.warn(
			`jobs: heartbeat failed (${this._heartbeatFailures}/${HEARTBEAT_FAIL_THRESHOLD}): ${reason}`,
		);
		if (this._heartbeatFailures < HEARTBEAT_FAIL_THRESHOLD) return;
		this.d.logger.warn("jobs: heartbeat threshold reached, reconnecting");
		this._heartbeatFailures = 0;
		this.supervisor.restart();
	}

	private stopHeartbeat(): void {
		if (this._heartbeatTimer) {
			clearInterval(this._heartbeatTimer);
			this._heartbeatTimer = null;
		}
	}
}
