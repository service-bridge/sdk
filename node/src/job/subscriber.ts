// @internal — см. ./README.md

import type { ClientReadableStream, ServiceError } from "@grpc/grpc-js";
import type { JobExecution, JobsClient } from "../pb/servicebridge/v1/jobs";
import type { JobDomain } from "./domain";
import type { JobHandler, JobHandlerCtx, JobOpts } from "./types";

const RECONNECT_LADDER_MS = [1000, 5000, 15000, 30000, 60000] as const;
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

function nextDelay(attempt: number): number {
	const idx = Math.min(Math.max(attempt, 0), RECONNECT_LADDER_MS.length - 1);
	return (
		RECONNECT_LADDER_MS[idx] ??
		RECONNECT_LADDER_MS[RECONNECT_LADDER_MS.length - 1] ??
		60000
	);
}

// Semaphore limits concurrent in-flight job executions.
class Semaphore {
	private _count: number;
	private readonly _queue: Array<() => void> = [];

	constructor(limit: number) {
		this._count = limit;
	}

	acquire(): Promise<void> {
		if (this._count > 0) {
			this._count--;
			return Promise.resolve();
		}
		return new Promise((resolve) => {
			this._queue.push(resolve);
		});
	}

	release(): void {
		const next = this._queue.shift();
		if (next) {
			next();
		} else {
			this._count++;
		}
	}
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
}

export class JobSubscriber {
	private _stream: ClientReadableStream<JobExecution> | null = null;
	private _closed = false;
	private _heartbeatTimer: ReturnType<typeof setInterval> | null = null;
	private _heartbeatFailures = 0;
	private readonly _semaphores = new Map<string, Semaphore>();
	private readonly runWithTrace: (
		xSbTrace: string,
		fn: () => Promise<void>,
	) => Promise<void>;

	constructor(private readonly d: SubscriberDeps) {
		this.runWithTrace = d.runWithTrace;
	}

	start(): void {
		void this.connectLoop(0);
		this.startHeartbeat();
	}

	async stop(): Promise<void> {
		this._closed = true;
		this.stopHeartbeat();
		this._stream?.cancel();
	}

	private async connectLoop(attempt: number): Promise<void> {
		while (!this._closed) {
			try {
				await this.runOnce();
				attempt = 0;
			} catch (err) {
				if (this._closed) return;
				this.d.logger.warn(
					`jobs subscriber: stream error: ${(err as Error).message}`,
				);
			}
			const delay = nextDelay(attempt++);
			await new Promise((r) => setTimeout(r, delay));
		}
	}

	private runOnce(): Promise<void> {
		return new Promise((resolve, reject) => {
			const id = this.d.identity();
			if (!id) {
				reject(new Error("jobs subscriber: no identity yet"));
				return;
			}
			const stream = this.d.rpcClient.subscribe({
				serviceId: id.serviceId,
				instanceId: id.instanceId,
			});
			this._stream = stream;

			stream.on("data", (exec: JobExecution) => {
				void this.dispatch(exec).catch((err) => {
					this.d.logger.error(
						`jobs: dispatch error for execution ${exec.executionId}: ${(err as Error).message}`,
					);
				});
			});
			stream.on("error", (err: ServiceError) => reject(err));
			stream.on("end", () => resolve());
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

		await sem.acquire();
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
		const sem = new Semaphore(limit);
		this._semaphores.set(jobName, sem);
		return sem;
	}

	private startHeartbeat(): void {
		this._heartbeatFailures = 0;
		this._heartbeatTimer = setInterval(() => {
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
							this._heartbeatFailures++;
							this.d.logger.warn(
								`jobs: heartbeat failed (${this._heartbeatFailures}/${HEARTBEAT_FAIL_THRESHOLD}): ${err.message}`,
							);
							if (this._heartbeatFailures >= HEARTBEAT_FAIL_THRESHOLD) {
								this.d.logger.warn(
									"jobs: heartbeat threshold reached, reconnecting",
								);
								this._stream?.cancel();
								this._heartbeatFailures = 0;
							}
						} else {
							this._heartbeatFailures = 0;
						}
					},
				);
			} catch {
				this.stopHeartbeat();
			}
		}, HEARTBEAT_INTERVAL_MS);
	}

	private stopHeartbeat(): void {
		if (this._heartbeatTimer) {
			clearInterval(this._heartbeatTimer);
			this._heartbeatTimer = null;
		}
	}
}
