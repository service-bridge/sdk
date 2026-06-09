// @public — см. ./README.md

export type CronTrigger = { cron: string; tz?: string };
export type DelayedTrigger = { delayed: { at: Date | string | number } };
export type IntervalTrigger = { interval: number }; // milliseconds
export type Trigger = CronTrigger | DelayedTrigger | IntervalTrigger;

export type CatchupPolicy = "skip" | "fire_once" | "fire_all";
export type OverlapPolicy = "skip" | "allow" | "buffer_one";

export type DeclaredDep =
	| { rpc: string } // "service.Method"
	| { event: string } // event name
	| { workflow: string }; // workflow name

export interface JobHandlerCtx {
	jobName: string;
	executionId: string;
	scheduledAt: Date;
	localScheduledAt: Date;
	attempt: number;
	idempotencyKey: string;
	signal: AbortSignal;
}

export type JobHandler = (ctx: JobHandlerCtx) => Promise<void>;

// RetryPolicy configures per-job exponential backoff.
export interface RetryPolicy {
	initialMs: number;
	maxMs: number;
	multiplier: number;
	jitter: number;
}

// JobOpts is the options object passed to sb.job.handle(name, opts, fn).
export interface JobOpts {
	trigger: Trigger;
	catchup?: CatchupPolicy;
	overlap?: OverlapPolicy;
	deps?: DeclaredDep[];
	maxAttempts?: number;
	leaseTtlMs?: number;
	maxConcurrent?: number;
	retry?: RetryPolicy;
}
