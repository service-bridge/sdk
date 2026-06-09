// @public — см. ./README.md

import { createHash } from "node:crypto";
import { MethodType } from "../pb/servicebridge/v1/registry";
import type { Registry } from "../registry/registry";
import type {
	CatchupPolicy,
	DeclaredDep,
	JobHandler,
	JobOpts,
	OverlapPolicy,
	RetryPolicy,
	Trigger,
} from "./types";

// JobDomain — namespace `sb.job` for declaring scheduled jobs. Mirrors
// EventDomain / WorkflowDomain shape: singular namespace, `handle()` verb,
// (name, opts, fn) positional signature.
//
// Registration goes through Registry.RegisterAndWatch via an IncomingMethod
// with type=JOB and a canonical-spec JSON in input_schema_json. The runtime
// side parses the canonical spec (CanonicalJobSpec) and creates the
// definition + schedule via jobs.Registrar.RegisterJob.
//
// JobDomain also stores a local lookup table (name → {opts, fn}) so the
// JobSubscriber can resolve incoming JobExecution messages to the right
// handler + per-job concurrency settings.
export class JobDomain {
	private readonly _byName = new Map<
		string,
		{ opts: JobOpts; fn: JobHandler }
	>();

	constructor(private readonly registry: Registry) {}

	handle(name: string, opts: JobOpts, fn: JobHandler): void {
		validateOpts(name, opts);
		if (this._byName.has(name)) {
			throw new Error(
				`sb.job.handle: duplicate job name ${JSON.stringify(name)}`,
			);
		}
		const canonical = canonicalJobSpec(opts);
		const json = JSON.stringify(canonical);
		const contractHash = sha256Hex(json);
		this.registry._handle.job(name, contractHash, json, fn);
		this._byName.set(name, { opts, fn });
	}

	/** @internal — used by JobSubscriber to dispatch incoming executions. */
	lookup(name: string): { opts: JobOpts; fn: JobHandler } | undefined {
		return this._byName.get(name);
	}

	/** @internal — used by ServiceBridge to skip subscriber startup when empty. */
	size(): number {
		return this._byName.size;
	}
}

// canonicalJobSpec converts user-facing JobOpts into the on-wire canonical
// shape matching runtime/internal/jobs/canonical.go::CanonicalJobSpec.
//
// Field order and key names MUST stay aligned with the Go struct — any drift
// silently changes contract_hash on either side.
interface CanonicalJobSpec {
	trigger: CanonicalTrigger;
	catchup?: CatchupPolicy;
	overlap?: OverlapPolicy;
	deps?: Array<{ kind: string; target: string }>;
	maxAttempts?: number;
	leaseTtlMs?: number;
	maxConcurrent?: number;
	retry?: RetryPolicy;
}

interface CanonicalTrigger {
	cron?: { expr: string; tz?: string };
	delayed?: { runAtUnixMs: number };
	interval?: { everyMs: number };
}

function canonicalJobSpec(opts: JobOpts): CanonicalJobSpec {
	const out: CanonicalJobSpec = {
		trigger: canonicalTrigger(opts.trigger),
	};
	if (opts.catchup) out.catchup = opts.catchup;
	if (opts.overlap) out.overlap = opts.overlap;
	if (opts.deps && opts.deps.length > 0) {
		out.deps = opts.deps.map(depToCanonical);
	}
	if (opts.maxAttempts !== undefined) out.maxAttempts = opts.maxAttempts;
	if (opts.leaseTtlMs !== undefined) out.leaseTtlMs = opts.leaseTtlMs;
	if (opts.maxConcurrent !== undefined) out.maxConcurrent = opts.maxConcurrent;
	if (opts.retry) out.retry = opts.retry;
	return out;
}

function canonicalTrigger(t: Trigger): CanonicalTrigger {
	if ("cron" in t) {
		const c: CanonicalTrigger["cron"] = { expr: t.cron };
		if (t.tz) c.tz = t.tz;
		return { cron: c };
	}
	if ("delayed" in t) {
		const at = t.delayed.at;
		let ms: number;
		if (at instanceof Date) ms = at.getTime();
		else if (typeof at === "number") ms = at;
		else ms = new Date(at).getTime();
		if (Number.isNaN(ms)) {
			throw new Error(
				"sb.job.handle: delayed.at is not a valid Date / number / string",
			);
		}
		return { delayed: { runAtUnixMs: ms } };
	}
	if ("interval" in t) {
		return { interval: { everyMs: t.interval } };
	}
	throw new Error("sb.job.handle: trigger must be cron | delayed | interval");
}

function depToCanonical(d: DeclaredDep): { kind: string; target: string } {
	if ("rpc" in d) return { kind: "rpc", target: d.rpc };
	if ("event" in d) return { kind: "event", target: d.event };
	if ("workflow" in d) return { kind: "workflow", target: d.workflow };
	throw new Error("sb.job.handle: dep must be {rpc} | {event} | {workflow}");
}

function validateOpts(name: string, opts: JobOpts): void {
	if (!name || typeof name !== "string") {
		throw new Error("sb.job.handle: name is required");
	}
	if (!opts || typeof opts !== "object") {
		throw new Error("sb.job.handle: opts is required");
	}
	if (!opts.trigger) {
		throw new Error("sb.job.handle: opts.trigger is required");
	}
	const t = opts.trigger as Record<string, unknown>;
	const keys = ["cron", "delayed", "interval"].filter(
		(k) => t[k] !== undefined,
	);
	if (keys.length !== 1) {
		throw new Error(
			`sb.job.handle: trigger must have exactly one of cron|delayed|interval, got ${keys.length}`,
		);
	}
	if ("cron" in t && typeof t.cron === "string") {
		const fields = t.cron.trim().split(/\s+/);
		if (fields.length !== 5) {
			throw new Error(
				`sb.job.handle: cron must be 5-field (no seconds); got ${fields.length} fields`,
			);
		}
	}
	if ("interval" in t && typeof t.interval === "number") {
		if (t.interval <= 0) {
			throw new Error("sb.job.handle: interval must be > 0 ms");
		}
	}
}

function sha256Hex(s: string): string {
	return createHash("sha256").update(s).digest("hex");
}
