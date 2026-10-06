// WorkflowDomain — `sb.workflow`: declare workflows (owner side) and steer runs
// (caller side). The runtime interprets the DAG; this class only encodes
// definitions and speaks the Workflows RPCs.
//
// @public — см. ./README.md

import type {
	RunSnapshot as PbRunSnapshot,
	RunStatusUpdate,
	WorkflowsClient,
} from "../pb/servicebridge/v1/workflows";
import type { Registry } from "../registry/registry";
import { currentTraceContext } from "../telemetry/context";
import { formatXSbTrace } from "../telemetry/wire-trace";
import { encodeDefinition, type LocalFns } from "./encode";
import {
	WorkflowAccessDeniedError,
	WorkflowNotFoundError,
	WorkflowRunFailedError,
	WorkflowTerminalError,
} from "./errors";
import type {
	RunSnapshot,
	RunStatus,
	StepStatus,
	WorkflowDef,
	WorkflowSignalOpts,
	WorkflowStartOpts,
} from "./types";

// gRPC status codes (numeric, as grpc-js reports them).
const GRPC_NOT_FOUND = 5;
const GRPC_PERMISSION_DENIED = 7;
const GRPC_FAILED_PRECONDITION = 9;

interface GrpcLikeError {
	code?: number;
	details?: string;
	message: string;
}

// Sink for call-time policy denials (emits `policy_violation`).
type PolicyViolationSink = (v: {
	declaration: string;
	value: string;
	denySide: string;
	reason: string;
}) => void;

// LocalDefinition is what the executor needs to run a local step.
// @internal
export interface LocalDefinition {
	version: string;
	locals: LocalFns;
}

export class WorkflowDomain {
	private rpc: WorkflowsClient | null = null;
	private readonly definitions = new Map<string, LocalDefinition>();

	constructor(
		private readonly registry: Registry,
		private readonly onPolicyViolation?: PolicyViolationSink,
	) {}

	// @internal — wired by ServiceBridge.start() once the gRPC channel is up.
	_attachRpc(rpc: WorkflowsClient): void {
		this.rpc = rpc;
	}

	// @internal — the executor's lookup of local functions.
	_definition(name: string): LocalDefinition | undefined {
		return this.definitions.get(name);
	}

	// @internal — number of declared workflows.
	_size(): number {
		return this.definitions.size;
	}

	// handle declares a workflow owned by this service. The runtime validates
	// the definition at registration and refuses an invalid one.
	handle(name: string, def: WorkflowDef): void {
		if (this.definitions.has(name))
			throw new Error(`workflow "${name}" is already declared`);
		const { definition, locals } = encodeDefinition(name, def);
		this.definitions.set(name, { version: definition.version, locals });
		this.registry._handle.workflow(name, definition);
	}

	// start creates a run of `service`'s workflow `name`.
	async start(
		service: string,
		name: string,
		input: unknown,
		opts?: WorkflowStartOpts,
	): Promise<{ runId: string }> {
		const rpc = this.requireRpc();
		const ctx = currentTraceContext();
		const xSbTrace = ctx ? formatXSbTrace(ctx.traceId, ctx.parentOpId) : "";
		const target = `${service}/${name}`;
		return new Promise((resolve, reject) => {
			rpc.start(
				{
					service,
					workflow: name,
					input: json(input),
					idempotencyKey: opts?.idempotencyKey ?? "",
					timeoutMs: opts?.timeoutMs ?? 0,
					xSbTrace,
				},
				(err, resp) => {
					if (err) return reject(this.mapError(target, err));
					resolve({ runId: resp.runId });
				},
			);
		});
	}

	// signal enqueues a signal for the run (FIFO). With `signalId`, a repeat of
	// the same id is accepted without enqueuing — `duplicate` reports it.
	async signal(
		runId: string,
		signalName: string,
		payload: unknown,
		opts?: WorkflowSignalOpts,
	): Promise<{ duplicate: boolean }> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			rpc.signal(
				{
					runId,
					signalName,
					payload: json(payload),
					signalId: opts?.signalId ?? "",
				},
				(err, resp) => {
					if (err) return reject(this.mapError(runId, err));
					resolve({ duplicate: resp.duplicate });
				},
			);
		});
	}

	// cancel stops the run; its compensations run before it ends cancelled.
	async cancel(runId: string): Promise<void> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			rpc.cancel({ runId }, (err) => {
				if (err) return reject(this.mapError(runId, err));
				resolve();
			});
		});
	}

	// await resolves with the run output (the state map) once the run succeeds
	// and rejects with WorkflowRunFailedError on any other terminal status.
	async await(runId: string): Promise<Record<string, unknown>> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			const stream = rpc.await({ runId });
			let last: RunStatusUpdate | null = null;
			stream.on("data", (u: RunStatusUpdate) => {
				last = u;
			});
			stream.on("error", (err: GrpcLikeError) =>
				reject(this.mapError(runId, err)),
			);
			stream.on("end", () => {
				const u = last as RunStatusUpdate | null;
				if (!u?.terminal)
					return reject(
						new WorkflowTerminalError(
							runId,
							"await stream ended before a terminal status",
						),
					);
				if (u.status !== "success")
					return reject(
						new WorkflowRunFailedError(
							runId,
							u.status,
							u.errorCode,
							u.errorMessage,
						),
					);
				resolve((parse(u.output) ?? {}) as Record<string, unknown>);
			});
		});
	}

	// query returns a point-in-time snapshot of the run, its steps and the
	// signals still queued.
	async query(runId: string): Promise<RunSnapshot> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			rpc.query({ runId }, (err, resp) => {
				if (err) return reject(this.mapError(runId, err));
				resolve(toSnapshot(resp));
			});
		});
	}

	// replay starts a new run from the source's frozen definition and input;
	// with `fromStepId` (a top-level step) the steps that do not depend on it
	// are copied instead of re-executed.
	async replay(
		runId: string,
		opts?: { fromStepId?: string },
	): Promise<{ runId: string }> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			rpc.replay({ runId, fromStepId: opts?.fromStepId ?? "" }, (err, resp) => {
				if (err) return reject(this.mapError(runId, err));
				resolve({ runId: resp.runId });
			});
		});
	}

	// retryCompensation re-runs the failed compensations of a
	// failed_compensated run.
	async retryCompensation(runId: string): Promise<void> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			rpc.retryCompensation({ runId }, (err) => {
				if (err) return reject(this.mapError(runId, err));
				resolve();
			});
		});
	}

	private requireRpc(): WorkflowsClient {
		if (!this.rpc)
			throw new Error(
				"workflow: caller-side operations need ServiceBridge.start() to have completed",
			);
		return this.rpc;
	}

	private mapError(target: string, err: GrpcLikeError): Error {
		const detail = err.details || err.message;
		switch (err.code) {
			case GRPC_PERMISSION_DENIED:
				this.onPolicyViolation?.({
					declaration: "workflow.run",
					value: target,
					denySide: "self_egress",
					reason: detail,
				});
				return new WorkflowAccessDeniedError(target, detail);
			case GRPC_NOT_FOUND:
				return new WorkflowNotFoundError(target);
			case GRPC_FAILED_PRECONDITION:
				return new WorkflowTerminalError(target, detail);
		}
		return err as unknown as Error;
	}
}

function json(v: unknown): Buffer {
	return Buffer.from(JSON.stringify(v ?? null), "utf8");
}

function parse(b: Uint8Array | undefined): unknown {
	if (!b || b.length === 0) return null;
	return JSON.parse(Buffer.from(b).toString("utf8"));
}

function toSnapshot(r: PbRunSnapshot): RunSnapshot {
	return {
		runId: r.runId,
		service: r.service,
		workflow: r.workflow,
		status: r.status as RunStatus,
		stopReason: r.stopReason,
		waitingReason: r.waitingReason,
		input: parse(r.input),
		output: parse(r.output) as Record<string, unknown> | null,
		errorCode: r.errorCode,
		errorMessage: r.errorMessage,
		parentRunId: r.parentRunId,
		startedAtMs: r.startedAtUnixMs,
		endedAtMs: r.endedAtUnixMs,
		steps: r.steps.map((s) => ({
			stepId: s.stepId,
			parentStepId: s.parentStepId,
			kind: s.kind,
			status: s.status as StepStatus,
			attempt: s.attempt,
			output: parse(s.output),
			errorCode: s.errorCode,
			errorMessage: s.errorMessage,
			waitingReason: s.waitingReason,
			waitKey: s.waitKey,
			childRunId: s.childRunId,
			compensatesStepId: s.compensatesStepId,
			startedAtMs: s.startedAtUnixMs,
			endedAtMs: s.endedAtUnixMs,
		})),
		signals: r.signals.map((s) => ({
			signalName: s.signalName,
			signalId: s.signalId,
			payload: parse(s.payload),
			enqueuedAtMs: s.enqueuedAtUnixMs,
		})),
	};
}
