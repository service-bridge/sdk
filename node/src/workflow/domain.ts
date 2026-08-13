// @public — см. ./README.md
import type {
	QueryRunResponse,
	RunStatusUpdate,
	WorkflowsClient,
} from "../pb/servicebridge/v1/workflows";
import type { Registry, WorkflowHandlerOpts } from "../registry/registry";
import { currentTraceContext } from "../telemetry/context";
import { formatXSbTrace } from "../telemetry/wire-trace";
import { canonicalize, fingerprint } from "./canonical";
import {
	WorkflowAccessDeniedError,
	WorkflowNotFoundError,
	WorkflowTerminalError,
} from "./errors";
import type { WorkflowDef, WorkflowStartOpts } from "./types";
import { validate } from "./validate";

export type { WorkflowDef } from "./types";

// CanonicalGraph is the on-wire shape that runtime persists in
// `workflow_definitions.graph` (ADR-W-002).
interface CanonicalGraph {
	graph: WorkflowDef["steps"];
	retry?: WorkflowDef["retry"];
	maxParallelism?: number;
	timeoutSec?: number;
	// Per ADR-W-009: the JSON Schema for run input lives inside the canonical
	// graph and is what the runtime stores in workflow_definitions.input_schema.
	inputSchema?: WorkflowDef["input"];
}

// gRPC status codes — match @grpc/grpc-js numeric values; kept numeric to
// avoid the dynamic import.
const GRPC_NOT_FOUND = 5;
const GRPC_PERMISSION_DENIED = 7;
const GRPC_FAILED_PRECONDITION = 9;

interface GrpcLikeError {
	code?: number;
	details?: string;
	message: string;
}

// Sink for call-time policy denials; the owner wires it to emit
// `policy_violation`. Structural type avoids importing from connection.
type PolicyViolationSink = (v: {
	declaration: string;
	value: string;
	denySide: string;
	reason: string;
}) => void;

export class WorkflowDomain {
	// rpc is set by ServiceBridge.start(); until then caller-side ops throw.
	private rpc: WorkflowsClient | null = null;

	constructor(
		private readonly registry: Registry,
		private readonly onPolicyViolation?: PolicyViolationSink,
	) {}

	// @internal — wired by ServiceBridge.start() once the gRPC channel is up.
	_attachRpc(rpc: WorkflowsClient): void {
		this.rpc = rpc;
	}

	handle(name: string, def: WorkflowDef, opts?: WorkflowHandlerOpts): void {
		validate(def, { workflowName: name });

		const inputSchema = opts?.input ?? def.input;
		const canonical: CanonicalGraph = {
			graph: def.steps,
			retry: def.retry,
			maxParallelism: def.maxParallelism,
			timeoutSec: def.timeoutSec,
			inputSchema,
		};
		const canonicalJson = canonicalize(canonical);
		const fp = fingerprint(canonical);
		const graphBuf = Buffer.from(canonicalJson, "utf8");

		this.registry._handle.workflow(
			name,
			def.steps,
			inputSchema ? { input: inputSchema } : undefined,
			graphBuf,
			fp,
		);
	}

	// start — schedule a new run. Per ADR-W-016 gate #5 PermissionDenied
	// surfaces as WorkflowAccessDeniedError.
	async start(
		name: string,
		input: unknown,
		opts?: WorkflowStartOpts,
	): Promise<{ runId: string }> {
		const rpc = this.requireRpc();
		// Trace context propagation (ADR 0006 §3 X-SB-Trace): when an active trace
		// context exists in ALS, format it into x_sb_trace so a nested workflow
		// run inherits the parent trace and root-op linkage. Empty string when
		// no scope is active — runtime mints a fresh root in that case.
		const ctx = currentTraceContext();
		const xSbTrace = ctx ? formatXSbTrace(ctx.traceId, ctx.parentOpId) : "";
		return new Promise((resolve, reject) => {
			rpc.start(
				{
					workflowName: name,
					input: Buffer.from(JSON.stringify(input ?? null), "utf8"),
					idempotencyKey: opts?.idempotencyKey ?? "",
					timeoutSec: opts?.timeoutSec ?? 0,
					parentRunId: opts?.parentRunId ?? "",
					xSbTrace,
				},
				(err: GrpcLikeError | null, resp: { runId: string }) => {
					if (err)
						return reject(mapStartError(name, err, this.onPolicyViolation));
					resolve({ runId: resp.runId });
				},
			);
		});
	}

	// signal — deliver an external signal to a parked wait_signal step.
	async signal(
		runId: string,
		signalName: string,
		payload: unknown,
	): Promise<void> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			rpc.signal(
				{
					runId,
					signalName,
					payload: Buffer.from(JSON.stringify(payload ?? null), "utf8"),
				},
				(err) => {
					if (err)
						return reject(mapRunError(runId, err, this.onPolicyViolation));
					resolve();
				},
			);
		});
	}

	// cancel — request cooperative cancellation; runtime moves the run to
	// `compensating` and dispatches compensation steps in reverse order.
	async cancel(runId: string): Promise<void> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			rpc.cancel({ runId }, (err) => {
				if (err) return reject(mapRunError(runId, err, this.onPolicyViolation));
				resolve();
			});
		});
	}

	// await — block until the run reaches a terminal status. Returns the
	// final state map as parsed JSON.
	async await(runId: string): Promise<Record<string, unknown>> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			const stream = rpc.await({ runId });
			let final: RunStatusUpdate | null = null;
			stream.on("data", (u: RunStatusUpdate) => {
				final = u;
			});
			stream.on("error", (err: GrpcLikeError) =>
				reject(mapRunError(runId, err, this.onPolicyViolation)),
			);
			stream.on("end", () => {
				if (!final) {
					return reject(
						new Error(`workflow.await(${runId}): stream ended with no update`),
					);
				}
				const f = final as RunStatusUpdate;
				const stateJson =
					f.state.length > 0 ? Buffer.from(f.state).toString("utf8") : "{}";
				if (f.status !== "success") {
					return reject(
						new Error(
							`workflow.await(${runId}): terminal status "${f.status}"`,
						),
					);
				}
				try {
					resolve(JSON.parse(stateJson) as Record<string, unknown>);
				} catch (parseErr) {
					reject(parseErr);
				}
			});
		});
	}

	// query — point-in-time snapshot of run status, state, and per-step info.
	async query(runId: string): Promise<{
		status: string;
		state: Record<string, unknown>;
		steps: Array<{
			stepId: string;
			status: string;
			output: unknown;
			lastError: string;
			compensatedBy?: string;
		}>;
	}> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			rpc.query({ runId }, (err, resp: QueryRunResponse) => {
				if (err) return reject(mapRunError(runId, err, this.onPolicyViolation));
				const stateJson =
					resp.state.length > 0
						? Buffer.from(resp.state).toString("utf8")
						: "{}";
				resolve({
					status: resp.status,
					state: JSON.parse(stateJson) as Record<string, unknown>,
					steps: resp.steps.map((s) => ({
						stepId: s.stepId,
						status: s.status,
						output:
							s.output.length > 0
								? JSON.parse(Buffer.from(s.output).toString("utf8"))
								: null,
						lastError: s.lastError,
						compensatedBy: s.compensatedBy || undefined,
					})),
				});
			});
		});
	}

	// replay — create a new run forked from `runId` at `fromStepId`. Reuses
	// the source frozen_plan and re-executes from the named step.
	async replay(
		runId: string,
		opts?: { fromStepId?: string },
	): Promise<{ runId: string }> {
		const rpc = this.requireRpc();
		return new Promise((resolve, reject) => {
			rpc.replay({ runId, fromStepId: opts?.fromStepId ?? "" }, (err, resp) => {
				if (err) return reject(mapRunError(runId, err, this.onPolicyViolation));
				resolve({ runId: resp.runId });
			});
		});
	}

	private requireRpc(): WorkflowsClient {
		if (!this.rpc) {
			throw new Error(
				"workflow: caller-side ops require ServiceBridge.start() to have completed (RPC channel not yet attached)",
			);
		}
		return this.rpc;
	}
}

function mapStartError(
	name: string,
	err: GrpcLikeError,
	onPolicyViolation?: PolicyViolationSink,
): Error {
	if (err.code === GRPC_PERMISSION_DENIED) {
		const reason = err.details ?? err.message;
		onPolicyViolation?.({
			declaration: "workflow.run",
			value: name,
			denySide: "self_egress",
			reason,
		});
		return new WorkflowAccessDeniedError(name, reason);
	}
	if (err.code === GRPC_NOT_FOUND) {
		return new WorkflowNotFoundError(name);
	}
	return err as unknown as Error;
}

function mapRunError(
	runId: string,
	err: GrpcLikeError,
	onPolicyViolation?: PolicyViolationSink,
): Error {
	if (err.code === GRPC_FAILED_PRECONDITION) {
		return new WorkflowTerminalError(runId, err.details ?? err.message);
	}
	if (err.code === GRPC_PERMISSION_DENIED) {
		const reason = err.details ?? err.message;
		onPolicyViolation?.({
			declaration: "workflow.run",
			value: runId,
			denySide: "self_egress",
			reason,
		});
		return new WorkflowAccessDeniedError(runId, reason);
	}
	return err as unknown as Error;
}
