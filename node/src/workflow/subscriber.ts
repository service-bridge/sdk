// Owner-side workflow subscriber: long-poll `Workflows.Subscribe`, hand each
// RunAssignment to the thin runner, manage lease heartbeat.
//
// Mirrors `events/subscriber.ts` ladder semantics. Reconnect is fixed.
//
// Telemetry (T-022): the runtime owns the WORKFLOW.RUN root op. The SDK wraps
// dispatch in runWithTrace(parent = run root) to establish the run-root trace
// scope; the runner then opens one USER.SUBOP step span per executed unit
// (step / fanout group / branch / compensation) inside that scope, so nested
// rpc/event/sub-workflow ops nest under their step span. Cross-RPC/EVENT
// propagation rides X-SB-Trace through `a.xSbTrace`.
//
// @internal — см. ./README.md

import type { ClientReadableStream, ServiceError } from "@grpc/grpc-js";
import type { ServiceBridge } from "../connection/service-bridge";
import type { Logger } from "../events/publisher";
import type {
	RunAssignment,
	WorkflowsClient,
} from "../pb/servicebridge/v1/workflows";
import { runWithTrace } from "../telemetry/context";
import { parseXSbTrace } from "../telemetry/wire-trace";
import { type RunnerDeps, RunnerParkedError, run } from "./runner";
import type { Step } from "./types";

const RECONNECT_LADDER_MS = [1000, 5000, 15000, 30000, 60000] as const;
const HEARTBEAT_INTERVAL_MS = 10_000;

function nextDelay(attempt: number): number {
	const idx = Math.min(Math.max(attempt, 0), RECONNECT_LADDER_MS.length - 1);
	return RECONNECT_LADDER_MS[idx]!;
}

interface SubscriberDeps {
	rpc: WorkflowsClient;
	serviceId: string;
	instanceId: string;
	deps: RunnerDeps;
	logger: Logger;
	// lookupLocalGraph resolves a workflow name to the locally-registered
	// `Step[]` (with `local.fn` retained — closures cannot survive
	// JSON-roundtrip via the wire `frozenPlan`). Returns null when the SDK
	// has no handler for that name.
	lookupLocalGraph: (workflowName: string) => Step[] | null;
	sb?: ServiceBridge;
}

// FrozenPlan — on-wire JSON shape produced by `WorkflowDomain.handle` and
// echoed back in RunAssignment.frozenPlan. ADR-W-002.
interface FrozenPlan {
	graph: Step[];
	maxParallelism?: number;
}

export class WorkflowSubscriber {
	private stream: ClientReadableStream<RunAssignment> | null = null;
	private closed = false;
	private heartbeats = new Map<
		string,
		{ timer: NodeJS.Timeout; leaseEpoch: number }
	>();

	constructor(private readonly d: SubscriberDeps) {}

	start(): void {
		void this.connectLoop(0);
	}

	close(): void {
		this.closed = true;
		this.stream?.cancel();
		for (const t of this.heartbeats.values()) clearInterval(t.timer);
		this.heartbeats.clear();
	}

	private async connectLoop(attempt: number): Promise<void> {
		while (!this.closed) {
			try {
				await this.runOnce();
				attempt = 0;
			} catch (err) {
				if (this.closed) return;
				this.d.logger.warn(
					"workflow subscriber: stream error",
					(err as Error).message,
				);
			}
			const delay = nextDelay(attempt++);
			await new Promise((r) => setTimeout(r, delay));
		}
	}

	private runOnce(): Promise<void> {
		return new Promise((resolve, reject) => {
			const stream = this.d.rpc.subscribe({
				serviceId: this.d.serviceId,
				instanceId: this.d.instanceId,
			});
			this.stream = stream;

			stream.on("data", (a: RunAssignment) => {
				void this.dispatch(a).catch((err) => {
					this.d.logger.error(
						`workflow run ${a.runId} dispatch failed: ${(err as Error).message}`,
					);
				});
			});
			stream.on("error", (err: ServiceError) => reject(err));
			stream.on("end", () => resolve());
		});
	}

	private async dispatch(a: RunAssignment): Promise<void> {
		if (this.closed) return;
		let plan: FrozenPlan;
		try {
			plan = JSON.parse(Buffer.from(a.frozenPlan).toString("utf8"));
		} catch (err) {
			this.d.logger.error(
				`workflow run ${a.runId}: bad frozenPlan: ${(err as Error).message}`,
			);
			return;
		}
		// Replace the wire graph with the locally-registered one so that
		// `local.fn` closures (stripped by JSON.stringify on the registration
		// path) are restored. ADR-W-002: the wire frozenPlan is for runtime
		// canonicalization + fingerprint; the SDK runs against its own copy.
		const localGraph = this.d.lookupLocalGraph(a.workflowName);
		if (localGraph) {
			plan.graph = localGraph;
		}
		const inputJson =
			a.input.length > 0 ? Buffer.from(a.input).toString("utf8") : "null";
		const stateJson =
			a.state.length > 0 ? Buffer.from(a.state).toString("utf8") : "{}";
		const state: Record<string, unknown> = JSON.parse(stateJson);
		state.input = JSON.parse(inputJson);

		this.startHeartbeat(a.runId, a.leaseEpoch);

		// T-017/T-022: trace context arrives in `a.xSbTrace` formatted as
		// "<traceId>-<rootOpId>". Wrap plan execution in runWithTrace to seed the
		// run-root trace scope; the runner's per-step USER.SUBOP spans nest under
		// it so each step's child rpc/event/sub-workflow ops emit X-SB-Trace
		// pointing at their step span, not the run root. Without this seed the
		// child ops land in a fresh trace and the tree fragments.
		const parsed = parseXSbTrace(a.xSbTrace);
		const runnerDeps: typeof this.d.deps = this.d.deps;
		const exec = async () => {
			try {
				const finalState = await run(
					plan.graph,
					{
						runId: a.runId,
						leaseEpoch: a.leaseEpoch,
						state,
						compensating: a.compensating,
						maxParallelism: plan.maxParallelism ?? 0,
					},
					runnerDeps,
				);
				// All steps completed successfully — mark the run terminal.
				// terminalStatus is derived from the assignment's compensating flag and cancel reason:
				// - forward run completing → 'success'
				// - compensation completing after step_failure → 'failed_compensated'
				// - compensation completing after user_cancel → 'cancelled'
				let terminalStatus = "success";
				if (a.compensating) {
					terminalStatus =
						a.cancelReason === "step_failure"
							? "failed_compensated"
							: "cancelled";
				}
				await this.d.deps.ops.completeRun({
					runId: a.runId,
					finalState,
					leaseEpoch: a.leaseEpoch,
					terminalStatus,
				});
			} catch (err) {
				if (!(err instanceof RunnerParkedError)) {
					this.d.logger.warn(
						`workflow run ${a.runId}: ${(err as Error).message}`,
					);
				}
			} finally {
				this.stopHeartbeat(a.runId);
				// Telemetry: WORKFLOW.RUN END is emitted by the runtime when
				// CompleteRun/Cancel commits. SDK no longer owns root-op lifecycle.
			}
		};

		if (parsed) {
			await runWithTrace(
				{ traceId: parsed.traceId, parentOpId: parsed.parentOpId },
				exec,
			);
		} else {
			await exec();
		}
	}

	private startHeartbeat(runId: string, leaseEpoch: number): void {
		if (this.closed) return;
		const timer = setInterval(() => {
			if (this.closed) {
				clearInterval(timer);
				return;
			}
			try {
				this.d.rpc.heartbeat(
					{ runId, instanceId: this.d.instanceId, leaseEpoch },
					(err) => {
						if (err)
							this.d.logger.warn(
								`workflow heartbeat ${runId} failed: ${err.message}`,
							);
					},
				);
			} catch {
				clearInterval(timer);
				this.heartbeats.delete(runId);
			}
		}, HEARTBEAT_INTERVAL_MS);
		this.heartbeats.set(runId, { timer, leaseEpoch });
	}

	private stopHeartbeat(runId: string): void {
		const t = this.heartbeats.get(runId);
		if (t) {
			clearInterval(t.timer);
			this.heartbeats.delete(runId);
		}
	}
}
