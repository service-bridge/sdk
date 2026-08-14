// Owner-side workflow subscriber: consume `Workflows.Subscribe`, hand each
// RunAssignment to the thin runner, manage lease heartbeats.
//
// Stream lifecycle (open / identity guard / reconnect ladder / cancellation)
// belongs to registry/StreamSupervisor — same machine as events and job
// subscribers.
//
// Telemetry (ADR 0003 §1): the runtime owns the WORKFLOW.RUN root op. The SDK wraps
// dispatch in runWithTrace(parent = run root) to establish the run-root trace
// scope; the runner then opens one USER.SUBOP step span per executed unit
// (step / fanout group / branch / compensation) inside that scope, so nested
// rpc/event/sub-workflow ops nest under their step span. Cross-RPC/EVENT
// propagation rides X-SB-Trace through `a.xSbTrace`.
//
// @internal — см. ./README.md

import type { ClientReadableStream } from "@grpc/grpc-js";
import type { ServiceBridge } from "../connection/service-bridge";
import type { Logger } from "../events/publisher";
import type {
	RunAssignment,
	WorkflowsClient,
} from "../pb/servicebridge/v1/workflows";
import { StreamSupervisor } from "../registry/stream-supervisor";
import { runWithTrace } from "../telemetry/context";
import { parseXSbTrace } from "../telemetry/wire-trace";
import type { ReconnectDelayOptions } from "../utils/reconnect-ladder";
import { type RunnerDeps, RunnerParkedError, run } from "./runner";
import type { Step } from "./types";

const HEARTBEAT_INTERVAL_MS = 10_000;

// SubscriberIdentity is the session identity the subscriber binds its stream
// and its lease heartbeats to. Resolved per call — `Control.RefreshCert` mints
// a fresh instance_id on every rotation.
// @internal
export interface SubscriberIdentity {
	serviceId: string;
	instanceId: string;
}

interface SubscriberDeps {
	rpc: WorkflowsClient;
	identity: () => SubscriberIdentity | null;
	deps: RunnerDeps;
	logger: Logger;
	// lookupLocalGraph resolves a workflow name to the locally-registered
	// `Step[]` (with `local.fn` retained — closures cannot survive
	// JSON-roundtrip via the wire `frozenPlan`). Returns null when the SDK
	// has no handler for that name.
	lookupLocalGraph: (workflowName: string) => Step[] | null;
	sb?: ServiceBridge;
	// reconnectOpts pins the backoff ladder/jitter; tests inject a short
	// deterministic ladder so reconnect behaviour is observable in milliseconds.
	// @internal
	reconnectOpts?: ReconnectDelayOptions;
	// onSchedule observes each reconnect delay. See StreamSupervisorDeps.
	onSchedule?: (delayMs: number) => void;
}

// FrozenPlan — on-wire JSON shape produced by `WorkflowDomain.handle` and
// echoed back in RunAssignment.frozenPlan. ADR-W-002.
interface FrozenPlan {
	graph: Step[];
	maxParallelism?: number;
}

export class WorkflowSubscriber {
	private closed = false;
	private readonly supervisor: StreamSupervisor<
		ClientReadableStream<RunAssignment>,
		RunAssignment
	>;
	// Leases in flight, keyed by runId. leaseEpoch pins the assignment a lease
	// belongs to: a re-assignment of the same runId bumps the epoch, and only
	// the matching exec may retire it.
	private readonly leases = new Map<string, number>();
	// One sweep timer drives every lease. A timer per run turns 500 concurrent
	// runs into 500 timers, all doing the same thing on the same period.
	private tickTimer: ReturnType<typeof setInterval> | null = null;
	// instanceId the live stream was opened with. Cert rotation mints a new one;
	// a stream still carrying the retired id keeps the runtime assigning runs to
	// an instance it has already torn down.
	private openedInstanceId: string | null = null;

	constructor(private readonly d: SubscriberDeps) {
		this.supervisor = new StreamSupervisor({
			open: () => this.openStream(),
			onData: (a) => {
				void this.dispatch(a).catch((err) => {
					this.d.logger.error(
						`workflow run ${a.runId} dispatch failed: ${(err as Error).message}`,
					);
				});
			},
			onError: (err) =>
				this.d.logger.warn("workflow subscriber: stream error", err.message),
			reconnectOpts: d.reconnectOpts,
			onSchedule: d.onSchedule,
		});
	}

	start(): void {
		this.closed = false;
		this.supervisor.start();
		const timer = setInterval(() => this.tick(), HEARTBEAT_INTERVAL_MS);
		// Heartbeats must not be the reason a finished process stays alive; the
		// reconnect timer (inside the supervisor) is the one that holds the loop.
		timer.unref();
		this.tickTimer = timer;
	}

	close(): void {
		this.closed = true;
		this.supervisor.stop();
		this.leases.clear();
		if (this.tickTimer !== null) {
			clearInterval(this.tickTimer);
			this.tickTimer = null;
		}
	}

	private openStream(): ClientReadableStream<RunAssignment> | null {
		const id = this.d.identity();
		if (!id) return null;
		this.openedInstanceId = id.instanceId;
		return this.d.rpc.subscribe({
			serviceId: id.serviceId,
			instanceId: id.instanceId,
		});
	}

	// tick reconciles identity and sends one heartbeat per live lease.
	private tick(): void {
		if (this.closed) return;
		const id = this.d.identity();
		if (!id) return;
		if (
			this.openedInstanceId !== null &&
			this.openedInstanceId !== id.instanceId
		) {
			this.supervisor.restart();
		}
		for (const [runId, leaseEpoch] of this.leases) {
			try {
				this.d.rpc.heartbeat(
					{ runId, instanceId: id.instanceId, leaseEpoch },
					(err) => {
						if (err) {
							this.d.logger.warn(
								`workflow heartbeat ${runId} failed: ${err.message}`,
							);
						}
					},
				);
			} catch (err) {
				// A synchronous throw means the channel is mid-teardown. Transient —
				// dropping the lease here would silently strand the run until the
				// runtime reclaims it, with nothing in the logs to explain why.
				this.d.logger.warn(
					`workflow heartbeat ${runId} threw: ${(err as Error).message}`,
				);
			}
		}
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

		// ADR 0006 §3/ADR 0003 §1: trace context arrives in `a.xSbTrace` formatted as
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
				this.stopHeartbeat(a.runId, a.leaseEpoch);
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
		this.leases.set(runId, leaseEpoch);
	}

	// stopHeartbeat retires a lease only if the caller still owns it. A run
	// re-assigned while the previous exec is still unwinding bumps the epoch;
	// without this check the old exec's `finally` kills the new assignment's
	// heartbeat, the lease expires, the run is re-assigned again — livelock.
	private stopHeartbeat(runId: string, leaseEpoch: number): void {
		if (this.leases.get(runId) !== leaseEpoch) return;
		this.leases.delete(runId);
	}
}
