import { ServiceBridgeError } from "../errors";

// Workflow-domain errors surfaced to callers.
//
// @public — см. ./README.md

// WorkflowAccessDeniedError — the runtime refused the operation by policy:
// Start (bilateral workflow.run / workflow.handle) or a run operation by a
// caller that is neither the owner, the starter nor explicitly granted.
export class WorkflowAccessDeniedError extends ServiceBridgeError {
	constructor(
		public readonly target: string,
		public readonly reason: string,
	) {
		super(`workflow ${target}: access denied — ${reason}`);
		this.name = "WorkflowAccessDeniedError";
	}
}

// WorkflowNotFoundError — no such workflow (Start) or run.
export class WorkflowNotFoundError extends ServiceBridgeError {
	constructor(public readonly target: string) {
		super(`workflow ${target}: not found`);
		this.name = "WorkflowNotFoundError";
	}
}

// WorkflowTerminalError — the operation needs a run that is not terminal
// (signal, cancel) or in a specific state (retryCompensation).
export class WorkflowTerminalError extends ServiceBridgeError {
	constructor(
		public readonly runId: string,
		public readonly detail: string,
	) {
		super(`workflow run ${runId}: ${detail}`);
		this.name = "WorkflowTerminalError";
	}
}

// WorkflowRunFailedError — await() on a run that ended other than success.
export class WorkflowRunFailedError extends ServiceBridgeError {
	constructor(
		public readonly runId: string,
		public readonly status: string,
		public readonly errorCode: string,
		public readonly errorMessage: string,
	) {
		super(
			`workflow run ${runId} ended ${status}: ${errorCode} ${errorMessage}`.trim(),
		);
		this.name = "WorkflowRunFailedError";
	}
}
