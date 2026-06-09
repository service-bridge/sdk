// Workflow-domain error classes surfaced to callers.
//
// @public — см. ./README.md

// WorkflowAccessDeniedError — gate #5 bilateral check failed on workflow.Start
// (ADR-0014 / ADR-W-016). Maps from gRPC PERMISSION_DENIED.
export class WorkflowAccessDeniedError extends Error {
	constructor(
		public readonly workflowName: string,
		public readonly reason: string,
	) {
		super(`workflow.start("${workflowName}"): access denied — ${reason}`);
		this.name = "WorkflowAccessDeniedError";
	}
}

// WorkflowNotFoundError — runtime has no definition with that name (after
// fingerprint resolution).
export class WorkflowNotFoundError extends Error {
	constructor(public readonly workflowName: string) {
		super(`workflow.start("${workflowName}"): not found`);
		this.name = "WorkflowNotFoundError";
	}
}

// WorkflowTerminalError — Signal/Cancel attempted against a run already in a
// terminal state.
export class WorkflowTerminalError extends Error {
	constructor(
		public readonly runId: string,
		public readonly status: string,
	) {
		super(`workflow run ${runId}: already terminal (${status})`);
		this.name = "WorkflowTerminalError";
	}
}
