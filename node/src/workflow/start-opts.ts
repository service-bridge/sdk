// WorkflowStartOpts — caller-side options for `sb.workflow.start`. Owned by
// the workflow module per ADR-W-018; templatable into `WorkflowStep.opts`.
//
// @public — см. ./README.md
export interface WorkflowStartOpts {
	idempotencyKey?: string;
	timeoutSec?: number;
	// parentRunId — when set, runtime treats this start as a sub-workflow of
	// the named parent run and enforces dynamic cycle detection (ADR-W-019).
	// Set automatically by the workflow runner for `type: "workflow"` steps;
	// callers normally leave it undefined.
	parentRunId?: string;
}
