// Declarative reverse action attached to `call` or `publish` workflow steps.
// Per ADR-W-007. Owned by the workflow module — compensation is a workflow
// construct, not an RPC option.
//
// @public — см. ./README.md

import type { RetryOpts } from "../rpc/client";
import type { JsonExpression } from "./jsonpath";

export interface CompensateSpec {
	type?: "call" | "publish";
	service?: string;
	method?: string;
	event?: string;
	input: JsonExpression;
	retry?: Partial<RetryOpts>;
	idempotencyKey?: string;
}
