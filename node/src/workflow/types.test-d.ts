// Compile-time type assertions for ADR-W-018 — workflow step opts MUST
// contain every key of the owner-module opts type. Adding a new field to
// CallOpts/PublishOpts/WorkflowStartOpts without updating TemplatableOpts<T>
// infrastructure → compile fail here.
//
// Run with: `bun x tsc --noEmit src/workflow/types.test-d.ts` (or via the
// project's `bun run check`, which already type-checks the whole src tree).

import type { PublishOpts } from "../events/publisher";
import type { CallOpts } from "../rpc/client";
import type {
	CallStep,
	PublishStep,
	WorkflowStartOpts,
	WorkflowStep,
} from "./types";

// Helper — strip optional modifier so that `keyof _Required<T>` enumerates
// every declared key regardless of `?:` annotations.
type _Required<T> = { [K in keyof T]-?: T[K] };

// keyof CallOpts MUST be ⊆ keyof CallStep["opts"].
type _CallAssert = keyof _Required<CallOpts> extends keyof _Required<
	NonNullable<CallStep["opts"]>
>
	? true
	: never;
const _callOk: _CallAssert = true;
void _callOk;

type _PublishAssert = keyof _Required<PublishOpts> extends keyof _Required<
	NonNullable<PublishStep["opts"]>
>
	? true
	: never;
const _publishOk: _PublishAssert = true;
void _publishOk;

type _WorkflowAssert =
	keyof _Required<WorkflowStartOpts> extends keyof _Required<
		NonNullable<WorkflowStep["opts"]>
	>
		? true
		: never;
const _workflowOk: _WorkflowAssert = true;
void _workflowOk;
