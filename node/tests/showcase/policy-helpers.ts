// policy-helpers.ts — showcase-shaped grants on top of the shared policy SQL.
//
// The raw SQL surface (withDb, addRule) is the e2e one — a second copy drifted
// from it silently, so there is exactly one now. What is showcase-specific are
// the grants below: wide, stable for the whole run, never cleaned up between
// scenarios, because the point is to leave traces in the DB for the owner to
// inspect after the run completes.

import { addRule } from "../e2e/_helpers/policy-db";

export type { Action, Direction } from "../e2e/_helpers/policy-db";
export { withDb } from "../e2e/_helpers/policy-db";

/**
 * grantRpcWildcard: caller can call the method on the named provider, and the
 * provider accepts that caller for it. Cheap blanket seed for fixture use —
 * the showcase isn't testing the policy surface, just exercising the channels.
 */
export async function grantRpcWildcard(
	callerID: string,
	providerID: string,
	method: string,
): Promise<void> {
	await addRule(callerID, "E", "rpc.call", providerID, method);
	await addRule(providerID, "A", "rpc.handle", callerID, method);
}

export async function grantEventFlow(
	publisherID: string,
	subscriberID: string,
	eventName: string,
): Promise<void> {
	await addRule(publisherID, "E", "event.publish", null, eventName);
	await addRule(subscriberID, "A", "event.handle", null, eventName);
}

export async function grantWorkflowFlow(
	callerID: string,
	ownerID: string,
	wfName: string,
): Promise<void> {
	await addRule(callerID, "E", "workflow.run", ownerID, wfName);
	await addRule(ownerID, "A", "workflow.handle", callerID, wfName);
}
