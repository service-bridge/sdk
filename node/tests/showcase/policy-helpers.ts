// policy-helpers.ts — minimal policy seeding for the showcase fixture.
//
// Same SQL surface as tests/e2e/_helpers/policy-db.ts but extracted to a local
// file so the showcase has no implicit dependency on the e2e harness layout.
// Showcase rules are wide and stable for the whole run — we never clean them
// up between scenarios; the goal is to leave traces in the DB for the owner to
// inspect after the run completes.

const PG_URL =
	process.env.TEST_DATABASE_URL ??
	"postgresql://servicebridge:servicebridge@localhost:5433/service-bridge";

export async function withDb<T>(
	fn: (sql: typeof import("bun").sql) => Promise<T>,
): Promise<T> {
	const prev = process.env.DATABASE_URL;
	process.env.DATABASE_URL = PG_URL;
	try {
		const { sql } = await import("bun");
		return await fn(sql);
	} finally {
		if (prev === undefined) delete process.env.DATABASE_URL;
		else process.env.DATABASE_URL = prev;
	}
}

export type Direction = "E" | "A";
export type Action =
	| "rpc.call"
	| "rpc.handle"
	| "event.publish"
	| "event.handle"
	| "workflow.run"
	| "workflow.handle";

export async function addRule(
	svcID: string,
	direction: Direction,
	action: Action,
	peerService: string | null,
	targetName: string,
): Promise<void> {
	await withDb(async (sql) => {
		if (peerService === null) {
			await sql`
				INSERT INTO service_policy_rules
					(service_id, direction, action, peer_service, target_name)
				VALUES (${svcID}, ${direction}, ${action}, NULL, ${targetName})
				ON CONFLICT DO NOTHING
			`;
		} else {
			await sql`
				INSERT INTO service_policy_rules
					(service_id, direction, action, peer_service, target_name)
				VALUES (${svcID}, ${direction}, ${action}, ${peerService}::uuid, ${targetName})
				ON CONFLICT DO NOTHING
			`;
		}
	});
}

/**
 * grantWildcardCallable: caller can call any method on any peer (E rpc.call * → *),
 * and the named provider accepts any caller for that method (A rpc.handle *).
 * Cheap blanket seed for fixture use — the showcase isn't testing the policy
 * surface, just exercising the channels.
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
