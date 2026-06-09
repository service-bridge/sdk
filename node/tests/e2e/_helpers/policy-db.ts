// policy-db.ts — shared Bun.sql helpers for access-policy e2e tests.
//
// Tests seed `service_policy_rules` rows / toggle `cap_*` flags in beforeEach,
// then clean up in afterEach. Runtime picks the changes up via NOTIFY (<1s)
// + 30s poll fallback. All helpers use parameterised tagged templates — no
// string interpolation into SQL.

const PG_URL =
	process.env.TEST_DATABASE_URL ??
	"postgresql://servicebridge:servicebridge@localhost:5433/servicebridge";

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

export async function serviceID(name: string): Promise<string> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT id FROM services
			WHERE name = ${name}
			ORDER BY created_at DESC LIMIT 1
		`) as Array<{ id: string }>;
		if (!rows[0]?.id) throw new Error(`service ${name} not in DB`);
		return String(rows[0].id);
	});
}

export async function allServiceIDs(name: string): Promise<string[]> {
	return withDb(async (sql) => {
		const rows = (await sql`
			SELECT id FROM services WHERE name = ${name}
		`) as Array<{ id: string }>;
		return rows.map((r) => String(r.id));
	});
}

export async function setCapability(
	svcID: string,
	column:
		| "cap_rpc_handle"
		| "cap_event_handle"
		| "cap_workflow_handle"
		| "cap_job_handle",
	value: boolean,
): Promise<void> {
	await withDb(async (sql) => {
		// column name is type-restricted at the TS layer — safe to inline.
		if (column === "cap_rpc_handle") {
			await sql`UPDATE services SET cap_rpc_handle = ${value} WHERE id = ${svcID}`;
		} else if (column === "cap_event_handle") {
			await sql`UPDATE services SET cap_event_handle = ${value} WHERE id = ${svcID}`;
		} else if (column === "cap_workflow_handle") {
			await sql`UPDATE services SET cap_workflow_handle = ${value} WHERE id = ${svcID}`;
		} else {
			await sql`UPDATE services SET cap_job_handle = ${value} WHERE id = ${svcID}`;
		}
	});
}

export type Direction = "E" | "A";
export type Action =
	| "rpc.call"
	| "workflow.run"
	| "event.publish"
	| "rpc.handle"
	| "workflow.handle"
	| "event.handle";

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

export async function clearRules(svcID: string): Promise<void> {
	await withDb(async (sql) => {
		await sql`DELETE FROM service_policy_rules WHERE service_id = ${svcID}`;
	});
}
