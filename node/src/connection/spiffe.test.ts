import { expect, test } from "bun:test";
import type { PeerCertificate } from "node:tls";
import { makeSpiffeCheck, RUNTIME_SPIFFE_URI } from "./spiffe";

test("runtime TLS requires exactly the runtime URI role", () => {
	const check = makeSpiffeCheck(RUNTIME_SPIFFE_URI);
	const cert = (subjectaltname: string) =>
		({ subjectaltname }) as PeerCertificate;
	expect(check("localhost", cert(`URI:${RUNTIME_SPIFFE_URI}`))).toBeUndefined();
	for (const san of [
		"",
		"DNS:runtime",
		"URI:spiffe://service-bridge/services/s/instances/i",
		`URI:${RUNTIME_SPIFFE_URI}, URI:${RUNTIME_SPIFFE_URI}`,
		"URI:spiffe://evil/runtime",
	]) {
		expect(check("localhost", cert(san))).toBeInstanceOf(Error);
	}
});
