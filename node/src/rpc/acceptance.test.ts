import { describe, expect, it } from "bun:test";
import type {
	PolicyEvaluation,
	PolicyRule,
} from "../pb/servicebridge/v1/registry";
import {
	checkAcceptance,
	evaluatePeerAcceptance,
	extractSpiffeServiceId,
	getPeerCertFromCall,
	parsePeerSpiffeUri,
} from "./acceptance";

function makeCallWithSpiffeSan(serviceId: string, instanceId = "i-1"): object {
	const altName = `URI:spiffe://service-bridge/service/${serviceId}/instance/${instanceId}`;
	return {
		call: {
			handler: {
				session: {
					socket: {
						getPeerCertificate: () => ({ subjectaltname: altName }),
					},
				},
			},
		},
	};
}

// ── parsePeerSpiffeUri ───────────────────────────────────────────────────────

describe("parsePeerSpiffeUri", () => {
	it("parses valid SPIFFE URI", () => {
		const result = parsePeerSpiffeUri(
			"spiffe://service-bridge/service/aaaaaaaa-0000-0000-0000-000000000001/instance/bbbbbbbb-0000-0000-0000-000000000002",
		);
		expect(result).toEqual({
			serviceId: "aaaaaaaa-0000-0000-0000-000000000001",
			instanceId: "bbbbbbbb-0000-0000-0000-000000000002",
		});
	});

	it("returns null for non-SPIFFE URI", () => {
		expect(parsePeerSpiffeUri("https://example.com")).toBeNull();
	});

	it("returns null for SPIFFE URI with wrong structure", () => {
		expect(parsePeerSpiffeUri("spiffe://service-bridge/foo/bar")).toBeNull();
	});
});

// ── extractSpiffeServiceId ───────────────────────────────────────────────────

describe("extractSpiffeServiceId", () => {
	it("extracts serviceId from valid subjectaltname", () => {
		const fakeAltName =
			"URI:spiffe://service-bridge/service/aaaaaaaa-0000-0000-0000-000000000001/instance/i-1";
		const id = extractSpiffeServiceId({ subjectaltname: fakeAltName });
		expect(id).toBe("aaaaaaaa-0000-0000-0000-000000000001");
	});

	it("returns null when no URI SAN", () => {
		expect(
			extractSpiffeServiceId({ subjectaltname: "DNS:foo.local" }),
		).toBeNull();
	});

	it("returns null when cert has no subjectaltname", () => {
		expect(extractSpiffeServiceId({})).toBeNull();
	});
});

// ── checkAcceptance ──────────────────────────────────────────────────────────

function makeAcceptanceRule(
	peerServiceId: string,
	targetName: string,
): PolicyRule {
	return {
		action: "rpc.handle",
		peerServiceId,
		peerServiceName: "",
		targetName,
	};
}

const callerA = "aaaaaaaa-0000-0000-0000-000000000001";
const callerB = "bbbbbbbb-0000-0000-0000-000000000002";

describe("checkAcceptance", () => {
	it("default-allow when no acceptance rules", () => {
		const policy: PolicyEvaluation = {
			capabilities: [],
			egress: [],
			acceptance: [],
			warnings: [],
		};
		expect(checkAcceptance(callerA, "charge", policy)).toBe(true);
	});

	it("allows matching peer + exact method", () => {
		const policy: PolicyEvaluation = {
			capabilities: [],
			egress: [],
			acceptance: [makeAcceptanceRule(callerA, "charge")],
			warnings: [],
		};
		expect(checkAcceptance(callerA, "charge", policy)).toBe(true);
	});

	it("allows matching peer + wildcard method", () => {
		const policy: PolicyEvaluation = {
			capabilities: [],
			egress: [],
			acceptance: [makeAcceptanceRule(callerA, "*")],
			warnings: [],
		};
		expect(checkAcceptance(callerA, "anything", policy)).toBe(true);
	});

	it("denies unlisted peer", () => {
		const policy: PolicyEvaluation = {
			capabilities: [],
			egress: [],
			acceptance: [makeAcceptanceRule(callerA, "charge")],
			warnings: [],
		};
		expect(checkAcceptance(callerB, "charge", policy)).toBe(false);
	});

	it("denies listed peer with wrong method", () => {
		const policy: PolicyEvaluation = {
			capabilities: [],
			egress: [],
			acceptance: [makeAcceptanceRule(callerA, "charge")],
			warnings: [],
		};
		expect(checkAcceptance(callerA, "refund", policy)).toBe(false);
	});

	it("allows when peer matches a wildcard-peer rule (empty peerServiceId)", () => {
		const policy: PolicyEvaluation = {
			capabilities: [],
			egress: [],
			acceptance: [makeAcceptanceRule("", "charge")],
			warnings: [],
		};
		expect(checkAcceptance(callerB, "charge", policy)).toBe(true);
	});
});

// ── getPeerCertFromCall ──────────────────────────────────────────────────────

describe("getPeerCertFromCall", () => {
	it("walks the private grpc-js path and returns the cert", () => {
		const call = makeCallWithSpiffeSan(callerA);
		const cert = getPeerCertFromCall(call);
		expect(cert).not.toBeNull();
		expect(cert?.subjectaltname).toContain("spiffe://service-bridge/");
	});

	it("returns null when accessor is missing", () => {
		expect(getPeerCertFromCall({})).toBeNull();
		expect(getPeerCertFromCall({ call: { handler: {} } })).toBeNull();
	});

	it("returns null when getPeerCertificate is not a function", () => {
		const broken = {
			call: { handler: { session: { socket: { getPeerCertificate: 42 } } } },
		};
		expect(getPeerCertFromCall(broken)).toBeNull();
	});

	it("returns null when getPeerCertificate throws", () => {
		const throwing = {
			call: {
				handler: {
					session: {
						socket: {
							getPeerCertificate: () => {
								throw new Error("socket closed");
							},
						},
					},
				},
			},
		};
		expect(getPeerCertFromCall(throwing)).toBeNull();
	});
});

// ── evaluatePeerAcceptance ───────────────────────────────────────────────────

describe("evaluatePeerAcceptance", () => {
	const emptyPolicy: PolicyEvaluation = {
		capabilities: [],
		egress: [],
		acceptance: [],
		warnings: [],
	};
	const restrictivePolicy: PolicyEvaluation = {
		capabilities: [],
		egress: [],
		acceptance: [
			{
				action: "rpc.handle",
				peerServiceId: callerA,
				peerServiceName: "",
				targetName: "charge",
			},
		],
		warnings: [],
	};

	it("fail-closed when peer-cert accessor is broken", () => {
		const denial = evaluatePeerAcceptance(
			false,
			restrictivePolicy,
			makeCallWithSpiffeSan(callerA),
			"charge",
		);
		expect(denial).toContain("fail-closed");
	});

	it("default-allow when policy is null", () => {
		expect(
			evaluatePeerAcceptance(true, null, makeCallWithSpiffeSan(callerA), "x"),
		).toBeNull();
	});

	it("default-allow when policy has no rpc.handle rules", () => {
		expect(
			evaluatePeerAcceptance(
				true,
				emptyPolicy,
				makeCallWithSpiffeSan(callerA),
				"x",
			),
		).toBeNull();
	});

	it("denies when cert is unreachable", () => {
		const denial = evaluatePeerAcceptance(
			true,
			restrictivePolicy,
			{},
			"charge",
		);
		expect(denial).toContain("could not extract peer certificate");
	});

	it("allows when peer is the runtime (no SPIFFE service URI)", () => {
		// Proxy path: peer cert is the runtime's server cert with CN only.
		// Runtime already enforced gate #3 — defer to it.
		const call = {
			call: {
				handler: {
					session: {
						socket: { getPeerCertificate: () => ({ subjectaltname: "DNS:x" }) },
					},
				},
			},
		};
		expect(
			evaluatePeerAcceptance(true, restrictivePolicy, call, "charge"),
		).toBeNull();
	});

	it("denies when acceptance rules reject the caller", () => {
		const denial = evaluatePeerAcceptance(
			true,
			restrictivePolicy,
			makeCallWithSpiffeSan(callerB),
			"charge",
		);
		expect(denial).toContain("acceptance denied");
		expect(denial).toContain(callerB);
	});

	it("allows when acceptance rules accept the caller", () => {
		expect(
			evaluatePeerAcceptance(
				true,
				restrictivePolicy,
				makeCallWithSpiffeSan(callerA),
				"charge",
			),
		).toBeNull();
	});
});
