// reflect-metadata must load before @peculiar/x509: it pulls in tsyringe, which
// hard-fails without the polyfill depending on module load order.
import "reflect-metadata";
import { beforeAll, describe, expect, it } from "bun:test";
import * as x509 from "@peculiar/x509";
import type {
	PolicyEvaluation,
	PolicyRule,
} from "../pb/servicebridge/v1/registry";
import {
	checkAcceptance,
	classifyPeer,
	evaluatePeerAcceptance,
	getPeerCertFromCall,
	parsePeerSpiffeUri,
	RUNTIME_PEER_COMMON_NAME,
} from "./acceptance";

const callerA = "aaaaaaaa-0000-0000-0000-000000000001";
const callerB = "bbbbbbbb-0000-0000-0000-000000000002";

// ── real certificate fixtures ────────────────────────────────────────────────
// The acceptance decision reads the DER, so the tests issue real certificates
// rather than hand-shaped objects. Anything less would not exercise the SAN
// parsing that the decision actually depends on.

interface CertFixture {
	raw: Uint8Array;
	subjectaltname?: string;
	subject: { CN?: string };
}

async function issueCert(
	commonName: string,
	uriSans: string[],
): Promise<CertFixture> {
	const alg = { name: "ECDSA", namedCurve: "P-256", hash: "SHA-256" };
	const keys = (await crypto.subtle.generateKey(alg, false, [
		"sign",
		"verify",
	])) as CryptoKeyPair;
	const extensions =
		uriSans.length > 0
			? [
					new x509.SubjectAlternativeNameExtension(
						uriSans.map((value) => ({ type: "url" as const, value })),
					),
				]
			: [];
	const cert = await x509.X509CertificateGenerator.createSelfSigned({
		name: `CN=${commonName}`,
		keys,
		extensions,
		signingAlgorithm: alg,
	});
	return {
		raw: new Uint8Array(cert.rawData),
		subjectaltname:
			uriSans.length > 0
				? uriSans.map((u) => `URI:${u}`).join(", ")
				: undefined,
		subject: { CN: commonName },
	};
}

function spiffeUri(serviceId: string, instanceId = "i-1"): string {
	return `spiffe://service-bridge/service/${serviceId}/instance/${instanceId}`;
}

let sdkLeafA: CertFixture;
let sdkLeafB: CertFixture;
let runtimeCert: CertFixture;
let foreignUriCert: CertFixture;
let noSanStrangerCert: CertFixture;

beforeAll(async () => {
	sdkLeafA = await issueCert("servicebridge-leaf", [spiffeUri(callerA)]);
	sdkLeafB = await issueCert("servicebridge-leaf", [spiffeUri(callerB)]);
	runtimeCert = await issueCert(RUNTIME_PEER_COMMON_NAME, []);
	foreignUriCert = await issueCert("servicebridge-leaf", [
		"https://attacker.example/service/x",
	]);
	noSanStrangerCert = await issueCert("some-other-service", []);
});

// makeCall builds the real grpc-js server-call shape:
// ServerUnaryCall.call → BaseServerInterceptingCall.stream → session.socket.
function makeCall(cert: CertFixture): object {
	return {
		call: {
			stream: {
				session: {
					socket: { getPeerCertificate: () => cert },
				},
			},
		},
	};
}

// makeAuthContextCall builds the public grpc-js accessor shape.
function makeAuthContextCall(cert: CertFixture): object {
	return {
		getAuthContext: () => ({
			transportSecurityType: "ssl",
			sslPeerCertificate: cert,
		}),
	};
}

// ── parsePeerSpiffeUri ───────────────────────────────────────────────────────

describe("parsePeerSpiffeUri", () => {
	it("parses valid SPIFFE URI", () => {
		const result = parsePeerSpiffeUri(spiffeUri(callerA, callerB));
		expect(result).toEqual({ serviceId: callerA, instanceId: callerB });
	});

	it("returns null for non-SPIFFE URI", () => {
		expect(parsePeerSpiffeUri("https://example.com")).toBeNull();
	});

	it("returns null for SPIFFE URI with wrong structure", () => {
		expect(parsePeerSpiffeUri("spiffe://service-bridge/foo/bar")).toBeNull();
	});

	it("returns null for a SPIFFE URI from another trust domain", () => {
		expect(
			parsePeerSpiffeUri("spiffe://other/service/svc-a/instance/i-1"),
		).toBeNull();
	});

	it("returns null when serviceId or instanceId is empty", () => {
		expect(
			parsePeerSpiffeUri("spiffe://service-bridge/service//instance/i-1"),
		).toBeNull();
		expect(
			parsePeerSpiffeUri("spiffe://service-bridge/service/svc-a/instance/"),
		).toBeNull();
	});
});

// ── classifyPeer ─────────────────────────────────────────────────────────────

describe("classifyPeer", () => {
	it("identifies an SDK peer from the DER SPIFFE URI SAN", () => {
		expect(classifyPeer(sdkLeafA)).toEqual({
			kind: "service",
			serviceId: callerA,
		});
	});

	it("identifies the SDK peer even when Node leaves subjectaltname empty", () => {
		// Node does this for certs whose SAN carries only URI entries — the DER
		// is what makes the identity recoverable.
		expect(
			classifyPeer({
				raw: sdkLeafA.raw,
				subject: { CN: "servicebridge-leaf" },
			}),
		).toEqual({ kind: "service", serviceId: callerA });
	});

	it("identifies the runtime by CN when the cert has no URI SAN", () => {
		expect(classifyPeer(runtimeCert)).toEqual({ kind: "runtime" });
	});

	it("rejects a cert whose URI SAN is not a ServiceBridge SPIFFE identity", () => {
		const peer = classifyPeer(foreignUriCert);
		expect(peer.kind).toBe("unknown");
		expect(peer.kind === "unknown" && peer.reason).toContain("URI SAN");
	});

	it("rejects a SAN-less cert that is not the runtime", () => {
		const peer = classifyPeer(noSanStrangerCert);
		expect(peer.kind).toBe("unknown");
	});

	it("rejects an unparseable DER instead of assuming the runtime", () => {
		const peer = classifyPeer({
			raw: new Uint8Array([1, 2, 3, 4]),
			subject: { CN: RUNTIME_PEER_COMMON_NAME },
		});
		expect(peer.kind).toBe("unknown");
		expect(peer.kind === "unknown" && peer.reason).toContain("unparseable");
	});

	it("rejects an empty cert object", () => {
		expect(classifyPeer({}).kind).toBe("unknown");
	});

	it("falls back to subjectaltname when no DER is present", () => {
		expect(
			classifyPeer({ subjectaltname: `URI:${spiffeUri(callerA)}` }),
		).toEqual({ kind: "service", serviceId: callerA });
	});

	it("reads the runtime CN from subject when no DER is present", () => {
		expect(classifyPeer({ subject: { CN: RUNTIME_PEER_COMMON_NAME } })).toEqual(
			{ kind: "runtime" },
		);
	});

	it("picks the SPIFFE URI out of a multi-entry SAN", () => {
		expect(
			classifyPeer({
				subjectaltname: `DNS:host.local, URI:${spiffeUri(callerB)}`,
			}),
		).toEqual({ kind: "service", serviceId: callerB });
	});

	it("does not treat a DNS-only SAN as the runtime when CN says otherwise", () => {
		expect(
			classifyPeer({
				subjectaltname: "DNS:host.local",
				subject: { CN: "servicebridge-leaf" },
			}).kind,
		).toBe("unknown");
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

function policyWith(rules: PolicyRule[]): PolicyEvaluation {
	return { capabilities: [], egress: [], acceptance: rules, warnings: [] };
}

describe("checkAcceptance", () => {
	it("default-allow when no acceptance rules", () => {
		expect(checkAcceptance(callerA, "charge", policyWith([]))).toBe(true);
	});

	it("allows matching peer + exact method", () => {
		const policy = policyWith([makeAcceptanceRule(callerA, "charge")]);
		expect(checkAcceptance(callerA, "charge", policy)).toBe(true);
	});

	it("allows matching peer + wildcard method", () => {
		const policy = policyWith([makeAcceptanceRule(callerA, "*")]);
		expect(checkAcceptance(callerA, "anything", policy)).toBe(true);
	});

	it("denies unlisted peer", () => {
		const policy = policyWith([makeAcceptanceRule(callerA, "charge")]);
		expect(checkAcceptance(callerB, "charge", policy)).toBe(false);
	});

	it("denies listed peer with wrong method", () => {
		const policy = policyWith([makeAcceptanceRule(callerA, "charge")]);
		expect(checkAcceptance(callerA, "refund", policy)).toBe(false);
	});

	it("allows when peer matches a wildcard-peer rule (empty peerServiceId)", () => {
		const policy = policyWith([makeAcceptanceRule("", "charge")]);
		expect(checkAcceptance(callerB, "charge", policy)).toBe(true);
	});

	it("ignores acceptance rules for other actions", () => {
		const policy = policyWith([
			{
				action: "event.subscribe",
				peerServiceId: callerA,
				peerServiceName: "",
				targetName: "charge",
			},
		]);
		expect(checkAcceptance(callerB, "charge", policy)).toBe(true);
	});
});

// ── getPeerCertFromCall ──────────────────────────────────────────────────────

describe("getPeerCertFromCall", () => {
	it("reads the cert through the public getAuthContext accessor", () => {
		const cert = getPeerCertFromCall(makeAuthContextCall(sdkLeafA));
		expect(cert?.raw).toEqual(sdkLeafA.raw);
	});

	it("walks call.stream.session.socket when getAuthContext is absent", () => {
		const cert = getPeerCertFromCall(makeCall(sdkLeafA));
		expect(cert?.raw).toEqual(sdkLeafA.raw);
	});

	it("walks a top-level stream.session.socket", () => {
		const cert = getPeerCertFromCall({
			stream: { session: { socket: { getPeerCertificate: () => sdkLeafA } } },
		});
		expect(cert?.raw).toEqual(sdkLeafA.raw);
	});

	it("falls through to the socket walk when getAuthContext yields no cert", () => {
		// grpc-js returns {} for a non-TLS socket.
		const call = {
			getAuthContext: () => ({}),
			call: {
				stream: {
					session: { socket: { getPeerCertificate: () => sdkLeafA } },
				},
			},
		};
		expect(getPeerCertFromCall(call)?.raw).toEqual(sdkLeafA.raw);
	});

	it("returns null when no accessor is present", () => {
		expect(getPeerCertFromCall({})).toBeNull();
		expect(getPeerCertFromCall({ call: {} })).toBeNull();
		expect(getPeerCertFromCall({ call: { stream: {} } })).toBeNull();
	});

	it("returns null when the session has no socket", () => {
		expect(
			getPeerCertFromCall({ call: { stream: { session: {} } } }),
		).toBeNull();
	});

	it("returns null when getPeerCertificate is not a function", () => {
		expect(
			getPeerCertFromCall({
				call: { stream: { session: { socket: { getPeerCertificate: 42 } } } },
			}),
		).toBeNull();
	});

	it("returns null when getPeerCertificate throws", () => {
		expect(
			getPeerCertFromCall({
				call: {
					stream: {
						session: {
							socket: {
								getPeerCertificate: () => {
									throw new Error("socket closed");
								},
							},
						},
					},
				},
			}),
		).toBeNull();
	});
});

// ── evaluatePeerAcceptance ───────────────────────────────────────────────────

describe("evaluatePeerAcceptance", () => {
	const restrictivePolicy = policyWith([makeAcceptanceRule(callerA, "charge")]);

	it("default-allow when policy is null", () => {
		expect(evaluatePeerAcceptance(null, makeCall(sdkLeafA), "x")).toBeNull();
	});

	it("default-allow when policy has no rpc.handle rules", () => {
		expect(
			evaluatePeerAcceptance(policyWith([]), makeCall(sdkLeafA), "x"),
		).toBeNull();
	});

	it("fail-closed when the peer certificate cannot be extracted", () => {
		const denial = evaluatePeerAcceptance(restrictivePolicy, {}, "charge");
		expect(denial).toContain("could not extract peer certificate");
	});

	it("fail-closed when the cert accessor throws", () => {
		const call = {
			getAuthContext: () => {
				throw new Error("no auth context");
			},
		};
		const denial = evaluatePeerAcceptance(restrictivePolicy, call, "charge");
		expect(denial).toContain("could not extract peer certificate");
	});

	it("fail-closed when the peer identity cannot be established", () => {
		const denial = evaluatePeerAcceptance(
			restrictivePolicy,
			makeCall(noSanStrangerCert),
			"charge",
		);
		expect(denial).toContain("peer identity not established");
	});

	it("fail-closed on an unresolvable URI SAN instead of assuming the runtime", () => {
		const denial = evaluatePeerAcceptance(
			restrictivePolicy,
			makeCall(foreignUriCert),
			"charge",
		);
		expect(denial).toContain("peer identity not established");
	});

	it("fail-closed on an unparseable peer certificate", () => {
		const denial = evaluatePeerAcceptance(
			restrictivePolicy,
			makeCall({
				raw: new Uint8Array([9, 9, 9]),
				subject: { CN: RUNTIME_PEER_COMMON_NAME },
			} as CertFixture),
			"charge",
		);
		expect(denial).toContain("unparseable");
	});

	it("allows the runtime proxy, which enforced gate #3 already", () => {
		expect(
			evaluatePeerAcceptance(
				restrictivePolicy,
				makeCall(runtimeCert),
				"charge",
			),
		).toBeNull();
	});

	it("denies when acceptance rules reject the caller", () => {
		const denial = evaluatePeerAcceptance(
			restrictivePolicy,
			makeCall(sdkLeafB),
			"charge",
		);
		expect(denial).toContain("acceptance denied");
		expect(denial).toContain(callerB);
	});

	it("denies an allowed caller invoking a method it may not call", () => {
		const denial = evaluatePeerAcceptance(
			restrictivePolicy,
			makeCall(sdkLeafA),
			"refund",
		);
		expect(denial).toContain("acceptance denied");
	});

	it("allows when acceptance rules accept the caller", () => {
		expect(
			evaluatePeerAcceptance(restrictivePolicy, makeCall(sdkLeafA), "charge"),
		).toBeNull();
	});

	it("allows through the public getAuthContext accessor too", () => {
		expect(
			evaluatePeerAcceptance(
				restrictivePolicy,
				makeAuthContextCall(sdkLeafA),
				"charge",
			),
		).toBeNull();
	});
});
