import "reflect-metadata";
import * as grpc from "@grpc/grpc-js";
import * as x509 from "@peculiar/x509";
import { BootstrapClient } from "../pb/servicebridge/v1/bootstrap";
import type { ControlClient } from "../pb/servicebridge/v1/control";
import type { BootstrapKey } from "./key";
import { derToPem } from "./pem";
import { ConnectionError } from "./service-bridge-error";

/** @internal см. ./README.md */
export interface Keypair {
	privateKey: CryptoKey;
	publicKey: CryptoKey;
	csrDer: Uint8Array;
}

export interface ProvisionResult {
	certDer: Buffer;
	caChainDer: Buffer;
	serviceId: string;
	serviceName: string;
	instanceId: string;
	notAfterUnix: bigint;
	privateKey: CryptoKey;
	privateKeyDer: Buffer; // PKCS#8 DER for sync mTLS credential construction
}

/** @internal см. ./README.md */
export function parseURL(url: string): { host: string; port: number } {
	const idx = url.lastIndexOf(":");
	if (idx < 0) throw new Error(`invalid URL (host:port required): ${url}`);
	const host = url.slice(0, idx);
	const port = Number(url.slice(idx + 1));
	if (!Number.isFinite(port) || port <= 0 || port > 65535) {
		throw new Error(`invalid port in URL: ${url}`);
	}
	return { host, port };
}

/** @internal см. ./README.md */
export async function generateKeypairAndCSR(): Promise<Keypair> {
	const keyPair = await crypto.subtle.generateKey(
		{ name: "ECDSA", namedCurve: "P-256" },
		true,
		["sign", "verify"],
	);

	const csr = await x509.Pkcs10CertificateRequestGenerator.create({
		name: "CN=servicebridge-instance",
		keys: keyPair,
		signingAlgorithm: { name: "ECDSA", hash: "SHA-256" },
	});

	return {
		privateKey: keyPair.privateKey,
		publicKey: keyPair.publicKey,
		csrDer: new Uint8Array(csr.rawData),
	};
}

/** @internal см. ./README.md */
export function buildPinnedCredentials(
	caCertDer: Buffer,
): grpc.ChannelCredentials {
	const caPem = derToPem(caCertDer);
	const verifyOptions: Parameters<typeof grpc.credentials.createSsl>[3] = {
		checkServerIdentity: () => undefined, // hostname check disabled, chain is enough
	};
	return grpc.credentials.createSsl(caPem, null, null, verifyOptions);
}

/** @internal см. ./README.md */
export function newBootstrapClient(
	url: string,
	caCertDer: Buffer,
): BootstrapClient {
	const creds = buildPinnedCredentials(caCertDer);
	return new BootstrapClient(url, creds);
}

/** @internal Factory for the bootstrap gRPC client; injectable so tests can
 * count channel creation/close without a live runtime. */
export type BootstrapClientFactory = (
	url: string,
	caCertDer: Buffer,
) => BootstrapClient;

/**
 * Runs the full Provision flow:
 *   1. Use the CA cert embedded in the bootstrap key as the trusted root.
 *   2. Generate keypair + CSR.
 *   3. Call Bootstrap.Provision over a gRPC channel rooted at that CA.
 *
 * The bootstrap channel is single-use: it is closed in `finally` on every path
 * (success, gRPC error, empty response). Leaking it would keep a TLS channel
 * with its own backoff timers alive after every reconnect attempt — the exact
 * shape of the production OOM in a runtime-down reconnect storm.
 */
export async function provision(
	url: string,
	key: BootstrapKey,
	clientFactory: BootstrapClientFactory = newBootstrapClient,
): Promise<ProvisionResult> {
	parseURL(url); // validate format early — throws on garbage
	const { privateKey, csrDer } = await generateKeypairAndCSR();
	const privateKeyDer = Buffer.from(
		await crypto.subtle.exportKey("pkcs8", privateKey),
	);
	const client = clientFactory(url, key.caCertDer);

	try {
		return await new Promise<ProvisionResult>((resolve, reject) => {
			client.provision(
				{
					keyId: key.keyID,
					secret: key.secret,
					csrDer: Buffer.from(csrDer),
				},
				(err, response) => {
					if (err) {
						reject(new ConnectionError("provision", err));
						return;
					}
					if (!response) {
						reject(
							new Error("provision: empty response", {
								cause: new Error("gRPC returned null response"),
							}),
						);
						return;
					}
					resolve({
						certDer: Buffer.from(response.certDer),
						caChainDer: Buffer.from(response.caChainDer),
						serviceId: response.serviceId,
						serviceName: response.serviceName,
						instanceId: response.instanceId,
						notAfterUnix: BigInt(response.notAfterUnix),
						privateKey,
						privateKeyDer,
					});
				},
			);
		});
	} finally {
		client.close();
	}
}

/**
 * Reissues a leaf cert via Control.RefreshCert under the existing mTLS channel.
 * No argon2 — identity is proven by the live client cert. The runtime generates
 * a fresh instance_id (preserving overlap-rotation semantics); the CA cert
 * and parent service identity carry over from the previous provision.
 *
 * Caller must reuse the existing mTLS ControlClient — RefreshCert is in the
 * default-deny set and rejects unauthenticated channels.
 */
export async function refresh(
	client: ControlClient,
	previous: ProvisionResult,
): Promise<ProvisionResult> {
	const { privateKey, csrDer } = await generateKeypairAndCSR();
	const privateKeyDer = Buffer.from(
		await crypto.subtle.exportKey("pkcs8", privateKey),
	);

	return new Promise((resolve, reject) => {
		client.refreshCert({ csrDer: Buffer.from(csrDer) }, (err, response) => {
			if (err) {
				reject(new ConnectionError("refresh", err));
				return;
			}
			if (!response) {
				reject(
					new Error("refresh: empty response", {
						cause: new Error("gRPC returned null response"),
					}),
				);
				return;
			}
			resolve({
				certDer: Buffer.from(response.certDer),
				caChainDer: Buffer.from(response.caChainDer),
				serviceId: previous.serviceId,
				serviceName: previous.serviceName,
				instanceId: response.instanceId,
				notAfterUnix: BigInt(response.notAfterUnix),
				privateKey,
				privateKeyDer,
			});
		});
	});
}
