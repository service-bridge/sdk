// TLS material of the live identity and the gRPC options every channel and the
// inbound server share. The instance_id is stable for the life of the process
// (a rotation only reissues the leaf), so a rotation must change the
// certificate new connections present without rebuilding a single channel or
// stream: in-flight jobs, deliveries and calls survive it.
//
// @internal — см. ./README.md

import type { PeerCertificate } from "node:tls";
import * as grpc from "@grpc/grpc-js";
import { derToPem } from "./pem";

/** DER material of one leaf, as Provision / RefreshCert return it. */
export interface LeafMaterial {
	caChainDer: Buffer;
	certDer: Buffer;
	privateKeyDer: Buffer;
}

type CaListener = (update: { caCertificate: Buffer } | null) => void;
type IdentityListener = (
	update: { certificate: Buffer; privateKey: Buffer } | null,
) => void;

// Keepalive of every SDK client channel. Matches the runtime's enforcement
// policy (MinTime 10s) and the SDK Call servers' (MinTime 20s): a ping every
// 30s, dead after 10s without the ack, also on idle connections so a half-open
// TCP connection is noticed before the next call needs it.
export const CLIENT_CHANNEL_OPTIONS: grpc.ChannelOptions = {
	"grpc.keepalive_time_ms": 30_000,
	"grpc.keepalive_timeout_ms": 10_000,
	"grpc.keepalive_permit_without_calls": 1,
};

/**
 * In-memory certificate provider (grpc-js CertificateProvider). Every client
 * channel is built from credentials bound to one store, so `update()` changes
 * what the NEXT TLS handshake of every channel presents; established
 * connections and their streams keep running.
 */
export class CertificateStore {
	private ca: Buffer;
	private cert: Buffer;
	private key: Buffer;
	private readonly caListeners = new Set<CaListener>();
	private readonly identityListeners = new Set<IdentityListener>();

	constructor(material: LeafMaterial) {
		this.ca = derToPem(material.caChainDer, "CERTIFICATE");
		this.cert = derToPem(material.certDer, "CERTIFICATE");
		this.key = derToPem(material.privateKeyDer, "PRIVATE KEY");
	}

	/** Replace the leaf for every future handshake. */
	update(material: LeafMaterial): void {
		this.ca = derToPem(material.caChainDer, "CERTIFICATE");
		this.cert = derToPem(material.certDer, "CERTIFICATE");
		this.key = derToPem(material.privateKeyDer, "PRIVATE KEY");
		for (const l of this.caListeners) l({ caCertificate: this.ca });
		for (const l of this.identityListeners)
			l({ certificate: this.cert, privateKey: this.key });
	}

	/** Current PEM material — for the inbound server bind. */
	pem(): { ca: Buffer; cert: Buffer; key: Buffer } {
		return { ca: this.ca, cert: this.cert, key: this.key };
	}

	// grpc-js registers its listener and only then starts watching for the
	// first value, so a synchronous delivery would be lost: deliver on the next
	// microtask, before any handshake can start.
	addCaCertificateListener(listener: CaListener): void {
		this.caListeners.add(listener);
		queueMicrotask(() => {
			if (this.caListeners.has(listener)) listener({ caCertificate: this.ca });
		});
	}

	removeCaCertificateListener(listener: CaListener): void {
		this.caListeners.delete(listener);
	}

	addIdentityCertificateListener(listener: IdentityListener): void {
		this.identityListeners.add(listener);
		queueMicrotask(() => {
			if (this.identityListeners.has(listener))
				listener({ certificate: this.cert, privateKey: this.key });
		});
	}

	removeIdentityCertificateListener(listener: IdentityListener): void {
		this.identityListeners.delete(listener);
	}

	/**
	 * Client credentials that follow this store. `checkServerIdentity` pins the
	 * peer's SPIFFE identity; the chain itself is verified against the CA.
	 */
	channelCredentials(
		checkServerIdentity: (
			hostname: string,
			cert: PeerCertificate,
		) => Error | undefined,
	): grpc.ChannelCredentials {
		return grpc.experimental.createCertificateProviderChannelCredentials(
			this,
			this,
			{ checkServerIdentity },
		);
	}

	/**
	 * Server credentials for the inbound Call server, pinned to the current
	 * leaf. The server is rebound with fresh credentials on rotation (see
	 * CallServer.rotate) — in-place secure-context updates are not honoured by
	 * every supported runtime.
	 */
	serverCredentials(): grpc.ServerCredentials {
		return grpc.ServerCredentials.createSsl(
			this.ca,
			[{ private_key: this.key, cert_chain: this.cert }],
			true,
		);
	}
}
