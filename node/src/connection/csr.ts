// PKCS#10 certification request for an ECDSA P-256 key, built with WebCrypto
// and a few lines of DER. The runtime takes nothing from the request but the
// public key (the identity comes from the bootstrap key or the live mTLS
// session), so the subject is a fixed CN and there are no attributes.
//
// @internal — см. ./README.md

const OID_COMMON_NAME = [0x55, 0x04, 0x03]; // 2.5.4.3
const OID_ECDSA_SHA256 = [0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02]; // 1.2.840.10045.4.3.2

function length(n: number): number[] {
	if (n < 0x80) return [n];
	const bytes: number[] = [];
	for (let v = n; v > 0; v = Math.floor(v / 256)) bytes.unshift(v & 0xff);
	return [0x80 | bytes.length, ...bytes];
}

function tlv(tag: number, body: Uint8Array | number[]): Uint8Array {
	const content = body instanceof Uint8Array ? body : Uint8Array.from(body);
	const head = [tag, ...length(content.length)];
	const out = new Uint8Array(head.length + content.length);
	out.set(head, 0);
	out.set(content, head.length);
	return out;
}

function concat(...parts: Uint8Array[]): Uint8Array {
	const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
	let offset = 0;
	for (const p of parts) {
		out.set(p, offset);
		offset += p.length;
	}
	return out;
}

const sequence = (...parts: Uint8Array[]) => tlv(0x30, concat(...parts));

// derInteger encodes an unsigned big-endian integer: leading zeros stripped,
// a 0x00 prepended when the high bit is set.
function derInteger(bytes: Uint8Array): Uint8Array {
	let start = 0;
	while (start < bytes.length - 1 && bytes[start] === 0) start++;
	const trimmed = bytes.subarray(start);
	const padded =
		(trimmed[0] ?? 0) & 0x80 ? concat(Uint8Array.of(0), trimmed) : trimmed;
	return tlv(0x02, padded);
}

/** DER of a PKCS#10 request for `keys`, subject CN=`commonName`. */
export async function buildCsr(
	keys: CryptoKeyPair,
	commonName = "servicebridge-instance",
): Promise<Uint8Array> {
	const spki = new Uint8Array(
		await crypto.subtle.exportKey("spki", keys.publicKey),
	);
	const subject = sequence(
		tlv(
			0x31,
			sequence(
				tlv(0x06, OID_COMMON_NAME),
				tlv(0x0c, new TextEncoder().encode(commonName)),
			),
		),
	);
	const info = sequence(
		tlv(0x02, [0]), // version v1
		subject,
		spki,
		tlv(0xa0, []), // attributes: none
	);
	// WebCrypto returns the raw r||s form; X.509 wants SEQUENCE { r, s }.
	const raw = new Uint8Array(
		await crypto.subtle.sign(
			{ name: "ECDSA", hash: "SHA-256" },
			keys.privateKey,
			info.slice(),
		),
	);
	const half = raw.length / 2;
	const signature = sequence(
		derInteger(raw.subarray(0, half)),
		derInteger(raw.subarray(half)),
	);
	return sequence(
		info,
		sequence(tlv(0x06, OID_ECDSA_SHA256)),
		tlv(0x03, concat(Uint8Array.of(0), signature)), // BIT STRING, 0 unused bits
	);
}
