import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Use valid TLS material even though the transport is fake: newer Bun/OpenSSL
// validates certificates when grpc-js creates credentials, before opening it.
export const testTls = (() => {
	const dir = mkdtempSync(join(tmpdir(), "sb-sdk-tls-"));
	try {
		execFileSync(
			"openssl",
			[
				"req",
				"-x509",
				"-newkey",
				"rsa:2048",
				"-nodes",
				"-keyout",
				join(dir, "key.pem"),
				"-out",
				join(dir, "cert.pem"),
				"-days",
				"1",
				"-subj",
				"/CN=localhost",
			],
			{ stdio: "ignore" },
		);
		const certDer = execFileSync("openssl", [
			"x509",
			"-in",
			join(dir, "cert.pem"),
			"-outform",
			"DER",
		]);
		const privateKeyDer = execFileSync("openssl", [
			"pkcs8",
			"-topk8",
			"-nocrypt",
			"-in",
			join(dir, "key.pem"),
			"-outform",
			"DER",
		]);
		return { certDer, privateKeyDer };
	} finally {
		rmSync(dir, { recursive: true, force: true });
	}
})();
