import { describe, expect, it } from "bun:test";
import {
	clientAddress,
	HttpRequestGuard,
	isCommonScannerPath,
} from "./security";

describe("HTTP edge security", () => {
	it("recognizes secret, VCS, PHP and CMS probes from production traces", () => {
		for (const path of [
			"/.env",
			"/api/staging/.env",
			"/.git/config",
			"/dev/phpinfo.php",
			"/wp/wp-json/batch/v1",
			"/?rest_route=%2Fbatch%2Fv1",
			"/vendor/phpunit/phpunit/src/Util/PHP/eval-stdin.php",
		]) {
			expect(isCommonScannerPath(path)).toBe(true);
		}
		expect(isCommonScannerPath("/api/v1/campaigns")).toBe(false);
		expect(isCommonScannerPath("/health")).toBe(false);
	});

	it("does not trust forwarded addresses unless proxy hops are explicit", () => {
		expect(clientAddress("10.0.0.2", "198.51.100.7", 0)).toBe("10.0.0.2");
		expect(clientAddress("10.0.0.2", "198.51.100.7", 1)).toBe("198.51.100.7");
		expect(clientAddress("10.0.0.2", "198.51.100.7, 10.0.0.3", 2)).toBe(
			"198.51.100.7",
		);
	});

	it("blocks probes before accounting and rate-limits normal traffic", () => {
		const guard = new HttpRequestGuard({
			rateLimit: { limit: 2, windowMs: 1_000 },
		});
		expect(guard.check({ method: "GET", pathname: "/.env" }, 0)).toEqual({
			allowed: false,
			status: 404,
		});
		const req = {
			method: "GET",
			pathname: "/api/v1/campaigns",
			remoteAddress: "192.0.2.10",
		};
		expect(guard.check(req, 0).allowed).toBe(true);
		expect(guard.check(req, 1).allowed).toBe(true);
		expect(guard.check(req, 2)).toEqual({
			allowed: false,
			status: 429,
			retryAfterSeconds: 1,
		});
		expect(guard.check(req, 1_001).allowed).toBe(true);
	});

	it("can disable scanner blocking and rate limiting independently", () => {
		const guard = new HttpRequestGuard({
			blockCommonScanners: false,
			rateLimit: false,
		});
		for (let i = 0; i < 1_000; i++) {
			expect(guard.check({ method: "GET", pathname: "/.env" }).allowed).toBe(
				true,
			);
		}
	});
});
