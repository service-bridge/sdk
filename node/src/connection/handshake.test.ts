import { expect, test } from "bun:test";
import pkg from "../../package.json";
import { SDK_VERSION } from "./handshake";

test("SDK_VERSION matches package.json", () => {
	expect(SDK_VERSION).toBe(pkg.version);
});
