import { createServer } from "node:net";

/**
 * Allocates a free TCP port by binding `0`, reading the assigned port from the
 * OS, then closing the temporary listener. Used by HTTP-integration e2e tests
 * so each test can run its own Express/Fastify/Hono server without conflicts.
 */
export function freePort(): Promise<number> {
	return new Promise((resolve, reject) => {
		const srv = createServer();
		srv.unref();
		srv.once("error", reject);
		srv.listen(0, "127.0.0.1", () => {
			const addr = srv.address();
			if (addr && typeof addr === "object") {
				const port = addr.port;
				srv.close(() => resolve(port));
			} else {
				srv.close(() => reject(new Error("no address from listener")));
			}
		});
	});
}
