/**
 * Subpath entry: `servicebridge/hono`.
 * См. ../README.md и ./README.md.
 */

export type {
	HttpRateLimitOptions,
	HttpSecurityOptions,
} from "../_common/security";
export type { HonoEndpoint } from "./plugin";
export { attachHono, collectHonoRoutes } from "./plugin";
