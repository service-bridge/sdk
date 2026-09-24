/**
 * Subpath entry: `servicebridge/express`.
 * См. ../README.md и ./README.md.
 */

export type {
	HttpRateLimitOptions,
	HttpSecurityOptions,
} from "../_common/security";
export { attachExpress, type ExpressEndpoint } from "./plugin";
