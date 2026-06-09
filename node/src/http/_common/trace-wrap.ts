// trace-wrap.ts — общий helper для HTTP plugins.
// Парсит X-SB-Trace header в TraceContext; при отсутствии/невалидном — мин’ит новый root.
// @internal — см. ../README.md.

import {
	mintRootContext,
	type TraceContext,
} from "../../telemetry/trace-context";
import { parseXSbTrace } from "../../telemetry/wire-trace";

/**
 * Парсит входящий X-SB-Trace header. Возвращает propagated context или
 * свежий root, если header отсутствует/невалиден. Никогда не throw'ит.
 */
export function contextFromXSbTrace(
	header: string | null | undefined,
): TraceContext {
	if (header == null) return mintRootContext();
	const parsed = parseXSbTrace(header);
	if (parsed == null) return mintRootContext();
	return parsed;
}
