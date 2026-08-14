// http-op.ts — старт и завершение HTTP.HANDLE-операции. Единственное место, где
// живёт логика «входящий запрос → op»: интеграции express/fastify/hono держат
// только фреймворк-специфичное (сбор роутов, доступ к телу, привязка к хукам).
// @internal — см. ../README.md

import type { ServiceBridge } from "../../connection/service-bridge";
import {
	Channel,
	HttpHandle,
	type OpHandle,
	Status,
} from "../../telemetry/ops";
import type { TraceContext } from "../../telemetry/trace-context";
import { contextFromXSbTrace } from "./trace-wrap";

/** Node отдаёт повторяющиеся заголовки массивом, Fetch API — строкой или null. */
export type HeaderValue = string | string[] | null | undefined;

/** Первое значение заголовка, независимо от формы, которую дал фреймворк. */
export function firstHeader(value: HeaderValue): string | undefined {
	if (value == null) return undefined;
	return Array.isArray(value) ? value[0] : value;
}

/** Всё, что общей логике нужно знать о входящем запросе. */
export interface HttpOpRequest {
	method: string;
	/** Путь в subject'е: route-паттерн там, где фреймворк его знает. */
	subjectPath: string;
	/** Путь для businessKey, когда нет заголовка `Idempotency-Key`. */
	keyPath: string;
	traceHeader: HeaderValue;
	idempotencyKey: HeaderValue;
}

export interface HttpOp {
	handle: OpHandle;
	/** Контекст, пришедший в `X-SB-Trace` (или свежий root, если его не было). */
	incoming: TraceContext;
	/** Trace-scope для downstream-кода: HTTP.HANDLE становится родителем. */
	scope: TraceContext;
	/**
	 * Захват тел имеет смысл. Пока `false`, тела не читаются и не сериализуются
	 * вообще: `OpHandle.capture` всё равно выбросит результат, а `JSON.stringify`
	 * тела (и клон стрима у Hono) дороже самой операции. Берётся с самого
	 * `OpHandle` — режим там уже отрезолвлен, включая per-handler сужение,
	 * которого вопрос к каналу не увидел бы.
	 */
	capturing: boolean;
}

/**
 * Стартует HTTP.HANDLE op: парсит `X-SB-Trace`, берёт businessKey из
 * `Idempotency-Key` (иначе `"<METHOD> <path>"`), считает trace-scope для
 * вложенных операций и режим захвата тел.
 */
export function startHttpOp(sb: ServiceBridge, req: HttpOpRequest): HttpOp {
	const ctx = contextFromXSbTrace(firstHeader(req.traceHeader));
	const handle = sb.telemetry.startOp({
		traceId: ctx.traceId,
		parentOpId: ctx.parentOpId,
		channel: Channel.HTTP,
		kind: HttpHandle,
		subject: `http.handle:${req.method}/${req.subjectPath}`,
		businessKey:
			firstHeader(req.idempotencyKey) ?? `${req.method} ${req.keyPath}`,
	});
	return {
		handle,
		incoming: ctx,
		scope: handle.scope,
		capturing: handle.capturing,
	};
}

/** Маппинг HTTP-кода в статус op'а: SUCCESS <400, ERROR >=400. */
export function statusForHttpCode(code: number): {
	status: Status;
	message?: string;
} {
	if (code >= 400) return { status: Status.ERROR, message: `HTTP ${code}` };
	return { status: Status.SUCCESS };
}
