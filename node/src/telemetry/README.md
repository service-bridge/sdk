# telemetry

## Зона ответственности

Per-kind ring buffers, примитивы для emit'а ops/logs/metrics, real bidi gRPC transport (`Telemetry.Report`) и ALS-trace-context для propagation между handler'ами.

Не занимается: persistence-решениями (runtime owns capture/persist config), форматированием UI, прокидыванием X-SB-Trace через wire (это делают конкретные protocol layers: HTTP plugins / RPC server / etc.).

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `TelemetryRing` | class | — | Unified ring для ops/logs/metrics/payloads с per-kind byte budget. Хранит ТИПИЗИРОВАННЫЕ proto-сообщения (не байты). Конструктор принимает optional `RingBudgets` для override (omitted kind → default). Defaults: ops=256KiB, logs=64KiB, metrics=16KiB, payloads=256KiB. ops sized под dense USER.SUBOP step-span emission workflow-run'а между flush-тиками; маленький budget молча drop'ал бы step spans. `service-bridge` прокидывает `ServiceBridgeOptions.telemetryRingSize` (байты) как override ops budget. |
| `RingBudgets` | `Partial<Record<RingKind, number>>` | — | Override per-kind byte budget. |
| `RingKind` | `"ops" \| "logs" \| "metrics" \| "payloads"` | — | Дискриминант kind ring'а |
| `RingMessage` | `type` | — | Маппинг kind → тип proto-сообщения (`ops`→`OpReport`, `logs`→`Log`, `metrics`→`MetricPoint`, `payloads`→`PayloadAttachment`) |
| `RingItem` | interface | — | `{id, kind, message, bytes}` — типизированный item в ring |
| `TelemetryRing.push(kind, message)` | `void` | — | Добавляет типизированное сообщение в ring; oldest-drop при overflow (byte-budget по дешёвой оценке размера, без сериализации) |
| `TelemetryRing.peek(maxPerKind)` | `RingItem[]` | 100 | Возвращает КОПИЮ head'а без удаления — at-least-once: items остаются до `commit` |
| `TelemetryRing.commit(items)` | `void` | — | Освобождает (удаляет) подтверждённые items по их id |
| `TelemetryRing.dropCount(kind)` | `number` | — | Счётчик dropped items per kind |
| `TelemetryRing.totalDropCount()` | `number` | — | Сумма dropped items по всем kind'ам |
| `OpHandle.start(ring, params)` | `OpHandle` | — | Enqueue START frame; auto-mint opId + inherit ALS trace. `params: StartOpParams` |
| `StartOpParams` | interface | — | Параметры `OpHandle.start`. Required: `channel`, `kind`, `subject`. Optional с дефолтами: `traceId`/`parentOpId` (← `currentTraceContext()`, иначе свежий UUIDv7 / `ZERO_OP_ID`), `opId` (auto UUIDv7), `peerServiceId`/`businessKey` (`""`), `attempt` (`0`), `startedAtMs` (`Date.now()`), `metaJson`/`attrsJson` (`{}`), `captureMode` (per-handler override, только narrow), `effectiveCaptureMode` (runtime-pushed, default `"none"`), `payloadMaxBytes` (per-direction cap байтов captured payload, default `DEFAULT_PAYLOAD_MAX_BYTES`) |
| `OpHandle.end(status, message?)` | `void` | — | Enqueue END frame; idempotent. На failure (status ≠ `SUCCESS` и ≠ `PENDING`) flush'ит errors-mode буферизованные payloads |
| `OpHandle.captureIn(bytes, contractHash)` | `void` | — | Capture inbound payload (IN, direction=1). Mode "all"→emit now, "errors"→buffer until end, "none"→no-op |
| `OpHandle.captureOut(bytes, contractHash)` | `void` | — | Capture outbound payload (OUT, direction=2) |
| `OpHandle.opId` / `traceId` | `string` (getter) | — | Identifiers for the in-flight op |
| `resolveCaptureMode(pushed, perHandler?)` | `CaptureMode` | — | Effective per-op mode = runtime-pushed mode FOR THE OP'S CHANNEL (`pushed`) narrowed by `perHandler` (privacy ordering none < errors < all; override may only narrow). The SDK does NOT read any env — the runtime pushes a per-channel `CaptureModes` set via the registry stream; the op picks its channel's mode |
| `capPayload(bytes, maxBytes?)` | `{bytes, originalSize}` | `maxBytes` = `DEFAULT_PAYLOAD_MAX_BYTES` (65536) | Truncate to `maxBytes`, keep original byte length. Non-finite/non-positive → default. |
| `DEFAULT_PAYLOAD_MAX_BYTES` | `const number` | `65536` | Дефолтный per-direction cap байтов; override через `ServiceBridgeOptions.payloadMaxBytes`. |
| `CaptureMode` | `"all" \| "errors" \| "none"` | — | Payload capture policy |
| `CapturedAttachment` | interface | — | `{direction (1=IN/2=OUT), bytes, originalSize, contractHash}` — одна capped-сторона захваченного payload |
| `TelemetryTransport` | class | — | Real bidi `Telemetry.Report` client; at-least-once доставка (peek→write→commit-по-ack); reconnect ladder сбрасывается по первому ack |
| `TelemetryTransport.start()` / `.stop()` | `Promise<void>` | — | Lifecycle; `stop()` flushes final batch и закрывает локальный конец стрима |
| `TelemetryTransportOptions.client` | `TelemetryClientLike` | — (required) | Источник bidi-стрима (`openStream()`) |
| `TelemetryTransportOptions.ring` | `TelemetryRing` | — (required) | Ring, из которого транспорт peek'ает батчи |
| `TelemetryTransportOptions.flushIntervalMs` | `number?` | `250` | Период flush-таймера (ms) |
| `TelemetryTransportOptions.maxBatchItems` | `number?` | `256` | Max items per kind в одном батче |
| `TelemetryTransportOptions.reconnectOpts` | `ReconnectDelayOptions?` | shared `RECONNECT_LADDER_MS` + ±20% jitter | Reconnect backoff options; тот же тип, что у events/job/workflow subscriber'ов (`../utils/reconnect-ladder.ts`) |
| `TelemetryTransportOptions.onDrop` | `DropObserver?` | — | Хук наблюдаемости: вызывается при росте server-side или local ring drop-счётчиков |
| `DropObserver` | `type` | — | `(info: {serverDrops, ringDrops, backpressureLevel}) => void` |
| `adaptTelemetryClient(client)` | `TelemetryClientLike` | — | Wrap generated `TelemetryClient` for transport |
| `TelemetryClientLike` | interface | — | Minimal client shape (`openStream(): ClientTelemetryStream`), тип реэкспортируется для интеграторов/моков |
| `ClientTelemetryStream` | interface | — | Minimal bidi-stream shape (`write`/`end`/`on('data'\|'end'\|'error')`), тип реэкспортируется для интеграторов/моков |
| `runWithTrace(ctx, fn)` | `T` | — | ALS scope для trace propagation |
| `currentTraceContext()` | `TraceContext \| undefined` | — | Active trace inside `runWithTrace` |
| `parseXSbTrace(value)` | `ParsedXSbTrace \| null` | — | Parse `<traceId>-<parentOpId>` wire format (ADR 0006 §3) |
| `formatXSbTrace(traceId, parentOpId)` | `string` | — | Format trace context as `<traceId>-<parentOpId>` (matches runtime) |
| `makeLogger(ring, instanceId)` | logger object | — | `debug`/`info`/`warn`/`error(message, fields?)` → logs ring. `source="sdk"`, `atUnixMs=Date.now()` |
| `LogFields` | `Record<string, unknown>` | — | Структурные поля лога; сериализуются в `fields_json` (JSON) |
| `makeCounter(ring, instanceId, name, labels?)` | counter | `labels={}` | `.inc(amount=1)` → metrics ring (`METRIC_KIND_COUNTER`, unit `"1"`) |
| `makeGauge(ring, instanceId, name, labels?)` | gauge | `labels={}` | `.set(value)` → metrics ring (`METRIC_KIND_GAUGE`, unit `"1"`) |
| `makeHistogram(ring, instanceId, name, unit?, labels?)` | histogram | `unit="s"`, `labels={}` | `.observe(value)` → metrics ring (`METRIC_KIND_HISTOGRAM`) |
| `Labels` | `Record<string, string>` | — | Метки метрики (wire `labels`) |
| `Channel` / `Status` / `LogLevel` / `MetricKind` | enums | — | Wire-shared с runtime |
| `cpuPercent(prev, cur, elapsedMs)` | `number` | — | Pure: `(userDelta+systemDelta)/elapsedMicros*100`. Single-core-equivalent — двухъядерное насыщение даёт ~200. Возвращает 0 при `elapsedMs≤0`. |
| `ProcessSampler` | class | — | Эмитит `process.cpu_percent` (%, gauge) и `process.rss_bytes` (bytes, gauge) в metrics ring с текущим `instance_id`. `start()` шлёт первый сэмпл сразу (CPU = среднее за жизнь процесса), далее каждые 30 s. Экземпляр живёт на `ServiceBridge`; `start()` при инициализации transport'а, `close()` при `stop()`. Таймер `unref()`'ится — не удерживает процесс. |
| `process.cpu_percent` | metric name | — | CPU-процент процесса: (userDelta+systemDelta)/elapsedMicros×100. Нормировано к одному ядру — multi-core может давать >100. Первый сэмпл сразу при старте, далее интервал 30 s. Тег: `instance_id`. |
| `process.rss_bytes` | metric name | — | Resident Set Size в байтах (`process.memoryUsage().rss`). Интервал 30 s. Тег: `instance_id`. |
| Kind constants | `number` | — | Per-channel op-kind numeric values (mirror Go `enums.go`): `RpcCall=1`; `EventPublish=1`, `EventDeliver=2`; `WorkflowRun=1`, `WorkflowSleep=2`, `WorkflowWaitEvent=3`, `WorkflowWaitSignal=4`; `JobExec=1`; `HttpHandle=1`; `UserSubOp=1`. |
| `OpHandle.setAttempt(n)` | method | — | Records the retry attempt on an in-flight op; END frame carries the final value (one row across retries, ADR-0001) |
| `TraceContext` | interface | — | `{ traceId: string; parentOpId: string }`. UUID strings per ADR 0006. |
| `ZERO_OP_ID` | constant | `"00000000-..."` | Sentinel: root operation (no parent). Never use as an explicit parent. |
| `mintRootContext()` | function | — | Fresh `TraceContext` with new UUIDv7 traceId and `ZERO_OP_ID` parentOpId. |
| `als` | `AsyncLocalStorage<TraceContext>` | — | ALS-носитель trace-контекста. Реэкспортируется для hook-style фреймворков (Fastify), которым нужен `als.enterWith(ctx)` без `next()`-callback. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `KindRing` (class, private) | `@internal` | — | Single-kind byte-budgeted FIFO с peek/commit |
| `DEFAULT_KIND_BUDGETS` | `Record<RingKind, number>` | ops=256KiB, logs=64KiB, metrics=16KiB, payloads=256KiB | Per-kind ring budgets |
| `estimateSize(kind, msg)` | `number` | — | Дешёвая оценка байт-стоимости сообщения для memory-учёта (без сериализации) |
| `childContext(parent, newOpId)` | function | — | Не реэкспортируется через `index.ts`; deep-import из `./trace-context`. Derive child: same traceId, parentOpId=newOpId. Используется `connection/service-bridge.ts` для дерева trace вокруг step-span. |
| `streamWithContext(ctx, gen)` | `AsyncIterable<T>` | — | Не реэкспортируется. Оборачивает async-generator так, что каждый `.next()` бежит внутри `als.run(ctx)` — ALS не пробрасывается через async-generator continuation по месту конструирования. |
| `EMPTY_JSON_OBJECT` | `Buffer` (`{}`) | — | Дефолт `meta_json`/`attrs_json` в START-фрейме: runtime queue'ит через `jsonOrNull(buf)`, пустой буфер → SQL NULL → нарушает NOT NULL на `operations.meta`/`attrs`. |
| `SAMPLE_INTERVAL_MS` | `number` | `30000` | Дефолтный интервал `ProcessSampler` (можно переопределить аргументом конструктора). |

## Архитектурные решения и почему

- **Per-kind ring buffers**: изолируют медленный kind (большие metrics) от ops. Overflow → oldest-drop + counter.
- **Byte-budget вместо item-count**: реалистичнее моделирует memory pressure. Размер item'а оценивается дёшево (длины строк/буферов), без сериализации — сериализация происходит ровно один раз, внутри `stream.write(batch)` grpc-js.
- **Ring хранит типизированные сообщения, не байты**: producer'ы (`OpHandle`/logs/metrics) пушат готовый proto-объект; transport кладёт его прямо в `OpBatch`/`LogBatch`/… без decode round-trip. Убирает лишний encode-на-push + decode-на-flush на горячем пути.
- **At-least-once доставка**: `peek` отдаёт КОПИЮ head'а, items остаются в ring (oldest-first). Transport помечает отправленный батч как in-flight (по id) и `commit`'ит (освобождает) его только по следующему ack от рантайма. Повторный flush до ack НЕ переотправляет уже-in-flight items (peek снова их видит, но они отфильтрованы по id) — иначе flush (250ms) гонял бы те же кадры до ack (2s). Если стрим умирает до ack — in-flight маркер сбрасывается, items остаются в ring и переотправляются на новом стриме. Рантайм идемпотентен по `op_id` (`ON CONFLICT`), так что дубль при переотправке безвреден. Сигнал подтверждения = ack от рантайма (минимально достаточный: ack означает, что рантайм получил батч).
- **Backpressure — advisory, без клиентской паузы**: уровень от рантайма больше НЕ ставит flusher на паузу. Пауза при продолжающемся push'е продюсеров привела бы к oldest-drop START-фреймов в ring (хуже, чем отправить). Рантайм сам shedding'ит на своей стороне через windowed-level. `drainReason` → final flush + local end; reconnect ladder поднимет stream обратно.
- **Reconnect backoff сбрасывается по первому ack**: `reconnectAttempt` обнуляется при первом успешном ack на новом стриме, а не сразу после `openStream`. Иначе ladder не растёт и получается reconnect-storm.
- **Reconnect ladder — общий модуль, не локальная копия**: задержка считается через `reconnectDelay(reconnectAttempt, reconnectOpts)` из `../utils/reconnect-ladder.ts` — та же лестница `[1000, 5000, 15000, 30000, 60000]` + jitter, что у events/job/workflow subscriber'ов. Локальная копия лестницы в этом домене недопустима: она расходится с общим модулем при изменении политики backoff.
- **Наблюдаемость потерь**: `onDrop` хук эмитит при росте server-side (`drop_count_server_side` из ack) или local ring drop-счётчиков — backpressure перестаёт быть тихим.
- **Auto-mint opId**: `OpHandle.start` минит UUIDv7 если caller не передал — упрощает user-side API (`sb.telemetry.startOp({ ... })`).

## Зависимости

Опирается на: `../pb/servicebridge/v1/telemetry` (generated proto), `node:async_hooks` (ALS), `@grpc/grpc-js` (stream types), npm-пакет `uuidv7` (генерация UUIDv7 opId/traceId в `ops.ts` и `trace-context.ts`; работает под Node и Bun), `../utils/reconnect-ladder` (`reconnectDelay`/`ReconnectDelayOptions` для `TelemetryTransport`).

Используется: `../rpc/server.ts` (RPC.HANDLE), `../http/*/plugin.ts` (HTTP.HANDLE + trace propagation), `../connection/service-bridge.ts` (lifecycle + `sb.telemetry` API).
