# telemetry

## Зона ответственности

Per-kind ring buffers, примитивы для emit'а ops/logs/metrics, real bidi gRPC transport (`Telemetry.Report`) и ALS-trace-context для propagation между handler'ами.

Не занимается: persistence-решениями (runtime owns capture/persist config), форматированием UI, прокидыванием X-SB-Trace через wire (это делают конкретные protocol layers: HTTP plugins / RPC server / etc.).

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `TelemetryRing` | class | — | Unified ring для ops/logs/metrics/payloads с per-kind byte budget. Хранит ТИПИЗИРОВАННЫЕ proto-сообщения (не байты). Конструктор принимает optional `RingBudgets` для override (omitted kind → default). Defaults: ops=256KiB, logs=64KiB, metrics=16KiB, payloads=256KiB. ops sized под dense USER.SUBOP step-span emission workflow-run'а между flush-тиками; маленький budget молча drop'ал бы step spans. Ops budget не настраивается снаружи: `service-bridge` передаёт внутреннюю константу `DEFAULT_TELEMETRY_RING_SIZE` (256 KiB, `connection/service-bridge.ts`). |
| `RingBudgets` | `Partial<Record<RingKind, number>>` | — | Override per-kind byte budget. |
| `RingKind` | `"ops" \| "logs" \| "metrics" \| "payloads"` | — | Дискриминант kind ring'а |
| `RingMessage` | `type` | — | Маппинг kind → тип proto-сообщения (`ops`→`OpReport`, `logs`→`Log`, `metrics`→`MetricPoint`, `payloads`→`PayloadAttachment`) |
| `RingItem` | interface | — | `{id, kind, message, bytes}` — типизированный item в ring |
| `TelemetryRing.push(kind, message)` | `void` | — | Добавляет типизированное сообщение в ring; oldest-drop при overflow (byte-budget по дешёвой оценке размера, без сериализации) |
| `TelemetryRing.peek(maxPerKind)` | `RingItem[]` | 100 | Возвращает КОПИЮ head'а без удаления — at-least-once: items остаются до `commit` |
| `TelemetryRing.metrics` | `MetricsAggregator` | — | Накопитель метрик, владеемый ring'ом. `make*`-фабрики и `sb.telemetry` идут через него; transport материализует его в ring раз за flush-цикл |
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
| `DEFAULT_PAYLOAD_MAX_BYTES` | `const number` | `65536` | Дефолтный per-direction cap байтов. Не настраивается из SDK: действующее значение приходит от рантайма через `CaptureModes.payload_max_bytes` (`WatchStream.pushedTelemetryConfig().payloadMaxBytes`) и резолвится живым на каждый op. |
| `CaptureMode` | `"all" \| "errors" \| "none"` | — | Payload capture policy |
| `CapturedAttachment` | interface | — | `{direction (1=IN/2=OUT), bytes, originalSize, contractHash}` — одна capped-сторона захваченного payload |
| `TelemetryTransport` | class | — | Real bidi `Telemetry.Report` client; at-least-once доставка (peek→write→release-по-подтверждённому маркеру); reconnect ladder сбрасывается по первому ack |
| `TelemetryTransport.start()` / `.stop()` | `Promise<void>` | — | Lifecycle; `stop()` дренит остаток ring'а и закрывает локальный конец стрима. Flush-таймер `unref()`'ится — не удерживает процесс живым |
| `TelemetryTransport.flushNow()` | `Promise<void>` | — | Немедленный flush-цикл: drain аггрегатора метрик в ring, затем batch за batch пока в ring есть неотправленное |
| `TelemetryTransportOptions.client` | `TelemetryClientLike` | — (required) | Источник bidi-стрима (`openStream()`) |
| `TelemetryTransportOptions.ring` | `TelemetryRing` | — (required) | Ring, из которого транспорт peek'ает батчи |
| `TelemetryTransportOptions.flushIntervalMs` | `number?` | `250` | Период flush-таймера (ms). Один тик дренит ring целиком, а не одну пачку |
| `TelemetryTransportOptions.maxBatchItems` | `number?` | `256` | Max items per kind в ОДНОЙ пачке. Не потолок пропускной способности: flush-цикл пишет столько пачек, сколько нужно |
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
| `makeLogger(ring, instanceId)` | logger object | — | `debug`/`info`/`warn`/`error(message, fields?)` → logs ring. `source="sdk"`, `atUnixMs=Date.now()`. `traceId`/`opId` берутся из активного `currentTraceContext()`: `opId` = `parentOpId` (охватывающая операция). Вне трейса оба пустые; `parentOpId === ZERO_OP_ID` тоже даёт пустой `opId` |
| `LogFields` | `Record<string, unknown>` | — | Структурные поля лога; сериализуются в `fields_json` (JSON) |
| `MetricsAggregator` | class | — | Накопитель метрик по ключу серии. Живёт на `TelemetryRing.metrics`. Хендлы (`counter`/`gauge`/`histogram`) пишут в объект серии, а не в ring; ровно одна `MetricPoint` на серию за окно агрегации |
| `MetricsAggregator.counter(instanceId, name, labels?)` | `Counter` | `labels={}` | Резолвит серию по ключу и возвращает хендл. Повторный вызов с тем же ключом отдаёт хендл той же серии |
| `MetricsAggregator.gauge(instanceId, name, labels?)` | `Gauge` | `labels={}` | Как `counter`, но серия хранит последнее значение |
| `MetricsAggregator.histogram(instanceId, name, unit?, labels?, bounds?)` | `Histogram` | `unit="s"`, `labels={}`, `bounds=DEFAULT_HISTOGRAM_BOUNDS` | Внимание: `unit` идёт ПЕРЕД `labels` (в отличие от `counter`/`gauge`, где третий параметр — `labels`). Повторная регистрация того же ключа с другими `bounds` кидает |
| `MetricsAggregator.drain(nowMs?)` | `MetricPoint[]` | `Date.now()` | Точки по всем изменившимся с прошлого дренажа сериям; сбрасывает аккумуляторы. Вызывающий сам решает, куда их деть |
| `MetricsAggregator.flush(nowMs?)` | `number` | `Date.now()` | `drain()` + push каждой точки в sink (ring); возвращает число точек |
| `makeCounter(ring, instanceId, name, labels?)` | `Counter` | `labels={}` | `.inc(amount=1)`. Делегирует в `ring.metrics.counter` (`METRIC_KIND_COUNTER`, unit `"1"`) |
| `makeGauge(ring, instanceId, name, labels?)` | `Gauge` | `labels={}` | `.set(value)`. Делегирует в `ring.metrics.gauge` (`METRIC_KIND_GAUGE`, unit `"1"`) |
| `makeHistogram(ring, instanceId, name, unit?, labels?, bounds?)` | `Histogram` | `unit="s"`, `labels={}`, `bounds=DEFAULT_HISTOGRAM_BOUNDS` | `.observe(value)`. Делегирует в `ring.metrics.histogram` (`METRIC_KIND_HISTOGRAM`) |
| `Counter` / `Gauge` / `Histogram` | interface | — | `{inc(amount?)}` / `{set(value)}` / `{observe(value)}` |
| `HistogramBucket` | interface | — | `{le: number \| "+Inf", count}` — кумулятивный бакет в `buckets_json`. `count` = число наблюдений `<= le`, поэтому запись `"+Inf"` несёт общее число наблюдений. Граница переполнения — строка `"+Inf"`, потому что в JSON нет литерала бесконечности |
| `DEFAULT_HISTOGRAM_BOUNDS` | `readonly number[]` | Prometheus-лестница `0.005…10` | Дефолтные границы бакетов, в секундах |
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
| `estimateSize(kind, msg)` | `number` | — | Дешёвая оценка байт-стоимости сообщения для memory-учёта (без сериализации). Для metrics учитывает labels: точка гистограммы с 12 бакетами ≈ 374 B против ≈ 85 B у counter/gauge |
| `MetricSink` | interface | — | `{push("metrics", point)}` — куда `MetricsAggregator.flush` кладёт точки. `TelemetryRing` его удовлетворяет |
| `MetricsHost` | interface | — | `{readonly metrics: MetricsAggregator}` — принимается `make*`-фабриками, чтобы вызывающие продолжали передавать ring |
| `childContext(parent, newOpId)` | function | — | Не реэкспортируется через `index.ts`; deep-import из `./trace-context`. Derive child: same traceId, parentOpId=newOpId. Используется `connection/service-bridge.ts` для дерева trace вокруг step-span. |
| `streamWithContext(ctx, gen)` | `AsyncIterable<T>` | — | Не реэкспортируется. Оборачивает async-generator так, что каждый `.next()` бежит внутри `als.run(ctx)` — ALS не пробрасывается через async-generator continuation по месту конструирования. При любом завершении (штатный конец, `break`, `throw`) форвардит `return()` в нижележащий итератор. |
| `EMPTY_JSON_OBJECT` | `Buffer` (`{}`) | — | Дефолт `meta_json`/`attrs_json` в START-фрейме: runtime queue'ит через `jsonOrNull(buf)`, пустой буфер → SQL NULL → нарушает NOT NULL на `operations.meta`/`attrs`. |
| `SAMPLE_INTERVAL_MS` | `number` | `30000` | Дефолтный интервал `ProcessSampler` (можно переопределить аргументом конструктора). |

## Архитектурные решения и почему

- **Per-kind ring buffers**: изолируют медленный kind (большие metrics) от ops. Overflow → oldest-drop + counter.
- **Метрики агрегируются локально, а не «вызов = точка»**: каждый `.inc()` раньше был отдельным item'ом в ring. Сервис на 1000 rps с одним инкрементом на запрос давал 1000 точек/с в 16 KiB кольцо, вмещающее ~190 — почти всё молча выбрасывалось oldest-drop'ом. Теперь `MetricsAggregator` копит состояние по ключу серии и отдаёт ровно одну `MetricPoint` на серию за окно. Горячий путь (`inc`/`set`/`observe`) трогает только объект серии, который захватил хендл — ни поиска в map, ни аллокации; map читается один раз, при создании хендла.
- **Ёмкость окна упирается в кардинальность меток, а не в транспорт**: агрегатор режет объём по числу СЕРИЙ, а не по числу вызовов, поэтому потолок теперь — сколько разных серий живёт в одном окне. Точка гистограммы весит ≈374 B (12 бакетов в `buckets_json`) против ≈85 B у counter/gauge, так что при 16 KiB бюджете metrics-кольца в окно помещается ≈43 серии гистограмм. Типичному сервису этого с запасом, но гистограмма с меткой высокой кардинальности (`user_id`, путь с подставленными параметрами) плодит серию на каждое значение и вернёт дропы. Это дисциплина меток на стороне вызывающего — чинится ограничением набора значений метки, а не размером кольца. Узнать об этом иначе можно только по растущему `dropCount("metrics")`, поэтому: метка — это перечислимое множество, а не идентификатор.
- **Ключ серии — labels, отсортированные, с печатным разделителем**: ключ собирается из имени, instance и пар label'ов в отсортированном порядке (иначе `{a,b}` и `{b,a}` были бы разными сериями). Разделитель — печатный символ, потому что значение метки может содержать что угодно и подделать разделитель, склеив две разные серии в одну.
- **Gauge хранит значение, но эмитится только при изменении**: повторная отправка неизменного gauge каждый тик — это 4 точки/с/серию на проводе без новой информации, ровно та перегрузка кольца, ради которой агрегатор и существует. Рантайм хранит сырые точки, поэтому последнее отправленное значение остаётся читаемым.
- **Конфликт `bounds`/`unit` для одной серии падает громко**: регистрация гистограммы с тем же ключом, но другой раскладкой бакетов — это ошибка в коде вызывающего, а не ситуация для тихого выбора одного из вариантов. У одной серии одна раскладка бакетов.
- **Окно агрегации = flush-цикл транспорта**: `pump()` зовёт `ring.metrics.flush()` один раз в начале цикла, поэтому окно совпадает с flush-тиком без второго таймера и без отдельного lifecycle. Дренаж именно в `pump`, а не в `writeBatchToStream`, — иначе точки цикла размазывались бы по всем пачкам, которые пишет цикл. Повторный дренаж внутри тика отдаёт пусто (сдренированная серия перестаёт быть dirty), так что дублей не будет.
- **`commit` по индексам, а не пересборкой массива**: деградация на глубоком кольце сидела в `commit`, который пересобирал массив items — 697 мс при глубине 26000. `shift()` в Bun при этом оказался плоским по глубине и реальной проблемой не был.
- **Byte-budget вместо item-count**: реалистичнее моделирует memory pressure. Размер item'а оценивается дёшево (длины строк/буферов), без сериализации — сериализация происходит ровно один раз, внутри `stream.write(batch)` grpc-js.
- **Ring хранит типизированные сообщения, не байты**: producer'ы (`OpHandle`/logs/metrics) пушат готовый proto-объект; transport кладёт его прямо в `OpBatch`/`LogBatch`/… без decode round-trip. Убирает лишний encode-на-push + decode-на-flush на горячем пути.
- **At-least-once доставка**: `peek` отдаёт КОПИЮ head'а, items остаются в ring (oldest-first). Transport помечает отправленный батч как in-flight (по id) и освобождает его только когда ack ДОКАЗЫВАЕТ, что рантайм его получил. Повторный flush до подтверждения НЕ переотправляет уже-in-flight items (peek снова их видит, но они отфильтрованы по id). Если стрим умирает до подтверждения — in-flight маркер сбрасывается, items остаются в ring и переотправляются на новом стриме. Рантайм идемпотентен по `op_id` (`ON CONFLICT`), так что дубль при переотправке безвреден.
- **Подтверждение — с лагом в один ack (epoch-маркер)**: `TelemetryAck` не несёт идентификатора пачки, а рантайм шлёт его по фиксированному тикеру (`ackIntervalDefault` = 2 s), а не в ответ на батч. Значит один ack не может подтвердить запись, которая разъехалась с ним в полёте. Каждый in-flight item помечается `epoch` = число полученных ack на момент записи; ack освобождает только `epoch < ackEpoch - 1`. Обоснование: item epoch E ушёл из процесса ДО прихода ack E, то есть рантайм получил его максимум через одну сетевую задержку после отправки ack E — заведомо раньше, чем через ack-интервал уйдёт следующий ack. Корректно пока RTT < ack-интервала (2 s). Освобождение всего in-flight разом (как было) выбрасывало из ring кадры, которых рантайм не видел: при смерти стрима они терялись молча, ломая заявленный at-least-once. Цена — ring держит ~2 ack-интервала телеметрии вместо одного; переполнение при этом наблюдаемо через `dropCount`/`onDrop`, в отличие от тихой потери.
- **Flush-цикл дренит ring, а не одну пачку**: `maxBatchItems` ограничивает размер ОДНОЙ пачки, не пропускную способность. Одна пачка на тик таймера давала бы потолок `maxBatchItems × 4` кадров/с (при 256/250 ms — 1024 кадра/с, то есть ~512 операций/с, поскольку каждая операция шлёт START и END), а всё сверх этого молча выбрасывалось бы oldest-drop'ом — причём выбрасывались бы именно START-кадры, и рантайм видел бы END без START. Хуже того, in-flight items лежат в голове ring'а до подтверждения, поэтому `peek`, ограниченный размером пачки, возвращал бы одни in-flight — транспорт вставал бы полностью до следующего ack (2 s). Отсюда два решения: `selectNextBatch` peek'ает ЗА in-flight-префикс, а `pump` пишет пачки пока в ring есть неотправленное.
- **Ack сразу тянет следующую пачку**: ack — самый дешёвый сигнал, что рантайм справляется, поэтому `handleAck` запускает тот же `pump` вместо ожидания следующего тика. Получается credit-based конвейер: в простое не стоит ничего, под нагрузкой убирает до 250 ms задержки на кадр.
- **Backpressure — advisory, без клиентской паузы**: уровень от рантайма больше НЕ ставит flusher на паузу. Пауза при продолжающемся push'е продюсеров привела бы к oldest-drop START-фреймов в ring (хуже, чем отправить). Рантайм сам shedding'ит на своей стороне через windowed-level. `drainReason` → final flush + local end; reconnect ladder поднимет stream обратно.
- **Reconnect backoff сбрасывается по первому ack**: `reconnectAttempt` обнуляется при первом успешном ack на новом стриме, а не сразу после `openStream`. Иначе ladder не растёт и получается reconnect-storm.
- **Reconnect ladder — общий модуль, не локальная копия**: задержка считается через `reconnectDelay(reconnectAttempt, reconnectOpts)` из `../utils/reconnect-ladder.ts` — та же лестница `[1000, 5000, 15000, 30000, 60000]` + jitter, что у events/job/workflow subscriber'ов. Локальная копия лестницы в этом домене недопустима: она расходится с общим модулем при изменении политики backoff.
- **Наблюдаемость потерь**: `onDrop` хук эмитит при росте server-side (`drop_count_server_side` из ack) или local ring drop-счётчиков — backpressure перестаёт быть тихим.
- **Auto-mint opId**: `OpHandle.start` минит UUIDv7 если caller не передал — упрощает user-side API (`sb.telemetry.startOp({ ... })`).
- **Логи коррелируются с трейсом на месте эмита**: `makeLogger` читает `currentTraceContext()` в момент push'а, а не принимает ids аргументом. Иначе `trace_id` уходил бы пустым, рантайм писал бы NULL (`logProtoToRow` заполняет колонку только при непустой строке), и фильтр `QueryLogs.trace_id` не находил бы ни одной строки — панель «логи этого трейса» была бы пуста для всех Node-сервисов. `ZERO_OP_ID` — сентинел «нет охватывающей операции», а не существующий op, поэтому на wire уходит пустая строка.
- **`streamWithContext` закрывает нижележащий итератор**: обёртка потребляет источник вручную (`iter.next()` внутри `als.run`), поэтому `return()`, который движок пробрасывает в обёртку при `break`/`throw` потребителя, до источника сам не доходит. Без форварда в `finally` внутренний генератор оставался бы висеть на своём yield, а gRPC-вызов под ним — открытым до конца жизни процесса: одна утёкшая HTTP/2-сессия на каждый прерванный `for await`.
- **Таймеры `unref()`'ятся**: flush-таймер транспорта и таймер `ProcessSampler` не должны удерживать процесс живым — телеметрия сама по себе не повод не дать процессу завершиться.

## Зависимости

Опирается на: `../pb/servicebridge/v1/telemetry` (generated proto), `node:async_hooks` (ALS), `@grpc/grpc-js` (stream types), npm-пакет `uuidv7` (генерация UUIDv7 opId/traceId в `ops.ts` и `trace-context.ts`; работает под Node и Bun), `../utils/reconnect-ladder` (`reconnectDelay`/`ReconnectDelayOptions` для `TelemetryTransport`).

Используется: `../rpc/server.ts` (RPC.HANDLE), `../http/*/plugin.ts` (HTTP.HANDLE + trace propagation), `../connection/service-bridge.ts` (lifecycle + `sb.telemetry` API).
