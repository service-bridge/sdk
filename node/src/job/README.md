# job — SDK-сторона scheduled jobs

## Зона ответственности

SDK-сторона интеграции с рантайм-подсистемой Jobs. `JobDomain` принимает декларации cron/delayed/interval-хендлеров через `sb.job.handle(name, opts, fn)` и складывает их в общий `Registry`-канал как `IncomingMethod{type=METHOD_TYPE_JOB}` — там же, где живут RPC/Event/Workflow. На стороне рантайма парсится canonical-spec JSON и создаётся definition + schedule.

Дата-плейн обслуживает `JobSubscriber`: открывает server-stream `Jobs.Subscribe`, на каждый `JobExecution` находит handler через `JobDomain.lookup()` и выполняет в trace-контексте с пер-job семафором. Heartbeat каждые 5 с — рантайм принудительно забирает lease после 3 пропущенных тиков.

Чего НЕ делает: не планирует расписание (это runtime), не сохраняет состояние выполнения, не занимается retry-логикой (тоже runtime), не управляет расписаниями отдельно от рантайма (нет manual trigger, pause/resume — out-of-scope v1).

> ⚠️ **Handler MUST be idempotent by `ctx.idempotencyKey`, not by `ctx.attempt`.**
>
> ServiceBridge delivers each job at-least-once. Тот же `ctx.idempotencyKey` может прийти несколько раз после lease expiry. Использовать `INSERT … ON CONFLICT (idempotency_key) DO NOTHING` в effects-таблице — не полагаться на `ctx.attempt === 1`.

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `JobDomain` | class | — | Namespace `sb.job`. Конструируется внутри `ServiceBridge`. |
| `JobDomain.handle(name, opts, fn)` | метод | — | Декларирует scheduled job. Валидирует cron (5-полевой), interval (>0), delayed.at; считает `contractHash = sha256(canonical-spec JSON)`; пушит в `Registry._handle.job()`. Запрещены дубликаты имён. Вызывать ДО `sb.start()`. |
| `JobOpts` | interface | — | `{ trigger, catchup?, overlap?, deps?, maxAttempts?, leaseTtlMs?, maxConcurrent?, retry? }`. |
| `JobHandler` | type | — | `(ctx: JobHandlerCtx) => Promise<void>`. |
| `JobHandlerCtx` | interface | — | `{ jobName, executionId, scheduledAt: Date, localScheduledAt: Date, attempt, idempotencyKey, signal: AbortSignal }`. `scheduledAt`/`localScheduledAt` строятся из `JobExecution.scheduledAtUnixMs`/`localScheduledAtUnixMs` (unix-ms) через `new Date(ms)`. |
| `Trigger` / `CronTrigger` / `DelayedTrigger` / `IntervalTrigger` | type | — | Discriminated union по ключу `cron` \| `delayed` \| `interval`. |
| `CatchupPolicy` | type | `"skip"` | `"skip" \| "fire_once" \| "fire_all"`. |
| `OverlapPolicy` | type | `"skip"` | `"skip" \| "allow" \| "buffer_one"`. |
| `DeclaredDep` | type | — | `{ rpc: string } \| { event: string } \| { workflow: string }`. |
| `RetryPolicy` | interface | — | `{ initialMs, maxMs, multiplier, jitter }`. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `JobDomain.lookup(name)` | метод @internal | — | Возвращает `{ opts, fn }` для `JobSubscriber.dispatch`. |
| `JobDomain.size()` | метод @internal | — | Число зарегистрированных job; `ServiceBridge` пропускает запуск subscriber-а если 0. |
| `JobSubscriber` | class @internal | — | gRPC stream-консьюмер `Jobs.Subscribe`. Жизненный цикл стрима держит `registry/StreamSupervisor`; subscriber даёт ему `open()` (subscribe с текущей identity) и `onData()` (dispatch). Читает `JobExecution.xSbTrace` (ADR 0006 §3, формат `<traceID>-<parentOpID>`) и оборачивает handler в `runWithTrace(xSbTrace, fn)` — разбор строки в trace-контекст и ALS-обёртку делает инжектируемый `runWithTrace`, не subscriber. JOB.EXEC op эмиттит runtime, SDK его не дублирует. На успехе шлёт `JobResult{success}`, на ошибке — `JobResult{failure:{errorMessage, retryable}}` (`retryable=false` если `error.retryable===false`). |
| `JobSubscriber.start()` | `() => void` | — | Открывает стрим через supervisor и запускает heartbeat-таймер (5 с, `unref`). |
| `JobSubscriber.stop()` | `async () => void` | — | Гасит heartbeat, отменяет pending-таймер реконнекта, `cancel()` стрима. |
| `SubscriberDeps` | interface @internal | — | Контракт зависимостей `JobSubscriber`: `rpcClient: JobsClient`, `identity: () => IdentityProvider \| null`, `domain: JobDomain`, `logger: Logger`, `runWithTrace: (xSbTrace, fn) => Promise<void>`, `reconnectOpts?: ReconnectDelayOptions`. Экспортируется из `subscriber.ts` для `connection/service-bridge`, но не реэкспортируется через `index.ts`. |
| `SubscriberDeps.reconnectOpts` | `ReconnectDelayOptions?` | общая лестница + ±20% jitter | Тестовый hook: пиннит лестницу/jitter, чтобы reconnect наблюдался за миллисекунды. |
| `Semaphore` | реэкспорт из `utils/semaphore` | — | Per-job in-flight cap по `opts.maxConcurrent` (`0` → `Number.MAX_SAFE_INTEGER`). Конструируется как `new Semaphore(limit, Number.MAX_SAFE_INTEGER)` — очередь ожидания намеренно безгранична. Своей копии класса модуль больше не держит. |
| `canonicalJobSpec(opts)` | функция @internal | — | Сериализует `JobOpts` в `CanonicalJobSpec` JSON (must match `runtime/internal/jobs.CanonicalJobSpec`): `trigger` + опциональные `catchup`, `overlap`, `deps`, `maxAttempts`, `leaseTtlMs`, `maxConcurrent`, `retry`. Поля времени — unix-ms (`runAtUnixMs`) и ms (`everyMs`, `leaseTtlMs`). |
| `Logger` / `IdentityProvider` | interface @internal | — | Контракты для `SubscriberDeps`: `Logger {warn,error}`, `IdentityProvider {serviceId, instanceId}`. |

## Архитектурные решения и почему

- **Регистрация через общий `Registry`-канал** (`registry._handle.job` → `IncomingMethod{type=METHOD_TYPE_JOB}`) — единый путь с `sb.rpc.handle` / `sb.event.handle` / `sb.workflow.handle`. Меньше путей, меньше синхронизационных проблем.
- **Canonical-spec JSON в `input_schema_json`** — повторяет паттерн workflows (graph JSON в том же поле). `contract_hash` = SHA-256 от canonical. SDK и Go обязаны держать одинаковую структуру (см. `domain.ts::CanonicalJobSpec` ↔ `runtime/internal/jobs/canonical.go::CanonicalJobSpec`).
- **Дата-плейн отдельным gRPC-сервисом `Jobs`** — Subscribe/JobResult/Heartbeat. То же что у events/workflows: registration унифицирована, дата-плейн доменно-специфичный.
- **Lookup через `JobDomain._byName`**, а не через `Handle._entries`: handlers есть в Handle entries для общего finalize/debugging, но per-job opts (`maxConcurrent` и т.д.) нужны для семафора — JobDomain держит их в своей мапе.
- **Очередь семафора безгранична — сознательно, в отличие от входящего RPC-пути.** Общий `utils/semaphore` по умолчанию режет нагрузку: переполнение очереди → `SemaphoreExhaustedError`. Для входящего RPC это правильно — вызывающий получает отказ и решает сам. Здесь `JobExecution` приходит уже с выданным рантаймом лизом, и рантайм сам ограничивает темп рассылки; сбросить исполнение на клиенте — не отказать в запросе, а бросить работу, которой рантайм считает владельцем этот инстанс, до истечения лиза. Поэтому `maxQueued` задан явно как `Number.MAX_SAFE_INTEGER`, а не оставлен на дефолт — молчаливое расхождение с RPC-путём читалось бы как недосмотр.
- **`stop()` отпускает ожидающих через `AbortSignal`.** Исполнение, стоящее в очереди за работающим handler'ом, иначе получало бы слот уже после остановки подписчика и запускало handler на выключенном инстансе. `acquire(signal)` бросает `SemaphoreAbortedError`, исполнение отбрасывается с логом; лиз истекает, рантайм переназначает.
- Cron строго 5-field: 6-field cron отклоняется на стороне SDK до отправки на рантайм.
- Heartbeat 5 с, порог 3 — соответствует lease reclaim логике рантайма. Таймер `unref`-нут: heartbeat не должен быть причиной, по которой процесс не завершается; удерживать event loop — работа reconnect-таймера супервизора.
- **Отказ heartbeat не гасит таймер.** И ошибка в callback, и синхронный throw идут в один счётчик подряд-идущих отказов и логируются (`heartbeat failed (n/3)`). Раньше синхронный throw молча убивал таймер: lease протухал, рантайм переназначал выполнения — вечно, без единой строки диагностики. Достижение порога — не терминал, а `supervisor.restart()`: стрим переоткрывается немедленно.
- **Автомат реконнекта — `registry/StreamSupervisor`**, общий с events- и workflow-подписчиками (лестница `[1, 5, 15, 30, 60]` с, ±20% jitter). Свой while-цикл по промисам сбрасывал счётчик попыток на `resolve()` из `stream.on("end")`, то есть на ЧИСТОМ закрытии стрима — рантайм, штатно закрывающий стримы, получал реконнект раз в секунду вечно. Счётчик сбрасывает только пришедший `JobExecution`.
- `error.retryable = false` — пользователь может явно отправить в DLQ.

## Зависимости

Зависит от: `../registry/registry` (type `Registry`; регистрация идёт через `registry._handle.job(name, contractHash, json, fn)`), `../registry/stream-supervisor` (`StreamSupervisor`), `../utils/reconnect-ladder` (тип `ReconnectDelayOptions`), `../utils/semaphore` (`Semaphore`, `SemaphoreAbortedError`), `../pb/servicebridge/v1/jobs` (`JobsClient`, `JobExecution`, `JobResultRequest`), `node:crypto` (sha256 для contract hash), `@grpc/grpc-js` (тип `ClientReadableStream`). Trace-пропагация в handler приходит инъекцией `runWithTrace` через `SubscriberDeps` — модуль `job` не импортирует `../telemetry/*` напрямую.

Runtime side: `runtime/internal/jobs/{canonical.go,register.go}` (адаптер `RegisterJobs` + parsing `CanonicalJobSpec`). Дата-плейн — gRPC-сервис `Jobs` (`Subscribe` / `JobResult` / `Heartbeat`).

Используется: `connection/service-bridge` (монтирует `JobDomain` как `sb.job`, конструирует `JobSubscriber` с `runWithTrace`, который парсит `X-SB-Trace` через `../telemetry/wire-trace` и оборачивает handler в ALS из `../telemetry/context`).
