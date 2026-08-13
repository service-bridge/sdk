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
| `JobSubscriber` | class @internal | — | gRPC stream-консьюмер `Jobs.Subscribe`. Конструктор принимает `SubscriberDeps` `{ rpcClient, identity, domain, logger, runWithTrace }` (все обязательны). Читает `JobExecution.xSbTrace` (ADR 0006 §3, формат `<traceID>-<parentOpID>`) и оборачивает handler в `runWithTrace(xSbTrace, fn)` — разбор строки в trace-контекст и ALS-обёртку делает инжектируемый `runWithTrace`, не subscriber. JOB.EXEC op эмиттит runtime, SDK его не дублирует. На успехе шлёт `JobResult{success}`, на ошибке — `JobResult{failure:{errorMessage, retryable}}` (`retryable=false` если `error.retryable===false`). |
| `SubscriberDeps` | interface @internal | — | Контракт зависимостей `JobSubscriber`: `rpcClient: JobsClient`, `identity: () => IdentityProvider \| null`, `domain: JobDomain`, `logger: Logger`, `runWithTrace: (xSbTrace, fn) => Promise<void>`. Экспортируется из `subscriber.ts` для `connection/service-bridge`, но не реэкспортируется через `index.ts`. |
| `Semaphore` | class @internal | — | Per-job in-flight cap по `opts.maxConcurrent`. |
| `canonicalJobSpec(opts)` | функция @internal | — | Сериализует `JobOpts` в `CanonicalJobSpec` JSON (must match `runtime/internal/jobs.CanonicalJobSpec`): `trigger` + опциональные `catchup`, `overlap`, `deps`, `maxAttempts`, `leaseTtlMs`, `maxConcurrent`, `retry`. Поля времени — unix-ms (`runAtUnixMs`) и ms (`everyMs`, `leaseTtlMs`). |
| `Logger` / `IdentityProvider` | interface @internal | — | Контракты для `SubscriberDeps`: `Logger {warn,error}`, `IdentityProvider {serviceId, instanceId}`. |

## Архитектурные решения и почему

- **Регистрация через общий `Registry`-канал** (`registry._handle.job` → `IncomingMethod{type=METHOD_TYPE_JOB}`) — единый путь с `sb.rpc.handle` / `sb.event.handle` / `sb.workflow.handle`. Меньше путей, меньше синхронизационных проблем.
- **Canonical-spec JSON в `input_schema_json`** — повторяет паттерн workflows (graph JSON в том же поле). `contract_hash` = SHA-256 от canonical. SDK и Go обязаны держать одинаковую структуру (см. `domain.ts::CanonicalJobSpec` ↔ `runtime/internal/jobs/canonical.go::CanonicalJobSpec`).
- **Дата-плейн отдельным gRPC-сервисом `Jobs`** — Subscribe/JobResult/Heartbeat. То же что у events/workflows: registration унифицирована, дата-плейн доменно-специфичный.
- **Lookup через `JobDomain._byName`**, а не через `Handle._entries`: handlers есть в Handle entries для общего finalize/debugging, но per-job opts (`maxConcurrent` и т.д.) нужны для семафора — JobDomain держит их в своей мапе.
- Cron строго 5-field: 6-field cron отклоняется на стороне SDK до отправки на рантайм.
- Heartbeat 5 с, порог 3 — соответствует lease reclaim логике рантайма.
- Reconnect ladder `[1, 5, 15, 30, 60]` с — идентично `workflow/subscriber.ts`. Таймер ожидания между попытками хранится в `_reconnectTimer` и отменяется в `stop()` — непрокинутый таймер держал бы event loop живым до следующей ступени лестницы после закрытия сабскрайбера.
- `error.retryable = false` — пользователь может явно отправить в DLQ.

## Зависимости

Зависит от: `../registry/registry` (type `Registry`; регистрация идёт через `registry._handle.job(name, contractHash, json, fn)`), `../pb/servicebridge/v1/jobs` (`JobsClient`, `JobExecution`, `JobResultRequest`), `node:crypto` (sha256 для contract hash), `@grpc/grpc-js` (типы `ClientReadableStream`, `ServiceError`). Trace-пропагация в handler приходит инъекцией `runWithTrace` через `SubscriberDeps` — модуль `job` не импортирует `../telemetry/*` напрямую.

Runtime side: `runtime/internal/jobs/{canonical.go,register.go}` (адаптер `RegisterJobs` + parsing `CanonicalJobSpec`). Дата-плейн — gRPC-сервис `Jobs` (`Subscribe` / `JobResult` / `Heartbeat`).

Используется: `connection/service-bridge` (монтирует `JobDomain` как `sb.job`, конструирует `JobSubscriber` с `runWithTrace`, который парсит `X-SB-Trace` через `../telemetry/wire-trace` и оборачивает handler в ALS из `../telemetry/context`).
