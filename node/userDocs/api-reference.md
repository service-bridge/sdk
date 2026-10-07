# API reference

← [Operations](./operations.md) · Дальше: [References](./references.md) →

Полный публичный API surface SDK. Внутренние типы (`@internal`) описаны в module README в `sdk/node/src/*/README.md`.

## Импорт

Пакет называется `service-bridge`. Корневой импорт даёт класс, ошибки и типы:

```ts
import {
  ServiceBridge,
  ServiceBridgeError,   // база всех ошибок SDK: .code, .retryable
  ConfigurationError,   // CONFIG
  StateError,           // STATE
  ValidationError,      // VALIDATION
  AccessDeniedError,    // ACCESS_DENIED
  TimeoutError,         // TIMEOUT
  NoLiveInstanceError,  // NO_LIVE_INSTANCE
  HandlerError,         // HANDLER: ответ хендлера callee, .handlerCode
  ConnectionError,      // CONNECTION: сбой control plane, .grpcCode
  InvalidEventNameError,
  WorkflowAccessDeniedError,
  WorkflowNotFoundError,
  WorkflowTerminalError,
  WorkflowValidationError,
  JsonPathError,
  // типы
  type ErrorCode,
  type ServiceBridgeOptions,
  type Logger,
  type AdvertiseConfig,
  type CallOpts,
  type RetryOpts,
  type Identity,
  type MethodDescriptor,
  type MethodType,
  type ServiceDeps,
  type RpcHandlerOpts,
  type RpcHandlerContext,
  type RpcHandlerFn,
  type RpcStreamHandlerFn,
  type EventHandlerFn,
  type EventHandlerOpts,
  type EventHandlerContext,
  type WorkflowHandlerOpts,
  type SchemaSpec,
  type PublishOpts,
  type JobOpts,
  type JobHandlerCtx,
  type Trigger,
  type CronTrigger,
  type DelayedTrigger,
  type IntervalTrigger,
  type CatchupPolicy,
  type OverlapPolicy,
  type RetryPolicy,
  type DeclaredDep,
  type TypedClient,
  type ConnectedEvent,
  type ReconnectingEvent,
  type DisconnectedEvent,
  type DrainingEvent,
  type PolicyViolationEvent,
} from "service-bridge";
```

HTTP-интеграции — отдельные subpath-импорты (`service-bridge/express`, `service-bridge/fastify`, `service-bridge/hono`), см. [Integrations](./integrations.md). Харнесс для юнит-тестов — `service-bridge/testing`, см. [Тестирование](./testing.md).

Пакет работает на Node.js 22, 24, 26 и Bun ≥ 1.3.13 (интеграция Fastify — только Node).

## ServiceBridge

Один объект на инстанс. Подключается к runtime по mTLS gRPC (control plane SDK на `:14445`), держит весь lifecycle и предоставляет домены `rpc`, `event`, `workflow`, `job`.

### Constructor

```ts
new ServiceBridge(
  url: string,                        // "host:port", напр. "localhost:14445"
  key: string,                        // bootstrap service key
  options?: ServiceBridgeOptions,
)
```

Конструктор проверяет `url` (нужен `host:port`), ключ и числовые опции; неверное значение — `ConfigurationError` сразу.

### Lifecycle

```ts
start(): Promise<void>   // резолвится после Welcome и первого snapshot реестра (startTimeoutMs)
ready(): Promise<void>   // текущая сессия жива и её snapshot применён
stop(): Promise<void>    // упорядоченная остановка, идемпотентна
```

Все объявления (`sb.service(...)`, `sb.rpc.handle(...)`, `sb.event.define(...)`, `sb.event.handle(...)`, `sb.workflow.handle(...)`, `sb.job.handle(...)`, `sb.useSchema(...)`, `sb.client(...)`) выполняются **до** `start()` — они уходят в первый RegisterRequest. Исходящие вызовы (`sb.rpc.call`, `sb.stream`, `sb.event.publish`, `sb.workflow.start`) работают после того, как `start()` резолвился; вызов до этого — `StateError`.

Если `start()` не дождался Welcome и snapshot за `startTimeoutMs`, или runtime ответил неустранимой ошибкой, бридж останавливается и `start()` бросает (`TimeoutError`, `ConnectionError`, `ConfigurationError`, `ValidationError`, `AccessDeniedError`). Повторный `start()` — `StateError`. Порядок `stop()` — [Operations §2](./operations.md#2-lifecycle-start--ready--stop).

### Identity & registry

```ts
identity(): Identity | null                              // null до первого Welcome / после stop()
serviceMap(): ReadonlyMap<string, ServiceMapEntry>
policyEvaluation(): PolicyEvaluation | null              // последний снапшот политики от runtime
instanceIdString(): string                               // "" до первого Welcome
```

`serviceMap()` группирует по `serviceName`: для каждого сервиса — видимые методы (`methods`), живые инстансы с endpoint'ами (`instances`), а также `eventSubscriptions` и `outgoingCalls` (ADR-0004).

### Домены

```ts
sb.rpc       // RpcDomain — входящие RPC-хендлеры и исходящие вызовы
sb.event     // EventDomain — объявление, подписка, публикация событий
sb.workflow  // WorkflowDomain — регистрация и запуск workflow
sb.job       // JobDomain — регистрация cron/delayed/interval джобов
```

### Outgoing declarations

```ts
sb.service(serviceName: string, deps: ServiceDeps): void

// deps: { rpc?: string[]; workflows?: string[]; http?: string[] }
// http: ["GET /api/foo"] — декларация для Service Map; runtime НЕ проксирует HTTP.
// Фактические вызовы делает пользователь (fetch и т. п.), см. ADR 0001.

sb.useSchema(
  serviceName: string,
  methodName: string,
  spec: SchemaSpec,
): Promise<void>
```

`useSchema` регистрирует SchemaPair на стороне caller'а для пары (service, method) до первого `sb.rpc.call`. Схема обязана совпадать со схемой целевого сервиса (тот же `.proto`). Эргономичная альтернатива, которая разом объявляет зависимость, грузит схемы и даёт типизированные вызовы, — `sb.client()`.

### Typed client

```ts
sb.client(
  serviceName: string,
  protoFile: string,
  opts?: { methods?: string[]; callDefaults?: CallOpts },
): Promise<TypedClient>
```

Читает `.proto`, объявляет все методы его `service`-блока как исходящие зависимости, грузит схемы и возвращает proxy с типизированными методами. Вызывать **до** `start()`. `methods` ограничивает набор; иначе экспонируются все методы service-блока.

```ts
const payment = await sb.client("payment-svc", "./payment.proto");
await sb.start();
const res = await payment.Charge({ userId: "u", amount: 100 });

// streaming-метод (rpc Generate(...) returns (stream Chunk)) определяется автоматически:
for await (const chunk of payment.Generate({ prompt: "..." })) { /* ... */ }
```

### Outbound calls

Unary RPC живёт в домене `rpc`; server-streaming — метод самого `sb`:

```ts
sb.rpc.call<Req, Res>(
  serviceName: string,
  methodName: string,
  payload: Req,
  opts?: CallOpts,
): Promise<Res>

sb.stream<Req, Chunk>(
  serviceName: string,
  methodName: string,
  payload: Req,
  opts?: CallOpts,
): AsyncIterable<Chunk>
```

Прерывание `for await`-цикла (break/return) закрывает gRPC-стрим, что доходит до callee. Стримы не ретраятся.

### RPC handlers (`sb.rpc`)

```ts
sb.rpc.handle<Req, Res>(
  name: string,
  fn: (req: Req, ctx: RpcHandlerContext) => Promise<Res> | Res,
  opts: RpcHandlerOpts,                 // { schema: SchemaSpec; captureMode?: "all"|"errors"|"none" }
): void

sb.rpc.handleStream<Req, Chunk>(
  name: string,
  fn: (req: Req, ctx: RpcHandlerContext) => AsyncIterable<Chunk>,
  opts: RpcHandlerOpts,
): void

interface RpcHandlerContext {
  signal: AbortSignal;          // отмена вызывающим или истёкший дедлайн
  deadline: number | null;      // абсолютный дедлайн, unix-ms
  requestId: string;
  idempotencyKey: string;       // "" если вызывающий не задал
  caller: { serviceId: string; instanceId: string } | null;  // instanceId "" при вызове через proxy
}
```

`schema` обязателен у каждого хендлера; второй хендлер на то же имя — `ValidationError`. `captureMode` может только сузить эффективный режим payload-capture, пушнутый runtime (порядок приватности `none < errors < all`), но не расширить.

Бизнес-ошибку хендлер бросает как `new HandlerError(code, message)` — вызывающий получит `HandlerError` с тем же `handlerCode`. Любая другая брошенная ошибка доходит до вызывающего как `HandlerError` с `handlerCode: "INTERNAL"`.

`sb.rpc.call` при отказе политики бросает `AccessDeniedError` и эмитит `policy_violation`.

### Events (`sb.event`)

```ts
sb.event.define(name: string, spec: SchemaSpec): void

sb.event.handle(
  pattern: string,                     // имя или AMQP-шаблон: "order.*", "order.#"
  fn: (payload: unknown, ctx: EventHandlerContext) => Promise<void> | void,
  opts?: EventHandlerOpts,             // { schema?: SchemaSpec; filter?: Record<string, unknown> }
): void

sb.event.publish<T>(
  name: string,
  payload: T,
  opts?: PublishOpts,
): Promise<{ eventId: string }>
```

`define` объявляет публикуемое событие; `spec` — тот же `SchemaSpec`, что у RPC-хендлеров (`.proto` или `.schema.json`). Публикация события без `define` — `StateError`. Имя события — точки-разделённые сегменты из `[a-z0-9_-]`; нарушение — `InvalidEventNameError`.

`handle` — подписка, одна на шаблон в процессе (дубль — `ValidationError`). Подписчик не вызывает `define`: payload декодирует `opts.schema`, без неё хендлер получает сырые байты (`Uint8Array`). `opts.filter` — объект `{"$.path": literal}`, все условия — равенства; вычисляет runtime.

```ts
interface EventHandlerContext {
  eventId: string;
  eventName: string;            // конкретное имя, под которым событие опубликовано
  attempt: number;
  deliveryId: string;
  leaseToken: string;
  partitionKey: string;
  headers: Record<string, string>;
  occurredAtMs: number;
  signal: AbortSignal;          // обрыв стрима доставки или остановка бриджа
}
```

`publish` резолвится, когда runtime сохранил событие. Пока runtime недоступен, событие ждёт в очереди в памяти (`maxPendingPublishes`, по умолчанию 10000; переполнение — `QUEUE_FULL`) не дольше `publishTimeoutMs` (по умолчанию 30 с; истёк — `TimeoutError`).

```ts
interface PublishOpts {
  idempotencyKey?: string;      // дедуп на runtime; повтор с тем же содержимым — успех с id исходного события
  partitionKey?: string;        // FIFO в рамках ключа
  fireAndForget?: boolean;      // резолв сразу после постановки в очередь; потеря при падении процесса
  headers?: Record<string, string>;
  occurredAtMs?: number;        // unix-ms, по умолчанию Date.now()
}
```

### Workflows (`sb.workflow`)

```ts
sb.workflow.handle(name: string, def: WorkflowDef): void

sb.workflow.start(service: string, name: string, input: unknown, opts?: { idempotencyKey?: string; timeoutMs?: number }): Promise<{ runId: string }>
sb.workflow.signal(runId: string, signalName: string, payload: unknown, opts?: { signalId?: string }): Promise<{ duplicate: boolean }>
sb.workflow.cancel(runId: string): Promise<void>
sb.workflow.await(runId: string): Promise<Record<string, unknown>>   // выход при "success"; иначе WorkflowRunFailedError
sb.workflow.query(runId: string): Promise<RunSnapshot>              // status, stopReason, waitingReason, output, steps, signals
sb.workflow.replay(runId: string, opts?: { fromStepId?: string }): Promise<{ runId: string }>
sb.workflow.retryCompensation(runId: string): Promise<void>
```

`def` — `WorkflowDef` (DAG из `steps`); DAG интерпретирует runtime, SDK исполняет задачи (`local`, `call`, `publish`, компенсации). Полная модель — [Workflows](./workflows.md).

```ts
interface WorkflowDef {
  steps: Step[];
  version?: string;                  // версия кода local-шагов
  input?: Record<string, unknown>;   // JSON Schema входа
  retry?: RetryPolicy;               // повторы задач по умолчанию
  maxParallelism?: number;           // одновременных задач прогона, 0 — без лимита
  timeoutMs?: number;                // таймаут прогона → timed_out
}
```

Caller-side операции требуют завершённого `start()`. Статусы прогона: `active`, `compensating`, `success`, `failed`, `cancelled`, `timed_out`, `failed_compensated`.

### Jobs (`sb.job`)

```ts
sb.job.handle(name: string, opts: JobOpts, fn: (ctx: JobHandlerCtx) => Promise<void>): void

interface JobOpts {
  version: string;                                     // обязательна: версия исполняемого поведения
  trigger: Trigger;                                    // ровно один из cron|delayed|interval
  catchup?: "skip" | "fire_once" | "fire_all";
  overlap?:  "skip" | "allow" | "buffer_one";
  deps?: Array<{ rpc: string } | { event: string } | { workflow: string }>;
  maxAttempts?: number;
  leaseTtlMs?: number;
  maxConcurrent?: number;
  retry?: { initialMs: number; maxMs: number; multiplier: number; jitter: number };
}

type Trigger =
  | { cron: string; tz?: string }                      // 5-польный cron, без секунд
  | { delayed: { at: Date | string | number } }
  | { interval: number };                              // период в ms, > 0

interface JobHandlerCtx {
  jobName: string;
  executionId: string;
  scheduledAt: Date;
  localScheduledAt: Date;
  attempt: number;
  idempotencyKey: string;
  signal: AbortSignal;
}
```

Сигнатура — `(name, opts, fn)` (opts **перед** функцией). Джобы — self-only: у них нет входящих/исходящих handler-зависимостей кроме явных `deps`. Подробности — [Jobs](./jobs.md).

### Connection events

```ts
sb.on("connected",       (e: ConnectedEvent)       => void): this
sb.on("reconnecting",    (e: ReconnectingEvent)    => void): this
sb.on("draining",        (e: DrainingEvent)        => void): this
sb.on("disconnected",    (e: DisconnectedEvent)    => void): this
sb.on("policy_violation",(e: PolicyViolationEvent) => void): this
```

`on` возвращает сам `sb` (чейнится). Метода `off` нет — слушатели живут до конца жизни объекта. Исключение в слушателе логируется и не влияет ни на бридж, ни на других слушателей.

## Types

### ServiceBridgeOptions

```ts
interface ServiceBridgeOptions {
  reconnectIntervalMs?: number;        // плоская задержка; без неё — лестница 1s,5s,15s,30s,60s ±20%
  reconnectAttempts?: number;          // default 0 = без лимита; считаются подряд идущие неудачи
  advertise?: AdvertiseConfig | false; // default: undefined → "127.0.0.1" на свободном порту (+warning)
  callDefaults?: CallOpts;             // дефолты для sb.rpc.call, sb.stream и typed-клиентов
  failOnPolicyViolation?: boolean;     // default false; true — warning политики останавливает бридж
  publishTimeoutMs?: number;           // default 30000 — сколько publish ждёт ACK runtime
  maxPendingPublishes?: number;        // default 10000 — очередь publish до QUEUE_FULL
  eventsMaxInFlight?: number;          // default 32 — параллельных inbound-доставок
  rpcMaxConcurrentCalls?: number;      // default 256 — одновременных inbound-хендлеров
  rpcMaxQueuedCalls?: number;          // default = rpcMaxConcurrentCalls — очередь до RESOURCE_EXHAUSTED
  startTimeoutMs?: number;             // default 30000 — дедлайн start()
  stopTimeoutMs?: number;              // default 10000 — дедлайн дренажа в stop()
  logger?: Logger;                     // диагностика SDK; default — warn/error в консоль
  telemetry?: {
    onDrop?: (info: { serverDrops: number; ringDrops: number; backpressureLevel: number }) => void;
  };
}

interface Logger {
  debug(message: string, attrs?: Record<string, unknown>): void;
  info(message: string, attrs?: Record<string, unknown>): void;
  warn(message: string, attrs?: Record<string, unknown>): void;
  error(message: string, attrs?: Record<string, unknown>): void;
}

interface AdvertiseConfig {
  host: string;
  port: number;     // 0 = OS подбирает свободный порт
}
```

`advertise: false` — явный caller-only режим: inbound Call-сервер не поднимается (инстанс никогда не обслуживает RPC). По умолчанию (`undefined`) SDK биндит `127.0.0.1` на свободном порту с предупреждением — loopback недостижим с других хостов, в контейнерах/k8s задавайте явный `{ host, port }`.

Telemetry on/off и payload cap задаются в UI рантайма (Settings → Telemetry), а не в конструкторе. Настройки `telemetry.enable` и `telemetry.payload_max_bytes` пушатся в SDK через поля `CaptureModes.telemetry_enabled` / `CaptureModes.payload_max_bytes` в registry snapshot. Fail-safe до первого снапшота: transport включён, cap = 65536 байт.

Режим захвата payload по каналам тоже пушит runtime (по умолчанию `errors`; до первого снапшота — `none`). SDK передаёт payload как есть, маскирование секретов делает runtime при приёме.

### CallOpts

```ts
interface CallOpts {
  signal?: AbortSignal;                      // отмена: ServiceBridgeError с кодом CANCELLED
  timeout?: string;                          // "500ms" | "10s" | "2m" — default "30s", на весь логический вызов
  requestId?: string;                        // авто UUID, если не задан
  idempotencyKey?: string;                   // уходит callee (ctx.idempotencyKey) и в дедуп runtime proxy
  transport?: "direct" | "proxy" | "auto";   // default "auto"
  retry?: Partial<RetryOpts>;                // только для pre-dispatch отказов
}

interface RetryOpts {
  maxAttempts: number;   // default 3 (1 = retry off)
  baseDelayMs: number;   // default 200
  factor: number;        // default 2 (exponential)
  maxDelayMs: number;    // default 5000
  jitter: number;        // default 0.3 (±30%), доля в [0,1]
}
```

`transport`: `direct` — caller → callee по mTLS, никогда не через runtime; `proxy` — всегда через runtime `Invoke`; `auto` — direct к выбранному инстансу, а после отказа, доказанно случившегося до отправки, следующая попытка идёт через proxy.

SDK повторяет вызов только при отказе, который доказанно произошёл до запуска хендлера: нет кандидата, канал к callee не стал готов в пределах дедлайна, или статус с трейлером `x-sb-not-dispatched`. `idempotencyKey` не делает отправленный вызов повторяемым. Подробнее — [RPC §6](./rpc.md#6-resilience-lb-cb-retry).

### Identity

```ts
interface Identity {
  sessionId: string;
  serviceId: string;
  serviceName: string;
  instanceId: string;
}
```

### MethodDescriptor

```ts
interface MethodDescriptor {
  serviceName: string;
  serviceId: string;
  instanceId: string;
  type: MethodType;          // enum: RPC | EVENT | WORKFLOW | JOB (METHOD_TYPE_*)
  name: string;
  contractHash: string;
  published: boolean;        // true для published-события, false для входящего хендлера
  inputSchema: Buffer;
  outputSchema: Buffer;
  streaming: boolean;
}
```

Endpoint'ы инстансов (`callEndpoint` для gRPC, `httpEndpoint` для HTTP-сервера пользователя) лежат на `ServiceInstanceInfo` в `ServiceMapEntry.instances`, а не на `MethodDescriptor`.

### Connection events

```ts
interface ConnectedEvent {
  sessionId: string;
  serviceId: string;
  serviceName: string;
  runtimeVersion: string;
}

interface ReconnectingEvent {
  attempt: number;       // начинается с 1
  delayMs: number;
  reason: string;
}

interface DrainingEvent {
  reason: string;        // runtime объявил остановку; reconnect последует сам
}

interface DisconnectedEvent {
  reason: string;        // текст ошибки, из-за которой бридж остановился окончательно
  error?: ServiceBridgeError;
}

interface PolicyViolationEvent {
  declaration: string;   // "rpc.call" | "rpc.handle" | "event.publish" | "workflow.run" | ...
  value: string;         // напр. "payments/charge", "orders.*"
  denySide: string;      // "capability" | "self_egress" | "self_acceptance" | "peer_acceptance"
  reason: string;
}
```

### Schemas

```ts
type SchemaSpec = ProtoFileSpec | JsonSchemaFileSpec;

interface ProtoFileSpec {
  protoFile: string;
  input?: string;     // имя input message — нужно только для .proto без service-блока
  output?: string;    // имя output message
  method?: string;    // имя метода в service-блоке (для multi-method файлов; обычно проставляется автоматически)
}

interface JsonSchemaFileSpec {
  schemaFile: string;   // путь к .schema.json с явными fieldNumber на каждое поле
}
```

`.schema.json` описывает оба сообщения с явными номерами полей:

```jsonc
{
  "input":  { "Charge":       { "userId": { "type": "string", "fieldNumber": 1 },
                                 "amount": { "type": "int64",  "fieldNumber": 2 } } },
  "output": { "ChargeResult": { "ok":     { "type": "bool",   "fieldNumber": 1 } } }
}
```

Типы полей: `string | bool | int32 | int64 | uint32 | uint64 | float | double | bytes | object | array`. Для `object` — вложенный `nested`, для `array` — `array: { type, nested? }`. `fieldNumber` обязателен (нужен для эволюции схемы); пропуск или дубль — ошибка загрузки.

### TypedClient

```ts
type TypedClient = Record<
  string,
  ((req: unknown, opts?: CallOpts) => Promise<unknown>) &
    ((req: unknown, opts?: CallOpts) => AsyncIterable<unknown>)
>;
```

Каждый ключ proxy — функция, типизированная как unary (`Promise<...>`) ИЛИ stream (`AsyncIterable<...>`) в зависимости от `responseStream` метода в `.proto`. Второй аргумент — per-call `opts`.

### HTTP integrations

HTTP-сервер запускает пользователь (Express / Fastify / Hono). SDK через subpath-импорт собирает роуты и публикует HTTP-endpoint в Service Map. Полный гайд — [Integrations](./integrations.md).

```ts
import { attachExpress } from "service-bridge/express";
import { sbFastify } from "service-bridge/fastify";
import { attachHono } from "service-bridge/hono";
```

### Testing

`createTestHarness()` — настоящий `ServiceBridge`, запущенный на in-memory runtime: хендлеры регистрируются обычным API, вызовы проходят через продакшен-код (схемы, маппинг ошибок, Publisher, Subscriber). Полный гайд — [Тестирование](./testing.md).

```ts
import {
  createTestHarness,
  matchPattern,
  TEST_IDENTITY,
  type TestHarness,
  type InvokeOpts,
  type Responder,
  type CallRecord,
  type PublishedRecord,
  type DeliverOpts,
  type DeliveryResult,
} from "service-bridge/testing";
```

## Ошибки

Каждая ошибка SDK — `ServiceBridgeError`:

```ts
class ServiceBridgeError extends Error {
  readonly code: ErrorCode;
  readonly retryable: boolean;   // code ∈ { CONNECTION, NO_LIVE_INSTANCE, OVERLOADED, QUEUE_FULL }
  // cause — исходная ошибка, если была
}

type ErrorCode =
  | "CONFIG" | "STATE" | "CONNECTION" | "TIMEOUT" | "CANCELLED" | "ACCESS_DENIED"
  | "NOT_FOUND" | "VALIDATION" | "CONFLICT" | "TERMINAL" | "NO_LIVE_INSTANCE"
  | "OVERLOADED" | "QUEUE_FULL" | "INVALID_EVENT_NAME" | "HANDLER" | "INTERNAL";
```

| Класс | `code` | Когда |
|---|---|---|
| `ConfigurationError` | `CONFIG` | Неверная опция, ключ, URL, таймаут; нет схемы вызывающего. |
| `StateError` | `STATE` | Операция не в той фазе: вызов до `start()`, повторный `start()`, publish незадекларированного события. |
| `ValidationError` | `VALIDATION` | Невалидная декларация или payload, пойманные локально; отказ runtime `INVALID_ARGUMENT`/`FAILED_PRECONDITION`/`OUT_OF_RANGE`. |
| `AccessDeniedError` | `ACCESS_DENIED` | Отказ политики доступа или отозванный пир. |
| `TimeoutError` | `TIMEOUT` | Дедлайн истёк; исход на стороне callee неизвестен. |
| `NoLiveInstanceError` | `NO_LIVE_INSTANCE` | Некуда отправить вызов: нет инстанса с совпадающим контрактом, нет endpoint, все в circuit-open. |
| `HandlerError` | `HANDLER` | Ответ хендлера callee; `handlerCode` — бизнес-код или `"INTERNAL"`; `remote: true` у полученной вызовом. |
| `ConnectionError` | `CONNECTION` | Сбой control plane (provision, сессия, реестр, сертификат); `grpcCode` — статус runtime или `-1`. |
| `InvalidEventNameError` | `INVALID_EVENT_NAME` | Имя события не проходит `^[a-z0-9_-]+(\.[a-z0-9_-]+)*$`. |
| `WorkflowAccessDeniedError` / `WorkflowNotFoundError` / `WorkflowTerminalError` | `ACCESS_DENIED` / `NOT_FOUND` / `TERMINAL` | См. [Workflows](./workflows.md). |
| `WorkflowValidationError` / `JsonPathError` | `VALIDATION` | Невалидный граф workflow / выражение `$.`. |

Коды без отдельного класса (`CANCELLED`, `NOT_FOUND`, `CONFLICT`, `OVERLOADED`, `QUEUE_FULL`, `INTERNAL`) приходят как `ServiceBridgeError` — различайте по `err.code`. `TIMEOUT` не входит в retryable: исход неизвестен, повтор безопасен только с ключом идемпотентности.

Как gRPC-статусы отображаются в коды и что делать с каждым — [RPC §9](./rpc.md#9-ошибки).

→ Дальше: [References](./references.md)
