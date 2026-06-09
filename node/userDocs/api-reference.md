# API reference

← [Operations](./operations.md) · Дальше: [References](./references.md) →

Полный публичный API surface SDK. Внутренние типы (`@internal`) описаны в module README в `sdk/node/src/*/README.md`.

## Импорт

Пакет называется `servicebridge`. Корневой импорт даёт класс, ошибки и типы:

```ts
import {
  ServiceBridge,
  ServiceBridgeError,
  RpcAccessDeniedError,
  WorkflowAccessDeniedError,
  InvalidEventNameError,
  OutboxFullError,
  // типы
  type ServiceBridgeOptions,
  type AdvertiseConfig,
  type CallOpts,
  type RetryOpts,
  type Identity,
  type MethodDescriptor,
  type ServiceDeps,
  type RpcHandlerOpts,
  type WorkflowHandlerOpts,
  type SchemaSpec,
  type PublishOpts,
  type JobOpts,
  type Trigger,
  type JobHandlerCtx,
  type TypedClient,
  type ConnectedEvent,
  type ReconnectingEvent,
  type DisconnectedEvent,
  type PolicyViolationEvent,
} from "servicebridge";
```

HTTP-интеграции — отдельные subpath-импорты (`servicebridge/express`, `servicebridge/fastify`, `servicebridge/hono`), см. [Integrations](./integrations.md).

## ServiceBridge

Один объект на инстанс. Подключается к runtime по mTLS gRPC (control plane SDK на `:14445`), держит весь lifecycle и предоставляет домены `rpc`, `event`, `workflow`, `job`.

### Constructor

```ts
new ServiceBridge(
  url: string,                        // напр. "https://localhost:14445"
  key: string,                        // bootstrap service key
  options?: ServiceBridgeOptions,
)
```

### Lifecycle

```ts
start(): Promise<void>
stop(): Promise<void>
```

Все объявления (`sb.service(...)`, `sb.rpc.handle(...)`, `sb.event.define(...)`, `sb.workflow.handle(...)`, `sb.job.handle(...)`, `sb.useSchema(...)`, `sb.client(...)`) должны выполняться **до** `start()` — они уходят в первый RegisterRequest. Исходящие вызовы (`sb.rpc.call`, `sb.stream`, `sb.event.publish`, `sb.workflow.start`) работают только после события `connected`.

### Identity & registry

```ts
identity(): Identity | null                              // null до первого Welcome / после stop()
serviceMap(): ReadonlyMap<string, ServiceMapEntry>
policyEvaluation(): PolicyEvaluation | null              // последний снапшот политики от runtime
instanceIdString(): string                               // "" до первого Welcome
```

`serviceMap()` группирует по `serviceName`: для каждого сервиса — видимые методы (`methods`), живые инстансы с endpoint'ами (`instances`), а также `eventSubscriptions` и `outgoingCalls` (ADR-0014).

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

Прерывание `for await`-цикла (break/return) закрывает gRPC-стрим, что доходит до callee. Retry к стримам не применяется (single-pick by design, ADR 0004).

### RPC handlers (`sb.rpc`)

```ts
sb.rpc.handle<Req, Res>(
  name: string,
  fn: (req: Req) => Promise<Res> | Res,
  opts: RpcHandlerOpts,                 // { schema: SchemaSpec; captureMode?: "all"|"errors"|"none" }
): void

sb.rpc.handleStream<Req, Chunk>(
  name: string,
  fn: (req: Req) => AsyncIterable<Chunk>,
  opts: RpcHandlerOpts,
): void
```

`schema` обязателен у каждого хендлера. `captureMode` может только сузить эффективный режим payload-capture, пушнутый runtime (порядок приватности `none < errors < all`), но не расширить.

`sb.rpc.call` бросает `RpcAccessDeniedError` при denial политики (gRPC PERMISSION_DENIED) и эмитит `policy_violation`.

### Events (`sb.event`)

```ts
sb.event.define(name: string, spec?: SchemaSpec): void

sb.event.handle(
  pattern: string,
  fn: (payload: unknown) => Promise<void> | void,
): void

sb.event.publish<T>(
  name: string,
  payload: T,
  opts?: PublishOpts,
): Promise<{ eventId: string }>
```

`define` объявляет публикуемое событие; `spec` — тот же `SchemaSpec`, что у RPC-хендлеров (`.proto` или `.schema.json`). Без `spec` событие публикуется как непроверяемый JSON. Имя события — точки-разделённые сегменты из `[a-z0-9_-]`; нарушение бросает `InvalidEventNameError`. Если outbox переполнен — `OutboxFullError`.

```ts
interface PublishOpts {
  idempotencyKey?: string;
  partitionKey?: string;
  fireAndForget?: boolean;
  headers?: Record<string, string>;
  occurredAtMs?: number;        // unix-ms
}
```

### Workflows (`sb.workflow`)

```ts
sb.workflow.handle(name: string, def: WorkflowDef, opts?: WorkflowHandlerOpts): void

sb.workflow.start(name: string, input: unknown, opts?: WorkflowStartOpts): Promise<{ runId: string }>
sb.workflow.signal(runId: string, signalName: string, payload: unknown): Promise<void>
sb.workflow.cancel(runId: string): Promise<void>
sb.workflow.await(runId: string): Promise<Record<string, unknown>>   // ждёт терминального статуса; reject если статус != "success"
sb.workflow.query(runId: string): Promise<{ status: string; state: Record<string, unknown>; steps: Array<{ stepId; status; output; lastError; compensatedBy? }> }>
sb.workflow.replay(runId: string, opts?: { fromStepId?: string }): Promise<{ runId: string }>
```

`def` — это `WorkflowDef` (DAG из `steps`), а не массив `{name, fn}`. У workflow нет финального output как сущности: каждый step возвращает обновлённый `state`. Полная модель шагов и DAG — [Workflows](./workflows.md).

```ts
interface WorkflowDef {
  steps: Step[];
  input?: Record<string, unknown>;   // JSON Schema для входа (ADR-W-009)
  retry?: Partial<RetryOpts>;
  maxParallelism?: number;
  timeoutSec?: number;
}
```

Caller-side операции (`start`/`signal`/…) требуют завершённого `start()`. `start` бросает `WorkflowAccessDeniedError` при denial политики; терминальный статус успеха — строка `"success"`.

### Jobs (`sb.job`)

```ts
sb.job.handle(name: string, opts: JobOpts, fn: (ctx: JobHandlerCtx) => Promise<void>): void

interface JobOpts {
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
sb.on("disconnected",    (e: DisconnectedEvent)    => void): this
sb.on("policy_violation",(e: PolicyViolationEvent) => void): this
```

`on` возвращает сам `sb` (чейнится). Метода `off` нет — слушатели живут до конца жизни объекта.

## Types

### ServiceBridgeOptions

```ts
interface ServiceBridgeOptions {
  reconnectIntervalMs?: number;        // default 3000
  reconnectAttempts?: number;          // default 3 (0 = unlimited)
  advertise?: AdvertiseConfig | false; // default: auto — SB_ADVERTISE_HOST env > "127.0.0.1" на свободном порту (+warning)
  callDefaults?: CallOpts;             // дефолты для каждого sb.rpc.call / sb.stream
  failOnPolicyViolation?: boolean;     // default false — иначе warning при нарушении политики делает start() → disconnected
}

interface AdvertiseConfig {
  host: string;
  port: number;     // 0 = OS подбирает свободный порт
}
```

`advertise: false` — явный caller-only режим: inbound Call-сервер не поднимается (инстанс никогда не обслуживает RPC). По умолчанию (`undefined`) SDK берёт `SB_ADVERTISE_HOST`, иначе откатывается на `127.0.0.1` с предупреждением — loopback недостижим с других хостов, в контейнерах/k8s задавайте `SB_ADVERTISE_HOST` или явный `{ host, port }`.

### CallOpts

```ts
interface CallOpts {
  timeout?: string;                          // "500ms" | "10s" | "2m" — default "30s"
  requestId?: string;                        // авто UUID v4 если не задан
  idempotencyKey?: string;                   // НЕ авто — задайте, чтобы включить runtime-side dedup (ADR 0012)
  transport?: "direct" | "proxy" | "auto";   // default "auto"
  retry?: Partial<RetryOpts>;
}

interface RetryOpts {
  maxAttempts: number;   // default 3 (1 = retry off)
  baseDelayMs: number;   // default 200
  factor: number;        // default 2 (exponential)
  maxDelayMs: number;    // default 5000
  jitter: number;        // default 0.3 (±30%), доля в [0,1]
}
```

`transport`: `direct` — caller → callee по mTLS (ошибка, если у callee нет call_endpoint); `proxy` — через runtime Invoke; `auto` — direct если endpoint известен, иначе proxy. `idempotencyKey` опциональный: без него INTERNAL/ABORTED/UNKNOWN-ошибки считаются non-retryable.

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
}

interface ReconnectingEvent {
  attempt: number;       // начинается с 1
  delayMs: number;
  reason: string;
}

interface DisconnectedEvent {
  reason: string;        // "exhausted" | "drain: ..." | "policy ..." | текст ошибки
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
import { attachExpress } from "servicebridge/express";
import { sbFastify } from "servicebridge/fastify";
import { attachHono } from "servicebridge/hono";
```

## ServiceBridgeError

```ts
class ServiceBridgeError extends Error {
  readonly code: number;   // gRPC status code; -1 если код не распознан
  // name всегда "ServiceBridgeError"; message = "<scope>: <cause>"
}
```

Не-retryable коды: `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT` — всё остальное (включая `-1`) считается транзиентным и ведёт к reconnect. Подробности про codes и retry: [RPC §9 Ошибки](./rpc.md#9-ошибки), [RPC §6 Retry](./rpc.md#6-resilience-lb-cb-retry).

→ Дальше: [References](./references.md)
