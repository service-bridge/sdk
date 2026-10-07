# RPC

← [Quickstart](./quickstart.md) · Дальше: [Events](./events.md) →

Полный гайд по RPC: регистрация хендлеров, схемы, вызовы, транспорт, resilience, идемпотентность, версионирование контракта, ошибки. Читается линейно. Для операционных тем (lifecycle, mTLS, env) — [Operations](./operations.md).

## Содержание

- [Краткая модель](#краткая-модель)
- [1. Регистрация хендлеров](#1-регистрация-хендлеров)
- [2. Схемы](#2-схемы)
- [3. Исходящие вызовы](#3-исходящие-вызовы)
- [4. CallOpts](#4-callopts)
- [5. Транспорт: direct, proxy, auto](#5-транспорт-direct-proxy-auto)
- [6. Resilience: LB, CB, Retry](#6-resilience-lb-cb-retry)
- [7. Идемпотентность](#7-идемпотентность)
- [8. Версионирование контракта](#8-версионирование-контракта)
- [9. Ошибки](#9-ошибки)
- [10. Шпаргалка](#10-шпаргалка)

---

## Краткая модель

ServiceBridge различает **unary** (один запрос → один ответ) и **server-side streaming** (один запрос → поток ответов). У каждого вызова две стороны:

- **Callee** — регистрирует обработчик через `sb.rpc.handle(...)` или `sb.rpc.handleStream(...)` со схемой.
- **Caller** — либо декларирует зависимость и зовёт `sb.rpc.call(...)` / `sb.stream(...)`, либо использует typed client `sb.client(svc, .proto)` (рекомендуется).

Один SDK-инстанс может одновременно быть и callee, и caller для разных методов.

---

## 1. Регистрация хендлеров

Все регистрации **до `sb.start()`**. SDK отправляет полную декларацию в первом `RegisterRequest` и на каждом reconnect. Второй хендлер на то же имя — `ValidationError`.

### Unary

```ts
sb.rpc.handle<Req, Res>(
  name: string,
  fn: (req: Req, ctx: RpcHandlerContext) => Promise<Res> | Res,
  opts: { schema: SchemaSpec; captureMode?: "all" | "errors" | "none" },
): void
```

```ts
sb.rpc.handle<{ userId: string; amount: number }, { transactionId: string; ok: boolean }>(
  "Charge",
  async (req) => ({ transactionId: `tx-${req.userId}`, ok: req.amount > 0 }),
  { schema: { protoFile: "./payment.proto" } },
);
```

### Контекст вызова

Второй аргумент хендлера — `ctx`:

```ts
interface RpcHandlerContext {
  signal: AbortSignal;        // прерывается, когда вызывающий отменил вызов или истёк дедлайн
  deadline: number | null;    // абсолютный дедлайн, unix-ms; null — вызывающий не задал
  requestId: string;          // CallOpts.requestId вызывающего
  idempotencyKey: string;     // CallOpts.idempotencyKey вызывающего; "" если не задан
  caller: { serviceId: string; instanceId: string } | null;
}
```

`caller` — проверенная идентичность пира из его сертификата. При вызове через runtime proxy `instanceId` пустой. `null` — идентичность установить не удалось.

```ts
sb.rpc.handle("Charge", async (req: { orderId: string; amount: number }, ctx) => {
  const res = await fetch(BANK_URL, { method: "POST", body: JSON.stringify(req), signal: ctx.signal });
  return { ok: res.ok };
}, { schema: { protoFile: "./payment.proto" } });
```

### Server-side streaming

```ts
sb.rpc.handleStream<Req, Chunk>(
  name: string,
  fn: (req: Req, ctx: RpcHandlerContext) => AsyncIterable<Chunk>,
  opts: { schema: SchemaSpec },
): void
```

```ts
sb.rpc.handleStream<{ prompt: string }, { token: string }>(
  "Generate",
  async function* (req) {
    for await (const token of llm.generate(req.prompt)) {
      yield { token };
    }
  },
  { schema: { protoFile: "./ai.proto" } },
);
```

**Cancellation:** когда caller прерывает stream или истекает дедлайн, `ctx.signal` прерывается, а у генератора вызывается `return()`. Освобождайте ресурсы в `try/finally`:

```ts
sb.rpc.handleStream("Generate", async function* (req, ctx) {
  const upstream = llm.start(req.prompt, { signal: ctx.signal });
  try {
    for await (const t of upstream) yield { token: t };
  } finally {
    upstream.cancel();
  }
}, { schema: { protoFile: "./ai.proto" } });
```

### Возврат ошибок из хендлера

Бизнес-ошибку бросайте как `HandlerError(handlerCode, message)`. Код уходит вызывающему как есть, и тот получает `HandlerError` с тем же `handlerCode`:

```ts
import { HandlerError } from "service-bridge";

sb.rpc.handle("Charge", async (req) => {
  if (req.amount <= 0) throw new HandlerError("INVALID_AMOUNT", "amount must be positive");
  // ...
}, { schema: { protoFile: "./payment.proto" } });
```

Любая другая брошенная ошибка доходит до вызывающего как `HandlerError` с `handlerCode: "INTERNAL"` и тем же `message`. Это касается и `HandlerError`, который хендлер получил из своего вложенного вызова и пробросил дальше: бизнес-код чужого сервиса не становится ответом этого (у такой ошибки `remote === true`).

Отказы до запуска хендлера идут gRPC-статусом, а не `HandlerError`: неизвестный метод — `NOT_FOUND`, unary-вызов streaming-метода — `VALIDATION`, недекодируемый запрос — `VALIDATION`, отказ политики — `ACCESS_DENIED` (см. §9).

### Ограничения callee

Входящие вызовы ограничены опциями конструктора: `rpcMaxConcurrentCalls` (по умолчанию 256 хендлеров одновременно) и `rpcMaxQueuedCalls` (по умолчанию столько же в очереди). Сверх очереди вызывающий получает `OVERLOADED`, и этот отказ безопасно повторяется на другом инстансе. До первого snapshot реестра (политика ещё неизвестна) и во время `stop()` callee отвечает `UNAVAILABLE` с той же пометкой «не отправлено в хендлер».

---

## 2. Схемы

RPC payload идёт по сети как **Protobuf binary**. Два источника схем:

| Источник | Когда выбрать |
|----------|---------------|
| `.proto` | Стандарт: protoc-тулинг, `service` блок → typed client. |
| `.schema.json` | Когда proto-тулинг не подходит: JSON со строгой типизацией и обязательными `fieldNumber`. |

> Один и тот же контракт в `.proto` и `.schema.json` даёт **разные `contract_hash`** — caller и callee должны использовать один source kind.

### 2.1 .proto с service-блоком (рекомендуется)

```proto
syntax = "proto3";

service PaymentService {
  rpc Charge(ChargeRequest) returns (ChargeResponse);
  rpc Refund(RefundRequest) returns (RefundResponse);
  rpc Stream(StreamRequest) returns (stream StreamChunk);
}

message ChargeRequest  { string user_id = 1; double amount = 2; }
message ChargeResponse { string transaction_id = 1; bool ok = 2; }
// ...
```

```ts
// input/output авторезолвятся из service блока — указывать не нужно
sb.rpc.handle("Charge", chargeFn, { schema: { protoFile: "./payment.proto" } });
sb.rpc.handle("Refund", refundFn, { schema: { protoFile: "./payment.proto" } });
```

### 2.2 .proto без service-блока

```ts
sb.rpc.handle("create-order", fn, {
  schema: {
    protoFile: "./legacy.proto",
    input: "CreateOrderReq",
    output: "CreateOrderResp",
  },
});
```

### 2.3 .schema.json

```json
{
  "input": {
    "ChargeRequest": {
      "userId": { "type": "string", "fieldNumber": 1 },
      "amount": { "type": "double", "fieldNumber": 2 }
    }
  },
  "output": {
    "ChargeResponse": {
      "transactionId": { "type": "string", "fieldNumber": 1 },
      "ok":            { "type": "bool",   "fieldNumber": 2 }
    }
  }
}
```

```ts
sb.rpc.handle("Charge", fn, { schema: { schemaFile: "./payment.schema.json" } });
```

Поддерживаемые типы: `string`, `bool`, `int32/64`, `uint32/64`, `float`, `double`, `bytes`, `object` (nested), `array` (`repeated`). `fieldNumber` обязателен — без него fail-fast (защита от silent data corruption при эволюции схем).

### 2.4 Резолюция input/output (.proto)

Если `input`/`output` не указаны явно, SDK резолвит ровно двумя путями:

1. **Явные `input` + `output`** в `SchemaSpec` — всегда побеждают.
2. **`rpc <method>(In) returns (Out);`** в любом `service` блоке файла, где `<method>` = имя из `rpc.handle(name, ...)` / `sb.useSchema(svc, method, ...)`.

Никаких convention-based (`<Method>Request`/`<Method>Response`) или unique-pair фолбэков нет — они скрывали ошибки контракта за похожестью имён, поэтому удалены. Ни один путь не сработал → fail-fast: `serde: cannot resolve input/output for /path/foo.proto (method=X). Add a service { rpc <method>(In) returns (Out); } block or pass input and output explicitly.`

### 2.5 Caller-сторона

С typed client (см. §3.1) — схема грузится автоматически. Без — `sb.useSchema()`:

```ts
sb.service("payment-svc", { rpc: ["Charge"] });
await sb.useSchema("payment-svc", "Charge", { protoFile: "./payment.proto" });
```

---

## 3. Исходящие вызовы

### 3.1 Typed client (рекомендуется)

`sb.client(svc, .proto)` — единая точка: декларация зависимостей + загрузка схем + proxy с типизированными методами.

```ts
const sb = new ServiceBridge(URL, KEY);
const payment = await sb.client("payment-svc", "./payment.proto");
await sb.start();

// Unary — Promise
const r = await payment.Charge({ userId: "u", amount: 100 });

// Streaming методы автодетектируются — AsyncIterable
for await (const chunk of payment.Stream({ count: 5 })) {
  console.log(chunk.i);
}

// Per-call опции
await payment.Charge({ userId: "u", amount: 100 }, { timeout: "5s" });
```

Сигнатура:

```ts
sb.client(
  serviceName: string,
  protoFile: string,
  opts?: { methods?: string[]; callDefaults?: CallOpts },
): Promise<TypedClient>
```

| Опция | Что делает |
|-------|-----------|
| `methods` | Подписать только перечисленные (default — все из `service`). |
| `callDefaults` | Дефолтные `CallOpts` для всех вызовов этого клиента. |

⚠️ Вызывайте **до `sb.start()`** — declarations попадают в первый `RegisterRequest`.
⚠️ Требует `service` блока в `.proto` — иначе `no service block found`.

### 3.2 Низкоуровневый API

```ts
sb.service("payment-svc", {
  rpc: ["Charge", "Refund"],            // unary и streaming методы
  workflows: ["process-payout"],        // см. workflows.md
});

await sb.useSchema("payment-svc", "Charge", { protoFile: "./payment.proto" });

const r = await sb.rpc.call<ChargeReq, ChargeRes>("payment-svc", "Charge", payload);
```

### 3.3 sb.rpc.call — unary

```ts
sb.rpc.call<Req, Res>(
  serviceName: string,
  methodName: string,
  payload: Req,
  opts?: CallOpts,
): Promise<Res>
```

### 3.4 sb.stream — server-side streaming

```ts
sb.stream<Req, Chunk>(
  serviceName: string,
  methodName: string,
  payload: Req,
  opts?: CallOpts,
): AsyncIterable<Chunk>
```

`break`/`return` из `for await` → SDK вызывает `stream.return()` → callee получает CANCELLED.

**Ограничения streams:** retry не применяется (mid-stream replay дублирует уже доставленные chunks), CB и LB работают (LB single-pick, CB пишет failure на transport-error и success на завершение).

---

## 4. CallOpts

```ts
interface CallOpts {
  signal?: AbortSignal;                          // отмена вызова — ServiceBridgeError с кодом CANCELLED
  timeout?: string;                              // "500ms" | "10s" | "2m" — default "30s", на весь логический вызов
  requestId?: string;                            // auto UUID, если не задан; доходит до ctx.requestId
  idempotencyKey?: string;                       // доходит до ctx.idempotencyKey и до дедупа runtime proxy (§7)
  transport?: "direct" | "proxy" | "auto";       // default "auto" (см. §5)
  retry?: Partial<RetryOpts>;                    // см. §6
}
```

`timeout` покрывает все попытки вместе с паузами между ними. Неверная строка — `ConfigurationError`.

### Глобальные дефолты + per-call override

```ts
const sb = new ServiceBridge(URL, KEY, {
  callDefaults: {
    timeout: "5s",
    retry: { maxAttempts: 5 },
    transport: "auto",
  },
});

await sb.rpc.call(svc, m, payload);                          // использует дефолты
await sb.rpc.call(svc, m, payload, { timeout: "30s" });      // 30s переопределяет 5s
```

Иерархия (от низшей к высшей): `ServiceBridgeOptions.callDefaults` → `sb.client(...).callDefaults` → per-call `opts`. `callDefaults` действуют на `sb.rpc.call`, `sb.stream` и typed-клиенты.

---

## 5. Транспорт: direct, proxy, auto

| Значение | Поведение |
|---------|-----------|
| `"auto"` (default) | Direct к выбранному инстансу, если у него есть `call_endpoint`. Если direct-попытка провалилась до отправки запроса, следующая попытка сразу (без паузы) идёт через runtime proxy, а недоступный инстанс runtime пробует последним. |
| `"direct"` | Только direct, никогда через runtime. |
| `"proxy"` | Всегда через runtime `Invoke`, даже если direct возможен. |

### Когда выбрать что

- **`"auto"`** — default, для большинства случаев. Локальный снимок реестра может ещё держать мёртвый под, а runtime уже знает живой — `auto` переживает это без ошибки.
- **`"direct"`** — когда вызов не должен идти через runtime.
- **`"proxy"`** — нужна дедупликация повторов силами runtime (§7) или одна точка наблюдения.

### advertise (у callee)

Чтобы callee принимал вызовы, нужен inbound CallServer. Управляется опцией `advertise` в `ServiceBridgeOptions`:

```ts
new ServiceBridge(URL, KEY, { advertise: { host, port } | false })
```

| Значение | Поведение |
|---------|-----------|
| **не указано** | Bind `127.0.0.1` на свободном порту + warning (dev only). |
| `{ host, port }` | Явный bind. `port: 0` = ОС подбирает. **Рекомендуется для production.** |
| `false` | Caller-only mode. CallServer не поднимается, runtime не получает `call_endpoint`. |

Балансировщик выбирает только инстансы с `call_endpoint`, в том числе для `transport: "proxy"`. Инстанс с `advertise: false` вызвать нельзя: вызывающий получит `NoLiveInstanceError` «the callee advertises no inbound address».

⚠️ Не используйте `0.0.0.0` как **advertise host** — другие сервисы попытаются буквально подключиться к `0.0.0.0:port`.

### mTLS и SPIFFE

Direct-вызов = mTLS:
- Client cert (caller leaf) подписан общим CA.
- Server cert (callee leaf) валидируется по CA-цепочке.
- SPIFFE SAN в server cert проверяется: `spiffe://servicebridge/service/<service_id>/instance/<instance_id>` — защита от подмены инстанса.

Никаких токенов или auth-headers — identity полностью встроена в сертификат. Callee видит идентичность вызывающего в `ctx.caller`.

---

## 6. Resilience: LB, CB, Retry

### Load Balancer

**Power-of-Two-Choices (P2C)** по inflight: SDK берёт два случайных eligible-инстанса и шлёт вызов туда, где меньше активных запросов. При одном кандидате — берёт его. Per-pod state, inflight считается по `instanceId`.

Инстанс eligible только если:
1. У него есть `call_endpoint`.
2. CB не в OPEN.
3. Runtime не пометил его unhealthy в последние 60s (health-hint).
4. `contract_hash` совпадает с caller's local hash (фильтр до P2C, см. §8).
5. Ни сервис, ни инстанс не отозваны runtime.

Если health-hint исключил всех кандидатов, которые иначе подходят, выбор идёт среди них: ошибочный hint вероятнее, чем мёртвый весь флот.

Inflight общий по инстансу, не разбит по методам.

### Circuit Breaker

Per-instance state (`{serviceId}:{instanceId}`), sliding window 10s:

| Параметр | Значение |
|---------|---------|
| Условие OPEN | **≥10** запросов в окне **И** error rate **>50%** |
| Длительность OPEN | **30s** → HALF_OPEN |
| HALF_OPEN → CLOSED | **один** успешный probe |
| HALF_OPEN → OPEN | **один** failed probe (ещё на 30s) |

```
CLOSED ──≥10 req & >50% errors / 10s──► OPEN ──30s──► HALF_OPEN ──1 success──► CLOSED
                                                          │
                                                          └──1 failure──► OPEN
```

Failure для breaker — только отказ транспорта: `CONNECTION`, `TIMEOUT`, `OVERLOADED`, `INTERNAL`-статус. Ответ хендлера (`HandlerError`), отказ политики или валидации — success: инстанс жив и ответил. Breaker учитывает только direct-вызовы.

Per-pod, без synchronization между caller-подами. См. [ADR 0001](../../../runtime/docs/adr/0001-rpc.md).

### Retry

Только для unary. SDK повторяет вызов **только** когда доказано, что хендлер не запускался:

- нет подходящего кандидата (`NO_LIVE_INSTANCE`);
- канал к callee или к runtime не стал готов в пределах оставшегося дедлайна — запрос не ушёл;
- callee или runtime ответил статусом с трейлером `x-sb-not-dispatched: 1`: callee ещё без политики, в дренаже или перегружен.

Всё остальное — голый `UNAVAILABLE`, истёкший дедлайн, ответ хендлера, отказ политики — возвращается вызывающему без повтора: исход неизвестен или повтор ответит тем же. `idempotencyKey` этого не меняет.

Дефолты:

```ts
interface RetryOpts {
  maxAttempts: number;   // 3
  baseDelayMs: number;   // 200
  factor: number;        // 2
  maxDelayMs: number;    // 5000
  jitter: number;        // 0.3 (±30%)
}
```

Пауза перед попыткой `n` (с нуля): `min(baseDelayMs * factor^n, maxDelayMs)` с разбросом `±jitter`, но не дольше остатка дедлайна. `maxAttempts` — всего попыток, включая первую. `maxAttempts: 1` отключает ретраи. Переход `auto` с direct на proxy после pre-dispatch отказа тратит попытку, но идёт без паузы.

```ts
// Отключить retry
await sb.rpc.call(svc, m, payload, { retry: { maxAttempts: 1 } });

// Больше попыток в пределах того же дедлайна
await sb.rpc.call(svc, m, payload, { retry: { maxAttempts: 10, baseDelayMs: 100 } });

// Глобальный override
new ServiceBridge(URL, KEY, { callDefaults: { retry: { maxAttempts: 5 } } });
```

Повторить вызов, который завершился `TIMEOUT` или `CONNECTION` после отправки, может только ваш код, и только если эффект на стороне callee идемпотентен (§7).

---

## 7. Идемпотентность

`idempotencyKey` — opt-in, по умолчанию пустой. Заданный ключ:

- доходит до хендлера в `ctx.idempotencyKey` — callee дедуплицирует по нему сам;
- через runtime proxy (`transport: "proxy"`) участвует в дедупе на стороне runtime.

Ключ не включает автоматических повторов: SDK повторяет только pre-dispatch отказы (§6) с ключом и без. Один и тот же ключ в разных вызовах = намеренная дедупликация; разные эффекты должны получать разные ключи.

### Дедуп на стороне callee

Работает на любом транспорте. Атомарность — в том же хранилище, где живёт эффект:

```ts
sb.rpc.handle("Charge", async (req: { orderId: string; amount: number }, ctx) => {
  const key = ctx.idempotencyKey || req.orderId;
  const done = await db.query(
    "INSERT INTO charges (idempotency_key, amount) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING id",
    [key, req.amount],
  );
  if (done.rows.length === 0) return await loadChargeResult(key); // повтор — вернуть сохранённый ответ
  return await processCharge(req);
}, { schema: { protoFile: "./payment.proto" } });
```

### Дедуп в runtime proxy

Runtime хранит ответы по ключу в Postgres с TTL из настройки `rpc.idempotency_rpc_ttl_ms` (default `300000` = 5 мин; правится в UI /settings, не через env). Повтор с тем же ключом в окне TTL получает сохранённый ответ, не доходя до callee.

```ts
const r1 = await sb.rpc.call("pay-svc", "Charge", payload, {
  idempotencyKey: "order-123",
  transport: "proxy",
});

// в течение TTL — ответ из runtime, callee не вызывается
const r2 = await sb.rpc.call("pay-svc", "Charge", payload, {
  idempotencyKey: "order-123",
  transport: "proxy",
});
```

### Кейсы

| Что хотите | Что использовать |
|-----------|------------------|
| Безопасно повторить вызов после `TIMEOUT` своим кодом | Тот же `idempotencyKey` в повторе + дедуп на callee или `transport: "proxy"` |
| Бизнес-ID гарантирует уникальность | `idempotencyKey: \`order-${orderId}\`` |
| Без dedup (default) | не задавать `idempotencyKey` |

---

## 8. Версионирование контракта

Когда callee выпускает новую версию схемы (добавляет поле), v1 и v2 инстансы могут работать **одновременно**. LB направляет вызов **только в совместимый инстанс**.

### Как работает

1. **Callee** при `rpc.handle(...)` загружает `SchemaSpec` → SDK вычисляет `contract_hash` (SHA-256 от canonical JSON входной/выходной схем).
2. Хеш отправляется в `RegisterRequest.incoming[].contract_hash` → runtime хранит как opaque строку.
3. **Caller** при `useSchema(...)` / `sb.client(...)` тоже вычисляет хеш локально.
4. LB фильтрует кандидатов: `descriptor.contractHash === callerLocalHash`.
5. Ни одного матча → `NoLiveInstanceError` «rpc: no live instance of <svc>/<method> matches caller contract <hash>» (`retryable: true`: SDK повторяет в пределах `retry` и дедлайна, callee может как раз стартовать).

### Пример: blue-green

```proto
// v1/payment.proto
message ChargeRequest { string user_id = 1; double amount = 2; }

// v2/payment.proto — новое поле region (field 3)
message ChargeRequest { string user_id = 1; double amount = 2; string region = 3; }
```

```ts
v1Provider.rpc.handle("Charge", v1Fn, { schema: { protoFile: "./v1/payment.proto" } });
v2Provider.rpc.handle("Charge", v2Fn, { schema: { protoFile: "./v2/payment.proto" } });

// Caller на v1 → попадает только на v1 инстанс
const payment = await caller.client("payment-svc", "./v1/payment.proto");
for (let i = 0; i < 100; i++) {
  await payment.Charge({ userId: "u", amount: 1 });   // все 100 в v1
}
```

### Алгоритм хеша

```
hash = "v2:" + sha256_hex(canon(input) + ":" + canon(output))
```

`canon(msg)` — канонический JSON описания сообщения, куда входит только то, что решает бинарную совместимость: номера полей, кардинальность и типы.

```json
{"f":[{"n":1,"c":"opt","t":"string"},
      {"n":2,"c":"rep","t":"m:.pay.Item"},
      {"n":3,"c":"map","k":"string","t":"m:.pay.Meta"}],
 "o":[{"f":[4,5]}],
 "r":{".pay.Item":{"f":[…]},".pay.Kind":{"e":[0,1,2]}}}
```

Ключи объектов отсортированы на всех уровнях, порядок массивов сохранён, пробелов нет.

**Имена полей в хеш не входят** — переименование поля wire-совместимо, и включение имён уводило бы трафик в никуда после безобидного рефакторинга. Идентичность контракта меняют только номер, тип и кардинальность. Имена типов, наоборот, входят: это единственный способ различить два структурно одинаковых, но семантически разных сообщения.

Префикс `v2:` несёт поколение алгоритма внутри самого значения. Пустая строка остаётся валидным хешем — так объявляет себя хендлер без схемы.

У события ответа нет, поэтому `canon(output)` там всегда `{"f":[]}` — канонический вид пустого сообщения. Объявленный в spec `output` в идентичность события не входит (см. [events.md §11](./events.md#11-schema-versioning)).

Runtime хранит хеш как opaque-строку, не парсит и не пересчитывает. Алгоритм зафиксирован golden-векторами в `sdk/contract-hash-vectors.json` — общем артефакте для всех языковых SDK. Полная спека: [ADR 0001](../../../runtime/docs/adr/0001-rpc.md).

### Ограничения

- **Разные source kinds → разные хеши**: `.proto` и `.schema.json` для одного контракта дают разные `contract_hash`.
- **Schema-файлы не централизованы** — распространяйте обоим сторонам (git submodule / npm package / shared volume).
- Разные **методы** одного сервиса работают независимо — версионирование per-method.

---

## 9. Ошибки

### Структура

Каждая ошибка из `sb.rpc.call` / `sb.stream` / typed-client — `ServiceBridgeError` с полями `code` и `retryable`. Различайте по `err.code` или по классу:

| Класс / `code` | Когда | `retryable` |
|---|---|---|
| `HandlerError` / `HANDLER` | Хендлер callee ответил ошибкой. `handlerCode` — код из `HandlerError` callee или `"INTERNAL"`. | нет |
| `NoLiveInstanceError` / `NO_LIVE_INSTANCE` | Некуда отправить: callee offline, нет инстанса с совпадающим `contract_hash`, нет `call_endpoint`, все кандидаты circuit-open. | да |
| `AccessDeniedError` / `ACCESS_DENIED` | Отказ политики доступа или вызывающий отозван. Дополнительно эмитится `policy_violation`. | нет |
| `TimeoutError` / `TIMEOUT` | Дедлайн истёк. Хендлер мог отработать. | нет |
| `ServiceBridgeError` / `CONNECTION` | Канал оборвался или runtime недоступен. | да |
| `ServiceBridgeError` / `OVERLOADED` | Callee или runtime перегружен (`RESOURCE_EXHAUSTED`). | да |
| `ServiceBridgeError` / `CANCELLED` | Вызов отменён через `opts.signal`. | нет |
| `ValidationError` / `VALIDATION` | Недекодируемый запрос, `call` на streaming-методе (и наоборот), отказ runtime по аргументу. | нет |
| `ServiceBridgeError` / `NOT_FOUND` | У callee нет такого метода. | нет |
| `ServiceBridgeError` / `CONFLICT` | `ALREADY_EXISTS` от runtime. | нет |
| `ServiceBridgeError` / `INTERNAL` | Неклассифицированный сбой. | нет |
| `ConfigurationError` / `CONFIG` | Нет схемы вызывающего, неверный `timeout`. | нет |
| `StateError` / `STATE` | Вызов до `start()`. | нет |

`retryable: true` значит: повтор того же запроса позже может пройти, и предыдущая попытка точно не имела эффекта. SDK уже повторил такие отказы в пределах `retry` и дедлайна (§6) — до вас они доходят, когда попытки кончились.

### Базовая обработка

```ts
import { HandlerError, ServiceBridgeError } from "service-bridge";

try {
  await payment.Charge(payload);
} catch (err) {
  if (err instanceof HandlerError) {
    switch (err.handlerCode) {
      case "INVALID_AMOUNT": return badRequest(err.message);  // бизнес-код callee
      case "INTERNAL":       log.error("callee failed", { message: err.message }); throw err;
    }
  }
  if (err instanceof ServiceBridgeError) {
    if (err.code === "TIMEOUT") { /* исход неизвестен — повтор только с idempotencyKey */ }
    if (err.retryable) return await retryLater(payload);
  }
  throw err;
}
```

### Отображение gRPC-статусов

Статусы от callee или runtime переводятся в коды одинаково в Node и Go SDK:

| gRPC-статус | `code` |
|---|---|
| `CANCELLED` (1) | `CANCELLED` |
| `UNKNOWN` (2) | `INTERNAL` |
| `INVALID_ARGUMENT` (3) | `VALIDATION` |
| `DEADLINE_EXCEEDED` (4) | `TIMEOUT` |
| `NOT_FOUND` (5) | `NOT_FOUND` |
| `ALREADY_EXISTS` (6) | `CONFLICT` |
| `PERMISSION_DENIED` (7) | `ACCESS_DENIED` |
| `RESOURCE_EXHAUSTED` (8) | `OVERLOADED` |
| `FAILED_PRECONDITION` (9) | `VALIDATION` |
| `ABORTED` (10) | `INTERNAL` |
| `OUT_OF_RANGE` (11) | `VALIDATION` |
| `UNIMPLEMENTED` (12) | `NOT_FOUND` |
| `INTERNAL` (13) | `INTERNAL` |
| `UNAVAILABLE` (14) | `CONNECTION` |
| `DATA_LOSS` (15) | `INTERNAL` |
| `UNAUTHENTICATED` (16) | `ACCESS_DENIED` |

Ответ хендлера приходит не статусом, а в теле ответа (`error_code`) и всегда становится `HandlerError`.

### Сообщения SDK

| Сообщение начинается с | Класс | Когда |
|-----------------------|-------|------|
| `rpc: no schema for <svc>/<method>` | `ConfigurationError` | Не вызван `sb.client()` / `sb.useSchema()`. |
| `rpc: <svc>/<method> is a streaming method — use sb.stream()` | `ValidationError` | `call` на stream-методе. |
| `rpc: <svc>/<method> is not a streaming method — use sb.rpc.call()` | `ValidationError` | `stream` на unary-методе. |
| `rpc: no live instance of <svc>/<method> matches caller contract <hash>` | `NoLiveInstanceError` | Нет инстанса с этим `contract_hash`: callee offline или другая версия (см. §8). |
| `rpc: no live instance of <svc>/<method> — all candidates circuit-open` | `NoLiveInstanceError` | Breaker открыт у всех инстансов. |
| `rpc: no endpoint for <svc>/<method> — the callee advertises no inbound address` | `NoLiveInstanceError` | Callee запущен с `advertise: false`. |
| `rpc: invalid timeout <value>` | `ConfigurationError` | `timeout` не в формате `<n>ms` / `<n>s` / `<n>m`. |
| `rpc: call before start()` | `StateError` | Вызов до `sb.start()`. |
| `serde: cannot resolve input/output for <proto>` | — | Auto-resolve не нашёл messages: нет service-блока и нет явных `input`/`output` (см. §2.4). |
| `rpc: client(<proto>): no service block found` | — | `sb.client()` на `.proto` без `service`. |

### Stream errors

```ts
try {
  for await (const chunk of sb.stream(svc, m, payload)) { ... }
} catch (err) {
  // chunks ДО ошибки уже обработаны.
  // HandlerError — хендлер бросил; иначе err.code — отказ транспорта или runtime.
  console.error("stream failed:", (err as ServiceBridgeError).code, (err as Error).message);
}
```

Обрыв сети посреди стрима → `CONNECTION`; стримы не ретраятся. Throw из stream-хендлера → `HandlerError` (`handlerCode` из `HandlerError` хендлера или `"INTERNAL"`).

---

## 10. Шпаргалка

### Минимальный E2E

```ts
// callee
const sb = new ServiceBridge(URL, KEY);
sb.rpc.handle("Charge", chargeFn, { schema: { protoFile: "./payment.proto" } });
await sb.start();

// caller
const sb2 = new ServiceBridge(URL, KEY2);
const payment = await sb2.client("payment-svc", "./payment.proto");
await sb2.start();
const r = await payment.Charge({ userId: "u", amount: 100 });
```

### Production callee

```ts
new ServiceBridge(URL, KEY, {
  advertise: { host: process.env.POD_IP!, port: 7777 },
  callDefaults: { timeout: "5s", retry: { maxAttempts: 3 } },
});
```

### Caller-only

```ts
new ServiceBridge(URL, KEY, { advertise: false });
```

### Selective methods

```ts
await sb.client("payment-svc", "./payment.proto", { methods: ["Charge"] });
```

### Streaming с cancel

```ts
for await (const chunk of payment.Generate({ prompt }, { timeout: "60s" })) {
  if (shouldStop()) break;   // ctx.signal у callee прерывается
  process.stdout.write(chunk.token);
}
```

### Повтор после TIMEOUT

```ts
const opts = { idempotencyKey: `order-${orderId}`, transport: "proxy" as const };
try {
  await payment.Charge(payload, opts);
} catch (err) {
  if ((err as ServiceBridgeError).code !== "TIMEOUT") throw err;
  await payment.Charge(payload, opts);   // тот же ключ — runtime вернёт сохранённый ответ
}
```

→ Дальше: [Events](./events.md)
