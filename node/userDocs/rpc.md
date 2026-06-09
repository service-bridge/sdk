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

Все регистрации **до `sb.start()`**. SDK отправляет полную декларацию в первом `RegisterRequest` и на каждом reconnect.

### Unary

```ts
sb.rpc.handle<Req, Res>(
  name: string,
  fn: (req: Req) => Promise<Res>,
  opts: { schema: SchemaSpec },
): void
```

```ts
sb.rpc.handle<{ userId: string; amount: number }, { transactionId: string; ok: boolean }>(
  "Charge",
  async (req) => ({ transactionId: `tx-${req.userId}`, ok: req.amount > 0 }),
  { schema: { protoFile: "./payment.proto" } },
);
```

### Server-side streaming

```ts
sb.rpc.handleStream<Req, Chunk>(
  name: string,
  fn: (req: Req) => AsyncGenerator<Chunk>,
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

**Cancellation:** когда caller прерывает stream, следующий `yield` бросит исключение — обрабатывайте через `try/finally` для очистки ресурсов:

```ts
sb.rpc.handleStream("Generate", async function* (req) {
  const upstream = llm.start(req.prompt);
  try {
    for await (const t of upstream) yield { token: t };
  } finally {
    upstream.cancel();
  }
}, { schema: { protoFile: "./ai.proto" } });
```

### Возврат ошибок из хендлера

Любой `throw` из хендлера → caller получает ошибку с `err.name === "INTERNAL"` и тем же `message`. Выбрать произвольный gRPC-код из хендлера нельзя.

```ts
sb.rpc.handle("Charge", async (req) => {
  if (req.amount <= 0) {
    throw new Error("amount must be positive"); // caller: err.name === "INTERNAL"
  }
  // ...
}, { schema: { protoFile: "./payment.proto" } });
```

> **Важно про коды.** Ошибки от callee (throw из хендлера, провал декода payload) приходят как `Error` со **строковым** `err.name` (`"INTERNAL"`, `"INVALID_ARGUMENT"`, …) и **без** числового `err.code`. Числовой `err.code` (gRPC status) есть только у ошибок транспорта/рантайма: недоступность, таймаут, отказ политики, rate limit. Различайте callee-ошибки по `err.name`, транспортные — по `err.code` (см. §9).

Декод payload по схеме провалился → `err.name === "INVALID_ARGUMENT"`. Access policy запретила вызов → отдельный класс `RpcAccessDeniedError` (маппится из gRPC `PERMISSION_DENIED`, §9). Бизнес-валидацию доносите до caller через поля ответа, а не через код.

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
  timeout?: string;                              // "500ms" | "10s" | "2m"  — default "30s"
  requestId?: string;                            // auto UUID v4, если не задан
  idempotencyKey?: string;                       // opt-in; по умолчанию "" = без dedup (см. §7)
  transport?: "direct" | "proxy" | "auto";       // default "auto" (см. §5)
  retry?: Partial<RetryOpts>;                    // см. §6
}
```

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

Иерархия (от низшей к высшей): `ServiceBridgeOptions.callDefaults` → `sb.client(...).callDefaults` → per-call `opts`.

---

## 5. Транспорт: direct, proxy, auto

| Значение | Поведение |
|---------|-----------|
| `"auto"` (default) | Direct, если у callee известен `call_endpoint`. Иначе proxy. |
| `"direct"` | Только direct. Кидает `transport="direct" requested but no endpoint...`, если endpoint неизвестен. |
| `"proxy"` | Только proxy — всегда через runtime `Invoke`, даже если direct возможен. |

### Когда выбрать что

- **`"auto"`** — default, для большинства случаев.
- **`"direct"`** — fail-fast если callee misconfig'нут (нет advertise).
- **`"proxy"`** — нужна централизованная idempotency через runtime cache (§7), либо отладка через единую точку логирования.

### advertise (у callee)

Чтобы callee принимал direct-вызовы, нужен inbound CallServer. Управляется опцией `advertise` в `ServiceBridgeOptions`:

```ts
new ServiceBridge(URL, KEY, { advertise: { host, port } | false })
```

| Значение | Поведение |
|---------|-----------|
| **не указано** | env `SB_ADVERTISE_HOST` → `port: 0`. Иначе `127.0.0.1:0` с warning (dev only). |
| `{ host, port }` | Явный bind. `port: 0` = ОС подбирает. **Рекомендуется для production.** |
| `false` | Caller-only mode. CallServer не поднимается, runtime не получает `call_endpoint`. |

⚠️ Не используйте `0.0.0.0` как **advertise host** — другие сервисы попытаются буквально подключиться к `0.0.0.0:port`.

### mTLS и SPIFFE

Direct-вызов = mTLS:
- Client cert (caller leaf) подписан общим CA.
- Server cert (callee leaf) валидируется по CA-цепочке.
- SPIFFE SAN в server cert проверяется: `spiffe://servicebridge/service/<service_id>/instance/<instance_id>` — защита от подмены инстанса.

Никаких токенов или auth-headers — identity полностью встроена в сертификат.

---

## 6. Resilience: LB, CB, Retry

### Load Balancer

**Power-of-Two-Choices (P2C)** по inflight: SDK берёт два случайных eligible-инстанса и шлёт вызов туда, где меньше активных запросов. При одном кандидате — берёт его; при равном inflight — берёт первый из двух выбранных (а так как оба выбраны случайно, выбор всё равно случайный). Per-pod state, inflight считается по `instanceId`.

Инстанс eligible только если:
1. У него есть `call_endpoint`.
2. CB не в OPEN.
3. Runtime не пометил его unhealthy в последние 60s (health-hint).
4. `contract_hash` совпадает с caller's local hash (фильтр до P2C, см. §8).

Inflight общий по инстансу, не разбит по методам.

### Circuit Breaker

Per-instance state (`{serviceId}:{instanceId}`), sliding window 10s из 10 buckets:

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

В HALF_OPEN пропускается ровно один пробный вызов — остальные ждут его исхода. Failure = любой throw из transport ИЛИ application-error от callee. Success = успешно декодированный response. Порог в ≥10 запросов значит, что на малом трафике CB не откроется от пары случайных ошибок.

Per-pod, без synchronization между caller-подами. См. [ADR 0001](../../../runtime/docs/adr/0001-rpc-architecture.md#per-pod-cb).

### Retry

Только для unary. Дефолты:

```ts
interface RetryOpts {
  maxAttempts: number;   // 3
  baseDelayMs: number;   // 200
  factor: number;        // 2
  maxDelayMs: number;    // 5000
  jitter: number;        // 0.3 (±30%)
}
```

Формула: `delay = round(min(baseDelayMs * factor^attempt, maxDelayMs) * (1 ± jitter))`, `attempt` с нуля. `maxAttempts` — это всего попыток, включая первую: при дефолтном `3` будет первая попытка + 2 ретрая с паузами ~200ms и ~400ms (±30%). `maxAttempts: 1` отключает ретраи.

| gRPC код | Всегда | Только с `idempotencyKey` |
|---------|--------|--------------------------|
| `UNAVAILABLE` (14), `RESOURCE_EXHAUSTED` (8), `DEADLINE_EXCEEDED` (4) | ✅ | ✅ |
| `INTERNAL` (13), `ABORTED` (10), `UNKNOWN` (2) | ❌ | ✅ |
| Все остальные (`INVALID_ARGUMENT`, `NOT_FOUND`, `PERMISSION_DENIED`, ...) | ❌ | ❌ |

Retry смотрит только на **числовой** `err.code`. Ошибки без него — non-retryable всегда. Сюда попадают **все ошибки от callee**: throw из хендлера (`err.name === "INTERNAL"`) и провал декода (`err.name === "INVALID_ARGUMENT"`) идут в теле ответа со строковым `name`, без `code` — поэтому даже с `idempotencyKey` они **не** ретраятся. Строка `INTERNAL`/`ABORTED`/`UNKNOWN` в таблице ретраится только когда числовой код пришёл от транспорта или рантайма (например, рантайм недоступен), а не когда хендлер бросил исключение.

```ts
// Отключить retry
await sb.rpc.call(svc, m, payload, { retry: { maxAttempts: 1 } });

// Агрессивный
await sb.rpc.call(svc, m, payload, { retry: { maxAttempts: 10, baseDelayMs: 100 } });

// Глобальный override
new ServiceBridge(URL, KEY, { callDefaults: { retry: { maxAttempts: 5 } } });
```

---

## 7. Идемпотентность

`idempotencyKey` дедуплицирует replay-вызовы в **proxy mode**. Это **opt-in**: по умолчанию ключ пустой (`""`) — runtime dedup выключен, и расширенный retry (`INTERNAL`/`ABORTED`/`UNKNOWN` по числовому коду, §6) не включается. Задаёте ключ — он едет на runtime и переиспользуется через все retry внутри одного `sb.rpc.call`. Один и тот же ключ в разных вызовах = намеренная дедупликация; разные эффекты должны получать разные ключи.

### Proxy mode — runtime дедуплицирует

Runtime хранит `{key → response}` в Postgres с TTL из настройки `rpc.idempotency_rpc_ttl_ms` (default `300000` = 5 мин; правится в UI /settings, не через env). Replay с тем же ключом в окне TTL возвращает закешированный response, **не доходя до callee**.

```ts
const r1 = await sb.rpc.call("pay-svc", "charge", payload, {
  idempotencyKey: "order-123",
  transport: "proxy",
});

// в течение 5 мин — кеш-хит, callee НЕ вызывается
const r2 = await sb.rpc.call("pay-svc", "charge", payload, {
  idempotencyKey: "order-123",
  transport: "proxy",
});
```

### Direct mode — runtime не дедуплицирует

Caller минует runtime → централизованного кеша нет. Хендлер получает только `req` (второго `ctx`-аргумента нет — `idempotencyKey` до тела хендлера не доходит). Если нужна дедупликация на direct-пути, передавайте бизнес-ключ как поле payload и дедуплицируйте по нему сами:

```ts
sb.rpc.handle("Charge", async (req: { orderId: string; amount: number }) => {
  const cached = await redis.get(`idemp:${req.orderId}`);
  if (cached) return JSON.parse(cached);

  const result = await processCharge(req);
  await redis.setex(`idemp:${req.orderId}`, 300, JSON.stringify(result));
  return result;
}, { schema: { protoFile: "./payment.proto" } });
```

Нужна дедупликация силами рантайма — используйте `transport: "proxy"` с `idempotencyKey` (выше).

### Кейсы

| Что хотите | Что использовать |
|-----------|------------------|
| Бизнес-ID гарантирует уникальность | `idempotencyKey: \`order-${orderId}\`` + `transport: "proxy"` |
| Защита от дубля при retry + расширенный retry на `INTERNAL`/`ABORTED` | задать `idempotencyKey` (любой стабильный для этого вызова) |
| Без dedup (default) | не задавать `idempotencyKey` (он `""`) |

---

## 8. Версионирование контракта

Когда callee выпускает новую версию схемы (добавляет поле), v1 и v2 инстансы могут работать **одновременно**. LB направляет вызов **только в совместимый инстанс**.

### Как работает

1. **Callee** при `rpc.handle(...)` загружает `SchemaSpec` → SDK вычисляет `contract_hash` (SHA-256 от canonical JSON входной/выходной схем).
2. Хеш отправляется в `RegisterRequest.incoming[].contract_hash` → runtime хранит как opaque строку.
3. **Caller** при `useSchema(...)` / `sb.client(...)` тоже вычисляет хеш локально.
4. LB фильтрует кандидатов: `descriptor.contractHash === callerLocalHash`.
5. Ни одного матча → `rpc: no instance of <svc>/<method> matches caller contract <hash>` (retryable как `UNAVAILABLE`).

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
hash = sha256_hex(
  canonical(input.toJsonSchema()) + ":" + canonical(output.toJsonSchema())
)
```

`canonical` — JSON с рекурсивно отсортированными ключами и без пробелов (массивы сохраняют порядок). Без префикса версии — голый hex SHA-256. Runtime хранит хеш как opaque-строку и не пересчитывает. Полная спека: [ADR 0005](../../../runtime/docs/adr/0005-contract-version-routing.md).

### Ограничения

- **Разные source kinds → разные хеши**: `.proto` и `.schema.json` для одного контракта дают разные `contract_hash`.
- **Schema-файлы не централизованы** — распространяйте обоим сторонам (git submodule / npm package / shared volume).
- Разные **методы** одного сервиса работают независимо — версионирование per-method.

---

## 9. Ошибки

### Структура

Ошибки из `sb.rpc.call` / `sb.stream` / typed-client бывают **двух форм** — различать их обязательно:

- **Ошибки от callee** (throw из хендлера, провал декода payload) — `Error` со **строковым** `err.name` (`"INTERNAL"`, `"INVALID_ARGUMENT"`, `"NOT_FOUND"`, `"FAILED_PRECONDITION"`). Числового `err.code` и `.details` у них **нет** — они едут в теле ответа, а не как gRPC status. Различайте по `err.name`.
- **Ошибки транспорта/рантайма** (недоступность, таймаут, rate limit, отказ политики) — обычные gRPC-js-ошибки с числовым `err.code` (gRPC status) и `.details`. Различайте по `err.code` (числу из таблицы ниже).

Имена в таблице кодов ниже — это и строковые `err.name` от callee, и имена соответствующих gRPC-статусов; число рядом — `err.code`, который есть **только** у транспортных/рантайм-ошибок.

Отдельный публичный класс — только для отказа политики:

```ts
class RpcAccessDeniedError extends Error {  // export из index
  serviceName: string;
  methodName: string;
  reason: string;
}
```

Он бросается, когда access policy запретила вызов (маппится из gRPC `PERMISSION_DENIED` / code 7). `ServiceBridgeError` (тоже export из index) несёт только числовой `.code` и используется на уровне lifecycle-соединения, а не на каждом RPC-вызове — у него нет полей `name`-как-gRPC-имя или `retryable`.

### Базовая обработка

```ts
try {
  await payment.Charge(payload);
} catch (err: any) {
  // Callee-ошибки приходят строковым err.name (без числового err.code):
  switch (err.name) {
    case "INVALID_ARGUMENT": return badRequest(err.message); // плохой payload (декод)
    case "NOT_FOUND":        return notFound(err.message);
    case "INTERNAL":         log.error("callee threw", err.message); throw err;
  }
  // Транспорт/рантайм — числовой err.code (gRPC status):
  switch (err.code) {
    case 14: return await fallback();          // UNAVAILABLE — рантайм/сеть недоступны
    case 4:  log.warn("timeout"); throw err;   // DEADLINE_EXCEEDED
    default: throw err;
  }
}
```

Отказ access policy на `rpc.call` приходит отдельным классом `RpcAccessDeniedError` (с `serviceName`/`methodName`/`reason`), а не gRPC-ошибкой с `code === 7` — ловите его через `instanceof`, если нужно различать.

### Полная таблица gRPC-кодов

Колонка «Форма»: `name` — callee-ошибка, у неё `err.name` = это имя, числового `err.code` нет; `code` — транспорт/рантайм, у неё `err.code` = это число. Retryable относится только к форме `code` (см. §6).

| Код | Имя | Форма | Retryable | Когда |
|----|------|-------|-----------|------|
| 1 | `CANCELLED` | code | ❌ | Caller прервал (timeout, abort) |
| 2 | `UNKNOWN` | code | с `idempotencyKey` | Неклассифицированная ошибка |
| 3 | `INVALID_ARGUMENT` | name (декод callee) / code (рантайм) | ❌ | Плохой payload |
| 4 | `DEADLINE_EXCEEDED` | code | ✅ | Timeout |
| 5 | `NOT_FOUND` | name | ❌ | Нет хендлера метода у callee |
| 6 | `ALREADY_EXISTS` | code | ❌ | Idempotency-ключ уже в работе (proxy) |
| 7 | `PERMISSION_DENIED` | code → `RpcAccessDeniedError` | ❌ | Access policy запретила |
| 8 | `RESOURCE_EXHAUSTED` | code | ✅ | Rate limit / quota |
| 9 | `FAILED_PRECONDITION` | name | ❌ | Вызов `call` на streaming-методе |
| 10 | `ABORTED` | code | с `idempotencyKey` | Concurrent conflict |
| 11 | `OUT_OF_RANGE` | code | ❌ | Out-of-bounds |
| 12 | `UNIMPLEMENTED` | code | ❌ | Метод не реализован |
| 13 | `INTERNAL` | name (throw хендлера) / code (рантайм) | с `idempotencyKey`* | Callee throw / runtime error |
| 14 | `UNAVAILABLE` | code | ✅ | Network / no eligible instance |
| 16 | `UNAUTHENTICATED` | code | ❌ | Bootstrap key invalid (lifecycle, не RPC) |

\* `INTERNAL` ретраится с `idempotencyKey` **только** в форме `code` (его выдал рантайм). Самый частый источник — throw хендлера — приходит формой `name` и **не** ретраится.

### SDK-специфичные ошибки

| Сообщение начинается с | Когда |
|-----------------------|------|
| `rpc: no descriptor for ...` | Метод не в `serviceMap()`: не подписан или callee offline. |
| `rpc: no SchemaPair for ...` | Не вызван `useSchema()`. |
| `rpc: ... is a streaming method — use sb.stream()` | Вы вызвали `call` на stream-методе. |
| `rpc: no instance of ... matches caller contract <hash>` | Все инстансы имеют другой `contract_hash` (см. §8). |
| `rpc: transport="direct" requested but no endpoint ...` | `transport: "direct"` явный, но callee без advertise. |
| `serde: cannot resolve input/output for <proto> (method=...)` | Auto-resolve не нашёл messages: нет service-блока и нет явных `input`/`output` (см. §2.4). |
| `rpc: client(...): no service block found` | `sb.client()` на `.proto` без `service`. |

### Stream errors

```ts
try {
  for await (const chunk of sb.stream(svc, m, payload)) { ... }
} catch (err: any) {
  // chunks ДО ошибки уже обработаны.
  // callee-ошибка → err.name (строка); обрыв транспорта → err.code (число).
  console.error("stream failed:", err.name ?? err.code, err.message);
}
```

Network drop mid-stream → транспортный `UNAVAILABLE` (`err.code === 14`, не retryable для streams). Throw из stream-хендлера → `err.name === "INTERNAL"`.

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
  if (shouldStop()) break;   // callee получит CANCELLED
  process.stdout.write(chunk.token);
}
```

### Idempotent retry

```ts
await payment.Charge(payload, {
  idempotencyKey: `order-${orderId}`,
  transport: "proxy",          // нужно для runtime-side dedup
  retry: { maxAttempts: 5 },
});
```

→ Дальше: [Events](./events.md)
