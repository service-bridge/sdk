# Events

← [RPC](./rpc.md) · Дальше: [Workflows](./workflows.md) →

Полный гайд по Durable Events: декларация, подписка, эмиссия, гарантии доставки (at-least-once), wildcard routing, идемпотентность, partition ordering, fire-and-forget, DLQ + replay, schema versioning, ошибки. Читается линейно. Для операционных тем — [Operations](./operations.md).

## Содержание

- [Краткая модель](#краткая-модель)
- [1. Декларация: event.define](#1-декларация-eventdefine)
- [2. Подписка: event.handle](#2-подписка-eventhandle)
- [3. Эмиссия: event.publish](#3-эмиссия-eventpublish)
- [4. Wildcard routing (AMQP)](#4-wildcard-routing-amqp)
- [5. Гарантии доставки](#5-гарантии-доставки)
- [6. Идемпотентность](#6-идемпотентность)
- [7. Partition key и ordering](#7-partition-key-и-ordering)
- [8. Fire-and-forget](#8-fire-and-forget)
- [9. Outbox и retries (SDK side)](#9-outbox-и-retries-sdk-side)
- [10. DLQ и replay](#10-dlq-и-replay)
- [11. Schema versioning](#11-schema-versioning)
- [12. Ошибки](#12-ошибки)
- [13. Шпаргалка](#13-шпаргалка)

---

## Краткая модель

ServiceBridge Events — это **durable** pub/sub поверх runtime с гарантией **at-least-once**:

- **Publisher** декларирует событие через `sb.event.define(name, spec?)` и эмитирует через `sb.event.publish(name, payload, opts?)`.
- **Subscriber** регистрирует обработчик через `sb.event.handle(pattern, fn)`.
- Runtime **гарантирует доставку**: каждое событие падает в локальный SQLite outbox SDK до отправки, runtime пишет в Postgres `event_log` + `event_deliveries` (по строке на каждого matching подписчика) в одной транзакции.
- Доставка идёт **push'ом** через long-lived bidi gRPC stream. Subscriber отвечает `Ack` после успеха или `Nack` при ошибке. При сбое — redelivery после **visibility timeout** или после crash'а consumer'а.
- Когда число попыток достигает порога `events.max_attempts` (default 5) — событие уходит в **DLQ** с копией payload.
- Один SDK-инстанс может одновременно быть publisher и subscriber для разных событий.

> Visibility timeout, max attempts, DLQ retention и backoff-лестница — это **настройки рантайма** (`events.*`), а не вшитые константы. Они правятся в UI `/settings` без рестарта. Значения ниже — дефолты.

> **Архитектурный аксиом:** runtime в системе всегда один. Горизонтальное масштабирование — на стороне SDK-консьюмеров (см. [`CLAUDE.md`](../../../CLAUDE.md)).

---

## 1. Декларация: event.define

Перед эмиссией событие нужно объявить — это формирует service map runtime'а и регистрирует Protobuf-схему, по которой SDK кодирует и декодирует payload.

```ts
sb.event.define(
  name: string,
  spec?: SchemaSpec,
): void
```

`SchemaSpec` — тот же тип, что и у `sb.rpc.handle`: либо `.proto` файл (`ProtoFileSpec`), либо `.schema.json` (`JsonSchemaFileSpec`) c явными `fieldNumber`. Inline JSON Schema не поддерживается (ADR-0002).

Вызывать **до `sb.start()`**.

`spec` опционален. Если вызвать `define(name)` без spec — регистрируется только имя (schema-less). Это валидно для service map, но реально публиковать/принимать такое событие нельзя: `publish` без схемы бросит «no schema registered», а subscriber ответит `Nack` `no_schema`. Для рабочего события spec обязателен. Повторный `define` с тем же объектом spec — no-op; с другим spec — бросает.

### Через `.proto` файл

```proto
// schemas/payment.proto
syntax = "proto3";
package payments;

message PaymentCharged {
  string transaction_id = 1;
  double amount         = 2;
  string user_id        = 3;
  string currency       = 4;
}

service Payments {
  rpc PaymentCharged (PaymentCharged) returns (PaymentCharged);
}
```

```ts
// `method` обязан совпадать с именем `rpc <method>` в .proto-файле
// (точки в proto-идентификаторах нельзя — поэтому имя события и имя rpc
// различаются). Альтернатива — задать input/output явно.
sb.event.define("payment.charged", {
  protoFile: "schemas/payment.proto",
  method: "PaymentCharged",
});
```

### Через `.schema.json` (если `.proto` не нужен)

Файл обязан иметь на верхнем уровне два блока — `input` и `output` — в каждом ровно одно сообщение. Для события payload — это `input`; `output` обязателен по формату (события его не используют, но парсер требует оба блока).

```json
{
  "input": {
    "PaymentCharged": {
      "transactionId": { "type": "string", "fieldNumber": 1 },
      "amount":        { "type": "double", "fieldNumber": 2 },
      "userId":        { "type": "string", "fieldNumber": 3 },
      "currency":      { "type": "string", "fieldNumber": 4 }
    }
  },
  "output": {
    "Empty": {}
  }
}
```

```ts
sb.event.define("payment.charged", { schemaFile: "schemas/payment.json" });
```

SDK строит Protobuf `Type` через `protobufjs`, считает `contract_hash` по структуре, регистрирует имя + хеш в runtime и хранит compiled `SchemaPair` локально. При `publish` payload кодируется в Protobuf binary, `type.verify()` ловит невалидный объект до записи в outbox — `await sb.event.publish(...)` rejected'ится этой ошибкой ещё до попадания в outbox.

### Имя события

| Правило | Пример |
|---|---|
| Только `[a-z0-9_-]` + `.` | `payment.charged`, `order.line-added` |
| Минимум 1 секция | `ok` (но обычно ≥2) |
| Без пустых сегментов | `payment.charged` ✅, `payment..charged` ❌ |
| Без leading/trailing dot | `payment.charged` ✅, `.payment` / `payment.` ❌ |

Имя проверяется на SDK-стороне при `publish` (`InvalidEventNameError` до RPC). Если имя всё же дошло до runtime невалидным — он отвечает `REJECTED_INVALID_NAME`, и drainer помечает row `failed` (terminal).

---

## 2. Подписка: event.handle

```ts
sb.event.handle(
  pattern: string,
  fn: (payload: unknown) => Promise<void> | void,
): void
```

Вызывать **до `sb.start()`**. Опций нет: для decode payload-а subscriber должен сам объявить `sb.event.define(name, spec)` с той же схемой, что у publisher'а — иначе на delivery он ответит `Nack` `no_schema`. Handler получает только декодированный payload (headers и метаданные envelope в handler не передаются).

### Точное совпадение имени

```ts
sb.event.define("payment.charged", { protoFile: "schemas/payment.proto", method: "PaymentCharged" });

sb.event.handle("payment.charged", async (payload) => {
  await sendReceipt((payload as { transactionId: string }).transactionId);
});
```

### Wildcard-подписки

Wildcard-pattern (`payment.*`, `order.#`) задаёт, **какие** события runtime будет слать этому сервису — server-side routing через `TopicMatch` (см. §4). Но фактический вызов handler'а в SDK идёт по **точному имени события**: SDK вызывает handler, чей `pattern` буквально равен имени пришедшего события. Поэтому, чтобы реально обработать `payment.charged`, нужен handler с `pattern === "payment.charged"` и `define("payment.charged", spec)` для decode.

```ts
sb.event.handle("payment.charged", auditCharge);
sb.event.handle("payment.charged", trackMetrics);
// оба вызываются на доставку payment.charged, последовательно; один Ack за delivery
```

Несколько handler'ов на одно и то же точное имя — runtime отдаёт **один delivery**, SDK вызывает все matching handlers последовательно. Один Ack за весь delivery; любой throw → `Nack` всего delivery (runtime ретраит и при исчерпании попыток уводит в DLQ).

---

## 3. Эмиссия: event.publish

`sb.event.publish(name, payload, opts?)` отправляет событие. Должно быть вызвано **после `sb.start()`** и для имени, ранее задекларированного через `sb.event.define(name, spec)`.

```ts
sb.event.publish<T>(
  name: string,
  payload: T,
  opts?: PublishOpts,
): Promise<{ eventId: string }>
```

```ts
await sb.event.publish("payment.charged", {
  transactionId: "tx-7",
  amount: 42.0,
  userId: "u-1",
  currency: "USD",
});
```

### Что происходит под капотом (default режим)

1. SDK проверяет имя по regex и существование декларации в локальном schema-index.
2. SDK кодирует payload через `SchemaPair` (Protobuf binary, тот же путь, что у RPC). `type.verify()` бросает на невалидный payload **до** записи в outbox — `await sb.event.publish(...)` rejected'ится сразу.
3. SDK INSERT'ит row в локальный SQLite `event_outbox` (WAL mode, `synchronous=NORMAL`). Возврат из `event.publish` происходит после COMMIT.
4. Фоновой drainer SDK читает pending rows батчами и шлёт runtime через `Events.Publish`.
5. Runtime дедупает по `idempotency_key` и INSERT'ит `event_log` + `event_deliveries` (по строке на каждый matching consumer service) в **одной транзакции**. Payload — opaque bytes; runtime не декодит и не валидирует (ADR-0002).
6. Dispatcher push'ит deliveries через open Subscribe stream подписчикам. Subscriber декодирует payload через свой `SchemaPair` (тот же `define(name, spec)` на subscriber-стороне).

### PublishOpts

| Поле | Тип | По умолчанию | Что делает |
|---|---|---|---|
| `idempotencyKey` | `string` | `""` | Дедупликация на ingest (TTL 24h). Два publish с одним ключом от одного publisher service → один event_log. |
| `partitionKey` | `string` | `""` | FIFO-гарантия в рамках key для одного consumer service (см. §7). |
| `fireAndForget` | `boolean` | `false` | Пропустить outbox, отправить sync (см. §8). |
| `headers` | `Record<string,string>` | `{}` | Метаданные envelope. Едут по wire в `EventEnvelope.headers`, но в handler subscriber'а **не передаются** (handler получает только декодированный payload). |
| `occurredAtMs` | `number` | `Date.now()` | Время бизнес-события (а не ingest), unix-ms. Едет в `EventEnvelope.occurred_at_unix_ms`. |

`event.publish` возвращает `{ eventId }` — UUID v7 через npm-пакет `uuidv7` (монотонен в пределах одной миллисекунды).

---

## 4. Wildcard routing (AMQP)

Pattern — `[a-z0-9_-]+(\.[a-z0-9_-]+)*` плюс два специальных символа.

| Символ | Значение | Пример pattern | Совпадает | Не совпадает |
|---|---|---|---|---|
| `*` | ровно одна секция | `payment.*` | `payment.charged`, `payment.refunded` | `payment`, `payment.charged.partial` |
| `#` | ноль или больше секций | `payment.#` | `payment`, `payment.charged`, `payment.charged.partial` | `paymentx` |

Wildcards комбинируются: `order.*.created` ловит `order.online.created`, не ловит `order.created` и не ловит `order.x.y.created`.

> Матчинг patterns — **только на стороне runtime** (`registry.TopicMatch`, ADR-0002); в SDK matcher'а нет. SDK лишь регистрирует pattern как подписку и диспатчит входящие deliveries по точному имени события (см. §2).

---

## 5. Гарантии доставки

| Свойство | Поведение |
|---|---|
| Доставка | **At-least-once.** Идемпотентность — на стороне consumer'а (см. §6). |
| Persistence | SDK коммитит row в локальный SQLite (WAL) до возврата из `event.publish`; runtime INSERT в Postgres `event_log` + `event_deliveries` атомарно. |
| Push vs pull | Push через bidi gRPC stream + Ack/Nack от consumer'а. |
| Visibility timeout | Если consumer не Ack'нул за `events.visibility_timeout_ms` (default 30 000ms) — runtime считает что упал и redeliver. |
| Conditional Ack | Late Ack (после visibility expired) — игнорируется, не клобберит уже redelivered delivery. Возвращается OK клиенту (idempotent semantics). |
| Retries | Доставка ретраится до `events.max_attempts` (default 5). |
| DLQ | По достижении `events.max_attempts` — событие в DLQ с копией payload, retention `events.dlq_retention_ms` (default 30d). |
| Ordering | Default параллельный fanout. Per-key FIFO через `partitionKey` (см. §7). |

> **Не EOS.** Effective exactly-once достигается через ALO + application-side idempotency. Это стандарт индустрии (RabbitMQ, NATS, SQS).

---

## 6. Идемпотентность

Доставка at-least-once → handler **обязан** переживать дубликаты. SDK **не** делает client-side dedup доставок (ADR-0002): один и тот же `event_id` может прийти повторно, и handler будет вызван снова. Дедупликация — на двух уровнях.

### Application-side dedup (обязателен для не-идемпотентных эффектов)

Бизнесовый ключ + state (Redis/Postgres):

```ts
sb.event.handle("payment.charged", async (payload) => {
  const { transactionId } = payload as { transactionId: string };
  const key = `receipt:${transactionId}`;
  if (await redis.set(key, "1", { EX: 86400, NX: true }) !== "OK") return; // already done
  await sendReceipt(payload);
});
```

### Publisher-side idempotency

`idempotencyKey` дедупит на ingest в пределах одного publisher service. Второй publish с тем же ключом runtime отвечает `REJECTED_DUPLICATE` — повторного INSERT в `event_log` и повторного fanout нет:

```ts
await sb.event.publish("payment.charged", payload, { idempotencyKey: "tx-7" });
await sb.event.publish("payment.charged", payload, { idempotencyKey: "tx-7" });
// → один event_log, один fanout
```

Каждый вызов `publish()` всё равно возвращает свой локально сгенерированный `eventId` (SDK минтит uuidv7 до отправки); дедуп происходит на стороне runtime, не в возвращаемом значении. TTL ключа — настройка рантайма `rpc.idempotency_event_ttl_ms` (default 24h).

---

## 7. Partition key и ordering

По умолчанию события для одного consumer service доставляются **параллельно** между его инстансами. Если порядок важен (например, операции над одним `orderId` должны идти строго по очереди) — используй `partitionKey`:

```ts
await sb.event.publish("order.line.added",   payload, { partitionKey: "order-42" });
await sb.event.publish("order.line.removed", payload, { partitionKey: "order-42" });
// → строго по очереди на ОДНОМ instance consumer'а
```

### Что это даёт

- **FIFO в рамках key.** Dispatcher не claim'ит новую delivery с `partitionKey="order-42"` для того же consumer service, пока предыдущая `in_flight`. Реализовано через `NOT EXISTS` gate в SQL claim'е.
- **Sticky instance.** Pick инстанса детерминирован: `hash(partitionKey) mod len(connected_instances)`. Все события с одним key уходят на один pod.
- **Параллелизм между разными keys.** Разные `partitionKey` — независимые потоки, обрабатываются параллельно.

### Цена

- При изменении set'а instances (pod restart/scale) hash mod даёт другой target → текущая `in_flight` дождётся ack, дальше события идут на новый pod. Между rebalance'ом возможен micro-batch на нескольких pod'ах.
- Без `partitionKey` — full parallelism.

---

## 8. Fire-and-forget

Если событие — метрика или телеметрия (потеря допустима, latency критична) — используй `fireAndForget: true`:

```ts
await sb.event.publish("metric.counter", { name: "page_view", value: 1 }, {
  fireAndForget: true,
});
```

| | default (`fireAndForget=false`) | `fireAndForget=true` |
|---|---|---|
| Persistence в SDK SQLite | да (outbox, WAL) | **нет** — прямой gRPC-вызов |
| Гарантия доставки | ALO | **best-effort** (теряется при runtime down) |
| Latency | + outbox INSERT + drainer tick | минимум — один RPC round-trip |
| Use case | бизнес-события | метрики, аудит-лог |

> При `fireAndForget=true` SDK всё равно проводит schema validation на ingest. Runtime сохраняет `event_log` для аудита; deliveries fanout'ятся как обычно.

---

## 9. Outbox и retries (SDK side)

### Локальный SQLite outbox

SDK хранит outbox в `<dataDir>/sdk.db`, где `dataDir` — опция конструктора (default `./.servicebridge`, т. е. `./.servicebridge/sdk.db`):

| Колонка | Назначение |
|---|---|
| `id` | UUID v7 (monotonic) |
| `status` | `pending` / `inflight` / `failed`. Успех — row **удаляется** (отдельного `done` нет). |
| `attempts` | счётчик попыток (для backoff) |
| `next_attempt_at_ms` | когда drainer может попробовать снова |
| `last_error` | terminal или transient ошибка |

### Crash recovery

При `Storage.open()` SDK выполняет:
```sql
UPDATE event_outbox SET status='pending' WHERE status='inflight';
```
Это закрывает разрыв ALO: если процесс упал между `UPDATE status='inflight'` и `Events.Publish` RPC ack, следующий запуск drainer'а заберёт row.

### Backoff на сетевые сбои

Drainer пытается до 5 раз с лестницей `[1s, 5s, 30s, 2m, 10m]` ± 25% jitter. После 5 неудач → `status='failed'`, terminal (видно через outbox metrics).

### Статусы ответа runtime (drainer)

| Ответ runtime | SDK action |
|---|---|
| `ACCEPTED` / `REJECTED_DUPLICATE` | DELETE row (успех / идемпотентный дубль) |
| `REJECTED_INVALID_NAME` | `status='failed'`, terminal — НЕ retry |
| `REJECTED_FORBIDDEN` | `status='failed'`, terminal; SDK дёргает `onPolicyViolation` (publish — это access-policy `event.publish`, denied) |
| Сетевая ошибка / `UNSPECIFIED` | retry с backoff до `MAX_ATTEMPTS` |

Отдельных schema-отказов в протоколе нет (ADR-0002): payload валидируется на SDK-стороне в момент `encode()` (Protobuf `type.verify()`), невалидные payload-ы вообще не попадают в outbox.

> `REJECTED_FORBIDDEN` — это асинхронный отказ: `publish()` уже вернул OK (row в outbox), поэтому ошибку нельзя бросить вызывающему. SDK помечает row `failed` и логирует warn; owner может подключить callback `onPolicyViolation`, чтобы заэмитить `policy_violation`.

### Outbox cap

Если runtime недоступен надолго и outbox растёт — при достижении опции `maxOutboxRows` (default 100000) `event.publish` бросает `OutboxFullError`. Это явный сигнал, что что-то не так — лучше fail-fast, чем disk-full.

---

## 10. DLQ и replay

Когда attempts достигает `events.max_attempts` (visibility timeout + Nack'и подряд) — delivery уходит в `events_dlq`:

```
events_dlq:
  event_id, delivery_id, consumer_service,
  payload (копия из event_log), event_name, headers,
  last_error, dlq_at, replay_count, total_attempts
```

DLQ retention — настройка `events.dlq_retention_ms` (default 30d). Payload копируется в `events_dlq` своей строкой. Но **replay всё равно требует исходную строку `event_log`**: он re-join'ит `event_log` за `partition_key`. Если `event_log` уже очищен — replay возвращает `ErrEventLogPurged`.

### Admin API (server-side)

Runtime gRPC `Events`:
- `ReplayDlq(event_id)` — создаёт новую `event_deliveries` row pending, инкрементит `replay_count`.
- `ListDlq(limit, cursor)` — постраничный list, opaque base64 cursor.

В Node SDK отдельных admin-helper'ов для DLQ нет — управление DLQ идёт через runtime dashboard (UI gateway, порт 14444) или прямой gRPC.

> Replay — **opt-in duplication**. Если handler всё ещё бажный, событие снова попадёт в DLQ. `replay_count` растёт — operator видит pattern.

---

## 11. Schema versioning

Каждая published-схема идентифицируется `contract_hash` (SHA-256 hex от канонического описания schema pair, считается на SDK-стороне). Несколько версий одного события могут жить одновременно — keep-history:

```proto
// payment-v1.proto
message PaymentCharged { string tx_id = 1; }

// payment-v2.proto
message PaymentCharged { string tx_id = 1; string user_id = 2; }
```

```ts
// .proto без service-блока → input/output задаём явно (для события достаточно input;
// output обязателен — укажи любое сообщение, в данном случае тот же PaymentCharged).
// publisher v1 (старый сервис)
sb.event.define("payment.charged", {
  protoFile: "payment-v1.proto",
  input: "PaymentCharged",
  output: "PaymentCharged",
});

// publisher v2 (новый сервис, рядом с v1)
sb.event.define("payment.charged", {
  protoFile: "payment-v2.proto",
  input: "PaymentCharged",
  output: "PaymentCharged",
});
```

Runtime хранит две строки в `service_methods` с одинаковым `method_name='payment.charged'` и разными `contract_hash`. Publisher v1 шлёт envelopes с hash v1, publisher v2 — с hash v2. Subscriber декодирует каждое событие через тот `SchemaPair`, который сам объявил под этим именем — версии работают параллельно, но subscriber видит только ту версию, которую сам понимает.

### Cleanup

При старте сервиса SDK вызывает `sb.event.define()` для своих event types, и runtime обновляет `last_seen_at` для актуальных hash'ей в `service_methods`. Фоновый registry-GC раз в сутки удаляет `service_methods`, чей `last_seen_at` старше `registry.gc_retention_days` (default 30). Так старые версии схем, к которым давно не было активности, вычищаются автоматически.

### Pattern change (subscriber side)

Если subscriber меняет patterns между deploy'ами — pending/in_flight deliveries для исчезнувших patterns уходят в DLQ с `last_error='orphaned_pattern'`. Оператор видит и решает: ручной replay vs forget.

---

## 12. Ошибки

| Ситуация | SDK / Runtime | Что делать |
|---|---|---|
| Невалидное имя | `InvalidEventNameError` (SDK) до RPC | Поменять имя на `[a-z0-9_-]+(\.[a-z0-9_-]+)*` |
| Outbox переполнен | `OutboxFullError` (SDK) | Поднять опцию `maxOutboxRows` или диагностировать почему drainer не катится |
| `event.publish` до `start()` | `Error: events publisher not ready` (SDK) | Перенести вызов после `await sb.start()` |
| Имя не задекларировано / без spec | `Error: events: no schema registered for event "..."` (SDK, до RPC) | Добавить `sb.event.define(name, spec)` (со spec) до `start()` |
| Невалидный payload | encode `type.verify()` throws (SDK, до outbox) | Поправить payload под схему |
| Runtime down | drainer ретраит с backoff | Outbox пишет → publish возвращает OK; данные доедут после recovery |
| Publish запрещён политикой | runtime `REJECTED_FORBIDDEN` → drainer `status='failed'` + `onPolicyViolation` | Дать сервису action-rule `event.publish` на это имя |
| Subscriber без схемы | `Nack` `no_schema` → runtime ретраит → DLQ | Объявить `sb.event.define(name, spec)` на subscriber-стороне |
| Handler throws | `Nack` (с текстом ошибки) → runtime ретраит с backoff → DLQ | Сделать handler идемпотентным |

---

## 13. Шпаргалка

### Publisher

```ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge(URL, SERVICE_KEY);

// spec — путь к .schema.json (с fieldNumber) или .proto. Inline-объект нельзя.
sb.event.define("payment.charged", { schemaFile: "schemas/payment.json" });

await sb.start();

await sb.event.publish("payment.charged", {
  transactionId: "tx-7",
  amount: 42.0,
}, {
  idempotencyKey: "tx-7",
  partitionKey: "user-42",
});
```

### Subscriber

```ts
import { ServiceBridge } from "service-bridge";

const sb = new ServiceBridge(URL, SERVICE_KEY);

// Subscriber объявляет ту же схему — для decode входящего payload.
sb.event.define("payment.charged", { schemaFile: "schemas/payment.json" });

// Handler вызывается по ТОЧНОМУ имени события (см. §2).
sb.event.handle("payment.charged", async (payload) => {
  // idempotent business work
  await processPayment(payload);
});

await sb.start();
```

### Fire-and-forget телеметрия

```ts
await sb.event.publish("metric.counter", { name, value }, { fireAndForget: true });
```

→ Дальше: [Workflows](./workflows.md)
