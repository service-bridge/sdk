# Events

← [RPC](./rpc.md) · Дальше: [Workflows](./workflows.md) →

Полный гайд по Durable Events: декларация, подписка, эмиссия, гарантии доставки (at-least-once), wildcard routing и фильтры, идемпотентность, partition ordering, fire-and-forget, очередь публикаций, DLQ + replay, schema versioning, ошибки. Читается линейно. Для операционных тем — [Operations](./operations.md).

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
- [9. Очередь публикаций и повторы (SDK side)](#9-очередь-публикаций-и-повторы-sdk-side)
- [10. DLQ и replay](#10-dlq-и-replay)
- [11. Schema versioning](#11-schema-versioning)
- [12. Ошибки](#12-ошибки)
- [13. Шпаргалка](#13-шпаргалка)

---

## Краткая модель

ServiceBridge Events — это **durable** pub/sub поверх runtime с гарантией **at-least-once**:

- **Publisher** декларирует событие через `sb.event.define(name, spec)` и эмитирует через `sb.event.publish(name, payload, opts?)`. `publish` резолвится, когда runtime записал событие в Postgres.
- **Subscriber** регистрирует обработчик через `sb.event.handle(pattern, fn, opts?)` со своей схемой и, при желании, фильтром.
- Runtime пишет `event_log` + `event_deliveries` (по строке на каждого matching подписчика) в одной транзакции, вычисляет шаблоны и фильтры подписок.
- Доставка идёт **push'ом** через long-lived bidi gRPC stream. Subscriber отвечает `Ack` после успеха или `Nack` при ошибке. При сбое — redelivery после **visibility timeout** или после crash'а consumer'а.
- Когда число попыток достигает порога `events.max_attempts` (default 5) — событие уходит в **DLQ** с копией payload.
- Один SDK-инстанс может одновременно быть publisher и subscriber для разных событий.

> Visibility timeout, max attempts, DLQ retention и backoff-лестница — это **настройки рантайма** (`events.*`), а не вшитые константы. Они правятся в UI `/settings` без рестарта. Значения ниже — дефолты.

> **Архитектурный аксиом:** runtime в системе всегда один. Горизонтальное масштабирование — на стороне SDK-консьюмеров.

---

## 1. Декларация: event.define

Publisher объявляет каждое событие, которое публикует, — это формирует service map runtime'а и регистрирует Protobuf-схему, по которой SDK кодирует payload. Subscriber `define` не вызывает (см. §2).

```ts
sb.event.define(
  name: string,
  spec: SchemaSpec,
): void
```

`SchemaSpec` — тот же тип, что и у `sb.rpc.handle`: либо `.proto` файл (`ProtoFileSpec`), либо `.schema.json` (`JsonSchemaFileSpec`) c явными `fieldNumber`. Inline JSON Schema не поддерживается (ADR-0002).

Вызывать **до `sb.start()`**. Повторный `define` с тем же объектом spec — no-op; с другим spec — `ValidationError`. Publish события, которое не объявлено через `define`, — `StateError`.

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

Файл обязан иметь на верхнем уровне два блока — `input` и `output` — в каждом ровно одно сообщение. Для события payload — это `input`; `output` обязателен по формату (парсер требует оба блока), но событие его не использует: им ничего не кодируется и в `contract_hash` он не входит — пиши что угодно.

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

SDK строит Protobuf `Type` через `protobufjs`, считает `contract_hash` по структуре payload-а (у события нет ответа, поэтому вторую половину пары всегда занимает пустое сообщение — точно так же считает Go SDK), регистрирует имя + хеш в runtime и хранит compiled `SchemaPair` локально. При `publish` payload кодируется в Protobuf binary; объект, который не кодируется схемой, отклоняется `ValidationError` до постановки в очередь.

### Имя события

| Правило | Пример |
|---|---|
| Только `[a-z0-9_-]` + `.` | `payment.charged`, `order.line-added` |
| Минимум 1 секция | `ok` (но обычно ≥2) |
| Без пустых сегментов | `payment.charged` ✅, `payment..charged` ❌ |
| Без leading/trailing dot | `payment.charged` ✅, `.payment` / `payment.` ❌ |

Имя проверяется на SDK-стороне при `publish` (`InvalidEventNameError` до отправки). Если runtime всё же ответил `REJECTED_INVALID_NAME`, `publish` отклоняется той же `InvalidEventNameError`.

---

## 2. Подписка: event.handle

```ts
sb.event.handle(
  pattern: string,
  fn: (payload: unknown, ctx: EventHandlerContext) => Promise<void> | void,
  opts?: { schema?: SchemaSpec; filter?: Record<string, unknown> },
): void
```

Вызывать **до `sb.start()`**. `pattern` — точное имя события или AMQP-шаблон (`*`, `#`, см. §4).

- **Один хендлер на шаблон** в процессе: повторный `handle` с тем же `pattern` — `ValidationError`.
- **`opts.schema`** — схема подписчика для декодирования payload. Subscriber не вызывает `define`: `define` объявляет «этот сервис публикует событие». Без `schema` хендлер получает сырые байты (`Uint8Array`). Для `.proto` без service-блока задайте `input` и `output` (или `method`) явно — имя шаблона не может быть именем rpc.
- **`opts.filter`** — Filter Expression: объект `{"$.path": literal}`, все условия — равенства. Runtime вычисляет его по JSON-виду payload до доставки; событие, не прошедшее фильтр, этому подписчику не доставляется. Невалидный фильтр runtime отклоняет при регистрации, и бридж останавливается с `ValidationError`.

```ts
sb.event.handle(
  "payment.charged",
  async (payload, ctx) => {
    const { transactionId } = payload as { transactionId: string };
    await sendReceipt(transactionId, { attempt: ctx.attempt });
  },
  {
    schema: { protoFile: "schemas/payment.proto", method: "PaymentCharged" },
    filter: { "$.currency": "USD" },
  },
);
```

### Контекст доставки

```ts
interface EventHandlerContext {
  eventId: string;
  eventName: string;            // конкретное имя, под которым событие опубликовано
  attempt: number;              // номер попытки доставки
  deliveryId: string;
  leaseToken: string;
  partitionKey: string;
  headers: Record<string, string>;   // PublishOpts.headers издателя
  occurredAtMs: number;         // PublishOpts.occurredAtMs издателя, unix-ms
  signal: AbortSignal;          // обрыв стрима доставки или остановка бриджа
}
```

### Маршрутизация

Шаблоны матчит runtime (ADR-0002). В каждой доставке он присылает список шаблонов этого сервиса, которые совпали с именем события и прошли фильтр. SDK вызывает хендлер каждого такого шаблона, который есть у процесса, по очереди:

```ts
const paymentSpec = { protoFile: "schemas/payment.proto", method: "PaymentCharged" };
sb.event.handle("payment.*", auditPayment, { schema: paymentSpec });
sb.event.handle("payment.charged", sendReceipt, { schema: paymentSpec });
// payment.charged → одна доставка, вызываются оба хендлера; один Ack за доставку
```

Ack уходит, только если все вызванные хендлеры успешны; первый throw → `Nack` с текстом ошибки (runtime ретраит и при исчерпании попыток уводит в DLQ). Если у процесса нет хендлера ни для одного совпавшего шаблона (rolling deploy: другой инстанс того же сервиса уже подписан на новый шаблон) — `Nack` «no handler for matched patterns», и runtime передоставит событие, вероятно, другому инстансу.

Доставки с одним `partitionKey` обрабатываются в инстансе строго по очереди; без ключа — параллельно, не больше `eventsMaxInFlight` (по умолчанию 32) одновременно.

---

## 3. Эмиссия: event.publish

`sb.event.publish(name, payload, opts?)` отправляет событие. Вызывается **после `sb.start()`** (вызов до него — `StateError`) и для имени, задекларированного через `sb.event.define(name, spec)`.

```ts
sb.event.publish<T>(
  name: string,
  payload: T,
  opts?: PublishOpts,
): Promise<{ eventId: string }>
```

```ts
const { eventId } = await sb.event.publish("payment.charged", {
  transactionId: "tx-7",
  amount: 42.0,
  userId: "u-1",
  currency: "USD",
});
```

### Что происходит под капотом (default режим)

1. SDK проверяет имя по regex и наличие декларации. Нарушение — отказ сразу.
2. SDK кодирует payload через `SchemaPair` (Protobuf binary, тот же путь, что у RPC) и кладёт рядом JSON-вид payload — по нему runtime вычисляет фильтры подписок и условия `wait_event` workflow.
3. Событие встаёт в очередь в памяти и получает `eventId` — UUIDv7, монотонный в порядке вызовов `publish`.
4. Отправитель шлёт очередь в `Events.Publish` пачками до 100 событий, один запрос в полёте.
5. Runtime дедупает по `idempotency_key` и INSERT'ит `event_log` + `event_deliveries` (по строке на каждый matching consumer service) в **одной транзакции**. Payload — opaque bytes; runtime не декодит и не валидирует (ADR-0002).
6. По ответу runtime `publish` резолвится `{ eventId }` или отклоняется ошибкой (§9).
7. Dispatcher push'ит deliveries через open Subscribe stream подписчикам. Subscriber декодирует payload своей схемой из `opts.schema`.

### PublishOpts

| Поле | Тип | По умолчанию | Что делает |
|---|---|---|---|
| `idempotencyKey` | `string` | `""` | Дедупликация на ingest (TTL 24h). Повтор с тем же ключом и тем же содержимым — успех с `eventId` исходного события; с другим содержимым — `CONFLICT`. |
| `partitionKey` | `string` | `""` | FIFO-гарантия в рамках key (см. §7). |
| `fireAndForget` | `boolean` | `false` | Резолв сразу после постановки в очередь, без ожидания ACK runtime (см. §8). |
| `headers` | `Record<string,string>` | `{}` | Метаданные envelope. Подписчик получает их в `ctx.headers`. |
| `occurredAtMs` | `number` | `Date.now()` | Время бизнес-события (а не ingest), unix-ms. Подписчик получает его в `ctx.occurredAtMs`. |

---

## 4. Wildcard routing (AMQP)

Pattern — `[a-z0-9_-]+(\.[a-z0-9_-]+)*` плюс два специальных символа.

| Символ | Значение | Пример pattern | Совпадает | Не совпадает |
|---|---|---|---|---|
| `*` | ровно одна секция | `payment.*` | `payment.charged`, `payment.refunded` | `payment`, `payment.charged.partial` |
| `#` | ноль или больше секций | `payment.#` | `payment`, `payment.charged`, `payment.charged.partial` | `paymentx` |

Wildcards комбинируются: `order.*.created` ловит `order.online.created`, не ловит `order.created` и не ловит `order.x.y.created`.

> Матчинг patterns — **только на стороне runtime** (`registry.TopicMatch`, ADR-0002); в SDK matcher'а нет. SDK регистрирует pattern как подписку и вызывает хендлеры тех шаблонов, которые runtime перечислил в доставке (см. §2).

---

## 5. Гарантии доставки

| Свойство | Поведение |
|---|---|
| Доставка | **At-least-once.** Идемпотентность — на стороне consumer'а (см. §6). |
| Persistence | `event.publish` резолвится после того, как runtime атомарно записал `event_log` + `event_deliveries` в Postgres. До этого событие живёт только в памяти процесса. |
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

`idempotencyKey` дедупит на ingest в пределах одного publisher service. Второй publish с тем же ключом runtime отвечает `REJECTED_DUPLICATE` — повторного INSERT в `event_log` и повторного fanout нет, а `publish` резолвится успешно с `eventId` **исходного** события:

```ts
const a = await sb.event.publish("payment.charged", payload, { idempotencyKey: "tx-7" });
const b = await sb.event.publish("payment.charged", payload, { idempotencyKey: "tx-7" });
// a.eventId === b.eventId → один event_log, один fanout
```

Тот же ключ с другим содержимым — `ServiceBridgeError` с кодом `CONFLICT`. TTL ключа — настройка рантайма `rpc.idempotency_event_ttl_ms` (default 24h).

Ключ делает безопасным повтор после `TimeoutError` «outcome unknown» (§9): если первая попытка дошла до runtime, повтор вернёт её `eventId`.

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
- **Порядок на стороне SDK.** Издатель отправляет события одного ключа в порядке вызовов `publish`, даже если какие-то попытки отправки упали (§9); подписчик обрабатывает доставки одного ключа последовательно (§2).

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
| Когда резолвится `publish` | после ACK runtime (событие в Postgres) | сразу после постановки в очередь в памяти |
| Отправка | очередь + повторы до `publishTimeoutMs` | та же очередь, те же повторы |
| Падение процесса до ACK | вызывающий не получил `eventId` — знает, что событие не подтверждено | событие **теряется** молча |
| Терминальный отказ runtime (`CONFLICT`, `ACCESS_DENIED`, ...) | ошибка вызывающему | только warn в `logger` |
| Use case | бизнес-события | метрики, аудит-лог |

`QUEUE_FULL` при переполненной очереди бросается и в режиме fire-and-forget. Валидация имени и схемы — тоже.

---

## 9. Очередь публикаций и повторы (SDK side)

SDK не пишет события на диск. Пока runtime не подтвердил событие, оно живёт в ограниченной очереди в памяти процесса.

### Очередь и таймаут

| Опция конструктора | По умолчанию | Что делает |
|---|---|---|
| `maxPendingPublishes` | `10000` | Сколько событий может ждать ACK. Сверх — `publish` сразу отклоняется `ServiceBridgeError` с кодом `QUEUE_FULL` (`retryable: true`). |
| `publishTimeoutMs` | `30000` | Сколько одно событие ждёт ACK. Истёк — `TimeoutError`. |

Текст `TimeoutError` говорит, ушло ли событие в runtime:

- «not sent» — запрос с событием не отправлялся ни разу, событие удалено из очереди. Повтор безопасен.
- «outcome unknown» — запрос ушёл, ответа нет; runtime мог сохранить событие. Повтор безопасен только с тем же `idempotencyKey` (§6).

### Повторы

Транспортная ошибка или ответ `UNSPECIFIED` — повтор того же envelope (тот же `eventId`) с паузами 100, 250, 500, 1000, 2000, 5000 мс (дальше 5000), пока не истёк `publishTimeoutMs`. После reconnect повтор идёт сразу. Повтор события, которое runtime уже сохранил, получает `REJECTED_DUPLICATE` и резолвится успешно.

### Порядок

В полёте один запрос `Publish`; в запросе не больше одного события на непустой `partitionKey`. Если событие ключа получило временный отказ, следующие события того же ключа ждут за ним — runtime получает события ключа в порядке вызовов `publish`.

### Статусы ответа runtime

| Ответ runtime | Результат `publish` |
|---|---|
| `ACCEPTED` | `{ eventId }` |
| `REJECTED_DUPLICATE` | `{ eventId }` исходного события |
| `REJECTED_CONFLICT` | `ServiceBridgeError` `CONFLICT` — тот же ключ/id с другим содержимым |
| `REJECTED_INVALID_NAME` | `InvalidEventNameError` |
| `REJECTED_FORBIDDEN` | `AccessDeniedError` + событие `policy_violation` (`declaration: "event.publish"`, `denySide: "self_egress"`) |
| Сетевая ошибка / `UNSPECIFIED` | повтор с backoff до `publishTimeoutMs` |

Отдельных schema-отказов в протоколе нет (ADR-0002): payload валидируется на SDK-стороне в момент кодирования.

### Остановка

`sb.stop()` досылает очередь в пределах `stopTimeoutMs`. То, что не успело получить ACK, отклоняется `ServiceBridgeError` с кодом `CONNECTION` («client stopped before the runtime acknowledged it»).

### Транзакционная граница

`publish` не атомарен с транзакцией в вашей базе. Если бизнес-изменение и событие должны зафиксироваться вместе, сохраните намерение опубликовать в той же транзакции, что и изменение, и публикуйте из своей таблицы с `idempotencyKey` = id записи.

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

Каждая published-схема идентифицируется `contract_hash` — отпечатком protobuf-провода вида `v2:<sha256 hex>`, который считается на SDK-стороне (алгоритм — в [rpc.md](./rpc.md#алгоритм-хеша)). У события хешируется только payload: ответа у него нет, поэтому вторую половину пары занимает пустое сообщение, а объявленный в spec `output` на хеш не влияет. Меняет идентичность события ровно то же, что ломает wire-совместимость payload-а: номер, тип или кардинальность поля — не его имя. Несколько версий одного события могут жить одновременно — keep-history:

```proto
// payment-v1.proto
message PaymentCharged { string tx_id = 1; }

// payment-v2.proto
message PaymentCharged { string tx_id = 1; string user_id = 2; }
```

```ts
// .proto без service-блока → input/output задаём явно. Смысл несёт только input:
// output обязателен по формату spec и в contract_hash не входит, здесь это тот же
// PaymentCharged.
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

Runtime хранит две строки в `service_methods` с одинаковым `method_name='payment.charged'` и разными `contract_hash`. Publisher v1 шлёт envelopes с hash v1, publisher v2 — с hash v2. Subscriber декодирует каждое событие схемой своей подписки (`opts.schema`) — версии работают параллельно; новые поля v2, которых нет в схеме подписчика, при декодировании отбрасываются.

### Cleanup

При старте сервиса SDK вызывает `sb.event.define()` для своих event types, и runtime обновляет `last_seen_at` для актуальных hash'ей в `service_methods`. Фоновый registry-GC раз в сутки удаляет `service_methods`, чей `last_seen_at` старше `registry.gc_retention_days` (default 30). Так старые версии схем, к которым давно не было активности, вычищаются автоматически.

### Pattern change (subscriber side)

Если subscriber меняет patterns между deploy'ами — pending/in_flight deliveries для исчезнувших patterns уходят в DLQ с `last_error='orphaned_pattern'`. Оператор видит и решает: ручной replay vs forget.

---

## 12. Ошибки

| Ситуация | SDK / Runtime | Что делать |
|---|---|---|
| Невалидное имя | `InvalidEventNameError` (SDK) до отправки | Поменять имя на `[a-z0-9_-]+(\.[a-z0-9_-]+)*` |
| `event.publish` до `start()` | `StateError` «events: publish before start()» | Перенести вызов после `await sb.start()` |
| Имя не задекларировано | `StateError` «events: no schema for event "..."» | Добавить `sb.event.define(name, spec)` до `start()` |
| Невалидный payload | `ValidationError` «payload of "..." does not match its schema» (SDK, до очереди) | Поправить payload под схему |
| Очередь переполнена | `ServiceBridgeError` `QUEUE_FULL` (`retryable`) | Runtime недоступен дольше, чем выдерживает очередь: подождать и повторить, поднять `maxPendingPublishes` |
| Runtime не ответил вовремя | `TimeoutError` «not sent» / «outcome unknown» | Повторить; после «outcome unknown» — с тем же `idempotencyKey` |
| Тот же `idempotencyKey`, другое содержимое | `ServiceBridgeError` `CONFLICT` | Разным событиям — разные ключи |
| Publish запрещён политикой | `AccessDeniedError` + `policy_violation` | Дать сервису право `event.publish` на это имя |
| Остановка с непосланными событиями | `ServiceBridgeError` `CONNECTION` | Увеличить `stopTimeoutMs` |
| Дубль `handle` на тот же шаблон | `ValidationError` при регистрации | Один хендлер на шаблон |
| Невалидный `filter` | runtime отклоняет регистрацию → бридж останавливается, `ValidationError` | Исправить фильтр |
| Subscriber не смог декодировать payload | `Nack` «decode for pattern ...» → runtime ретраит → DLQ | Согласовать схему подписчика со схемой издателя |
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

const { eventId } = await sb.event.publish("payment.charged", {
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

// Схема подписчика — в opts.schema; define на стороне подписчика не нужен.
sb.event.handle("payment.*", async (payload, ctx) => {
  // idempotent business work, ключ дедупа — ctx.eventId или бизнес-id
  await processPayment(payload, ctx.eventId);
}, {
  schema: { schemaFile: "schemas/payment.json" },
  filter: { "$.currency": "USD" },
});

await sb.start();
```

### Fire-and-forget телеметрия

```ts
await sb.event.publish("metric.counter", { name, value }, { fireAndForget: true });
```

→ Дальше: [Workflows](./workflows.md)
