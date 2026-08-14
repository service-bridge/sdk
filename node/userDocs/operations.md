# Operations

← [Integrations](./integrations.md) · Дальше: [API reference](./api-reference.md) →

Операционные темы для всех доменов: конструктор и lifecycle, identity, advertise, security (bootstrap key, mTLS, rotation), env-переменные, troubleshooting.

## Содержание

- [1. Конструктор и опции](#1-конструктор-и-опции)
- [2. Lifecycle: start / stop](#2-lifecycle-start--stop)
- [3. События](#3-события)
- [4. Identity и serviceMap](#4-identity-и-servicemap)
- [5. Inbound CallServer (advertise)](#5-inbound-callserver-advertise)
- [6. Security: bootstrap key, mTLS, ротация](#6-security-bootstrap-key-mtls-ротация)
- [7. Environment variables](#7-environment-variables)
- [8. Troubleshooting](#8-troubleshooting)
- [9. Telemetry: операции, логи, метрики](#9-telemetry-операции-логи-метрики)

---

## 1. Конструктор и опции

```ts
import { ServiceBridge } from "service-bridge";

new ServiceBridge(url: string, key: string, options?: ServiceBridgeOptions)
```

```ts
interface ServiceBridgeOptions {
  reconnectIntervalMs?: number;        // default 3000
  reconnectAttempts?: number;          // default 3 (0 = безлимит)
  advertise?: { host: string; port: number } | false; // default: 127.0.0.1 на свободном порту (+warning)
  callDefaults?: CallOpts;             // default {}
  failOnPolicyViolation?: boolean;     // default false
  dataDir?: string;                    // default "./.servicebridge"
  maxOutboxRows?: number;              // default 100000
  eventsDrainerBatch?: number;         // default 50
  eventsMaxInFlight?: number;          // default 32
}
```

| Поле | Что делает |
|------|-----------|
| `reconnectIntervalMs` | Задержка между попытками reconnect. |
| `reconnectAttempts` | Максимум попыток. По исчерпании — `disconnected` с `reason: "exhausted"`. `0` = бесконечный. |
| `advertise` | Inbound CallServer. См. §5. |
| `callDefaults` | Дефолтные `CallOpts` для всех `sb.rpc.call`/`sb.stream`. См. [RPC §4](./rpc.md#4-callopts). |
| `failOnPolicyViolation` | `true` → нарушение политики в первом снапшоте обрывает `start()` (`disconnected`, `reason: "policy"`). По умолчанию `false` — только событие `policy_violation`. |
| `dataDir` | Каталог локального SQLite-outbox. По умолчанию `"./.servicebridge"`. |
| `maxOutboxRows` | Потолок строк в event-outbox до back-pressure на publish. По умолчанию `100000`. |
| `eventsDrainerBatch` | Сколько строк дренер событий тянет за тик. По умолчанию `50`. |
| `eventsMaxInFlight` | Максимум параллельно обрабатываемых inbound-событий. По умолчанию `32`. |

Telemetry on/off и payload cap управляются runtime-настройками UI (Settings → Telemetry):
- `telemetry.enable` (`true`/`false`) — рантайм пушит в SDK через `CaptureModes.telemetry_enabled`. Когда `false`, transport не стартует (ops/logs/metrics буферизуются в ring, не отправляются). Fail-safe до первого снапшота: включён.
- `telemetry.payload_max_bytes` — per-direction cap payload'а в байтах. Пушится в SDK через `CaptureModes.payload_max_bytes`. Fail-safe до первого снапшота: `65536`.

Overlap-rotation leaf-сертификата выполняется автоматически (за 30 минут до expiry) — публичной опции у неё нет.

### Типичные пресеты

```ts
// Локальная разработка
const sb = new ServiceBridge(URL, KEY);

// Production callee
const sb = new ServiceBridge(URL, KEY, {
  advertise: { host: process.env.POD_IP!, port: 7777 },
  reconnectAttempts: 0,
  callDefaults: { timeout: "5s", retry: { maxAttempts: 3 } },
});

// Caller-only сервис
const sb = new ServiceBridge(URL, KEY, { advertise: false });
```

---

## 2. Lifecycle: start / stop

```ts
await sb.start();
await sb.stop();
```

### Что делает start()

1. Парсит `key` → извлекает `(key_id, secret, ca_cert)`.
2. Открывает временный TLS-канал, зовёт `Bootstrap.Provision` → получает signed leaf cert.
3. Открывает long-lived mTLS-канал.
4. (Если `advertise !== false`) поднимает локальный CallServer.
5. Отправляет `RegisterRequest` со всеми хендлерами и зависимостями.
6. Ждёт `Welcome` от рантайма — после этого вызовы безопасны.

`start()` **НЕ** бросает на не-retryable connect-ошибки (`UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT`) — они приходят через `disconnected` event.

### Что делает stop()

Graceful shutdown: heartbeat off → cert refresh таймеры off → CallServer останавливается с graceful drain → mTLS-канал закрывается.

### Graceful shutdown

```ts
process.on("SIGTERM", async () => { await sb.stop(); process.exit(0); });
process.on("SIGINT",  async () => { await sb.stop(); process.exit(0); });
```

---

## 3. События

```ts
sb.on("connected",    (e: { sessionId, serviceId, serviceName }) => {});
sb.on("reconnecting", (e: { attempt, delayMs, reason }) => {});
sb.on("disconnected", (e: { reason, error }) => {});
```

`connected` не несёт `instanceId` — его берите из `sb.identity()` (см. §4).

| Событие | Когда |
|---------|-------|
| `connected` | После `Welcome`. Срабатывает на первом connect И на каждом успешном reconnect (включая overlap-rotation). |
| `reconnecting` | Соединение упало, переподключаемся с backoff. `attempt` начинается с 1. |
| `disconnected` | Сессии больше нет. См. таблицу `reason` ниже. |

### reason values

| `reason` | Что произошло |
|----------|---------------|
| `"stopped"` | Вы вызвали `sb.stop()`. |
| `"drain"` | Runtime запросил graceful drain (рестарт runtime / удаление сервиса). |
| `"exhausted"` | Все `reconnectAttempts` исчерпаны. |
| `"error"` | Не-retryable ошибка (`UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT`). |

### Пример: критическая остановка

```ts
sb.on("disconnected", ({ reason, error }) => {
  if (reason === "drain" || reason === "stopped") return;
  console.error("[fatal]", error?.name, error?.message);
  process.exit(1);   // pod restart
});
```

---

## 4. Identity и serviceMap

### identity()

```ts
sb.identity(): { sessionId, serviceId, serviceName, instanceId } | null
```

`null` до первого `connected` и после `stop()`.

```ts
sb.on("connected", () => {
  const id = sb.identity()!;
  sb.logger.info("ready", { service: id.serviceName, instance: id.instanceId });
});
```

`instanceId` (12-символьная Crockford-base32 строка сессии) доступен также напрямую через `sb.instanceIdString()` — пустая строка до первого `connected`.

### serviceMap()

```ts
sb.serviceMap(): ReadonlyMap<string, MethodDescriptor>
```

Живой snapshot всех методов, на которые этот сервис подписан. Автообновляется при connect/disconnect провайдеров (через `RegisterAndWatch`).

```ts
interface MethodDescriptor {
  serviceName: string;
  serviceId: string;
  instanceId: string;
  type: "rpc" | "stream" | "event" | "workflow" | "job" | "http";
  name: string;
  published: boolean;
  streaming: boolean;
  contractHash: string;     // "v1:<hex>" или ""
  callEndpoint: string;     // host:port или "" если callee без advertise
}
```

### Ожидание появления метода

```ts
async function waitForMethod(sb: ServiceBridge, svc: string, name: string, timeoutMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    for (const d of sb.serviceMap().values()) {
      if (d.serviceName === svc && d.name === name) return;
    }
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`timeout waiting for ${svc}/${name}`);
}
```

---

---

## 5. Inbound CallServer (advertise)

SDK-инстанс способен принимать **входящие** RPC только если у него поднят локальный CallServer и runtime знает его `call_endpoint`.

| `advertise` значение | Поведение |
|---------------------|-----------|
| **не указано** | Bind `127.0.0.1` на свободном порту + warning. **Только для dev**: 127.0.0.1 недоступен из других подов. |
| `{ host, port }` | Явный bind. `port: 0` = ОС выбирает. **Рекомендуется для production.** |
| `false` | Caller-only mode. CallServer не поднимается, в реестре нет `call_endpoint`. |

### Сценарии

```ts
// Production callee — конкретный pod IP
new ServiceBridge(URL, KEY, {
  advertise: { host: process.env.POD_IP!, port: 7777 },
});

// Caller-only — экономит порт, не светит endpoint
new ServiceBridge(URL, KEY, { advertise: false });

// Local dev — оставляем default или
new ServiceBridge(URL, KEY, { advertise: { host: "127.0.0.1", port: 0 } });
```

⚠️ **Не указывайте `0.0.0.0` как advertise host** — другие сервисы попытаются подключиться к `0.0.0.0:port`, что не работает. Используйте конкретный IP пода/контейнера.

### Что попадает в реестр

После старта CallServer, `Descriptor.call_endpoint = "host:port"` (с фактическим портом если был `0`). Это значение видят все другие SDK через `serviceMap()` и используют для direct-вызовов.

```ts
for (const d of caller.serviceMap().values()) {
  console.log(d.serviceName, "→", d.callEndpoint || "(proxy-only)");
}
```

---

## 6. Security: bootstrap key, mTLS, ротация

### Bootstrap key

Каждому сервису нужен `sb.<base64url>`-ключ, содержащий:
- `key_id` (8 байт, идентификатор ключа в БД runtime)
- `secret` (32 байта, proof of possession)
- `ca_cert_der` (CA рантайма для TLS trust при bootstrap-вызове)

### Генерация

Дашборд рантайма на `http://localhost:14444`: **Services → Create service**, задайте имя, скопируйте выданную строку `sb.Cgj...`. CA автоматически хранится в Postgres (таблица `runtime_ca`), файлы сертификатов не нужны.

Сохраните строку `sb.Cgj...` как env-переменную.

```sh
# .env (в .gitignore!)
SERVICEBRIDGE_URL=localhost:14445
SERVICEBRIDGE_SERVICE_KEY=sb.Cgj...XYZ
```

⚠️ **Никогда не коммитьте ключи.** Не логируйте их. Эквивалентны паролю.

### mTLS lifecycle

После `sb.start()`:
1. Bootstrap.Provision → получаем leaf cert (TTL 1 час).
2. Long-lived mTLS-канал с runtime.
3. За 30 минут до expiry — **overlap rotation**: новый канал с новым cert, ждём первый `Welcome`, переключаемся, старый канал drain.
4. Все этапы — без потери in-flight вызовов. На каждой rotation эмитится повторный `connected`.

### SPIFFE identity

Leaf cert содержит SPIFFE URI SAN:
```
spiffe://servicebridge/service/<service_id>/instance/<instance_id>
```

В direct mode SDK валидирует SPIFFE SAN сервера — защита от подмены инстанса.

### Ротация скомпрометированного ключа

Создайте новый ключ для того же сервиса через дашборд (**Services → Create service**) и обновите env-переменные.

После rotation:
1. Обновите env-переменную.
2. Restart сервиса (SDK подхватит новый ключ).
3. Удалите старую запись через runtime API/dashboard — инвалидирует leaf certs со старым `key_id`.

### Несколько процессов с одним ключом

Поддерживается — каждый получает свой `instance_id` при `start()`, runtime видит их как разные instances одного сервиса. Это стандартный horizontal scaling.

---

## 7. Environment variables

### SDK

| Переменная | Default | Что делает |
|-----------|---------|-----------|
| `SERVICEBRIDGE_URL` | — | Адрес runtime (`host:port`). Передаётся в конструктор. |
| `SERVICEBRIDGE_SERVICE_KEY` | — | Bootstrap-ключ. Передаётся в конструктор. |

> Это **ваша** конвенция имён, а не то, что читает SDK. SDK не читает из env ничего — ни `URL`/`SERVICE_KEY`, ни advertise host. Вы сами передаёте `process.env.X!` в конструктор, а `advertise` задаёте в коде. Это явно по дизайну.

### Runtime

Из env рантайм читает **только** Postgres-подключение — единственное, что не может жить в БД. Остальное (порты gRPC `14445` и UI gateway `14444`, shutdown-таймаут, session TTL, idempotency TTL) — настройки в БД, редактируются в UI на странице Settings.

| Переменная | Default | Что делает |
|-----------|---------|-----------|
| `POSTGRES_HOST` | — (required) | Postgres host. |
| `POSTGRES_PORT` | — (required) | Postgres port. |
| `POSTGRES_USER` | — (required) | DB user. |
| `POSTGRES_PASSWORD` | — (required) | DB password. |
| `POSTGRES_DB` | — (required) | DB name. |
| `POSTGRES_SSLMODE` | `disable` | libpq-режим: `disable` / `allow` / `prefer` / `require` / `verify-ca` / `verify-full`. |
| `POSTGRES_MAX_CONNS` | `10` | Pool max conns. |
| `POSTGRES_MIN_CONNS` | `0` | Pool min conns. |
| `POSTGRES_CONNECT_TIMEOUT` | `5s` | DB connect timeout. |

> Требуется PostgreSQL 18+.

---

## 8. Troubleshooting

### disconnected сразу после start()

`error.code = 16 (UNAUTHENTICATED)` → bootstrap key invalid:
- Пустая/обрезанная env-переменная.
- Запись удалена из БД runtime.
- БД runtime пересоздана.

Сгенерируйте новый ключ через дашборд рантайма (**Services → Create service**), обновите env.

### no descriptor for `<svc>/<method>`

Метод не в `serviceMap()`:
1. Не вызван `sb.service(svc, { rpc: [...] })` или `sb.client(svc, ...)`.
2. Callee offline / не зарегистрировал handler.
3. Опечатка в имени (case-sensitive).

```ts
console.log("snapshot:");
for (const d of sb.serviceMap().values()) {
  console.log(" ", d.serviceName, d.type, d.name);
}
```

### no SchemaPair for `<svc>/<method>`

Caller не вызвал `useSchema()` (и не использует typed client). Решение: `await sb.useSchema(svc, method, { protoFile: "..." })` или `await sb.client(svc, "...")`.

### no instance ... matches caller contract `<hash>`

Все инстансы callee имеют другой `contract_hash` — version mismatch. См. [RPC §8](./rpc.md#8-версионирование-контракта). Обычно решается deploy совместимой версии callee.

### transport="direct" requested but no endpoint

Callee запущен с `advertise: false` или без advertise (в loopback default). Решения:
- Используйте `transport: "auto"` для fallback на proxy.
- Включите advertise на callee (production: `{ host: POD_IP, port: ... }`).

### no service block found

`sb.client()` на `.proto` без `service`. Добавьте service block или используйте низкоуровневый API.

### cannot resolve input/output for method "X"

Auto-resolve не нашёл messages. Укажите явно: `{ protoFile, input: "XReq", output: "XRes" }`. Подробности резолюции — [RPC §2.4](./rpc.md#24-резолюция-inputoutput-proto).

### advertise not configured warning

Не указана `advertise`, поэтому inbound CallServer сел на недостижимый `127.0.0.1`. Для production: задайте `advertise: { host, port }` явно. Для caller-only: `advertise: false`.

### Все retry исчерпались с UNAVAILABLE

1. Все инстансы реально offline?
2. CB всех инстансов в OPEN?
3. Все инстансы имеют несовместимый `contract_hash`?
4. Network partition?

```ts
for (const d of sb.serviceMap().values()) {
  console.log(d.serviceName, d.name, d.callEndpoint, d.contractHash);
}
```

### Stream висит без chunks

Возможные причины:
- Handler никогда не `yield` (баг в callee).
- Slow consumer — handler ждёт backpressure.

Добавьте timeout: `sb.stream(svc, m, payload, { timeout: "30s" })`.

### Reconnect-loop при ротации сертификатов

Это **нормально** — overlap rotation выглядит как mini-reconnect. Признак: за `reconnecting` идёт `connected` без error. Реальная проблема — `disconnected` с error.

### Memory leak при долгоживущем процессе

- Создавайте **один** `ServiceBridge` на процесс.
- Всегда `sb.stop()` на graceful shutdown.

```ts
process.on("SIGTERM", () => sb.stop().then(() => process.exit(0)));
```

### Tests виснут после sb.stop()

В тестах `await sb?.stop()` в `afterEach` — без `await` процесс не закроет соединения.

---

## 9. Telemetry: операции, логи, метрики

Каждый встроенный домен (RPC, HTTP, events, workflows, jobs) рантайм трейсит **сам** — каждый входящий вызов, доставка события, шаг workflow и запуск job уже становятся операциями в трейсе без единой строки кода с вашей стороны. `sb.telemetry` нужен, только чтобы добавить поверх этого **свои** логи и метрики.

Доступ — через геттер `sb.telemetry`. Эмитить можно ещё до `start()`: данные буферизуются в ring-буфере и уходят в рантайм, как только появится сессия.

### Логи

```ts
sb.telemetry.log.info("charge ok", { orderId, amountCents });
sb.telemetry.log.error("charge failed", { orderId, err: String(err) });
```

Уровни: `debug` / `info` / `warn` / `error`. Второй аргумент — произвольные структурные поля (сериализуются в JSON). `sb.logger` — короткий алиас того же `sb.telemetry.log`.

Каждая запись авто-тегируется текущим `instance_id`, так что лог привязан к вашему инстансу.

### Метрики

```ts
const charges = sb.telemetry.counter("charges_total", { currency: "usd" });
charges.inc();              // +1; .inc(n) — на n

const queueDepth = sb.telemetry.gauge("queue_depth");
queueDepth.set(42);

const latency = sb.telemetry.histogram("charge_latency", "s");  // unit по умолчанию "s"
latency.observe(0.137);
```

Третий аргумент-объект (для counter/gauge — второй) — метки (`labels`): только строки. Каждая метрика тегируется текущим `instance_id` автоматически.

### Свой спан вокруг куска работы

Встроенных операций хватает почти всегда, но иногда полезно увидеть в трейсе свой этап — «сверка», «пересчёт корзины» — как один узел, внутри которого лежат его вызовы.

```ts
import { Channel, UserSubOp } from "service-bridge";

await sb.telemetry
  .startOp({ channel: Channel.USER, kind: UserSubOp, subject: "reconcile" })
  .run(async () => {
    await sb.rpc.call("billing", "Charge", payload);
    await sb.event.publish("order.reconciled", { orderId });
  });
```

`run()` держит спан открытым на время колбэка и закрывает его сам: `SUCCESS`, если колбэк завершился, `ERROR` с текстом исключения, если бросил — исключение при этом пробрасывается наружу. Всё, до чего колбэк дотянется, становится дочерним узлом: `rpc.call`, `publish`, вложенный `startOp`.

Нужен статус точнее — закройте спан внутри колбэка, `run()` повторно закрывать не станет:

```ts
await sb.telemetry
  .startOp({ channel: Channel.USER, kind: UserSubOp, subject: "reconcile" })
  .run(async (op) => {
    if (await timedOut()) op.end(Status.TIMEOUT, "upstream slow");
  });
```

Сам по себе `startOp()` спан открывает, но **область видимости не создаёт** — вызовы после него останутся соседями спана, а не его детьми. Он нужен для спанов, которые живут дольше одного блока и закрываются вручную через `op.end(...)`. Для обычного «обернуть кусок работы» берите `.run()`.

### Как читается операция в трейсе

Операции, которые рантайм пишет за вас, в UI и в таблице `operations` имеют единый набор полей — полезно понимать словарь:

| Поле | Смысл |
|------|-------|
| `channel` / `kind` | Канал (`HTTP`/`RPC`/`EVENT`/`WORKFLOW`/`JOB`/`USER`) и тип операции внутри него. |
| `actor` | Кто **исполняет** операцию — ваш инстанс (`instance_id`). Проставляется из сессии. |
| `peer` | **Контрагент** — другой сервис, к которому операция обращается. Пусто для чисто локального шага. |
| `subject` | Человекочитаемый идентификатор, формат `<channel>.<kind>:<parts>` (части склеены через `/`). Например `rpc.call:billing-service/charge`, `http.handle:GET//api/v1/users`, `event.publish:order.created`. |
| `businessKey` | Ключ корреляции/идемпотентности (например, id заказа). |
| `status` | Жизненный цикл операции (см. ниже). |

Время — `int64` unix-ms (`startedAtMs` / `finishedAtMs`).

### Статусы операции

In-flight операция стартует в `PENDING`; завершается одним из терминальных статусов. Терминальный успех на wire — строка `"success"` (**не** `"completed"`).

| Статус | Когда |
|--------|-------|
| `PENDING` | In-flight, ещё не завершена. |
| `SUCCESS` | Успешно завершена. |
| `ERROR` | Упала с ошибкой. |
| `TIMEOUT` | Превысила дедлайн. |
| `ABANDONED` | Инстанс пропал, не закрыв операцию — рантайм сам помечает такие операции при disconnect-sweep. |

> Эмиссия самих START/END-кадров операций — внутренняя забота SDK и его доменных слоёв; в публичный пакет `servicebridge` низкоуровневый op-API не экспортируется. Из пользовательского кода вы добавляете наблюдаемость через логи и метрики выше, а операции получаете бесплатно от встроенных доменов.

---

→ Дальше: [API reference](./api-reference.md)
