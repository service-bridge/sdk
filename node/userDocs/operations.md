# Operations

← [Integrations](./integrations.md) · Дальше: [API reference](./api-reference.md) →

Операционные темы для всех доменов: конструктор и lifecycle, identity, advertise, security (bootstrap key, mTLS, rotation), env-переменные, troubleshooting.

## Содержание

- [1. Конструктор и опции](#1-конструктор-и-опции)
- [2. Lifecycle: start / ready / stop](#2-lifecycle-start--ready--stop)
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

`url` — адрес runtime `host:port` (например `localhost:14445`), `key` — bootstrap-ключ (§6). Конструктор проверяет оба и числовые опции; неверное значение — `ConfigurationError` сразу.

| Опция | Тип | По умолчанию | Что делает |
|------|-----|--------------|-----------|
| `reconnectIntervalMs` | `number` | лестница 1s, 5s, 15s, 30s, 60s ±20% | Плоская задержка между попытками reconnect вместо лестницы. |
| `reconnectAttempts` | `number` | `0` (без лимита) | Сколько **подряд идущих** неудач допустимо; счётчик сбрасывается на каждом Welcome. Превышение → `disconnected` и остановка. |
| `advertise` | `{ host, port } \| false` | `127.0.0.1` на свободном порту + warning | Inbound CallServer. См. §5. |
| `callDefaults` | `CallOpts` | `{}` | Дефолтные `CallOpts` для `sb.rpc.call`, `sb.stream` и typed-клиентов. См. [RPC §4](./rpc.md#4-callopts). |
| `failOnPolicyViolation` | `boolean` | `false` | `true` → warning политики в snapshot останавливает бридж (`AccessDeniedError`). Иначе — только событие `policy_violation`. См. [Access Policy](./access-policy.md). |
| `publishTimeoutMs` | `number` | `30000` | Сколько `publish` ждёт ACK runtime. См. [Events §9](./events.md#9-очередь-публикаций-и-повторы-sdk-side). |
| `maxPendingPublishes` | `number` | `10000` | Событий в очереди до `QUEUE_FULL`. |
| `eventsMaxInFlight` | `number` | `32` | Параллельно обрабатываемые доставки событий. |
| `rpcMaxConcurrentCalls` | `number` | `256` | Одновременные входящие RPC-хендлеры. |
| `rpcMaxQueuedCalls` | `number` | = `rpcMaxConcurrentCalls` | Очередь входящих вызовов; сверх — вызывающий получает `OVERLOADED`. |
| `startTimeoutMs` | `number` | `30000` | Дедлайн `start()`. |
| `stopTimeoutMs` | `number` | `10000` | Дедлайн дренажа в `stop()`. |
| `logger` | `Logger` | warn/error в консоль | Куда SDK пишет свою диагностику (§3). |
| `telemetry.onDrop` | `(info) => void` | нет | Сообщение о потерянной телеметрии (§9). |

Telemetry on/off и payload cap управляются runtime-настройками UI (Settings → Telemetry):
- `telemetry.enable` (`true`/`false`) — рантайм пушит в SDK через `CaptureModes.telemetry_enabled`. Когда `false`, transport не стартует (ops/logs/metrics буферизуются в ring, не отправляются). Fail-safe до первого снапшота: включён.
- `telemetry.payload_max_bytes` — per-direction cap payload'а в байтах. Пушится в SDK через `CaptureModes.payload_max_bytes`. Fail-safe до первого снапшота: `65536`.
- Режим захвата payload по каналам (`none` / `errors` / `all`) — тоже runtime-настройка, по умолчанию `errors`. SDK передаёт payload как есть; маскирование секретов делает runtime при приёме.

Продление leaf-сертификата выполняется автоматически (за 30 минут до expiry) — публичной опции у него нет.

### Типичные пресеты

```ts
// Локальная разработка
const sb = new ServiceBridge(URL, KEY);

// Production callee
const sb = new ServiceBridge(URL, KEY, {
  advertise: { host: process.env.POD_IP!, port: 7777 },
  callDefaults: { timeout: "5s", retry: { maxAttempts: 3 } },
  logger: appLogger,   // { debug, info, warn, error }
});

// Caller-only сервис
const sb = new ServiceBridge(URL, KEY, { advertise: false });
```

---

## 2. Lifecycle: start / ready / stop

```ts
await sb.start();
await sb.ready();
await sb.stop();
```

### Что делает start()

1. Дожидается загрузки всех схем, объявленных до `start()`; ошибка загрузки схемы бросается отсюда.
2. Обменивает ключ на leaf-сертификат (`Bootstrap.Provision`).
3. (Если `advertise !== false`) поднимает локальный CallServer.
4. Открывает сессию `Control.Open` и стрим реестра, отправляет все хендлеры и зависимости.
5. Резолвится, когда runtime прислал `Welcome` **и** первый snapshot реестра (вид на mesh и политику доступа) — после этого вызовы безопасны.

Если за `startTimeoutMs` (по умолчанию 30 с) этого не случилось или runtime ответил неустранимой ошибкой, бридж останавливается и `start()` бросает: `TimeoutError`, `ConnectionError` (например, `UNAUTHENTICATED` — ключ неверен), `ConfigurationError` (несовместимый протокол runtime), `ValidationError` (runtime отклонил декларации), `AccessDeniedError` (`failOnPolicyViolation`). Повторный `start()` или `start()` после `stop()` — `StateError`; для нового запуска создайте новый `ServiceBridge`.

### ready()

`sb.ready()` резолвится, когда текущая сессия жива и её snapshot применён — сразу, если это уже так. После обрыва связи он ждёт переподключения. Отклоняется, если бридж остановлен.

```ts
await sb.ready();   // например, в health-check перед приёмом трафика
```

### Reconnect

Потеря сессии → повторное подключение по лестнице 1s, 5s, 15s, 30s, 60s ±20% (или плоско `reconnectIntervalMs`). Считаются **подряд идущие** неудачи, счётчик сбрасывается на Welcome; по умолчанию попытки не ограничены. Бридж останавливается без reconnect только на неустранимых ответах runtime: `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT` (в том числе невалидный фильтр подписки), `FAILED_PRECONDITION` (несовместимый протокол).

### Что делает stop()

Упорядоченная остановка в пределах `stopTimeoutMs` (по умолчанию 10 с):

1. Снимает анонс: перерегистрируется с пустым `call_endpoint`, чтобы пиры перестали выбирать этот инстанс.
2. CallServer отвечает на новые вызовы `UNAVAILABLE` с пометкой «не отправлено в хендлер» — вызывающий повторит их на другом инстансе. Подписчики событий и jobs перестают брать новую работу; runtime передоставит её другим инстансам.
3. Ждёт выполняющиеся входящие вызовы, event-хендлеры и jobs.
4. Досылает очередь `publish`; не подтверждённые runtime события отклоняются `CONNECTION`.
5. Отправляет остаток телеметрии и ждёт ACK последней пачки (не дольше 2 с).
6. Закрывает стримы, каналы и сервер.

`stop()` идемпотентен.

### Graceful shutdown

```ts
process.on("SIGTERM", async () => { await sb.stop(); process.exit(0); });
process.on("SIGINT",  async () => { await sb.stop(); process.exit(0); });
```

---

## 3. События

```ts
sb.on("connected",        (e: { sessionId, serviceId, serviceName, runtimeVersion }) => {});
sb.on("reconnecting",     (e: { attempt, delayMs, reason }) => {});
sb.on("draining",         (e: { reason }) => {});
sb.on("disconnected",     (e: { reason, error }) => {});
sb.on("policy_violation", (e: { declaration, value, denySide, reason }) => {});
```

`connected` не несёт `instanceId` — его берите из `sb.identity()` (см. §4).

| Событие | Когда |
|---------|-------|
| `connected` | После `Welcome`. Срабатывает на первом connect И на каждом успешном reconnect. Продление сертификата его не вызывает — сессия не переоткрывается. |
| `reconnecting` | Сессия потеряна или попытка не удалась; `attempt` — номер подряд идущей неудачи, начиная с 1. |
| `draining` | Runtime объявил остановку (`Drain`). Reconnect последует сам, когда runtime закроет стрим. |
| `disconnected` | Бридж остановился окончательно: неустранимая ошибка, исчерпан `reconnectAttempts` или `failOnPolicyViolation`. `reason` — текст ошибки, `error` — `ServiceBridgeError` (`ConnectionError`, `ValidationError`, `ConfigurationError`, `AccessDeniedError`). Обычный `sb.stop()` его не эмитит. |
| `policy_violation` | Warning политики из snapshot и отказы политики во время вызова (`rpc.call`, `event.publish`). |

Исключение в слушателе логируется и не влияет ни на бридж, ни на других слушателей.

### Пример: критическая остановка

```ts
sb.on("disconnected", ({ reason, error }) => {
  console.error("[fatal]", error?.code, reason);
  process.exit(1);   // pod restart
});
```

### Диагностика SDK: logger

Свою диагностику (reconnect, отказы политики, сбои publish, предупреждения advertise) SDK пишет только в `options.logger`. По умолчанию — warn/error в консоль с префиксом `[servicebridge]`, debug/info отбрасываются. Подключите свой логгер, чтобы направить её в общий поток логов приложения:

```ts
import pino from "pino";
const log = pino();

const sb = new ServiceBridge(URL, KEY, {
  logger: {
    debug: (msg, attrs) => log.debug(attrs ?? {}, msg),
    info:  (msg, attrs) => log.info(attrs ?? {}, msg),
    warn:  (msg, attrs) => log.warn(attrs ?? {}, msg),
    error: (msg, attrs) => log.error(attrs ?? {}, msg),
  },
});
```

Это не то же, что `sb.logger`: `sb.logger` — структурные логи вашего приложения, которые уходят в runtime (§9).

---

## 4. Identity и serviceMap

### identity()

```ts
sb.identity(): { sessionId, serviceId, serviceName, instanceId } | null
```

`null` до первого `connected` и после `stop()`. `instanceId` не меняется ни при reconnect, ни при продлении сертификата. Новый `instanceId` появляется, только если сертификат успел истечь (долгий обрыв) и SDK получил новый через `Provision`.

```ts
await sb.start();
const id = sb.identity()!;
sb.logger.info("ready", { service: id.serviceName, instance: id.instanceId });
```

`instanceId` доступен также напрямую через `sb.instanceIdString()` — пустая строка до первого `connected`.

### serviceMap()

```ts
sb.serviceMap(): ReadonlyMap<string, ServiceMapEntry>   // ключ — имя сервиса

interface ServiceMapEntry {
  methods: MethodDescriptor[];                       // видимые этому сервису методы
  instances: ServiceInstanceInfo[];                  // живые инстансы: callEndpoint, httpEndpoint, status, ...
  eventSubscriptions: EventSubscriptionDescriptor[];
  outgoingCalls: OutgoingCallDescriptor[];
}

interface MethodDescriptor {
  serviceName: string;
  serviceId: string;
  instanceId: string;
  type: MethodType;          // enum METHOD_TYPE_*
  name: string;
  contractHash: string;      // "v2:<hex>" или ""
  published: boolean;
  inputSchema: Buffer;
  outputSchema: Buffer;
  streaming: boolean;
}
```

Живой snapshot: обновляется при connect/disconnect провайдеров (через `RegisterAndWatch`). Видны собственный сервис и сервисы из исходящих зависимостей. Адрес инстанса — `instances[].callEndpoint`, не поле метода.

### Ожидание появления метода

```ts
async function waitForMethod(sb: ServiceBridge, svc: string, name: string, timeoutMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (sb.serviceMap().get(svc)?.methods.some((m) => m.name === name)) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`timeout waiting for ${svc}/${name}`);
}
```

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

После старта CallServer, `call_endpoint = "host:port"` (с фактическим портом если был `0`). Это значение видят все другие SDK через `serviceMap()` и используют для вызовов.

```ts
for (const [name, entry] of caller.serviceMap()) {
  for (const i of entry.instances) console.log(name, "→", i.callEndpoint || "(no inbound)");
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
1. `Bootstrap.Provision` → короткоживущий leaf-сертификат и `instance_id`. Ключ и сертификат живут только в памяти, на диск SDK ничего не пишет.
2. Каналы к runtime и к другим инстансам строятся один раз и берут TLS-материал из общего хранилища сертификатов.
3. За 30 минут до expiry (±5 мин разброса) — `Control.RefreshCert`: новый leaf для того же `instance_id`. Меняется только то, что предъявит следующее TLS-рукопожатие; ни сессия, ни стримы, ни каналы не пересоздаются, `connected` не эмитится. Отказ из-за лимита частоты или временной недоступности повторяется через 60 с.
4. Reconnect переиспользует действующий leaf. Новый `Provision` (и новый `instance_id`) нужен, только если сертификат успел истечь.

### SPIFFE identity

Leaf cert содержит SPIFFE URI SAN:
```
spiffe://servicebridge/service/<service_id>/instance/<instance_id>
```

В direct mode SDK валидирует SPIFFE SAN сервера — защита от подмены инстанса.

### Ротация скомпрометированного ключа

Выпустите новый ключ того же сервиса: `sb service key rotate <name>` (новый ключ показывается один раз) или в дашборде. Ротация отзывает прежние креды: инстансы со старым ключом переподключиться не смогут.

1. Обновите секрет с ключом.
2. Перезапустите сервис — SDK получит leaf по новому ключу.

### Отзыв сервисов и инстансов

Runtime сообщает SDK об отозванных сервисах и инстансах. Отозванные не выбираются целями вызовов, их прямые каналы закрываются, их входящие вызовы отклоняются `ACCESS_DENIED` сразу. Отзыв инстанса действует до конца жизни процесса; отзыв сервиса снимается, когда у него появляется новый инстанс.

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

Подключение рантайма к Postgres задаётся не через env, а флагом `-pg-url` (по умолчанию `postgres://postgres:postgres@postgres:5432/service-bridge?sslmode=disable` — контейнер `postgres` из поставляемого docker-compose). Остальное (порты gRPC `14445` и UI gateway `14444`, таймауты, idempotency TTL, режимы захвата payload) — настройки в БД, редактируются в UI на странице Settings.

> Требуется PostgreSQL 18+.

---

## 8. Troubleshooting

### start() бросает ConnectionError

`err.grpcCode === 16` (`UNAUTHENTICATED`) → bootstrap key invalid:
- Пустая/обрезанная env-переменная.
- Запись удалена из БД runtime.
- БД runtime пересоздана.

Сгенерируйте новый ключ через дашборд рантайма (**Services → Create service**), обновите env.

### start() бросает TimeoutError

Runtime не прислал `Welcome` и первый snapshot за `startTimeoutMs`: runtime недоступен по `url`, порт не тот, сеть режет gRPC. Проверьте `url` (`host:port` control plane, по умолчанию `14445`).

### rpc: no schema for `<svc>/<method>`

Caller не вызвал `useSchema()` (и не использует typed client). Решение: `await sb.useSchema(svc, method, { protoFile: "..." })` или `await sb.client(svc, "...")` до `start()`.

### rpc: no live instance of `<svc>/<method>` matches caller contract `<hash>`

Ни у одного живого инстанса нет метода с этим `contract_hash`:
1. Callee offline / не зарегистрировал handler.
2. Опечатка в имени (case-sensitive).
3. Callee на другой версии схемы — см. [RPC §8](./rpc.md#8-версионирование-контракта).

```ts
for (const [name, entry] of sb.serviceMap()) {
  for (const m of entry.methods) console.log(name, m.name, m.contractHash);
}
```

### rpc: no endpoint for `<svc>/<method>`

Callee запущен с `advertise: false` — его нельзя вызвать ни напрямую, ни через proxy. Включите advertise на callee (production: `{ host: POD_IP, port: ... }`).

### no service block found

`sb.client()` на `.proto` без `service`. Добавьте service block или используйте низкоуровневый API.

### cannot resolve input/output for method "X"

Auto-resolve не нашёл messages. Укажите явно: `{ protoFile, input: "XReq", output: "XRes" }`. Подробности резолюции — [RPC §2.4](./rpc.md#24-резолюция-inputoutput-proto).

### advertise not configured warning

Не указана `advertise`, поэтому inbound CallServer сел на недостижимый `127.0.0.1`. Для production: задайте `advertise: { host, port }` явно. Для caller-only: `advertise: false`.

### Вызов падает с CONNECTION или NO_LIVE_INSTANCE

1. Все инстансы реально offline?
2. CB всех инстансов в OPEN («all candidates circuit-open»)?
3. Все инстансы имеют несовместимый `contract_hash`?
4. Network partition?

```ts
for (const [name, entry] of sb.serviceMap()) {
  for (const i of entry.instances) console.log(name, i.instanceId, i.callEndpoint, i.status);
}
```

### Stream висит без chunks

Возможные причины:
- Handler никогда не `yield` (баг в callee).
- Slow consumer — handler ждёт backpressure.

Добавьте timeout: `sb.stream(svc, m, payload, { timeout: "30s" })`.

### Reconnect-loop

Продление сертификата сессию не трогает, поэтому повторяющиеся `reconnecting` — это реальные обрывы: runtime перезапускается (перед этим приходит `draining`), сеть рвёт долгие соединения, балансировщик между SDK и runtime режет HTTP/2. Бридж продолжает попытки, пока не получит неустранимую ошибку; тогда приходит `disconnected`.

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
import { Channel, Status, UserSubOp } from "service-bridge";

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

> Из пользовательского кода SDK принимает только свои спаны `Channel.USER` / `UserSubOp` (через `startOp` выше), логи и метрики. Операции RPC, HTTP, событий, workflow и jobs пишут встроенные домены.

### Потеря телеметрии: onDrop

Телеметрия копится в кольцевом буфере в памяти; при переполнении или когда runtime отбрасывает пачки, данные теряются. Узнать об этом можно через `telemetry.onDrop` — колбэк получает приращения с прошлого вызова:

```ts
const sb = new ServiceBridge(URL, KEY, {
  telemetry: {
    onDrop: ({ serverDrops, ringDrops, backpressureLevel }) =>
      appLogger.warn("telemetry dropped", { serverDrops, ringDrops, backpressureLevel }),
  },
});
```

Те же потери SDK отправляет в runtime метрикой `sb_sdk_telemetry_dropped_total{source="ring"|"server"}`.

---

→ Дальше: [API reference](./api-reference.md)
