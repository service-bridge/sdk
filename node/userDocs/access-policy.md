# Access Policy

[← к индексу](./index.md)

ServiceBridge поддерживает гранулярную политику доступа: оператор может ограничить, что сервис **может регистрировать** (capabilities), что он **может отправлять** (egress), и **кто может его адресовать** (acceptance). По умолчанию всё разрешено — ограничения опциональны.

## Концепты

Семь capability-флагов: четыре acceptance (`*.handle`) и три egress.

| Handler-side / acceptance (что регистрирую, кто меня зовёт) | Egress / action (что я отправляю) |
|---|---|
| `rpc.handle` | `rpc.call` |
| `event.handle` (включая wildcard) | `event.publish` |
| `workflow.handle` | `workflow.run` |
| `job.handle` | — (jobs self-only: нет egress и нет внешнего caller'а) |

- **Capability** — boolean-флаг на сервисе. Грубый kill-switch: выключенный флаг денит независимо от правил.
- **Egress rule** — что сервис может отправлять. Хранится в `service_policy_rules` с `direction='E'`.
- **Acceptance rule** — кто/что может адресовать этот сервис. `direction='A'`.

При вызове RPC runtime делает **bilateral check**: caller'у должно быть разрешено отправить (`rpc.call`), callee — принять (`rpc.handle`). Оба должны разрешать. Так же устроен `workflow.run`/`workflow.handle`. Для `event.publish` проверка только egress-сторона (у событий нет per-service маршрутизации).

## Default-allow

Сразу после регистрации через UI консоль сервис может всё. В БД нет ни одного `service_policy_rules` для него; runtime трактует «нет правил для (service, action)» как разрешение.

Оператор добавляет правила, чтобы ограничить.

## SDK warnings при старте

Runtime в первом снапшоте после `RegisterAndWatch` посылает SDK `PolicyEvaluation` со списком нарушений в декларациях. SDK:

- пишет warn в `logger` (опция конструктора) на каждое нарушение,
- эмитит `policy_violation` event:

```ts
sb.on('policy_violation', ({ declaration, value, denySide, reason }) => {
  // declaration: 'rpc.call' | 'rpc.handle' | 'event.publish' | 'event.handle'
  //              | 'workflow.run' | 'workflow.handle'
  // value: 'payments/charge' | 'orders.*' | ...
  // denySide: 'capability' | 'self_egress' | 'self_acceptance' | 'peer_acceptance'
  // reason: человекочитаемое объяснение от runtime
});
```

Для строгого режима (prod):

```ts
import { AccessDeniedError } from 'service-bridge';

const sb = new ServiceBridge(url, key, {
  failOnPolicyViolation: true,
});
// Snapshot политики с warning'ами останавливает бридж: start() бросает
// AccessDeniedError («policy violations on start: ...»), и эмитится
// `disconnected` с той же ошибкой. Если warning придёт в snapshot уже после
// start(), бридж остановится так же — через `disconnected`.
try {
  await sb.start();
} catch (err) {
  if (err instanceof AccessDeniedError) {
    console.error(err.message);
    process.exit(1);
  }
  throw err;
}
```

Также доступен геттер — последний снапшот политики, который runtime прислал
для твоего сервиса (`null`, пока не пришёл первый кадр реестра):

```ts
const evaluation = sb.policyEvaluation();
// { capabilities: string[],   // ['rpc.handle', 'event.handle', ...]
//   egress: PolicyRule[],     // мои egress-правила
//   acceptance: PolicyRule[], // мои acceptance-правила
//   warnings: PolicyViolation[] }
```

## Service Map с policy

`sb.serviceMap()` возвращает `ReadonlyMap<string, ServiceMapEntry>` (ключ — имя сервиса):

```ts
interface ServiceMapEntry {
  methods: MethodDescriptor[];                       // rpc.handle / workflow / job / http / published events
  instances: ServiceInstanceInfo[];                  // живые инстансы + endpoint'ы + health
  eventSubscriptions: EventSubscriptionDescriptor[]; // мои event.handle паттерны (с wildcards)
  outgoingCalls: OutgoingCallDescriptor[];           // мои rpc.call / workflow.run / http зависимости
}
```

Виден только caller'у — его собственный сервис и сервисы из его outgoing-deps (через `sb.service(...)`).

## Создание сервиса

Сервисы регистрируются через UI консоль рантайма (Services → Create service). Создайте сервис, получите ключ через дашборд, обновите env. По умолчанию новый сервис не ограничен — всё разрешено. Ограничения добавляются правилами политики (ниже).

## CLI: редактирование политики

Политику правят в консоли рантайма или CLI `sb` (поставляется в образе рантайма):

```sh
# Посмотреть capabilities и allow-листы сервиса
sb service policy show <service-id>

# Задать политику. Неуказанные измерения сохраняют текущее значение;
# --cap полностью заменяет набор capabilities. Флаги повторяемые.
sb service policy set <service-id> \
  --cap rpc.handle --cap event.handle \
  --allow-call payments \
  --allow-caller analytics \
  --allow-publish 'orders.#' \
  --allow-subscribe 'payments.*' \
  --allow-run-wf checkout \
  --allow-wf-caller storefront
```

## Wildcards

В `event.publish` и `event.handle` поддерживаются AMQP wildcards:
- `*` — ровно один сегмент (между точками)
- `#` — ноль или более сегментов

При публикации event'а runtime матчит его имя против patterns каждого подписчика через `TopicMatch`.

При **регистрации подписки** на pattern P, runtime проверяет, что у сервиса есть acceptance rule R, который **накрывает** P через `PatternContains` (P ⊆ R). Например:

- Acceptance rule `orders.#` разрешает подписки `orders.*`, `orders.created`, `orders.payment.received`.
- Acceptance rule `orders.*` (один сегмент) разрешает `orders.created`, но не `orders.#` или `orders.payment.received`.

## Что происходит при нарушении

| Где | Что видит код |
|---|---|
| **Регистрация handler'а с отключённой capability** | Handler **не регистрируется** (runtime тихо пропускает его), но `start()` не падает — сервис остаётся жив. Пропуск приходит как warning (`policy_violation`). |
| **Объявление outgoing dep / subscription не покрытое правилами** | Регистрация проходит. SDK получает warning (`policy_violation`). Реальная попытка вызова денится в рантайме. |
| **`sb.rpc.call(...)`** | Промис реджектится `AccessDeniedError` (`code: "ACCESS_DENIED"`, маппинг с gRPC `PERMISSION_DENIED`); SDK эмитит `policy_violation` с `declaration: 'rpc.call'`. |
| **Входящий вызов от запрещённого или отозванного сервиса** | Callee отвечает `PERMISSION_DENIED` до хендлера; вызывающий получает `AccessDeniedError`. |
| **`sb.workflow.start(...)`** | Промис реджектится `WorkflowAccessDeniedError`. |
| **`sb.event.publish(...)`** | Runtime отвечает `PUBLISH_STATUS_REJECTED_FORBIDDEN`; промис реджектится `AccessDeniedError`, а SDK эмитит `policy_violation` с `declaration: 'event.publish'`, `denySide: 'self_egress'`. С `fireAndForget: true` отказ только логируется. |

## Глобальный граф для UI

Полный граф сервисов строит консоль рантайма из своей базы; через SDK-порт (`:14445`) он недоступен.

## Ссылки

- ADR-0004 (`runtime/docs/adr/0004-access-security-tls.md`) — детальное обоснование
- `runtime/internal/access/README.md` — internals реализации
- `runtime/internal/sbcli/README.md` — CLI `sb`
