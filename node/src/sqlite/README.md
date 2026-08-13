# sqlite

## Зона ответственности

Локальное постоянное хранилище SDK поверх нативного SQLite-драйвера, выбираемого по рантайму: `bun:sqlite` под Bun, `better-sqlite3` под Node. Управляет созданием базы данных, WAL-режимом, inline-схемой `event_outbox` через `CREATE IF NOT EXISTS`, проверкой версии схемы, кэшем подготовленных выражений, кэшем числа строк outbox и сбросом зависших `inflight`-строк при старте (crash-recovery). Не содержит бизнес-логики — только инфраструктура персистентности.

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Storage` | class | — | Обёртка над нативной SQLite `Database` (`bun:sqlite` под Bun, `better-sqlite3` под Node); точка входа через `Storage.open()` |
| `Storage.open(opts?)` | `(StorageOpenOpts?) => Storage` | — | Открывает/создаёт `sdk.db`, включает WAL, проверяет `PRAGMA user_version`, гарантирует наличие `event_outbox`, сбрасывает `inflight→pending`, читает стартовое число строк. Бросает `Error` при чужой версии схемы |
| `Storage.transaction(fn)` | `<T>(fn: () => T) => T` | — | Выполняет fn внутри SQLite-транзакции (сериализовано). Не реентерабельна — вложенный вызов бросает `Error`. Дельты `adjustOutboxRowCount` применяются к кэшу только после коммита |
| `Storage.prepare(sql)` | `(string) => SqliteStatement` | — | Возвращает подготовленный statement (`run`/`get`/`all` поверх позиционных `?`-байндов). Компилирует SQL один раз и переиспользует |
| `Storage.outboxRowCount()` | `() => number` | — | Число строк `event_outbox` из кэша в памяти, без запроса к БД. Внутри открытой транзакции включает её незакоммиченную дельту |
| `Storage.adjustOutboxRowCount(delta)` | `(number) => void` | — | Учитывает свои INSERT (`+n`) / DELETE (`-n`) в кэше. Обязателен для любого кода, меняющего число строк `event_outbox` |
| `Storage.close()` | `() => void` | — | Закрывает соединение с БД |
| `StorageOpenOpts` | interface | — | `{ dataDir?: string }` |
| `StorageOpenOpts.dataDir` | `string` | `./.servicebridge` | Директория для `sdk.db`. `service-bridge` прокидывает сюда `ServiceBridgeOptions.dataDir`. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `SCHEMA_VERSION` | `number` (`@internal`) | `1` | Версия схемы `event_outbox`, штампуется в файл через `PRAGMA user_version`. Меняется вместе с любым изменением DDL. |
| `assertSchemaVersion(db, dir, file)` | function (`@internal`) | — | Бросает `Error` с инструкцией оператору, если `user_version` файла не равен `SCHEMA_VERSION`. Пустой файл (версия 0, таблиц нет) считается свежим. |
| `openDatabase(path)` | function (`@internal`) | — | Загружает нативный SQLite-драйвер для текущего рантайма и открывает файл по `path`. Под Bun (`globalThis.Bun` определён) — `bun:sqlite`; иначе — `better-sqlite3`. Оба загружаются синхронно через `createRequire`, чтобы `Storage.open` оставался sync, а специфаер был runtime-строкой — Node-бандлер не пытается резолвить `bun:sqlite`. |
| `SqliteDatabase` | interface (`@internal`) | — | Общий subset обоих драйверов, на который опирается outbox: `exec`/`prepare`/`transaction`/`close`. |
| `SqliteStatement` | interface (`@internal`) | — | Surface подготовленного statement: `run`/`get`/`all` поверх позиционных `?`-байндов. Удовлетворяется обоими драйверами. |

## Архитектурные решения и почему

**Dual-driver выбор по рантайму.** SDK работает и под Bun, и под чистым Node, поэтому драйвер выбирается в рантайме внутри `openDatabase`: под Bun — встроенный `bun:sqlite` (без внешних зависимостей), под Node — `better-sqlite3` (npm-зависимость в `package.json`). Оба драйвера делят общий surface `exec`/`prepare`/`run`/`get`/`all`/`transaction`/`close` с позиционными `?`-байндами, на котором написан весь outbox — поведение `Storage` идентично на обеих платформах. Драйвер грузится синхронно через `createRequire(import.meta.url)`, чтобы `Storage.open()` оставался синхронным; специфаер `bun:sqlite` передаётся как runtime-строка, поэтому Node-бандлер не пытается его резолвить.

WAL-режим обеспечивает concurrent reads без блокировок на write. `PRAGMA synchronous = NORMAL` — компромисс между надёжностью и latency: данные на диске после каждого WAL-checkpoint, не после каждого write. Crash-recovery (`inflight→pending`) закрывает сценарий SIGKILL drainer'а: при следующем старте все записи повторно попадают в очередь.

Схема таблицы `event_outbox` вшита inline через `CREATE TABLE IF NOT EXISTS`, без миграционного фреймворка: у SDK нет требований эволюции схемы между релизами, проще пересоздать `.servicebridge/sdk.db` при breaking change. Единственное, что нужно от версионирования, — не дать SDK молча писать в файл чужой формы: версия штампуется в `PRAGMA user_version`, и при несовпадении `Storage.open()` падает с сообщением, где сказано удалить каталог данных. Выбор в пользу громкого падения, а не смены имени файла: смена имени тихо оставляет буферизованные события в осиротевшем файле, то есть теряет их без единого слова оператору.

Индекс `event_outbox_pending_order_idx(enqueued_at_ms, id) WHERE status='pending'` покрывает `ORDER BY` запроса дренажа, а не его `WHERE`: `next_attempt_at_ms <= ?` остаётся остаточным фильтром. Диапазонный предикат на первой колонке индекса заставил бы SQLite материализовать все due-строки во временное b-дерево ради сортировки, и `LIMIT` не обрывал бы скан. С таким порядком колонок скан идёт сразу в порядке доставки и заканчивается после `batchSize` строк. Плата — ретраи с отложенным `next_attempt_at_ms`, стоящие в начале по `enqueued_at_ms`, скан прочитает и пропустит; их мало. Второй индекс не заводится: он бы замедлил INSERT, а публикация — горячий путь.

Число строк `event_outbox` кэшируется в памяти (`outboxRowCount`): `SELECT COUNT(*)` — O(строк), а публикация сверяется с cap на каждом событии при лимите в 100 000 строк. Кэш читается из БД один раз при `open()`, а дальше его ведут сами мутирующие вызовы через `adjustOutboxRowCount`. Внутри транзакции дельта копится отдельно и применяется только после коммита, поэтому откат (например, `OutboxFullError`) счётчик не сдвигает. Обратная сторона — учёт держится на дисциплине вызывающего: код, который вставляет или удаляет строки outbox мимо `adjustOutboxRowCount`, разъедет счётчик до следующего `open()`.

`prepare()` кэширует statement по строке SQL: без кэша дренаж компилировал бы SQL на каждую строку батча. SQL на горячем пути — константы, а списки плейсхолдеров ограничены `batchSize`, поэтому кэш ограничен. `transaction()` компилирует BEGIN/COMMIT/ROLLBACK один раз в конструкторе — оба драйвера пересобирают их на каждый вызов `db.transaction()`, а publish открывает транзакцию на каждое событие. Реентерабельность запрещена: вложенный вызов означает, что колбэк потребителя (например, `onPolicyViolation`) выполняется внутри чужой транзакции — это hard-fail, а не тихий savepoint.

Колонка `status` (`pending`/`inflight`/`failed`) — локальный жизненный цикл строки outbox, не путать с wire-статусом доставки (`PublishStatus` в proto): на gRPC уходит результат `Events.Publish`, а локальный `status` лишь управляет повторной выборкой строк drainer'ом. Успешная доставка не пишет терминальный статус — drainer удаляет строку (`DELETE`), поэтому `done` в CHECK не нужен.

Имя пакета `sqlite` — явный technology choice (как `serde`), не слой-ориентированное имя (не `storage`).

## Зависимости

- Использует: `bun:sqlite` (встроен в Bun) либо `better-sqlite3` (npm, под Node) — выбор в рантайме; `node:fs`, `node:module` (`createRequire`)
- Используется: `sdk/node/src/connection/` (`service-bridge.ts` вызывает `Storage.open()` и владеет инстансом); `sdk/node/src/events/` (`publisher.ts`, `drainer.ts` принимают `Storage` как тип в опциях и ведут `adjustOutboxRowCount` на своих INSERT/DELETE)
