# sqlite

## Зона ответственности

Локальное постоянное хранилище SDK поверх нативного SQLite-драйвера, выбираемого по рантайму: `bun:sqlite` под Bun, `better-sqlite3` под Node. Управляет созданием базы данных, WAL-режимом, inline-схемой `event_outbox` через `CREATE IF NOT EXISTS` и сбросом зависших `inflight`-строк при старте (crash-recovery). Не содержит бизнес-логики — только инфраструктура персистентности.

## Публичный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Storage` | class | — | Обёртка над нативной SQLite `Database` (`bun:sqlite` под Bun, `better-sqlite3` под Node); точка входа через `Storage.open()` |
| `Storage.open(opts?)` | `(StorageOpenOpts?) => Storage` | — | Открывает/создаёт `sdk.db`, включает WAL, гарантирует наличие `event_outbox`, сбрасывает `inflight→pending` |
| `Storage.transaction(fn)` | `<T>(fn: () => T) => T` | — | Выполняет fn внутри SQLite-транзакции (сериализовано) |
| `Storage.prepare(sql)` | `(string) => SqliteStatement` | — | Возвращает подготовленный statement (`run`/`get`/`all` поверх позиционных `?`-байндов) |
| `Storage.close()` | `() => void` | — | Закрывает соединение с БД |
| `StorageOpenOpts` | interface | — | `{ dataDir?: string }` |
| `StorageOpenOpts.dataDir` | `string` | `./.servicebridge` | Директория для `sdk.db`. `service-bridge` прокидывает сюда `ServiceBridgeOptions.dataDir`. |

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `openDatabase(path)` | function (`@internal`) | — | Загружает нативный SQLite-драйвер для текущего рантайма и открывает файл по `path`. Под Bun (`globalThis.Bun` определён) — `bun:sqlite`; иначе — `better-sqlite3`. Оба загружаются синхронно через `createRequire`, чтобы `Storage.open` оставался sync, а специфаер был runtime-строкой — Node-бандлер не пытается резолвить `bun:sqlite`. |
| `SqliteDatabase` | interface (`@internal`) | — | Общий subset обоих драйверов, на который опирается outbox: `exec`/`prepare`/`transaction`/`close`. |
| `SqliteStatement` | interface (`@internal`) | — | Surface подготовленного statement: `run`/`get`/`all` поверх позиционных `?`-байндов. Удовлетворяется обоими драйверами. |

## Архитектурные решения и почему

**Dual-driver выбор по рантайму.** SDK работает и под Bun, и под чистым Node, поэтому драйвер выбирается в рантайме внутри `openDatabase`: под Bun — встроенный `bun:sqlite` (без внешних зависимостей), под Node — `better-sqlite3` (npm-зависимость в `package.json`). Оба драйвера делят общий surface `exec`/`prepare`/`run`/`get`/`all`/`transaction`/`close` с позиционными `?`-байндами, на котором написан весь outbox — поведение `Storage` идентично на обеих платформах. Драйвер грузится синхронно через `createRequire(import.meta.url)`, чтобы `Storage.open()` оставался синхронным; специфаер `bun:sqlite` передаётся как runtime-строка, поэтому Node-бандлер не пытается его резолвить.

WAL-режим обеспечивает concurrent reads без блокировок на write. `PRAGMA synchronous = NORMAL` — компромисс между надёжностью и latency: данные на диске после каждого WAL-checkpoint, не после каждого write. Crash-recovery (`inflight→pending`) закрывает сценарий SIGKILL drainer'а: при следующем старте все записи повторно попадают в очередь.

Схема таблицы `event_outbox` вшита inline через `CREATE TABLE IF NOT EXISTS`, без миграционного фреймворка и поля версии схемы: у SDK нет требований эволюции схемы между релизами, проще пересоздать `.servicebridge/sdk.db` при breaking change. Недостающие колонки (`payload_json`, `x_sb_trace`) добавляются идемпотентно через `PRAGMA table_info` + `ALTER TABLE ADD COLUMN`.

Колонка `status` (`pending`/`inflight`/`failed`) — локальный жизненный цикл строки outbox, не путать с wire-статусом доставки (`PublishStatus` в proto): на gRPC уходит результат `Events.Publish`, а локальный `status` лишь управляет повторной выборкой строк drainer'ом. Успешная доставка не пишет терминальный статус — drainer удаляет строку (`DELETE`), поэтому `done` в CHECK не нужен.

Имя пакета `sqlite` — явный technology choice (как `serde`), не слой-ориентированное имя (не `storage`).

## Зависимости

- Использует: `bun:sqlite` (встроен в Bun) либо `better-sqlite3` (npm, под Node) — выбор в рантайме; `node:fs`, `node:module` (`createRequire`)
- Используется: `sdk/node/src/connection/` (`service-bridge.ts` вызывает `Storage.open()` и владеет инстансом); `sdk/node/src/events/` (`publisher.ts`, `drainer.ts` принимают `Storage` как тип в опциях)
