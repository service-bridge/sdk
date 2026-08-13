# serde

## Зона ответственности

Динамическая (de)serialization пользовательских payload-объектов между JS-значениями и Protobuf binary через `protobufjs`, плюс вычисление contract-hash схемы. Источник схем — `.proto` файл или `.schema.json` с явными `fieldNumber`. Используется и call-сервером (callee), и call-клиентом (caller), и событийным слоем.

Не делает: не отвечает за gRPC транспорт (это `rpc/`), не валидирует прикладную логику, не управляет статусами/временем wire-контракта (payload-only), не хранит contract-hash на стороне реестра (это `registry/`, runtime хранит hash непрозрачно и не пересчитывает).

## Публичный контракт

Единственный реэкспорт модуля через публичный API пакета (`index.ts`) — тип `SchemaSpec`. Через него прикладной код объявляет схему хендлера/вызова.

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `SchemaSpec` | `ProtoFileSpec \| JsonSchemaFileSpec` | нет | Union источника схемы: `.proto` файл либо `.schema.json` файл. |

Форма `ProtoFileSpec` (поля задаются прикладным кодом внутри `SchemaSpec`):

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `protoFile` | `string` | нет (обязательно) | Путь к `.proto`, загружается через `protobuf.load`. |
| `input` | `string?` | резолв из `service`-блока | Имя input-message. Нужен, только если в `.proto` нет подходящего `service { rpc <method>(In) returns (Out); }`. |
| `output` | `string?` | резолв из `service`-блока | Имя output-message. Условие то же, что у `input`. |
| `method` | `string?` | имя метода/события | Метод для резолва против `service`-блока. Подставляется автоматически в `registry`/`connection`; задавать вручную обычно не нужно. |

Форма `JsonSchemaFileSpec`:

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `schemaFile` | `string` | нет (обязательно) | Путь к `.schema.json` с секциями `input`/`output`, каждая — ровно один message с явным `fieldNumber` на свойство. |

## Приватный контракт

Не реэкспортируется через публичный `index.ts` пакета. Часть символов потребляется другими доменами SDK прямым импортом из файлов модуля (`Serializer`/`SchemaPair`/`SchemaSpec`-форма, `buildSchemaPair`, `computeContractHash`); `SchemaPairCache`, `canonicalMessageDescriptor`, `attachWireDescriptor`, `wireDescriptor`, `canonicalize` — серде-внутренние. Ключевые символы помечены `@public`/`@internal`-маркером в коде на месте символа (`computeContractHash` — `@public`, остальное — `@internal`).

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Serializer` | interface | нет | `encode(value)`/`decode(bytes)`/`toJsonSchema()` для одного message-типа. |
| `SchemaPair` | `{ input: Serializer; output: Serializer }` | нет | Пара сериализаторов на метод/событие. |
| `ProtoFileSpec` | interface | нет | Описание `.proto`-источника (см. публичную таблицу полей). |
| `JsonSchemaFileSpec` | interface | нет | Описание `.schema.json`-источника. |
| `buildSchemaPair(spec)` | `(spec: SchemaSpec) => Promise<SchemaPair>` | нет | Диспетчер: `.proto` грузит через `protobuf.load` (async) и резолвит input/output; `.schema.json` — через `buildSchemaPairFromJsonFile`. Бросает на ошибке загрузки/резолва. |
| `buildSchemaPairFromJsonFile(spec)` | `(spec: JsonSchemaFileSpec) => SchemaPair` | нет | Синхронно читает `.schema.json`, валидирует `fieldNumber`, строит `protobuf.Type` динамически. |
| `computeContractHash(pair)` | `(pair: SchemaPair) => string` | нет | `"v2:" + hex(sha256(canon(input) + ":" + canon(output)))`. Бросает, если пара собрана не через `buildSchemaPair`. |
| `canonicalMessageDescriptor(type)` | `(type: protobuf.Type) => string` | нет | Канонический wire-дескриптор одного message: номера, кардинальность, типы. Формат — ниже. |
| `attachWireDescriptor(serializer, type)` | `(serializer: Serializer, type: protobuf.Type) => void` | нет | Привязывает дескриптор к сериализатору по identity. Вызывается фабриками сериализаторов модуля. |
| `wireDescriptor(serializer)` | `(serializer: Serializer) => string` | нет | Возвращает дескриптор, с которым собран сериализатор. Бросает, если его нет. |
| `canonicalize(value)` | `(value: unknown) => string` | нет | Детерминированная стрингификация: объекты — sorted keys рекурсивно, массивы сохраняют порядок, без пробелов. |
| `SchemaPairCache` | class | нет | Дедупликация `SchemaPair` по ключу `SchemaSpec` (`get`/`clear`/`size`), с in-flight-промисами против гонок. |

### Формат canonical wire descriptor

```json
{"f":[{"n":1,"c":"opt","t":"string"},
      {"n":2,"c":"rep","t":"m:.pay.Item"},
      {"n":3,"c":"map","k":"string","t":"m:.pay.Meta"}],
 "o":[{"f":[4,5]}],
 "r":{".pay.Item":{"f":[]},".pay.Kind":{"e":[0,1,2]}}}
```

| Ключ | Что содержит |
|------|--------------|
| `f` | Поля message, отсортированы по номеру. Всегда присутствует; у пустого message — `[]`. |
| `f[].n` | Номер поля. |
| `f[].c` | Кардинальность: `opt` (singular, включая proto3 `optional`), `rep`, `map`, `req` (proto2 `required`). |
| `f[].t` | Тип: имя скаляра как в `.proto`, либо `m:<полное.имя>`, либо `e:<полное.имя>`. Для map — тип значения. |
| `f[].k` | Тип ключа map. Только при `c == "map"`. |
| `o` | Группы oneof: `{"f":[номера]}`, номера по возрастанию, группы — по первому номеру. Ключ опускается, если oneof нет. proto3 `optional` группой не считается. |
| `r` | Плоский справочник всех транзитивно упомянутых message/enum по полному имени с ведущей точкой, ключи отсортированы. Message → `{"f":…,"o":…}` без собственного `r`; enum → `{"e":[номера значений по возрастанию, алиасы схлопнуты]}`. Message, достижимый из самого себя, тоже попадает в `r`. Ключ опускается, если справочник пуст. |

Синтетические map-entry message в `r` не попадают — map выражается прямо в поле через `c`/`k`/`t`. Канонический JSON: ключи объектов отсортированы лексикографически на всех уровнях, порядок массивов сохранён, пробелов нет.

Golden-векторы — `sdk/contract-hash-vectors.json`, общий артефакт для всех языковых SDK. Фикстуры к ним — `./testdata/vectors-*`.

## Архитектурные решения и почему

- **Protobuf binary, не JSON.** gRPC главная фишка — компактная схема-driven сериализация. Передача JSON-блобов как `bytes payload` теряет смысл gRPC. Все payload-ы encode/decode-ятся через `protobufjs`.
- **Два источника схем: `.proto` и `.schema.json`.** Inline JSON schema убрана навсегда (silent data corruption при auto-numbering полей). `.schema.json` с явными `fieldNumber` на свойство — безопасный для эволюции формат: номера полей закрепляет автор схемы.
- **`protobufjs` динамически, не codegen.** Wire-stubs ServiceBridge генерируются через `ts-proto` статически; пользовательские payload schemas загружаются в runtime через `protobuf.load`/динамический `protobuf.Type`. Разделение ответственностей чёткое.
- **Резолв input/output только явно или из `service`-блока.** Convention-based и unique-pair fallback'и удалены: они прятали ошибки контракта за похожестью имён вместо громкого фейла.
- **contract hash — язык-нейтральный wire-дескриптор, не внутренний формат библиотеки.** Хешируется собственная структура из номеров, кардинальности и типов, а не `protobufjs Type.toJSON()`. `toJSON()` — деталь реализации конкретной JS-библиотеки (camelCase-имена, свой порядок ключей); хешируя её, любой не-JS SDK был бы обязан побайтово эмулировать protobufjs и ломался бы при её обновлении. Wire-дескриптор описан спецификацией, зафиксирован golden-векторами и воспроизводим на любом языке из дескрипторов protobuf.
- **Имена полей в хеш не входят.** Contract hash — идентичность маршрутизации: рантайм фильтрует инстансы цели по совпадению хеша. Переименование поля wire-совместимо в protobuf, поэтому включение имён уводило бы трафик в никуда после безвредного рефакторинга. Меняют идентичность только номер, тип и кардинальность — ровно то, что ломает бинарную совместимость.
- **Имена типов в хеш входят.** Полное имя message/enum — единственный способ различить два структурно одинаковых, но семантически разных типа, и единственный якорь для разрыва циклических ссылок в справочнике `r`.
- **Префикс `v2:`.** Генерация алгоритма едет внутри самого значения, поэтому хеши разных поколений не совпадут случайно. Пустая строка остаётся валидным значением: хендлер без схемы объявляет `""`, и равенство с `""` выполняется тривиально.
- **Дескриптор хранится в side table по identity сериализатора.** `Serializer` — контракт encode/decode, на который опираются `events`/`registry`; дескриптор описывает protobuf-тип за сериализатором, а не этот контракт, поэтому живёт в `WeakMap` внутри `contract-hash.ts`, а не в интерфейсе.
- **Кросс-эквивалентность источников — не цель.** `.schema.json` выводит имя вложенного message из имени поля (`items` → `.Input_items_elem`), поэтому переименование поля-контейнера там меняет хеш, а `.proto` с тем же смыслом даёт другое имя типа. Caller и callee обязаны использовать один источник (ADR 0001).

## Зависимости

- `protobufjs` — динамическая загрузка/(de)serialization `.proto`, построение `Type`.
- `node:crypto` — SHA-256 для contract-hash.
- `node:fs` — чтение `.schema.json`.

Используется: `sdk/node/src/rpc/` (caller-сторона, `computeContractHash`), `sdk/node/src/registry/registry.ts` (`buildSchemaPair`, `computeContractHash`, передача JSON-schema в RegisterRequest), `sdk/node/src/events/` (publisher/subscriber/domain — `SchemaPair`/`SchemaSpec`), `sdk/node/src/connection/service-bridge.ts` (`buildSchemaPair`, реэкспорт `SchemaSpec`).
