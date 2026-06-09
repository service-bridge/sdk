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

Не реэкспортируется через публичный `index.ts` пакета. Часть символов потребляется другими доменами SDK прямым импортом из файлов модуля (`Serializer`/`SchemaPair`/`SchemaSpec`-форма, `buildSchemaPair`, `computeContractHash`); `SchemaPairCache`, `computeSerializerHash`, `canonicalize` — серде-внутренние. Ключевые символы помечены `@public`/`@internal`-маркером в коде на месте символа (`computeContractHash` — `@public`, `canonicalize` и `SchemaPairCache` — `@internal`).

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Serializer` | interface | нет | `encode(value)`/`decode(bytes)`/`contractHash()`/`toJsonSchema()` для одного message-типа. Per-serializer `contractHash` — информационный. |
| `SchemaPair` | `{ input: Serializer; output: Serializer }` | нет | Пара сериализаторов на метод/событие. |
| `ProtoFileSpec` | interface | нет | Описание `.proto`-источника (см. публичную таблицу полей). |
| `JsonSchemaFileSpec` | interface | нет | Описание `.schema.json`-источника. |
| `buildSchemaPair(spec)` | `(spec: SchemaSpec) => Promise<SchemaPair>` | нет | Диспетчер: `.proto` грузит через `protobuf.load` (async) и резолвит input/output; `.schema.json` — через `buildSchemaPairFromJsonFile`. Бросает на ошибке загрузки/резолва. |
| `buildSchemaPairFromJsonFile(spec)` | `(spec: JsonSchemaFileSpec) => SchemaPair` | нет | Синхронно читает `.schema.json`, валидирует `fieldNumber`, строит `protobuf.Type` динамически. |
| `computeContractHash(pair)` | `(pair: SchemaPair) => string` | нет | SHA-256 (hex) от `<input_canonical>:<output_canonical>`. Одинаковая структура схемы на caller и callee → одинаковый hash. |
| `computeSerializerHash(s)` | `(s: Serializer) => string` | нет | SHA-256 (hex) одного `Serializer` (одной стороны пары). |
| `canonicalize(value)` | `(value: unknown) => string` | нет | Детерминированная стрингификация: объекты — sorted keys рекурсивно, массивы сохраняют порядок, без пробелов. |
| `SchemaPairCache` | class | нет | Дедупликация `SchemaPair` по ключу `SchemaSpec` (`get`/`clear`/`size`), с in-flight-промисами против гонок. |

## Архитектурные решения и почему

- **Protobuf binary, не JSON.** gRPC главная фишка — компактная схема-driven сериализация. Передача JSON-блобов как `bytes payload` теряет смысл gRPC. Все payload-ы encode/decode-ятся через `protobufjs`.
- **Два источника схем: `.proto` и `.schema.json`.** Inline JSON schema убрана навсегда (silent data corruption при auto-numbering полей). `.schema.json` с явными `fieldNumber` на свойство — безопасный для эволюции формат: номера полей закрепляет автор схемы.
- **`protobufjs` динамически, не codegen.** Wire-stubs ServiceBridge генерируются через `ts-proto` статически; пользовательские payload schemas загружаются в runtime через `protobuf.load`/динамический `protobuf.Type`. Разделение ответственностей чёткое.
- **Резолв input/output только явно или из `service`-блока.** Convention-based и unique-pair fallback'и удалены: они прятали ошибки контракта за похожестью имён вместо громкого фейла.
- **contractHash от `Type.toJSON()`, не от исходного текста.** Whitespace, комментарии, форматирование не влияют. Hash идентичен на caller и callee при совпадении структуры schema — основа fail-fast contract-mismatch проверки. Runtime хранит hash непрозрачно и не пересчитывает.
- **Per-serializer hash информационный.** Кросс-сторонняя совместимость считается по `computeContractHash(pair)` (input + output вместе); per-serializer hash из одной стороны — вспомогательный.

## Зависимости

- `protobufjs` — динамическая загрузка/(de)serialization `.proto`, построение `Type`.
- `node:crypto` — SHA-256 для contract-hash.
- `node:fs` — чтение `.schema.json`.

Используется: `sdk/node/src/rpc/` (caller-сторона, `computeContractHash`), `sdk/node/src/registry/registry.ts` (`buildSchemaPair`, `computeContractHash`, передача JSON-schema в RegisterRequest), `sdk/node/src/events/` (publisher/subscriber/domain — `SchemaPair`/`SchemaSpec`), `sdk/node/src/connection/service-bridge.ts` (`buildSchemaPair`, реэкспорт `SchemaSpec`).
