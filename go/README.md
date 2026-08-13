# go

## Зона ответственности

Go SDK для рантайма ServiceBridge. Сейчас в модуле есть фундамент: генерация gRPC-стабов из протоколов рантайма, объявление модуля, каркас публичного API (`Client`, функциональные опции, единый тип ошибки).

Не делает: пока ничего из рантайм-функциональности — соединение, регистрация хендлеров, RPC, события, workflow, jobs и телеметрия не реализованы. `New`, `Start` и `Stop` возвращают ошибку с `Code == CodeUnimplemented`.

## Публичный контракт

Пакет `servicebridge` (корень модуля `github.com/service-bridge/sdk/go`).

### `func New(url, key string, opts ...Option) (*Client, error)`

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `url` | `string` | — (обязательный) | Адрес рантайма `host:port`. |
| `key` | `string` | — (обязательный) | Bootstrap-ключ формата `sb.<base64url(proto.Marshal(BootstrapKeyPayload))>`. CA-cert встроен в ключ как доверенный якорь. |
| `opts` | `...Option` | пусто | Опции конфигурации, см. ниже. |

### `type Client struct{}`

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Start(ctx context.Context) error` | метод | — | Провижининг mTLS-идентичности, открытие control-стрима, регистрация хендлеров. |
| `Stop(ctx context.Context) error` | метод | — | Дренаж in-flight работы и закрытие всех ресурсов клиента. |

### `type Option func(*config)`

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `WithAdvertise(host string, port int)` | `Option` | не задан | Адрес, по которому другие инстансы дозваниваются для Direct RPC. Без него inbound Call-сервер не поднимается. |
| `WithCallerOnly()` | `Option` | `false` | Инстанс только исходящий: не регистрирует хендлеры и не слушает входящие. |
| `WithDataDir(dir string)` | `Option` | `./.servicebridge` | Каталог локальной базы outbox. |
| `WithReconnectAttempts(n int)` | `Option` | `3` | Предел подряд идущих реконнектов. `0` — без предела. |

### `type Error struct`

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Code` | `Code` | — | Классификация сбоя — единственная ось, по которой ветвится вызывающий код. |
| `Op` | `string` | `""` | Операция, где возникла ошибка (`Client.Start`). |
| `Msg` | `string` | `""` | Человекочитаемое описание. Пусто — берётся текст `Err`. |
| `Err` | `error` | `nil` | Обёрнутая причина, доступна через `errors.Unwrap`. |
| `Error() string` | метод | — | `<Op>: <Code>: <Msg>`, пустые части опускаются. |
| `Unwrap() error` | метод | — | Возвращает `Err`. |
| `Is(target error) bool` | метод | — | Сравнивает только `Code`, игнорируя `Op`, `Msg` и причину. |

### `type Code string` и sentinel-переменные

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `CodeUnimplemented` / `ErrUnimplemented` | `Code` / `*Error` | — | Функциональность ещё не реализована. |
| `CodeConfig` / `ErrConfig` | `Code` / `*Error` | — | Неверная конфигурация клиента. |
| `CodeConnection` / `ErrConnection` | `Code` / `*Error` | — | Сбой соединения с рантаймом. |
| `CodeAccessDenied` / `ErrAccessDenied` | `Code` / `*Error` | — | Запрет по access policy рантайма. |
| `CodeNotFound` / `ErrNotFound` | `Code` / `*Error` | — | Сущность не найдена. |
| `CodeValidation` / `ErrValidation` | `Code` / `*Error` | — | Payload или декларация не прошли валидацию. |
| `CodeTerminal` / `ErrTerminal` | `Code` / `*Error` | — | Терминальный сбой, ретрай бессмыслен. |
| `CodeOutboxFull` / `ErrOutboxFull` | `Code` / `*Error` | — | Локальный outbox переполнен. |
| `CodeNoLiveInstance` / `ErrNoLiveInstance` | `Code` / `*Error` | — | Нет живого инстанса целевого сервиса. |

Sentinel'ы несут только `Code`, поэтому `errors.Is(err, ErrNotFound)` матчит любую ошибку с этим кодом.

### `scripts/gen-proto.sh`

Регенерация gRPC-стабов. Публичных опций нет; аргументы не принимает. Требует `buf` и `go` в `PATH` и репозиторий рантайма рядом: `<parent>/runtime/proto/servicebridge/v1`. Недостающие `protoc-gen-go` / `protoc-gen-go-grpc` ставит сам через `go install` на пиннутых версиях. Идемпотентен: чистит `internal/pb` и генерирует заново.

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `config` | `struct` | `defaultConfig()` | Собранная конфигурация клиента: `advertiseHost`, `advertisePort`, `callerOnly`, `dataDir`, `reconnectAttempts`. |
| `defaultConfig()` | `func() config` | — | Дефолты: `dataDir = "./.servicebridge"`, `reconnectAttempts = 3`. |
| `newError(code Code, op, msg string, cause error) *Error` | `func` | — | Конструктор ошибки SDK. |
| `internal/pb/servicebridge/v1` | пакет `pb` | — | Сгенерированные protobuf/gRPC-стабы. Не редактируется руками, не реэкспортируется. |

## Архитектурные решения и почему

**Своя генерация стабов.** Рантайм держит стабы в `runtime/internal/pb/servicebridge/v1` с `go_package = ".../runtime/internal/pb/..."`. Каталог `internal/` закрыт для внешних модулей, импортировать их нельзя. SDK генерирует собственную копию из тех же `.proto` под свой `go_package`, объявленный `M`-переопределениями в `buf.gen.yaml`.

**`buf.gen.yaml` объявляет свой вход.** `inputs` указывает на `../../runtime/proto`, поэтому шаблон генерации целиком живёт в этом репозитории и не требует правок в репозитории рантайма. Пути в `paths` резолвятся относительно рабочего каталога `buf generate`, а не относительно `directory`, — отсюда полный относительный путь `../../runtime/proto/servicebridge/v1`.

**`servicebridge/ui` исключён.** Это протоколы дашборда рантайма, SDK они не нужны, а генерируются они долго и объёмно. `servicebridge/v1/ui.proto` — другое: это сервис `UI.GetServiceGraph` из основного пространства v1, он входит в десятку и генерируется.

**Локальные плагины вместо remote.** Выполнение remote-плагинов `buf.build/protocolbuffers/go` и `buf.build/grpc/go` требует логина в реестр buf.build (иначе `permission_denied: 403`). `buf.gen.yaml` вызывает локальные `protoc-gen-go` и `protoc-gen-go-grpc`, версии которых пиннит `scripts/gen-proto.sh`, — генерация воспроизводима и работает офлайн.

**Один тип ошибки.** В Node SDK семь классов ошибок не имеют общего предка, поэтому один `catch` не ловит их все. Здесь всё наоборот: единственный тип `*Error`, таксономия живёт в поле `Code`, sentinel'ы сравниваются по коду через `errors.Is`.

**`go 1.24`.** Директива держит нижнюю границу поддерживаемых версий Go. Из-за этого `google.golang.org/grpc` пиннут на `v1.79.3`, а `golang.org/x/sync` на `v0.19.0` — в рантайме версии выше (`v1.81.1` / `v0.20.0`), но они требуют Go 1.25. GRPC-контракт от этого не меняется.

## Зависимости

Опирается на:

| Модуль | Версия | Зачем |
|--------|--------|-------|
| `google.golang.org/grpc` | `v1.79.3` | Транспорт control- и data-plane. |
| `google.golang.org/protobuf` | `v1.36.11` | Рантайм сгенерированных стабов. |
| `github.com/google/uuid` | `v1.6.0` | UUIDv7 для идентификаторов (ADR 0006). |
| `golang.org/x/sync` | `v0.19.0` | Примитивы конкурентности. |

Опирается на `runtime/proto/servicebridge/v1/*.proto` как на источник контракта — на этапе генерации, не в рантайме.

На модуль опираются: прикладные Go-сервисы (внешние потребители). Внутри репозитория потребителей нет.
