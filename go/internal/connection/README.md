# connection

## Зона ответственности

Владеет управляющим соединением SDK с рантаймом: разбор bootstrap-ключа, provisioning mTLS-идентичности, пиннинг CA, SPIFFE, сессия `Control.Open`, проверка версии протокола, переподключение по лестнице, продление сертификата на месте и раздача mTLS-материала всем его держателям.

Не делает: бизнес-RPC, события, workflow, jobs, телеметрию — эти домены только регистрируются как потребители кредов и берут канал текущей сессии. Не держит собственный backoff: лестница приходит из `internal/stream`.

## Публичный контракт

### Идентичность и TLS

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `ParseBootstrapKey(raw string) (BootstrapKey, error)` | функция | — | Разбирает ключ вида `sb.<base64url(BootstrapKeyPayload)>`; отдаёт keyID, secret и CA-cert как якорь доверия. |
| `BootstrapKey` | struct | — | `KeyID`, `Secret`, `CACertDER`, `CACert`. |
| `Provision(ctx, addr string, key BootstrapKey) (*ProvisionResult, error)` | функция | — | `Bootstrap.Provision`: одноразовый канал, argon2id на стороне рантайма, выдача leaf-сертификата. |
| `ProvisionResult` | struct | — | `Identity`, `ServiceName`, `CertDER`, `CAChainDER`, `PrivateKey`, `NotAfterUnixMs`, `TLSCert`. |
| `NewCSR() (*ecdsa.PrivateKey, []byte, error)` | функция | — | Новая пара P-256 и PKCS#10 CSR к ней. |
| `NewTLSCertificate(certDER, caChainDER []byte, priv *ecdsa.PrivateKey) (tls.Certificate, error)` | функция | — | Собирает клиентский credential из выданного leaf, цепочки и приватного ключа. |
| `PinnedTLSConfig(ca *x509.Certificate) *tls.Config` | функция | — | TLS 1.3, доверие ровно одному корню, проверка цепочки в `VerifyConnection`. |
| `MutualTLSConfig(ca *x509.Certificate, clientCert tls.Certificate) *tls.Config` | функция | — | То же плюс один фиксированный клиентский сертификат. |
| `RotatingTLSConfig(ca *x509.Certificate, current func() *tls.Certificate) *tls.Config` | функция | — | `PinnedTLSConfig`, чей клиентский сертификат читается на каждом handshake (`GetClientCertificate`). Из неё строятся все каналы SDK. |
| `ClientKeepalive() keepalive.ClientParameters` | функция | 30s / 10s / без вызовов | Keepalive каждого канала SDK. |
| `ProtocolVersion` · `SDKLanguage` · `SDKVersion` | константы | `1` · `go` · версия модуля | Идентичность в `OpenRequest`/`RegisterRequest`. |
| `Identity` | struct | — | `ServiceID` + `InstanceID` из URI SAN. |
| `FormatSPIFFE(id Identity) string` · `ParseSPIFFE(raw string) (Identity, error)` | функции | — | `spiffe://service-bridge/service/<id>/instance/<id>`. |
| `SPIFFETrustDomain` | `string` | `service-bridge` | Домен доверия; обязан совпадать байт в байт с рантаймом и Node SDK. |

### Ошибки

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Error` | struct | — | Единственный тип ошибки пакета: `Kind`, `Op`, `Msg`, `Err`; `Unwrap` и `Is` по `Kind`. |
| `Kind` | `string` | — | `KEY`, `PROVISION`, `TLS`, `IDENTITY`, `SESSION`, `ROTATE`, `PROTOCOL`. |
| `ErrKey`, `ErrProvision`, `ErrTLS`, `ErrIdentity`, `ErrSession`, `ErrRotate`, `ErrProtocol` | `*Error` | — | Сентинелы для `errors.Is`; сравнение идёт только по `Kind`. `ErrProtocol` — рантайм говорит на другой версии протокола; терминально. |
| `ErrDrained` | `error` | — | Причина конца сессии, которую рантайм закрыл после `Drain`. Реконнект после неё — штатный. |
| `IsTerminal(err) bool` | функция | — | Терминальна ли ошибка: коды `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT`, `FAILED_PRECONDITION` или `ErrProtocol`. |

### Лизы и креды

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Lease` | struct | — | Один leaf-сертификат: идентичность, имя сервиса, DER, ключ, `tls.Certificate`, `NotAfterUnixMs`. |
| `Credentials` | struct | — | `Addr`, `Lease`, `TLS` (общая вращаемая конфигурация жизненного цикла). |
| `CredentialConsumer` | interface | — | `UseCredentials(ctx, Credentials) error`. Вызывается при каждом новом листе. Потребитель, у которого лист того же инстанса, ничего не пересоздаёт; лист другого инстанса (переподготовка после истечения) пересоздаёт каналы. Внутри нельзя звать реестр: он держит лок. |
| `NewCredentialRegistry() *CredentialRegistry` | функция | — | Пустой реестр потребителей. |
| `(*CredentialRegistry) Register(ctx, name string, c CredentialConsumer) error` | метод | — | Регистрирует потребителя; если креды уже опубликованы, отдаёт текущие сразу. |
| `(*CredentialRegistry) Update(ctx, creds Credentials) error` | метод | — | Один проход по всем потребителям. Пробует каждого даже после сбоя; ошибки объединяются. |
| `(*CredentialRegistry) Current() (Credentials, bool) `| метод | — | Последние опубликованные креды. |
| `SessionIdentity` | struct | — | `SessionID`, `ServiceID`, `ServiceName`, `InstanceID` живой сессии. |
| `IdentitySource` | interface | — | `Identity() SessionIdentity`. Читать по требованию: переподготовка истёкшего листа даёт новый `InstanceID`. |

### Зависимости жизненного цикла

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Provisioner` | interface | — | `Provision(ctx) (Lease, error)` — дорогой путь через argon2id. |
| `BootstrapProvisioner` | struct | — | Реализация поверх `Bootstrap.Provision`: `Addr`, `Key`. |
| `Refresher` | interface | — | `Refresh(ctx, conn, prev Lease) (Lease, error)` — продление по живому mTLS-каналу. |
| `ControlRefresher` | struct | — | Реализация через `Control.RefreshCert`; отклоняет лист другого инстанса. |
| `Dialer` | interface | — | `Dial(ctx, Credentials) (*grpc.ClientConn, error)`. |
| `MTLSDialer` | struct | — | Канал к рантайму с текущим leaf. |
| `InboundServer` | interface | — | `Start(ctx) (string, error)` / `Close(ctx) error`. Адрес нужен до первой регистрации; сертификат берётся из реестра, не из `Start`. |
| `Registrar` · `RegistrarFactory` | interfaces | — | Поток `Registry.RegisterAndWatch` одной сессии: строится из её канала и умирает вместе с ней. |
| `Observer` | interface | — | `Connected`, `Reconnecting`, `Draining`, `Disconnected`. Колбэки выполняются на горутинах жизненного цикла и не должны блокировать. |

### `LifecycleConfig`

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Addr` | `string` | — (обязательный) | `host:port` рантайма. |
| `CACert` | `*x509.Certificate` | — (обязательный) | Якорь доверия из bootstrap-ключа, а не с провода. |
| `Provisioner` | `Provisioner` | — (обязательный) | Источник первого лиза. |
| `Refresher` | `Refresher` | `ControlRefresher{}` | Источник продлённого лиза. |
| `Dialer` | `Dialer` | `MTLSDialer{}` | Канал одной сессии. |
| `Credentials` | `*CredentialRegistry` | новый пустой | Реестр держателей mTLS-материала. |
| `Inbound` | `InboundServer` | `nil` | `nil` — инстанс caller-only, входящий слушатель не поднимается. |
| `Registrars` | `RegistrarFactory` | `nil` | `nil` — сессия не открывает registry-поток. |
| `Observer` | `Observer` | no-op | Наблюдатель переходов. |
| `Backoff` | `stream.Backoff` | `stream.NewBackoff()` | Лестница переподключений. |
| `MaxAttempts` | `int` | `0` | Предел подряд идущих неудачных попыток (сбрасывается на `Welcome`); `0` — без предела. |
| `WelcomeTimeout` | `time.Duration` | `10s` | Сколько ждать `Welcome` на новом стриме. |
| `RotateLead` | `time.Duration` | `30m` | За сколько до истечения продлевать сертификат. |
| `RotateJitter` | `time.Duration` | `5m` | Окно случайного сдвига продления. |
| `MinRotateDelay` | `time.Duration` | `5s` | Нижняя граница интервала продления. |
| `RotateRetry` | `time.Duration` | `60s` | Пауза после неудачного нетерминального продления (включая `RESOURCE_EXHAUSTED` — продление ограничено по частоте). |
| `Random` | `func() float64` | `rand.Float64` | Источник джиттера. |
| `Now` | `func() time.Time` | `time.Now` | Часы расписания. |
| `Logger` | `*slog.Logger` | `slog.Default()` | Структурный лог. |

### `Lifecycle`

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `NewLifecycle(cfg LifecycleConfig) (*Lifecycle, error)` | функция | — | Проверяет конфиг, подставляет дефолты. |
| `Start(ctx) error` | метод | — | Первый коннект синхронно, затем передаёт надзор своей горутине. `ctx` ограничивает только первую попытку: сессия переживает вызов. |
| `Stop(ctx) error` | метод | — | Останавливает надзор, закрывает сессию, registrar, канал и входящий сервер. Идемпотентен, корректен во время летящего коннекта. |
| `Rotate()` | метод | — | Просит продлить сертификат сейчас. Неблокирующий, схлопывающийся. |
| `IsTerminal(err)` | функция | — | См. «Ошибки». |
| `Identity() SessionIdentity` | метод | — | Идентичность живой сессии; реализует `IdentitySource`. |
| `Conn() (grpc.ClientConnInterface, error)` | метод | — | Канал живой сессии; спрашивать на каждое открытие потока, не запоминать. |
| `Credentials() *CredentialRegistry` | метод | — | Реестр, в который регистрируются потребители кредов. |

Константы: `DefaultWelcomeTimeout`, `DefaultRotateLead`, `DefaultRotateJitter`, `DefaultMinRotateDelay`, `DefaultRotateRetry`.

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `session` | struct | — | Один живой `Control.Open` и канал под ним. Ровно одна горутина `run` читает стрим, ровно один владелец закрывает канал. |
| `newSession(ctx, Dialer, Credentials, *slog.Logger, func(string)) (*session, error)` | функция | — | Дозвон плюс открытие стрима; `ctx` управляет всей жизнью сессии. |
| `(*session) awaitWelcome(ctx, timeout)` | метод | — | Ждёт единственное доказательство живости; все точки ожидания селектятся с `ctx` и смертью стрима. |
| `(*session) shutdown(ctx)` | метод | — | Registrar → стрим → канал. Идемпотентен и синхронен. |
| `state` | struct | — | Сессия, её идентичность и лиз как одно значение: неудачный своп откатывается целиком. |
| `leaseSource` | `func(ctx) (Lease, error)` | — | Закэшированный лист или свежеподготовленный. |
| `(*Lifecycle) connect(attemptCtx, sessionCtx, leaseSource) error` | метод | — | Единственный путь открытия сессии: первый коннект и реконнекты. Проверяет `Welcome.protocol_version`. |
| `(*Lifecycle) cert` · `tlsConfig` · `credentials(lease)` | поля, метод | — | Текущий лист (атомарно), общая вращаемая TLS-конфигурация, креды для публикации. |
| `(*session) drained` | `atomic.Bool` | — | Сессия получила `Drain`; её конец — штатный. |
| `(*Lifecycle) leaseForConnect(ctx)` | метод | — | Кэш сертификата: переиспользует лист, пока до истечения больше `RotateLead`. |
| `(*Lifecycle) run(ctx)` | метод | — | Одна горутина владеет и реконнектом, и ротацией. |
| `(*Lifecycle) waitLadder(ctx, *int, error) bool` | метод | — | Одна ступень лестницы `stream.Backoff` плюс `MaxAttempts`. |
| `(*Lifecycle) rotateOnce / rotate / armRotation / rotateDelay` | методы | — | Продление на месте и его расписание. |
| `(*Lifecycle) adopt / restore / drop / discard` | методы | — | Своп текущей сессии, откат свопа, закрытие потерянной и закрытие непринятой. |
| `isTerminal(err) bool` · `terminalCodes` | функция, map | — | `Unauthenticated`, `PermissionDenied`, `NotFound`, `InvalidArgument`, `FailedPrecondition`, `ErrProtocol` — терминально. |
| `leafIdentity(leaf) (Identity, error)` | функция | — | Идентичность из URI SAN выданного сертификата. |
| `resetTimer` · `stopTimer` | функции | — | Безопасный перевзвод таймера расписания. |
| `nopObserver` | struct | — | Наблюдатель по умолчанию. |

## Архитектурные решения и почему

**Один путь подключения.** `connect` вызывается при первом коннекте и при каждом реконнекте; различается только `leaseSource`. Надзор, публикация кредов и обновление кэша сертификата живут внутри него, поэтому забыть их негде.

**Живость = стрим.** Хартбита нет (ADR-0005): закрытие `Control.Open` и есть сигнал отключения, а первый `Welcome` — единственное доказательство, что сессия поднялась. `Welcome.protocol_version`, отличная от 0 и `ProtocolVersion`, — терминальная ошибка `ErrProtocol`; рантайм со своей стороны отвечает `FAILED_PRECONDITION`, тоже терминально.

**Продление на месте.** `RefreshCert` за 30 мин ±5 мин до истечения; `instance_id` при этом не меняется. Новый лист кладётся в атомарный указатель, который читает `GetClientCertificate` единой вращаемой TLS-конфигурации, и публикуется потребителям (Call-сервер ставит его в свой `GetConfigForClient`). Ни стрим, ни канал, ни сервер, ни сессия не пересоздаются; `Control.Open` не открывается повторно — рантайм считает второй `Control.Open` того же инстанса заменой и обрывает первый с `Aborted`. Новый лист применяется к новым соединениям; живые продолжают работать. Лист с другим `instance_id` отклоняется: идентичность сессии и данных разошлась бы. `RESOURCE_EXHAUSTED` (продление ограничено по частоте) и временные ошибки — повтор через 60 с; терминальные коды — конец жизненного цикла.

**Drain.** `Drain{reason}` от рантайма — info-лог и `Observer.Draining`; сессия живёт, пока рантайм её не закроет, после чего реконнект идёт по обычной лестнице с причиной `ErrDrained` и без error-логов.

**Реестр потребителей вместо списка вызовов.** Потребители регистрируются там, где создаются; ротация делает один проход по реестру. Рукописный список «кого обновить» устаревает при появлении первого нового потребителя.

**Идентичность по требованию.** `instance_id` стабилен в пределах листа и его продлений, но меняется при переподготовке истёкшего листа, поэтому потребители зовут `Identity()` на каждое использование.

**Кэш сертификата.** `Bootstrap.Provision` — argon2id на 64 МиБ на рантайме. Прогон на каждый транспортный реконнект превращает шторм переподключений в самообстрел, поэтому закэшированный лист переиспользуется, пока до истечения больше `RotateLead`.

**Терминальные коды.** `Unauthenticated`, `PermissionDenied`, `NotFound`, `InvalidArgument`, `FailedPrecondition` и `ErrProtocol` останавливают жизненный цикл и уходят наружу через `Observer.Disconnected`. Счётчик реконнектов считает подряд идущие неудачи и обнуляется на `Welcome`.

**Одна горутина состояния.** Реконнект и ротация выполняются в одном `select`, поэтому не могут гонять за текущую сессию. Всё, что создаётся, закрывается: незакрытый `grpc.ClientConn` держит собственные горутины переподключения до конца процесса.

**Почему не `stream.Supervisor`.** Жизненному циклу нужны кэш листа, публикация кредов и расписание продления в одной горутине с реконнектом. Лестница (`stream.Backoff`) переиспользуется как есть.

**Keepalive.** Каждый канал SDK пингует раз в 30 с, ответа ждёт 10 с, пингует и без вызовов (`ClientKeepalive`); это укладывается в политики сервера рантайма (MinTime 10 с) и Call-сервера SDK (20 с).

**Единицы времени.** Всё на проводе — `int64` unix-ms (ADR-0006); срок сертификата и внутри SDK хранится как `NotAfterUnixMs`.

**Синхронный первый коннект.** `Start` возвращает ошибку первой попытки, а не прячет её за лестницей: отклонённый bootstrap-ключ не должен выглядеть как медленный старт.

## Зависимости

Опирается на: `internal/stream` (`Backoff` — лестница), `internal/pb` (стабы `Bootstrap`, `Control`, `Registry`), `google.golang.org/grpc`, `crypto/x509`, `crypto/tls`, `log/slog`.

На него опираются: корневой пакет `servicebridge` (сборка графа зависимостей клиента), `internal/registry` (берёт канал живой сессии через `Conn`), и все держатели mTLS-материала — входящий Call-сервер, исходящие транспорты, каналы событий, workflow, job и телеметрии — через `CredentialRegistry` и `IdentitySource`; `internal/registry` и `internal/rpc` берут `IsTerminal` и `ClientKeepalive`.

Runtime TLS требует закреплённую цепочку CA, ServerAuth и ровно один URI `spiffe://service-bridge/runtime`; проверка CN не используется. Прямое соединение отдельно проверяет точный service/instance URI. URI с query, fragment, userinfo или escaped path отвергается. Терминальный отказ авторизации отменяет активный SDK lifetime; после incident rotation старого bootstrap key требуется развёртывание нового ключа.
