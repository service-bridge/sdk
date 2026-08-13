# connection

## Зона ответственности

Владеет управляющим соединением SDK с рантаймом: разбор bootstrap-ключа, provisioning mTLS-идентичности, пиннинг CA, SPIFFE, сессия `Control.Open`, переподключение по лестнице, ротация сертификата с перекрытием и раздача mTLS-кредов всем их держателям.

Не делает: бизнес-RPC, события, workflow, jobs, телеметрию — эти домены только регистрируются как потребители кредов и берут канал текущей сессии. Не держит собственный backoff: лестница приходит из `internal/stream`.

## Публичный контракт

### Идентичность и TLS

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `ParseBootstrapKey(raw string) (BootstrapKey, error)` | функция | — | Разбирает ключ вида `sb.<base64url(BootstrapKeyPayload)>`; отдаёт keyID, secret и CA-cert как якорь доверия. |
| `BootstrapKey` | struct | — | `KeyID`, `Secret`, `CACertDER`, `CACert`. |
| `Provision(ctx, addr string, key BootstrapKey) (*ProvisionResult, error)` | функция | — | `Bootstrap.Provision`: одноразовый канал, argon2id на стороне рантайма, выдача leaf-сертификата. |
| `ProvisionResult` | struct | — | `Identity`, `ServiceName`, `CertDER`, `CAChainDER`, `PrivateKey`, `NotAfter`, `TLSCert`. |
| `NewCSR() (*ecdsa.PrivateKey, []byte, error)` | функция | — | Новая пара P-256 и PKCS#10 CSR к ней. |
| `NewTLSCertificate(certDER, caChainDER []byte, priv *ecdsa.PrivateKey) (tls.Certificate, error)` | функция | — | Собирает клиентский credential из выданного leaf, цепочки и приватного ключа. |
| `NotAfterFromUnixSeconds(sec int64) time.Time` | функция | — | Конвертирует `not_after_unix` — единственное поле протокола в СЕКУНДАХ. |
| `PinnedTLSConfig(ca *x509.Certificate) *tls.Config` | функция | — | TLS 1.3, доверие ровно одному корню, проверка цепочки в `VerifyConnection`. |
| `MutualTLSConfig(ca *x509.Certificate, clientCert tls.Certificate) *tls.Config` | функция | — | То же плюс клиентский сертификат. |
| `Identity` | struct | — | `ServiceID` + `InstanceID` из URI SAN. |
| `FormatSPIFFE(id Identity) string` · `ParseSPIFFE(raw string) (Identity, error)` | функции | — | `spiffe://service-bridge/service/<id>/instance/<id>`. |
| `SPIFFETrustDomain` | `string` | `service-bridge` | Домен доверия; обязан совпадать байт в байт с рантаймом и Node SDK. |

### Ошибки

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Error` | struct | — | Единственный тип ошибки пакета: `Kind`, `Op`, `Msg`, `Err`; `Unwrap` и `Is` по `Kind`. |
| `Kind` | `string` | — | `KEY`, `PROVISION`, `TLS`, `IDENTITY`, `SESSION`, `ROTATE`. |
| `ErrKey`, `ErrProvision`, `ErrTLS`, `ErrIdentity`, `ErrSession`, `ErrRotate` | `*Error` | — | Сентинелы для `errors.Is`; сравнение идёт только по `Kind`. |

### Лизы и креды

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Lease` | struct | — | Один leaf-сертификат: идентичность, имя сервиса, DER, ключ, `tls.Certificate`, `NotAfter`. Единица, вокруг которой построен весь жизненный цикл. |
| `Credentials` | struct | — | `Addr`, `Lease`, `TLS` — материал одной сессии для всех, кто говорит по mTLS. |
| `CredentialConsumer` | interface | — | `UseCredentials(ctx, Credentials) error`. Реализуют Call-сервер, оба исходящих транспорта, клиенты событий, workflow, job, телеметрии. Внутри нельзя звать реестр: он держит лок. |
| `NewCredentialRegistry() *CredentialRegistry` | функция | — | Пустой реестр потребителей. |
| `(*CredentialRegistry) Register(ctx, name string, c CredentialConsumer) error` | метод | — | Регистрирует потребителя; если креды уже опубликованы, отдаёт текущие сразу. |
| `(*CredentialRegistry) Update(ctx, creds Credentials) error` | метод | — | Один проход по всем потребителям. Пробует каждого даже после сбоя; ошибки объединяются. |
| `(*CredentialRegistry) Current() (Credentials, bool) `| метод | — | Последние опубликованные креды. |
| `SessionIdentity` | struct | — | `SessionID`, `ServiceID`, `ServiceName`, `InstanceID` живой сессии. |
| `IdentitySource` | interface | — | `Identity() SessionIdentity`. Читать по требованию: каждая ротация даёт новый `InstanceID`. |

### Зависимости жизненного цикла

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `Provisioner` | interface | — | `Provision(ctx) (Lease, error)` — дорогой путь через argon2id. |
| `BootstrapProvisioner` | struct | — | Реализация поверх `Bootstrap.Provision`: `Addr`, `Key`. |
| `Refresher` | interface | — | `Refresh(ctx, conn, prev Lease) (Lease, error)` — продление по живому mTLS-каналу. |
| `ControlRefresher` | struct | — | Реализация через `Control.RefreshCert`. |
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
| `Backoff` | `stream.Backoff` | `stream.NewBackoff()` | Лестница переподключений и повторов ротации. |
| `MaxAttempts` | `int` | `0` | Предел подряд идущих попыток; `0` — без предела. |
| `WelcomeTimeout` | `time.Duration` | `10s` | Сколько ждать `Welcome` на новом стриме. |
| `RotateLead` | `time.Duration` | `30m` | За сколько до истечения продлевать сертификат. |
| `RotateJitter` | `time.Duration` | `5m` | Окно случайного сдвига продления. |
| `MinRotateDelay` | `time.Duration` | `5s` | Нижняя граница интервала продления. |
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
| `Identity() SessionIdentity` | метод | — | Идентичность живой сессии; реализует `IdentitySource`. |
| `Conn() (grpc.ClientConnInterface, error)` | метод | — | Канал живой сессии; спрашивать на каждое открытие потока, не запоминать. |
| `Credentials() *CredentialRegistry` | метод | — | Реестр, в который регистрируются потребители кредов. |

Константы: `DefaultWelcomeTimeout`, `DefaultRotateLead`, `DefaultRotateJitter`, `DefaultMinRotateDelay`.

## Приватный контракт

| Имя | Тип | По умолчанию | Что делает |
|-----|-----|--------------|------------|
| `session` | struct | — | Один живой `Control.Open` и канал под ним. Ровно одна горутина `run` читает стрим, ровно один владелец закрывает канал. |
| `newSession(ctx, Dialer, Credentials, *slog.Logger, func(string)) (*session, error)` | функция | — | Дозвон плюс открытие стрима; `ctx` управляет всей жизнью сессии. |
| `(*session) awaitWelcome(ctx, timeout)` | метод | — | Ждёт единственное доказательство живости; все точки ожидания селектятся с `ctx` и смертью стрима. |
| `(*session) shutdown(ctx)` | метод | — | Registrar → стрим → канал. Идемпотентен и синхронен. |
| `state` | struct | — | Сессия, её идентичность и лиз как одно значение: неудачный своп откатывается целиком. |
| `leaseSource` | `func(ctx) (Lease, error)` | — | Единственное различие между первым коннектом, реконнектом и ротацией. |
| `(*Lifecycle) connect(attemptCtx, sessionCtx, leaseSource) error` | метод | — | Тот самый единственный путь подключения. |
| `(*Lifecycle) leaseForConnect(ctx)` | метод | — | Кэш сертификата: переиспользует лист, пока до истечения больше `RotateLead`. |
| `(*Lifecycle) run(ctx)` | метод | — | Одна горутина владеет и реконнектом, и ротацией. |
| `(*Lifecycle) waitLadder(ctx, *int, error) bool` | метод | — | Одна ступень лестницы `stream.Backoff` плюс `MaxAttempts`. |
| `(*Lifecycle) rotateOnce / rotate / armRotation / rotateDelay` | методы | — | Продление с перекрытием и его расписание. |
| `(*Lifecycle) adopt / restore / drop / discard` | методы | — | Своп текущей сессии, откат свопа, закрытие потерянной и закрытие непринятой. |
| `isTerminal(err) bool` · `terminalCodes` | функция, map | — | `Unauthenticated`, `PermissionDenied`, `NotFound`, `InvalidArgument` — терминально. |
| `leafIdentity(leaf) (Identity, error)` | функция | — | Идентичность из URI SAN выданного сертификата. |
| `resetTimer` · `stopTimer` | функции | — | Безопасный перевзвод таймера расписания. |
| `nopObserver` | struct | — | Наблюдатель по умолчанию. |

## Архитектурные решения и почему

**Один путь подключения.** `connect` вызывается при первом коннекте, при каждом реконнекте и при каждой ротации; различается только `leaseSource`. Надзор, публикация кредов и обновление кэша сертификата живут внутри него, поэтому забыть их негде. В Node SDK ротация была написана отдельной веткой и не унаследовала ни надзор (сессия умирала в тишине, сервис навсегда терял управляющий канал), ни рассылку кредов (обновлялись 2 канала из 8), ни кэш (argon2id на каждый реконнект).

**Живость = стрим.** Хартбита нет (ADR-0005): закрытие `Control.Open` и есть сигнал отключения, а первый `Welcome` — единственное доказательство, что сессия поднялась.

**Перекрытие при ротации.** Порядок: `RefreshCert` по живому каналу → новые креды → новый канал → новый `Control.Open` → дождаться `Welcome` → и только теперь закрыть старое. Рантайм выдаёт НОВЫЙ `instance_id` при том же `service_id`, поэтому старая и новая сессии различимы и закрытие старой не помечает новую отключённой. Совпадение `instance_id` со старым — ошибка протокола, продление отклоняется.

**Откат.** Любой сбой после дозвона закрывает непринятый канал и оставляет старую сессию работать; продление повторяется по лестнице, пока старый сертификат ещё жив. Отказ хотя бы одного потребителя кредов откатывает своп целиком: половина каналов на новом сертификате и половина на старом — ровно то состояние, ради предотвращения которого существует реестр.

**Реестр потребителей вместо списка вызовов.** Потребители регистрируются там, где создаются; ротация делает один проход по реестру. Рукописный список «кого обновить» устаревает при появлении первого нового потребителя.

**Идентичность по требованию.** `instance_id` меняется каждой ротацией, поэтому потребители обязаны звать `Identity()` на каждое использование, а не копировать при создании.

**Кэш сертификата.** `Bootstrap.Provision` — argon2id на 64 МиБ на рантайме. Прогон на каждый транспортный реконнект превращает шторм переподключений в самообстрел, поэтому закэшированный лист переиспользуется, пока до истечения больше `RotateLead`.

**Терминальные коды.** `Unauthenticated`, `PermissionDenied`, `NotFound`, `InvalidArgument` останавливают жизненный цикл и уходят наружу через `Observer.Disconnected`: долбиться в них бессмысленно и вредно.

**Одна горутина состояния.** Реконнект и ротация выполняются в одном `select`, поэтому не могут гонять за текущую сессию. Всё, что создаётся, закрывается: незакрытый `grpc.ClientConn` держит собственные горутины переподключения до конца процесса.

**Почему не `stream.Supervisor`.** Супервизор владеет ровно одним поколением потока и рвёт старое до открытия нового — это исключает перекрытие, без которого ротация теряет сессию на время дозвона. Лестница (`stream.Backoff`) при этом переиспользуется как есть, своей нет.

**Единицы времени.** Всё на проводе — `int64` unix-ms (ADR-0006), кроме `not_after_unix` в `ProvisionResponse` и `RefreshCertResponse`: там СЕКУНДЫ. Конвертация одна — `NotAfterFromUnixSeconds`; прочтение как миллисекунд отправляет срок в 1970 и запускает продление в горячем цикле.

**Синхронный первый коннект.** `Start` возвращает ошибку первой попытки, а не прячет её за лестницей: отклонённый bootstrap-ключ не должен выглядеть как медленный старт.

## Зависимости

Опирается на: `internal/stream` (`Backoff` — лестница), `internal/pb` (стабы `Bootstrap`, `Control`, `Registry`), `google.golang.org/grpc`, `crypto/x509`, `crypto/tls`, `log/slog`.

На него опираются: корневой пакет `servicebridge` (сборка графа зависимостей клиента), `internal/registry` (берёт канал живой сессии через `Conn`), и все будущие держатели mTLS-кредов — входящий Call-сервер, исходящие транспорты, клиенты событий, workflow, job и телеметрии — через `CredentialRegistry` и `IdentitySource`.
