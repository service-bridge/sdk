# API reference

[← к индексу](./index.md)

Компактный справочник публичных сигнатур. Подробности и пояснения — в доменных документах.

## Содержание

- [Импорты](#импорты)
- [Клиент](#клиент)
- [RPC](#rpc)
- [События](#события)
- [Задачи](#задачи-cjob)
- [Workflow](#workflow-cworkflow)
- [Телеметрия](#телеметрия-ctelemetry)
- [Опции конструктора](#опции-конструктора)
- [HTTP-интеграции](#http-интеграции)
- [Тестирование](#тестирование)
- [Ошибки](#ошибки)

## Импорты

```go signature
import (
	sb "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/job"
	"github.com/service-bridge/sdk/go/sbhttp"
	"github.com/service-bridge/sdk/go/sbtest"
	wf "github.com/service-bridge/sdk/go/workflow"
)
```

`github.com/service-bridge/sdk/go/sbgin` — отдельный модуль, ставится своим `go get`.

## Клиент

```go signature
func New(url, key string, opts ...Option) (*Client, error)

func (c *Client) Start(ctx context.Context) error
func (c *Client) Stop(ctx context.Context) error

func (c *Client) Identity() Identity
func (c *Client) ServiceMap() ServiceMap
func (c *Client) PolicyEvaluation() PolicyEvaluation
func (c *Client) Service(name string, deps ServiceDeps) error

func (c *Client) OnConnected(fn func(Identity))
func (c *Client) OnReconnecting(fn func(attempt int, cause error))
func (c *Client) OnDraining(fn func(reason string))
func (c *Client) OnDisconnected(fn func(cause error))
func (c *Client) OnPolicyViolation(fn func(PolicyViolation))
```

Поля-домены: `c.Job *JobDomain`, `c.Workflow *WorkflowDomain`, `c.Telemetry *TelemetryDomain`.

Типы:

```go signature
type Identity struct {
	SessionID   string
	ServiceID   string
	ServiceName string
	InstanceID  string
}

type ServiceDeps struct {
	RPC       []string
	Workflows []string
	HTTP      []string
}

type ServiceMap struct {
	Instances []InstanceInfo
	Methods   []MethodInfo
}

type InstanceInfo struct {
	ServiceID        string
	ServiceName      string
	InstanceID       string
	CallEndpoint     string
	HTTPEndpoint     string
	Status           string
	UnhealthySinceMs int64
}

type MethodInfo struct {
	ServiceName  string
	ServiceID    string
	InstanceID   string
	Name         string
	Type         string
	ContractHash string
	Streaming    bool
}

type PolicyEvaluation struct {
	Capabilities []string
	Warnings     []PolicyViolation
}

type PolicyViolation struct {
	Declaration string
	Value       string
	DenySide    string
	Reason      string
}
```

`Recorder()`, `Declarations()`, `RestartRegistry()`, `NewRegistrar()`, `RegistryClient()` и `Steps()` экспортированы ради HTTP-интеграций и внутренних потребителей. Прикладному коду они не нужны — единственный поддерживаемый способ их использовать — передать сам клиент в `sbhttp.New`.

## RPC

```go signature
func Handle[Req, Resp proto.Message](c *Client, name string,
	fn func(ctx context.Context, req Req) (Resp, error)) error

func HandleStream[Req, Chunk proto.Message](c *Client, name string,
	fn func(ctx context.Context, req Req, send func(Chunk) error) error) error

func Call[Req, Resp proto.Message](ctx context.Context, c *Client,
	service, method string, req Req, opts ...CallOption) (Resp, error)

func Stream[Req, Chunk proto.Message](ctx context.Context, c *Client,
	service, method string, req Req, opts ...CallOption) iter.Seq2[Chunk, error]

func NewClient(c *Client, service string) *ServiceClient
func NewMethod[Req, Resp proto.Message](sc *ServiceClient, method string) (*Method[Req, Resp], error)

func (m *Method[Req, Resp]) Call(ctx context.Context, req Req, opts ...CallOption) (Resp, error)
func (m *Method[Req, Resp]) Stream(ctx context.Context, req Req, opts ...CallOption) iter.Seq2[Resp, error]
```

`CallOption`:

```go signature
func WithTimeout(d time.Duration) CallOption
func WithTransport(t Transport) CallOption
func WithIdempotencyKey(key string) CallOption
func WithBusinessKey(key string) CallOption
```

```go signature
type Transport uint8

const (
	TransportDirect Transport = iota // по умолчанию
	TransportProxy
)
```

## События

```go signature
func DefineEvent[T proto.Message](c *Client, name string) (*Event[T], error)
func (e *Event[T]) Name() string
func (e *Event[T]) Publish(ctx context.Context, payload T, opts ...PublishOption) (string, error)

func PublishEvent[T proto.Message](ctx context.Context, c *Client, name string,
	payload T, opts ...PublishOption) (string, error)

func SubscribeEvent[T proto.Message](c *Client, name string,
	fn func(ctx context.Context, event T) error) error

func SubscribeEventRaw(c *Client, name string,
	fn func(ctx context.Context, payload []byte) error) error
```

`PublishOption`:

```go signature
func WithEventIdempotencyKey(key string) PublishOption
func WithPartitionKey(key string) PublishOption
func WithFireAndForget() PublishOption
func WithHeaders(h map[string]string) PublishOption
func WithOccurredAt(unixMs int64) PublishOption
```

## Задачи (`c.Job`)

```go signature
func (d *JobDomain) Handle(name string, spec job.Spec, fn job.Handler) error
```

Пакет `job`:

```go signature
func Cron(expr, tz string) (Trigger, error)
func Interval(d time.Duration) (Trigger, error)
func At(t time.Time) (Trigger, error)

func NewSpec(t Trigger, opts ...Option) Spec

func WithCatchup(p CatchupPolicy) Option
func WithOverlap(p OverlapPolicy) Option
func WithDeps(deps ...Dep) Option
func WithMaxAttempts(n int) Option
func WithLeaseTTL(d time.Duration) Option
func WithMaxConcurrent(n int) Option
func WithRetry(p RetryPolicy) Option

func RPC(target string) Dep
func Event(name string) Dep
func Workflow(name string) Dep
```

```go signature
type Handler func(ctx context.Context, exec Execution) error

type Execution struct {
	Name                   string
	ID                     string
	ScheduledAtUnixMs      int64
	LocalScheduledAtUnixMs int64
	Attempt                int
	IdempotencyKey         string
}

type RetryPolicy struct {
	InitialMs  int64
	MaxMs      int64
	Multiplier float64
	Jitter     float64
}
```

Константы политик: `CatchupSkip`, `CatchupFireOnce`, `CatchupFireAll`; `OverlapSkip`, `OverlapAllow`, `OverlapBufferOne`.

Сентинел: `job.ErrPermanent`. Ошибки объявления: `ErrNoTrigger`, `ErrCronFieldCount`, `ErrCronExpr`, `ErrCronTZ`, `ErrInterval`, `ErrRunAt`, `ErrCatchupPolicy`, `ErrOverlapPolicy`, `ErrDepKind`, `ErrDepTarget`, `ErrRetryInitial`, `ErrNegativeLimit`, `ErrEmptyName`, `ErrNoHandler`, `ErrDuplicateName`.

## Workflow (`c.Workflow`)

```go signature
func (d *WorkflowDomain) Handle(name string, def wf.Definition) error
func (d *WorkflowDomain) Start(ctx context.Context, name string, input any, opts ...StartOption) (string, error)
func (d *WorkflowDomain) Signal(ctx context.Context, runID, signal string, payload any) error
func (d *WorkflowDomain) Cancel(ctx context.Context, runID string) error
func (d *WorkflowDomain) Await(ctx context.Context, runID string) (map[string]any, error)
func (d *WorkflowDomain) Query(ctx context.Context, runID string) (RunSnapshot, error)
func (d *WorkflowDomain) Replay(ctx context.Context, runID, fromStepID string) (string, error)

func WithRunIdempotencyKey(key string) StartOption
func WithRunTimeoutSec(sec int) StartOption
```

```go signature
type RunSnapshot struct {
	RunID  string
	Status string
	State  map[string]any
	Steps  []StepSnapshot
}

type StepSnapshot struct {
	StepID        string
	Status        string
	Output        any
	LastError     string
	CompensatedBy string
}
```

Пакет `workflow`:

```go signature
type Definition struct {
	Input          map[string]any
	Steps          []Step
	Retry          *RetryPolicy
	MaxParallelism int
	TimeoutSec     int
}

type Control struct {
	ID         string
	WaitFor    []string
	When       Predicate
	Compensate *Compensation
	TimeoutSec int
	Retry      *RetryPolicy
}
```

Виды шагов: `Call`, `Publish`, `Sleep`, `WaitEvent`, `WaitSignal`, `SubWorkflow`, `Parallel`, `Sequence`, `Local`. Дискриминаторы: `KindCall`, `KindPublish`, `KindSleep`, `KindWaitEvent`, `KindWaitSignal`, `KindWorkflow`, `KindParallel`, `KindSequence`, `KindLocal`.

Выражения и цели: `Path`, `Name`, `Target`.

Предикаты: `Truthy(Path)`, `Not(Predicate)`, `Equals(any, any)`, `In(any, any)`, `And(...Predicate)`, `Or(...Predicate)`.

Опции шагов:

```go signature
type CallOpts struct {
	Timeout        time.Duration
	Transport      Transport
	IdempotencyKey any
	RequestID      any
	Retry          *RetryPolicy
}

type PublishOpts struct {
	IdempotencyKey any
	PartitionKey   any
	FireAndForget  bool
	Headers        map[string]any
	OccurredAtMs   int64
}

type StartOpts struct {
	IdempotencyKey any
	TimeoutSec     int
}

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	Factor      float64
	MaxDelay    time.Duration
	Jitter      float64
}

type Compensation struct {
	Kind           CompensationKind
	Service        Target
	Method         Target
	Event          Target
	Input          any
	Retry          *RetryPolicy
	IdempotencyKey any
}

type ForEach struct {
	From Path
	As   string
}
```

Транспорт шага: `wf.TransportAuto`, `wf.TransportDirect`, `wf.TransportProxy`. Вид компенсации: `wf.CompensateCall`, `wf.CompensatePublish`.

## Телеметрия (`c.Telemetry`)

```go signature
func (d *TelemetryDomain) StartOp(ctx context.Context, name string, opts ...OpOption) (context.Context, *Operation)
func (d *TelemetryDomain) Logger() *slog.Logger
func (d *TelemetryDomain) Counter(name string, labels map[string]string) *Counter
func (d *TelemetryDomain) Gauge(name, unit string, labels map[string]string) *Gauge
func (d *TelemetryDomain) Histogram(name, unit string, labels map[string]string, bounds []float64) *Histogram

func WithOpPeer(serviceID string) OpOption
func WithOpBusinessKey(key string) OpOption

func (o *Operation) End()
func (o *Operation) Fail(err error)

func (c *Counter) Inc()
func (c *Counter) Add(delta float64)
func (g *Gauge) Set(value float64)
func (h *Histogram) Observe(value float64)
```

## Опции конструктора

```go signature
func WithAdvertise(host string, port int) Option
func WithCallerOnly() Option
func WithCallDefaults(opts ...CallOption) Option
func WithCallAttempts(n int) Option
func WithFailOnPolicyViolation() Option
func WithDataDir(dir string) Option
func WithMaxOutboxRows(n int) Option
func WithDrainBatchSize(n int) Option
func WithMaxInFlightEvents(n int) Option
func WithInboundLimits(maxCalls, maxStreams int) Option
func WithReconnectAttempts(n int) Option
func WithReconnectLadder(rungs ...time.Duration) Option
func WithLogger(log *slog.Logger) Option
```

Экспортированные умолчания:

| Константа | Значение |
|---|---|
| `sb.DefaultDataDir` | `"./.servicebridge"` |
| `sb.DefaultMaxOutboxRows` | `10000` |
| `sb.DefaultDrainBatchSize` | `100` |
| `sb.DefaultMaxInFlightEvents` | `32` |
| `sb.DefaultMaxConcurrentCalls` | `512` |
| `sb.DefaultMaxConcurrentStreams` | `512` |
| `sb.DefaultCallAttempts` | `3` |
| `sb.DefaultAdvertiseHost` | `"127.0.0.1"` |

## HTTP-интеграции

Пакет `sbhttp`:

```go signature
func New(rt Runtime, opts ...Option) (*Integration, error)
func WithLogger(log *slog.Logger) Option

func (i *Integration) Middleware(next http.Handler) http.Handler
func (i *Integration) Begin(r *http.Request) (*http.Request, *Operation, error)
func (i *Integration) Logger() *slog.Logger
func (i *Integration) Publish(routes []Route, ep Endpoint) error
func (i *Integration) PublishMux(m *Mux, ep Endpoint) error
func (i *Integration) PublishChi(r chi.Routes, ep Endpoint) error

func NewMux() *Mux
func (m *Mux) Handle(pattern string, handler http.Handler)
func (m *Mux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request)
func (m *Mux) Routes() []Route

func (o *Operation) Capturing() bool
func (o *Operation) PayloadLimit() int
func (o *Operation) CaptureResponse(body []byte)
func (o *Operation) Finish(out Outcome)
```

```go signature
type Endpoint struct {
	Host string
	Port int
}

type Route struct {
	Method  string
	Pattern string
}

type Outcome struct {
	StatusCode int
	Aborted    bool
	Hijacked   bool
	Panicked   bool
}
```

Сентинелы: `sbhttp.ErrNoRuntime`, `ErrNoRouter`, `ErrPort`.

Пакет `sbgin` (отдельный модуль):

```go signature
func Middleware(integration *sbhttp.Integration) gin.HandlerFunc
func Publish(integration *sbhttp.Integration, engine *gin.Engine, endpoint sbhttp.Endpoint) error
```

## Тестирование

```go signature
func New() *Harness
func (h *Harness) Reset()
func NewRPC() *RPC
func NewEvent() *Event

func Handle[Req, Res any](r *RPC, method string, fn Handler[Req, Res]) error
func Invoke[Req, Res any](ctx context.Context, r *RPC, method string, req Req) (Res, error)
func Respond[Req, Res any](r *RPC, service, method string, fn Responder[Req, Res]) error
func RespondWith[Res any](r *RPC, service, method string, res Res) error
func Call[Req, Res any](ctx context.Context, r *RPC, service, method string, req Req) (Res, error)
func (r *RPC) Calls() []CallRecord
func (r *RPC) Reset()

func Define[T any](e *Event, name string) error
func Subscribe[T any](e *Event, name string, fn Subscriber[T]) error
func Publish[T any](ctx context.Context, e *Event, name string, payload T) (Delivery, error)
func (e *Event) Published() []PublishRecord
func (e *Event) Deliveries() []Delivery
func (e *Event) Reset()
```

```go signature
type CallRecord struct {
	Service string
	Method  string
	Input   any
}

type PublishRecord struct {
	Name    string
	Payload any
}

type Delivery struct {
	Name  string
	Acked bool
	Err   error
}
```

Сентинелы: `sbtest.ErrNoHandler`, `ErrNoResponse`, `ErrTypeMismatch`, `ErrDuplicate`, `ErrInvalidArg`.

## Ошибки

```go signature
type Error struct {
	Code Code
	Op   string
	Msg  string
	Err  error
}

func (e *Error) Error() string
func (e *Error) Unwrap() error
func (e *Error) Is(target error) bool
```

| Код | Сентинел | Когда |
|---|---|---|
| `CodeConfig` | `ErrConfig` | Конфигурация, с которой SDK отказывается работать. До лестницы переподключения не доходит: повторами это не лечится. |
| `CodeState` | `ErrState` | Не та фаза жизненного цикла: объявление после `Start`, публикация до него, использование остановленного клиента. |
| `CodeConnection` | `ErrConnection` | Провижининг, сессия или стрим не открываются. |
| `CodeAccessDenied` | `ErrAccessDenied` | Политика доступа mesh отказала в вызове, публикации или запуске. |
| `CodeNotFound` | `ErrNotFound` | Имени, для которого в mesh нет определения. |
| `CodeValidation` | `ErrValidation` | Объявление или аргумент, который рантайм отверг бы, пойманный там, где он написан. |
| `CodeTerminal` | `ErrTerminal` | Прогон workflow уже завершён. |
| `CodeOutboxFull` | `ErrOutboxFull` | Локальный буфер событий на пределе. |
| `CodeNoLiveInstance` | `ErrNoLiveInstance` | Вызову некуда идти. |
| `CodeInvalidEventName` | `ErrInvalidEventName` | Имя, которое отвергает грамматика событий рантайма. |
| `CodeHandler` | `ErrHandler` | Обработчик callee вернул ошибку. Это ответ, а не сбой транспорта. |
| `CodeInternal` | `ErrInternal` | Всё остальное. |

Пакеты `job`, `sbhttp` и `sbtest` несут собственные сентинелы для того, что они отвергают локально; сравниваются так же, через `errors.Is`.

---

Дальше: [References](./references.md)
