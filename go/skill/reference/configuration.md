# Configuration — Go SDK reference

Constructor options, lifecycle, errors and telemetry.

## Constructor

```go signature
func New(url, key string, opts ...Option) (*Client, error)
func (c *Client) Start(ctx context.Context) error
func (c *Client) Stop(ctx context.Context) error
```

`sb.New` performs **no I/O**. Every bad bound is reported here with `CodeConfig`, before anything connects — a misconfigured limit must never look like a network condition and feed the reconnect ladder.

`url` accepts both `host:port` and a URL-shaped form. `key` is the `sb.…` bootstrap key from the dashboard; it carries the CA certificate, so the SDK trusts exactly one root and nothing from the system store.

**The SDK reads no environment variables.** Read them yourself and pass the values in.

## Options and real defaults

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

| Option | Default | Effect |
|---|---|---|
| `WithAdvertise(host, port)` | `127.0.0.1`, port `0` | Address peers dial for direct RPC. Port `0` asks the OS for a free one and announces what it hands back. Advertised **as-is** — pass a real address in a container. |
| `WithCallerOnly()` | off | Outbound-only: no inbound listener, handler registration refused. Contradicts `WithAdvertise`. |
| `WithCallDefaults(opts...)` | none | `CallOption`s applied under every call that does not override them. |
| `WithCallAttempts(n)` | `3` | **Total** tries of one logical call, counting the first. |
| `WithFailOnPolicyViolation()` | off | Stop the client on a policy violation instead of only reporting it. |
| `WithDataDir(dir)` | `./.servicebridge` | Holds the outbox database, file `sdk.db`. |
| `WithMaxOutboxRows(n)` | `10000` | Event buffer cap; past it `Publish` returns `CodeOutboxFull`. `0` lifts the cap — an uncapped buffer turns a long outage into a full disk. |
| `WithDrainBatchSize(n)` | `100` | Buffered events claimed per drain iteration. |
| `WithMaxInFlightEvents(n)` | `32` | Concurrent inbound deliveries. At the cap the delivery stream stops being read — real backpressure. |
| `WithInboundLimits(calls, streams)` | `512` / `512` | Concurrent handlers across all connections, and HTTP/2 streams per connection. Past the first, callers get `ResourceExhausted` — load is shed, not queued. |
| `WithReconnectAttempts(n)` | `0` — unlimited | Cap on consecutive reconnect attempts. |
| `WithReconnectLadder(rungs...)` | `1s, 5s, 15s, 30s, 60s` | Reconnect delays; the last rung repeats forever, every rung jittered ±20 %. |
| `WithLogger(log)` | `slog.Default()` | Where the SDK writes its **own** diagnostics. |

Exported constants: `sb.DefaultDataDir`, `sb.DefaultMaxOutboxRows`, `sb.DefaultDrainBatchSize`, `sb.DefaultMaxInFlightEvents`, `sb.DefaultMaxConcurrentCalls`, `sb.DefaultMaxConcurrentStreams`, `sb.DefaultCallAttempts`, `sb.DefaultAdvertiseHost`.

Rejected at `New` with `CodeConfig`: empty runtime address; unparseable key; `WithCallerOnly` together with `WithAdvertise`; empty advertise host when not caller-only; port outside `0..65535`; empty data dir; negative outbox cap; non-positive drain batch, in-flight limit, inbound limits or call-attempt budget; negative reconnect cap; negative default call timeout; nil logger; a non-positive ladder rung.

## Lifecycle

`Start` seals declarations, opens the outbox, provisions the mTLS identity, binds the inbound server, registers, waits for the first registry snapshot, then starts subscriptions. It returns once the instance is registered and routable.

**The client outlives the context passed to `Start`**: that context's cancellation is dropped and its values kept, so a request-scoped context cannot take the client down. `Stop` is the way down. `Stop` is idempotent and reports the first failure without skipping the rest.

A second `Start`, or a `Start` after `Stop`, returns `CodeState`.

## Complete program

```go
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/orders/paymentpb"
	sb "github.com/service-bridge/sdk/go"
)

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("ORDERS_KEY"),
		sb.WithAdvertise(os.Getenv("POD_IP"), 50051),
		sb.WithCallDefaults(sb.WithTimeout(10*time.Second)),
		sb.WithCallAttempts(3),
		sb.WithDataDir("/var/lib/orders/sb"),
		sb.WithMaxOutboxRows(50_000),
		sb.WithInboundLimits(256, 256),
		sb.WithReconnectLadder(time.Second, 5*time.Second, 30*time.Second),
		sb.WithFailOnPolicyViolation(),
		sb.WithLogger(slog.Default()),
	)
	if err != nil {
		log.Fatal(err) // CodeConfig: nothing has connected yet
	}

	c.OnConnected(func(id sb.Identity) {
		log.Println("connected as", id.ServiceName, id.InstanceID)
	})
	c.OnReconnecting(func(attempt int, cause error) {
		log.Println("reconnecting", attempt, cause)
	})
	c.OnDraining(func(reason string) { log.Println("runtime draining:", reason) })
	c.OnDisconnected(func(cause error) { log.Println("disconnected:", cause) })
	c.OnPolicyViolation(func(v sb.PolicyViolation) {
		log.Printf("policy refused %s %q: %s", v.Declaration, v.Value, v.Reason)
	})

	// Declare everything before Start.
	payment := sb.NewClient(c, "payment-svc")
	if _, err := sb.NewMethod[*paymentpb.ChargeRequest, *paymentpb.ChargeReply](payment, "Charge"); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// Identity is read per use, NEVER cached: rotation mints a fresh
	// InstanceID under the same ServiceID.
	log.Println("identity:", c.Identity().ServiceID)
	log.Println("instances in the mesh:", len(c.ServiceMap().Instances))
	log.Println("capabilities:", c.PolicyEvaluation().Capabilities)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Stop(shutdown); err != nil {
		log.Println("stop:", err)
	}
}
```

## Callbacks

| Callback | Fires |
|---|---|
| `OnConnected(func(sb.Identity))` | Once per live session, after the runtime's Welcome — so again after every certificate rotation. |
| `OnReconnecting(func(attempt int, cause error))` | Before each attempt to rebuild a lost session. |
| `OnDraining(func(reason string))` | The runtime announced it is shutting down. |
| `OnDisconnected(func(cause error))` | Once, when the client stopped trying to reconnect. |
| `OnPolicyViolation(func(sb.PolicyViolation))` | Every declaration the policy refused, and every publish it refused. |

Callbacks run on the client's own goroutines and **must not block**.

## mTLS and rotation

The bootstrap key is `sb.` + base64url of a payload carrying a key id, a secret and the **CA certificate** — so the very first connection is already pinned, with no trust-on-first-use window.

On connect the SDK generates a P-256 key and a CSR and receives a leaf certificate with a SPIFFE identity. Renewal happens ahead of expiry — roughly 30 minutes before, with up to 5 minutes of random spread — over the live mTLS channel. Rotation yields a **new `InstanceID` under the same `ServiceID`**, and the new session is welcomed before the old one closes, so long-running instances do not drop traffic. A failed rotation swaps nothing and retries on the ladder.

The lease is cached **in memory only**. The single file the SDK writes anywhere is the outbox database.

## Telemetry

```go signature
func (d *TelemetryDomain) StartOp(ctx context.Context, name string, opts ...OpOption) (context.Context, *Operation)
func (d *TelemetryDomain) Logger() *slog.Logger
func (d *TelemetryDomain) Counter(name string, labels map[string]string) *Counter
func (d *TelemetryDomain) Gauge(name, unit string, labels map[string]string) *Gauge
func (d *TelemetryDomain) Histogram(name, unit string, labels map[string]string, bounds []float64) *Histogram
func WithOpPeer(serviceID string) OpOption
func WithOpBusinessKey(key string) OpOption
```

```go
package telemetry

import (
	"context"
	"log/slog"

	sb "github.com/service-bridge/sdk/go"
)

func Reprice(ctx context.Context, c *sb.Client, cartID string) error {
	// StartOp returns a context carrying the operation as the parent.
	// Pass THAT context down, or nested calls start their own trace root
	// and one request becomes two trees.
	ctx, op := c.Telemetry.StartOp(ctx, "reprice-cart", sb.WithOpBusinessKey(cartID))
	if err := reprice(ctx, cartID); err != nil {
		op.Fail(err)
		return err
	}
	op.End()

	c.Telemetry.Counter("carts_repriced_total", map[string]string{"tier": "gold"}).Inc()
	c.Telemetry.Gauge("queue_depth", "", nil).Set(42)
	c.Telemetry.Histogram("reprice_ms", "ms", nil, []float64{1, 5, 10, 50, 100}).Observe(12.5)

	// This logger writes ONLY into the telemetry buffer — not to stdout.
	// Put its handler into your own chain to get both.
	app := slog.New(c.Telemetry.Logger().Handler())
	app.Info("cart repriced", "cart", cartID)
	return nil
}

func reprice(ctx context.Context, cartID string) error { return nil }
```

Notes:

- `c.Telemetry.Logger()` is an ordinary `*slog.Logger` at level `Info`, and its handler writes **only** to the telemetry ring — nothing reaches stdout through it. `sb.WithLogger` is a different knob: it sets where the SDK writes its own diagnostics.
- Metric handles re-resolve on identity rotation, so a handle held for the process lifetime keeps reporting under the live instance.
- Anything recorded before `Start` waits in an in-memory ring and drains once connected.
- The client also samples two process gauges every 30 s.
- Request/response body capture is off unless the runtime enables it; a local setting can only narrow it.

## Errors

```go signature
type Error struct {
	Code Code
	Op   string
	Msg  string
	Err  error
}
```

`*sb.Error` is the only error type the SDK returns, so one `errors.As` is exhaustive. Sentinels match on `Code` alone.

| Code | Sentinel | Raised when |
|---|---|---|
| `CodeConfig` | `sb.ErrConfig` | A configuration the SDK refuses to run with. |
| `CodeState` | `sb.ErrState` | Wrong lifecycle phase: declaring after `Start`, publishing before it, using a stopped client. |
| `CodeConnection` | `sb.ErrConnection` | Provisioning, the session or a stream will not open. |
| `CodeAccessDenied` | `sb.ErrAccessDenied` | The access policy refused. |
| `CodeNotFound` | `sb.ErrNotFound` | A name the mesh has no definition for. |
| `CodeValidation` | `sb.ErrValidation` | A declaration or argument the runtime would reject, caught locally. |
| `CodeTerminal` | `sb.ErrTerminal` | A workflow run that already finished. |
| `CodeOutboxFull` | `sb.ErrOutboxFull` | Local event buffer at its cap. |
| `CodeNoLiveInstance` | `sb.ErrNoLiveInstance` | A call with nowhere to go. |
| `CodeInvalidEventName` | `sb.ErrInvalidEventName` | A name the event grammar rejects. |
| `CodeHandler` | `sb.ErrHandler` | The callee's handler returned a failure — an answer, not a transport fault. |
| `CodeInternal` | `sb.ErrInternal` | Everything else. |

`job`, `sbhttp` and `sbtest` carry their own sentinels for what they refuse locally, matched the same way.

## Units of time

Wire format is `int64` unix-ms for instants, `int64` ms for durations. Write `time.Duration` in Go; numeric fields spell their unit (`OccurredAtMs`, `ScheduledAtUnixMs`, `LeaseTTLMs`, `UnhealthySinceMs`, `InitialMs`, `MaxMs`).

Seconds appear only in `wf.Control.TimeoutSec`, `wf.Definition.TimeoutSec`, `wf.Sleep.DurationSec`, `wf.StartOpts.TimeoutSec` and `sb.WithRunTimeoutSec`.
