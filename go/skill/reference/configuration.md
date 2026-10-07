# Configuration — Go SDK reference

Constructor options, lifecycle, errors and telemetry.

## Constructor

```go signature
func New(url, key string, opts ...Option) (*Client, error)
func (c *Client) Start(ctx context.Context) error
func (c *Client) Ready(ctx context.Context) error
func (c *Client) Stop(ctx context.Context) error
```

`sb.New` performs **no I/O**. Every bad bound is reported here with `CodeConfig`, before anything connects — a misconfigured limit must never look like a network condition and feed the reconnect ladder.

`url` is `host:port`; a leading scheme such as `https://` and a trailing `/` are stripped. `key` is the `sb.…` bootstrap key from the dashboard; it carries the CA certificate, so the SDK trusts exactly one root and nothing from the system store.

**The SDK reads no environment variables.** Read them yourself and pass the values in.

## Options and real defaults

```go signature
func WithAdvertise(host string, port int) Option
func WithCallerOnly() Option
func WithCallDefaults(opts ...CallOption) Option
func WithCallAttempts(n int) Option
func WithFailOnPolicyViolation() Option
func WithMaxPendingPublishes(n int) Option
func WithPublishTimeout(d time.Duration) Option
func WithMaxInFlightEvents(n int) Option
func WithInboundLimits(maxCalls, maxQueued int) Option
func WithReconnectAttempts(n int) Option
func WithReconnectLadder(rungs ...time.Duration) Option
func WithTelemetryDropHandler(fn func(TelemetryDrop)) Option
func WithLogger(log *slog.Logger) Option

type TelemetryDrop struct {
	ServerDrops       uint64 // dropped by the runtime under load
	RingDrops         uint64 // evicted from the local buffer
	BackpressureLevel uint32
}
```

| Option | Default | Effect |
|---|---|---|
| `WithAdvertise(host, port)` | `127.0.0.1`, port `0` | Address peers dial for direct RPC. Port `0` asks the OS for a free one and announces what it hands back. Advertised **as-is** — pass a real address in a container. |
| `WithCallerOnly()` | off | Outbound-only: no inbound listener, handler registration refused. Contradicts `WithAdvertise`. |
| `WithCallDefaults(opts...)` | timeout `30s`, `TransportAuto` | `CallOption`s applied under every call path that does not override them. |
| `WithCallAttempts(n)` | `3` | **Total** tries of one logical call, counting the first. Only proven pre-dispatch failures are retried. |
| `WithFailOnPolicyViolation()` | off | Stop the client on a policy violation instead of only reporting it. |
| `WithMaxPendingPublishes(n)` | `10000` | Events waiting for the runtime's acknowledgement; past it `PublishEvent` fails at once with `CodeQueueFull`. |
| `WithPublishTimeout(d)` | `30s` | How long one publication may wait from enqueue to acknowledgement before `CodeTimeout`. |
| `WithMaxInFlightEvents(n)` | `32` | Concurrent inbound deliveries. At the cap the delivery stream stops being read — real backpressure. |
| `WithInboundLimits(calls, queued)` | `256` / `256` | Handlers running at once, and calls waiting for one. Past both, callers get `RESOURCE_EXHAUSTED` with the not-dispatched proof and retry elsewhere. HTTP/2 streams per connection are capped at the sum. |
| `WithReconnectAttempts(n)` | `0` — unlimited | Cap on consecutive failed reconnects; the count resets on every successful session. |
| `WithReconnectLadder(rungs...)` | `1s, 5s, 15s, 30s, 60s` | Reconnect delays; the last rung repeats forever, every rung jittered ±20 %. |
| `WithTelemetryDropHandler(fn)` | none | Called with the telemetry lost since the previous report. The same counts reach the runtime as `sb_sdk_telemetry_dropped_total{source}`. |
| `WithLogger(log)` | `slog.Default()` | Where the SDK writes its **own** diagnostics. |

Exported constants: `sb.DefaultCallTimeout` (30 s), `sb.DefaultCallAttempts` (3), `sb.DefaultMaxPendingPublishes` (10000), `sb.DefaultPublishTimeout` (30 s), `sb.DefaultMaxInFlightEvents` (32), `sb.DefaultMaxConcurrentCalls` (256), `sb.DefaultMaxQueuedCalls` (256), `sb.DefaultStopTimeout` (10 s), `sb.DefaultAdvertiseHost` (`127.0.0.1`).

Rejected at `New` with `CodeConfig`: empty runtime address; unparseable key; `WithCallerOnly` together with `WithAdvertise`; empty advertise host when not caller-only; port outside `0..65535`; a non-positive pending-publish cap, publish timeout, in-flight limit, inbound call limit or call-attempt budget; a negative inbound queue; a negative reconnect cap; a non-positive default call timeout; an unknown transport; a nil logger; a non-positive ladder rung.

## Lifecycle

`Start` checks that every workflow call step names a declared dependency, seals the declarations, declares the event subscriptions with their filters, provisions the mTLS identity (or reuses the cached one), binds the inbound server, opens the control session, waits for the runtime's `Welcome`, registers, waits for the first registry snapshot, then starts the publisher, telemetry and subscriptions. It has 30 s for all of it; on failure it stops what it started and returns the error. It returns once the instance is registered and routable.

`c.Ready(ctx)` blocks while the client is reconnecting and returns once the current session is live and has applied a snapshot. Before `Start` or after `Stop` it is `CodeState`.

**The client outlives the context passed to `Start`**: that context's cancellation is dropped and its values kept, so a request-scoped context cannot take the client down. `Stop` is the way down.

Reconnects follow the ladder with no attempt cap by default; the count resets on every `Welcome`. The client ends for good (`OnDisconnected`) only on refusals a retry cannot fix: rejected credentials, an unknown service, a refused registration (for example an invalid subscription filter), or a protocol version the runtime does not speak. A runtime `Drain` is routine: `OnDraining` fires and the client reconnects once the runtime closes the session.

`Stop` shuts down in the order that loses nothing it can keep:

1. drain — the inbound server refuses new calls with `UNAVAILABLE` and the not-dispatched proof, the instance re-registers without a call endpoint so peers stop routing to it, event and job subscribers stop taking new work;
2. wait for in-flight calls, event handlers and job handlers;
3. flush the publish queue — what is left fails with `CodeConnection` "client stopped";
4. flush telemetry and wait up to 2 s for its last acknowledgement;
5. close the session, the registry stream, the data channels and the server.

Steps 2 and 3 share `ctx`'s deadline, `sb.DefaultStopTimeout` (10 s) when it has none. Past the deadline `Stop` cancels the handlers' contexts and returns `CodeTimeout`. `Stop` is idempotent and reports every failure without skipping the rest.

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
		sb.WithMaxPendingPublishes(50_000),
		sb.WithPublishTimeout(10*time.Second),
		sb.WithInboundLimits(256, 256),
		sb.WithReconnectLadder(time.Second, 5*time.Second, 30*time.Second),
		sb.WithFailOnPolicyViolation(),
		sb.WithTelemetryDropHandler(func(d sb.TelemetryDrop) {
			log.Printf("telemetry lost: server=%d ring=%d", d.ServerDrops, d.RingDrops)
		}),
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

	// Identity is read per use, not cached: the InstanceID survives
	// certificate renewal but changes if an expired leaf is provisioned again.
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
| `OnConnected(func(sb.Identity))` | Once per live session, after the runtime's `Welcome`. Certificate renewal does not rebuild the session and does not fire it. |
| `OnReconnecting(func(attempt int, cause error))` | Before each attempt to rebuild a lost session. |
| `OnDraining(func(reason string))` | The runtime announced it is shutting down; the client reconnects by itself once the runtime closes the session. |
| `OnDisconnected(func(cause error))` | Once, when the client stopped trying to reconnect. |
| `OnPolicyViolation(func(sb.PolicyViolation))` | Every declaration the policy refused, and every publish it refused. |

Callbacks run on the client's own goroutines and **must not block**. A panic in one is recovered and logged; the others still run.

## mTLS and rotation

The bootstrap key is `sb.` + base64url of a payload carrying a key id, a secret and the **CA certificate** — so the very first connection is already pinned, with no trust-on-first-use window.

On connect the SDK generates a P-256 key and a CSR and receives a leaf certificate with a SPIFFE identity. Renewal happens in place ahead of expiry — 30 minutes before, with up to 5 minutes of random spread — over the live control session. The `InstanceID` does not change, and no session, stream, channel or server is rebuilt: new connections present the new leaf, live ones keep working. A failed renewal swaps nothing and retries in 60 s. Only a leaf that expired is provisioned again, which yields a new `InstanceID`.

The lease is cached **in memory only**. The SDK writes no files.

A revoked service or instance takes effect at once: peers stop picking it and close their channels to it, and its inbound calls get `PERMISSION_DENIED`.

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
- Anything recorded before `Start` waits in an in-memory ring and drains once connected; at its cap the oldest entry is evicted and counted as a ring drop.
- The client also samples two process gauges every 30 s (`process.cpu_percent`, `process.rss_bytes`).
- The runtime pushes the body-capture mode (`none`, `errors`, `all`); its default is `errors`. Nothing is captured before the first snapshot. The SDK does not mask bodies — the runtime masks payloads on ingest.

## Errors

```go signature
type Error struct {
	Code Code
	Op   string
	Msg  string
	Err  error
}

func (e *Error) Retryable() bool
```

`*sb.Error` is the only error type the SDK returns, so one `errors.As` is exhaustive. Sentinels match on `Code` alone.

| Code | Sentinel | Raised when | `Retryable()` |
|---|---|---|---|
| `CodeConfig` | `sb.ErrConfig` | A configuration the SDK refuses to run with, or a runtime speaking another protocol version. | no |
| `CodeState` | `sb.ErrState` | Wrong lifecycle phase: declaring after `Start`, publishing before it, using a stopped client. | no |
| `CodeConnection` | `sb.ErrConnection` | The runtime or the callee could not be reached (`UNAVAILABLE`), a channel never became ready, a publish left over at `Stop`. | yes |
| `CodeTimeout` | `sb.ErrTimeout` | A deadline passed with the outcome unknown. Repeat only with an idempotency key. | no |
| `CodeCancelled` | `sb.ErrCancelled` | The caller cancelled. | no |
| `CodeAccessDenied` | `sb.ErrAccessDenied` | The access policy, a revoked identity or rejected credentials. | no |
| `CodeNotFound` | `sb.ErrNotFound` | A name the mesh has no definition for. | no |
| `CodeValidation` | `sb.ErrValidation` | A declaration or argument that is refused. | no |
| `CodeConflict` | `sb.ErrConflict` | An id already used with other content. | no |
| `CodeTerminal` | `sb.ErrTerminal` | A workflow run that already finished. | no |
| `CodeNoLiveInstance` | `sb.ErrNoLiveInstance` | A call with nowhere to go. | yes |
| `CodeOverloaded` | `sb.ErrOverloaded` | The callee or the runtime is shedding load. | yes |
| `CodeQueueFull` | `sb.ErrQueueFull` | The publish queue is at its cap. | yes |
| `CodeInvalidEventName` | `sb.ErrInvalidEventName` | A name the event grammar rejects. | no |
| `CodeHandler` | `sb.ErrHandler` | The callee's handler answered with a failure; `errors.As` reaches its `*sb.HandlerError`. | no |
| `CodeInternal` | `sb.ErrInternal` | Everything else. | no |

`(*sb.Error).Retryable()` is true exactly for the rows marked yes. A gRPC status from the far side maps as: `CANCELLED`→`CANCELLED`; `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`→`VALIDATION`; `DEADLINE_EXCEEDED`→`TIMEOUT`; `NOT_FOUND`, `UNIMPLEMENTED`→`NOT_FOUND`; `ALREADY_EXISTS`→`CONFLICT`; `PERMISSION_DENIED`, `UNAUTHENTICATED`→`ACCESS_DENIED`; `RESOURCE_EXHAUSTED`→`OVERLOADED`; `UNAVAILABLE`→`CONNECTION`; `UNKNOWN`, `ABORTED`, `INTERNAL`, `DATA_LOSS`→`INTERNAL`.

`job`, `sbhttp` and `sbtest` carry their own sentinels for what they refuse locally, matched the same way.

## Units of time

Wire format is `int64` unix-ms for instants, `int64` ms for durations. Write `time.Duration` in Go; numeric fields spell their unit (`OccurredAtMs`, `ScheduledAtUnixMs`, `LeaseTTLMs`, `UnhealthySinceMs`, `InitialMs`, `MaxMs`).

Seconds appear only in `wf.Control.TimeoutSec`, `wf.Definition.TimeoutSec`, `wf.Sleep.DurationSec`, `wf.StartOpts.TimeoutSec` and `sb.WithRunTimeoutSec`.
