# Jobs — Go SDK reference

Cron, interval and one-shot scheduled work. The runtime owns the schedule, the lease and the retries.

## Signatures

```go signature
func (d *JobDomain) Handle(name string, spec job.Spec, fn job.Handler) error

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

Import: `github.com/service-bridge/sdk/go/job`.

## Triggers

A `job.Trigger` can only come from one of three constructors, so a job carries exactly one by construction.

| Constructor | Notes |
|---|---|
| `job.Cron(expr, tz)` | **Five fields** — minute, hour, day-of-month, month, day-of-week. Seconds are not a field, and `@daily`-style descriptors are not supported. `tz` is an IANA name; empty leaves the choice to the runtime. Parsed at declaration by the same parser the runtime registers with, so a typo fails where you wrote it instead of never firing. |
| `job.Interval(d)` | Whole milliseconds on the wire; sub-millisecond is refused. The runtime enforces the minimum. |
| `job.At(t)` | Fires once. A zero time is refused. |

## Options

| Option | Default |
|---|---|
| `job.WithCatchup(p)` | runtime decides |
| `job.WithOverlap(p)` | runtime decides |
| `job.WithDeps(deps...)` | none |
| `job.WithMaxAttempts(n)` | runtime decides |
| `job.WithLeaseTTL(d)` | runtime decides |
| `job.WithMaxConcurrent(n)` | runtime decides |
| `job.WithRetry(job.RetryPolicy{InitialMs, MaxMs, Multiplier, Jitter})` | runtime decides |

**The SDK keeps no copy of the defaults.** An option never applied is simply absent from the spec and the runtime fills it in. Do not hard-code a "default" value you read somewhere.

Policies: `job.CatchupSkip` / `CatchupFireOnce` / `CatchupFireAll`; `job.OverlapSkip` / `OverlapAllow` / `OverlapBufferOne`.

Deps: `job.RPC("service.Method")`, `job.Event("name")`, `job.Workflow("name")` — drawn on the service map, checked against the access policy.

## The handler contract

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
```

Jobs carry **no input and no output**. The only outcome is an error or its absence.

**Be idempotent by `IdempotencyKey`, never by `Attempt`.** The key is the same across every attempt of one scheduled fire; `Attempt` changes on each retry, so keying on it makes every retry look like new work.

Wrap a failure in `job.ErrPermanent` to stop the runtime from spending the remaining attempts on it. The original error text still travels.

`ctx` is cancelled when the client stops.

## Complete program

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	sb "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/job"
)

var errPoisonedFeed = errors.New("feed is malformed")

func main() {
	c, err := sb.New("localhost:14445", os.Getenv("BILLING_KEY"),
		sb.WithAdvertise(os.Getenv("POD_IP"), 50051))
	if err != nil {
		log.Fatal(err)
	}

	// Cron: five fields, no seconds. Parsed here, so a typo fails now.
	nightly, err := job.Cron("0 3 * * *", "UTC")
	if err != nil {
		log.Fatal(err)
	}
	if err := c.Job.Handle("nightly-rollup",
		job.NewSpec(nightly,
			job.WithCatchup(job.CatchupFireOnce), // one fire for the whole gap
			job.WithOverlap(job.OverlapSkip),     // never two at once
			job.WithMaxAttempts(5),
			job.WithLeaseTTL(5*time.Minute),
			job.WithDeps(
				job.RPC("billing-svc.Rollup"),
				job.Event("billing.rolled_up"),
			),
		),
		func(ctx context.Context, exec job.Execution) error {
			// Idempotent by the key, NOT by exec.Attempt.
			fresh, err := insertIfAbsent(ctx, "rollup:"+exec.IdempotencyKey)
			if err != nil {
				return err
			}
			if !fresh {
				return nil // this scheduled fire already ran
			}
			if err := rollup(ctx, exec.IdempotencyKey); err != nil {
				if errors.Is(err, errPoisonedFeed) {
					// Do not burn the remaining attempts on bad input.
					return fmt.Errorf("%w: %w", job.ErrPermanent, err)
				}
				return err // retried by the runtime's backoff
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// Interval.
	beat, err := job.Interval(30 * time.Second)
	if err != nil {
		log.Fatal(err)
	}
	if err := c.Job.Handle("heartbeat", job.NewSpec(beat, job.WithMaxConcurrent(1)),
		func(ctx context.Context, exec job.Execution) error {
			log.Printf("job=%s exec=%s attempt=%d scheduled=%d",
				exec.Name, exec.ID, exec.Attempt, exec.ScheduledAtUnixMs)
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	// One shot.
	once, err := job.At(time.Now().Add(2 * time.Hour))
	if err != nil {
		log.Fatal(err)
	}
	if err := c.Job.Handle("migrate-v2", job.NewSpec(once),
		func(ctx context.Context, exec job.Execution) error {
			return migrate(ctx, exec.IdempotencyKey)
		}); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Stop(ctx) }()

	select {}
}

func insertIfAbsent(ctx context.Context, key string) (bool, error) { return true, nil }
func rollup(ctx context.Context, key string) error                { return nil }
func migrate(ctx context.Context, key string) error               { return nil }
```

## Multi-instance behaviour

Declare the job on every instance — that is the normal mode. The runtime hands one execution to **one** instance under a lease; the rest get nothing. If the holder goes silent past the lease TTL, the execution is reassigned, which is exactly why delivery is at-least-once and the handler must be idempotent. A result sent by a stale lease holder is dropped — that is the fencing.

The client heartbeats the runtime every 5 seconds; three consecutive failures reopen the execution subscription.

## Declaration errors

All checked at declaration, all matched with `errors.Is`, all wrapped in `*sb.Error` with `CodeValidation`:

`job.ErrNoTrigger`, `ErrCronFieldCount`, `ErrCronExpr`, `ErrCronTZ`, `ErrInterval`, `ErrRunAt`, `ErrCatchupPolicy`, `ErrOverlapPolicy`, `ErrDepKind`, `ErrDepTarget`, `ErrRetryInitial`, `ErrNegativeLimit`, `ErrEmptyName`, `ErrNoHandler`, `ErrDuplicateName`.

## Gotchas

- Six-field cron (with seconds) → `job.ErrCronFieldCount`. Use five.
- `c.Job.Handle` after `Start` → `CodeState`.
- Do not invent default values for the runtime-owned options; leave them unset.
- `job.Execution` has no payload — a job that needs input should read it from your own store.
