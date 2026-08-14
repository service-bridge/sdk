// Package job is the public surface for declaring scheduled jobs: when they
// fire, how they behave under failure and what runs when they do.
package job

import (
	"time"

	internaljob "github.com/service-bridge/sdk/go/internal/job"
)

// Trigger says when a job fires. There is no exported way to build one but the
// three constructors below, so a job carries exactly one trigger by
// construction — a struct with three optional fields would allow zero or three.
type Trigger = internaljob.Trigger

// Cron fires the job on a five-field cron expression — minute, hour, day of
// month, month, day of week. Seconds are not a field. tz is an IANA location
// name; empty leaves the choice to the runtime.
//
// The expression is parsed here, by the same parser the runtime registers with,
// so a typo fails at the declaration instead of never firing.
func Cron(expr, tz string) (Trigger, error) {
	return internaljob.NewCronTrigger(expr, tz)
}

// Interval fires the job every d. The runtime enforces the minimum interval.
func Interval(d time.Duration) (Trigger, error) {
	return internaljob.NewIntervalTrigger(d)
}

// At fires the job once, at t.
func At(t time.Time) (Trigger, error) {
	return internaljob.NewAtTrigger(t)
}
