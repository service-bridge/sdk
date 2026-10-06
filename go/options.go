package servicebridge

import (
	"log/slog"
	"time"

	"github.com/service-bridge/sdk/go/internal/rpc"
	"github.com/service-bridge/sdk/go/internal/testkit"
)

// Transport selects the path an outbound call travels.
type Transport uint8

const (
	// TransportAuto dials the picked instance directly and falls back to the
	// runtime proxy for the next attempt when the direct path fails before
	// the request was sent. It is the default.
	TransportAuto Transport = iota
	// TransportDirect dials the callee instance over mTLS and never proxies.
	TransportDirect
	// TransportProxy always goes through the runtime, which resolves the
	// instance itself.
	TransportProxy
)

func (t Transport) internal() rpc.Transport {
	switch t {
	case TransportDirect:
		return rpc.TransportDirect
	case TransportProxy:
		return rpc.TransportProxy
	default:
		return rpc.TransportAuto
	}
}

// Defaults applied when the caller states no preference. They are named so a
// reader can see what a bare New gives them, and they match the other SDKs.
const (
	DefaultCallTimeout         = 30 * time.Second
	DefaultCallAttempts        = 3
	DefaultMaxPendingPublishes = 10_000
	DefaultPublishTimeout      = 30 * time.Second
	DefaultMaxInFlightEvents   = 32
	DefaultMaxConcurrentCalls  = 256
	DefaultMaxQueuedCalls      = 256
	DefaultStopTimeout         = 10 * time.Second
	// DefaultAdvertiseHost keeps a bare New usable on a laptop. It is announced
	// to the mesh as-is, so a cross-host deployment must pass WithAdvertise:
	// guessing an address from the environment is wrong more often than it is
	// right in a container.
	DefaultAdvertiseHost = "127.0.0.1"
)

const maxPort = 65535

// Option mutates the client configuration at construction time.
type Option func(*config)

type config struct {
	advertiseHost     string
	advertisePort     int
	advertiseExplicit bool
	callerOnly        bool

	callDefaults          callOptions
	callAttempts          int
	failOnPolicyViolation bool

	maxPendingPublishes int
	publishTimeout      time.Duration
	maxInFlightEvents   int

	maxConcurrentCalls int
	maxQueuedCalls     int

	reconnectAttempts int
	reconnectLadder   []time.Duration

	onTelemetryDrop func(TelemetryDrop)

	// memory is the sbtest harness's in-memory transport; nil in production.
	memory *testkit.Memory

	logger *slog.Logger
}

func defaultConfig() config {
	return config{
		advertiseHost:       DefaultAdvertiseHost,
		callDefaults:        callOptions{timeout: DefaultCallTimeout, transport: TransportAuto},
		callAttempts:        DefaultCallAttempts,
		maxPendingPublishes: DefaultMaxPendingPublishes,
		publishTimeout:      DefaultPublishTimeout,
		maxInFlightEvents:   DefaultMaxInFlightEvents,
		maxConcurrentCalls:  DefaultMaxConcurrentCalls,
		maxQueuedCalls:      DefaultMaxQueuedCalls,
		logger:              slog.Default(),
	}
}

// validate rejects every configuration the SDK cannot honour, before any I/O
// happens. A bound that is wrong must fail at New with CodeConfig rather than
// during the first connect, where it would look like a network condition and
// feed the reconnect ladder forever.
func (c *config) validate() error {
	const op = "servicebridge.New"
	switch {
	case c.callerOnly && c.advertiseExplicit:
		return configError(op, "WithCallerOnly and WithAdvertise contradict each other")
	case !c.callerOnly && c.advertiseHost == "":
		return configError(op, "advertise host must not be empty")
	case c.advertisePort < 0 || c.advertisePort > maxPort:
		return configError(op, "advertise port must be within 0..65535")
	case c.maxPendingPublishes <= 0:
		return configError(op, "pending publish cap must be positive")
	case c.publishTimeout <= 0:
		return configError(op, "publish timeout must be positive")
	case c.maxInFlightEvents <= 0:
		return configError(op, "in-flight event limit must be positive")
	case c.maxConcurrentCalls <= 0:
		return configError(op, "inbound call limit must be positive")
	case c.maxQueuedCalls < 0:
		return configError(op, "inbound queue limit must not be negative")
	case c.callAttempts <= 0:
		return configError(op, "call attempt budget must be at least one")
	case c.reconnectAttempts < 0:
		return configError(op, "reconnect attempt cap must not be negative")
	case c.callDefaults.timeout <= 0:
		return configError(op, "call timeout must be positive")
	case c.callDefaults.transport > TransportProxy:
		return configError(op, "unknown call transport")
	case c.logger == nil:
		return configError(op, "logger must not be nil")
	}
	for _, rung := range c.reconnectLadder {
		if rung <= 0 {
			return configError(op, "every reconnect ladder rung must be positive")
		}
	}
	return nil
}

// WithAdvertise sets the address peers dial for direct RPC. Port zero asks the
// operating system for a free one and announces what it hands back.
func WithAdvertise(host string, port int) Option {
	return func(c *config) {
		c.advertiseHost = host
		c.advertisePort = port
		c.advertiseExplicit = true
	}
}

// WithCallerOnly declares the instance outbound-only: no inbound listener is
// bound and no handler may be registered.
func WithCallerOnly() Option {
	return func(c *config) { c.callerOnly = true }
}

// WithCallDefaults applies call options to every outbound call that does not
// override them. It takes the same options Call takes, so there is one
// vocabulary rather than two.
func WithCallDefaults(opts ...CallOption) Option {
	return func(c *config) {
		for _, opt := range opts {
			opt(&c.callDefaults)
		}
	}
}

// WithCallAttempts caps the total tries of one logical call, counting the first
// one: three means one call and two retries.
func WithCallAttempts(n int) Option {
	return func(c *config) { c.callAttempts = n }
}

// WithFailOnPolicyViolation stops the client when the runtime reports a policy
// violation instead of only surfacing it. Off by default: the runtime registers
// what it can and warns about the rest, and a service that half-registers is
// usually better than a service that will not start.
func WithFailOnPolicyViolation() Option {
	return func(c *config) { c.failOnPolicyViolation = true }
}

// WithMaxPendingPublishes caps the in-memory publish queue. At the cap a
// publish fails at once with CodeQueueFull.
func WithMaxPendingPublishes(n int) Option {
	return func(c *config) { c.maxPendingPublishes = n }
}

// WithPublishTimeout bounds one publication from enqueue to the runtime's
// acknowledgement. Past it the publish fails with CodeTimeout, saying whether
// the event was ever sent.
func WithPublishTimeout(d time.Duration) Option {
	return func(c *config) { c.publishTimeout = d }
}

// WithMaxInFlightEvents caps concurrently processed inbound deliveries. At the
// cap the delivery stream stops being read, which is what the runtime feels as
// backpressure.
func WithMaxInFlightEvents(n int) Option {
	return func(c *config) { c.maxInFlightEvents = n }
}

// WithInboundLimits bounds inbound RPC: handlers running at once across every
// connection, and calls waiting for a free handler. Past both bounds a caller
// gets RESOURCE_EXHAUSTED with the not-dispatched proof, so it retries on
// another instance. HTTP/2 streams per connection are capped at the sum.
func WithInboundLimits(maxCalls, maxQueued int) Option {
	return func(c *config) {
		c.maxConcurrentCalls = maxCalls
		c.maxQueuedCalls = maxQueued
	}
}

// TelemetryDrop reports telemetry lost since the previous report: dropped by
// the runtime under load, or evicted from the local buffer.
type TelemetryDrop struct {
	ServerDrops       uint64
	RingDrops         uint64
	BackpressureLevel uint32
}

// WithTelemetryDropHandler is called whenever telemetry was lost. The same
// counts reach the runtime as the metric sb_sdk_telemetry_dropped_total.
func WithTelemetryDropHandler(fn func(TelemetryDrop)) Option {
	return func(c *config) { c.onTelemetryDrop = fn }
}

// WithReconnectAttempts caps consecutive reconnect attempts. Zero, the default,
// means the client keeps trying: a service that gives up mid rolling restart is
// a service that needs a human.
func WithReconnectAttempts(n int) Option {
	return func(c *config) { c.reconnectAttempts = n }
}

// WithReconnectLadder replaces the reconnect delay ladder. The last rung
// repeats forever, and every rung is jittered.
func WithReconnectLadder(rungs ...time.Duration) Option {
	return func(c *config) { c.reconnectLadder = rungs }
}

// WithLogger sets the structured logger the client and every component under it
// write to.
func WithLogger(log *slog.Logger) Option {
	return func(c *config) { c.logger = log }
}

// CallOption tunes one outbound call.
type CallOption func(*callOptions)

type callOptions struct {
	timeout        time.Duration
	transport      Transport
	idempotencyKey string
	businessKey    string
}

// WithTimeout bounds one call. The default is DefaultCallTimeout; the caller's
// ctx deadline still applies when it is earlier.
func WithTimeout(d time.Duration) CallOption {
	return func(o *callOptions) { o.timeout = d }
}

// WithTransport picks the dispatch path for one call.
func WithTransport(t Transport) CallOption {
	return func(o *callOptions) { o.transport = t }
}

// WithIdempotencyKey supplies the caller's own deduplication key; the callee
// reads it from CallInfo. It does not widen what the SDK retries — only a
// proven pre-dispatch failure is retried — and the SDK never invents one.
func WithIdempotencyKey(key string) CallOption {
	return func(o *callOptions) { o.idempotencyKey = key }
}

// WithBusinessKey labels the call in the trace view with a domain identifier —
// an order id, a customer id — so an operator can find it without an op id.
func WithBusinessKey(key string) CallOption {
	return func(o *callOptions) { o.businessKey = key }
}

func (c *config) callOptions(opts []CallOption) callOptions {
	o := c.callDefaults
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
