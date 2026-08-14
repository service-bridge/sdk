// Package servicebridge is the Go SDK for the ServiceBridge runtime.
//
// Typed operations are free functions taking the client first — Go has no
// generic methods, so Call, Stream, Handle, PublishEvent and their siblings
// cannot be methods without losing their type parameters. Everything that needs
// no type parameter stays a method on the domain it belongs to.
package servicebridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/service-bridge/sdk/go/internal/connection"
	"github.com/service-bridge/sdk/go/internal/events"
	jobi "github.com/service-bridge/sdk/go/internal/job"
	"github.com/service-bridge/sdk/go/internal/outbox"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/registry"
	"github.com/service-bridge/sdk/go/internal/rpc"
	"github.com/service-bridge/sdk/go/internal/serde"
	"github.com/service-bridge/sdk/go/internal/stream"
	"github.com/service-bridge/sdk/go/internal/telemetry"
	wfi "github.com/service-bridge/sdk/go/internal/workflow"
	wf "github.com/service-bridge/sdk/go/workflow"
)

// Client owns every resource the SDK holds: the control-plane session, the
// inbound Call server, one mTLS channel per data-plane domain and the local
// outbox. Stop releases them all.
type Client struct {
	cfg config
	log *slog.Logger
	key connection.BootstrapKey

	// Declarations everything registers into before Start. After Start the set
	// is sealed: a handler added later exists locally and nowhere else, and the
	// mesh would never route to it.
	decls    *registry.Declarations
	dispatch *rpc.Dispatcher
	jobDecls *jobi.Declarations
	// callSchemas is the caller half of the same declarations: which pair of
	// message types each named dependency is bound to. It never reaches the
	// wire, and it is what lets a workflow call step — which holds a JSON tree
	// and a method name — encode for a typed handler and route to it.
	callSchemas *registry.CallSchemas

	graphMu sync.RWMutex
	graphs  map[string][]wf.Step

	// Telemetry core. Built first because every other component records
	// through it.
	ring     *telemetry.Ring
	policy   *telemetry.Policy
	recorder *telemetry.Recorder
	metrics  *telemetry.Metrics
	tport    *telemetry.Transport
	sampler  *telemetry.Sampler
	logs     *slog.Logger

	// Connection and the live mesh view.
	creds *connection.CredentialRegistry
	life  *connection.Lifecycle
	watch *registry.Watch
	// session is the channel of the live control session. The registry stream
	// belongs to that session, so it is handed the channel directly instead of
	// asking the lifecycle, which has not adopted the session yet.
	session      atomic.Value
	watchStarted atomic.Bool

	// One mTLS channel per data-plane domain. Each is a credential consumer:
	// a rotation that reached only some of them leaves the rest talking over a
	// certificate about to expire.
	eventsCh   *channel
	jobsCh     *channel
	workflowCh *channel
	telemCh    *channel
	invokeCh   *channel

	// RPC, both directions.
	server   *rpc.Server
	direct   *rpc.Direct
	proxy    *rpc.Proxy
	balancer *rpc.Balancer
	breaker  *rpc.Breaker
	callers  map[Transport]*rpc.Client

	// Events.
	buffer    *outbox.Storage
	publisher *events.Publisher
	drainer   *events.Drainer
	eventSub  *events.Subscriber

	// Jobs and workflows.
	jobSub *jobi.Subscriber
	wfSub  *wfi.Subscriber
	wfCall *wfi.Caller

	// Job declares scheduled work. Workflow declares and steers runs.
	// Telemetry opens operations, metrics and the log bridge.
	Job       *JobDomain
	Workflow  *WorkflowDomain
	Telemetry *TelemetryDomain

	obsMu       sync.RWMutex
	onConnected []func(Identity)
	onReconnect []func(attempt int, cause error)
	onDrain     []func(reason string)
	onDisconn   []func(cause error)
	onViolation []func(PolicyViolation)

	lifeMu  sync.Mutex
	runCtx  context.Context
	cancel  context.CancelFunc
	started bool
	stopped bool
}

// New builds a client for the runtime at url, authenticating with the service
// bootstrap key. It performs no I/O: everything wrong with the configuration is
// reported here, with CodeConfig, so a misconfigured bound can never be
// mistaken for a network condition and fed to the reconnect ladder.
func New(url, key string, opts ...Option) (*Client, error) {
	const op = "servicebridge.New"
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	addr := runtimeAddr(url)
	if addr == "" {
		return nil, configError(op, "runtime address must not be empty")
	}
	bootstrap, err := connection.ParseBootstrapKey(key)
	if err != nil {
		return nil, newError(CodeConfig, op, "bootstrap key is not usable", err)
	}

	c := &Client{
		cfg:         cfg,
		log:         cfg.logger,
		key:         bootstrap,
		decls:       registry.NewDeclarations(),
		dispatch:    rpc.NewDispatcher(cfg.logger),
		jobDecls:    jobi.NewDeclarations(),
		callSchemas: registry.NewCallSchemas(),
		graphs:      map[string][]wf.Step{},
		creds:       connection.NewCredentialRegistry(),
		callers:     map[Transport]*rpc.Client{},
	}

	c.buildTelemetry()
	c.buildChannels()
	if err := c.buildConnection(addr); err != nil {
		return nil, wrap(op, err)
	}
	if err := c.buildRPC(); err != nil {
		return nil, wrap(op, err)
	}
	if err := c.buildDomains(); err != nil {
		return nil, wrap(op, err)
	}
	return c, nil
}

// runtimeAddr accepts both the bare host:port the runtime listens on and a
// URL-shaped form, because both appear in deployment configuration.
func runtimeAddr(url string) string {
	addr := strings.TrimSpace(url)
	if i := strings.Index(addr, "://"); i >= 0 {
		addr = addr[i+3:]
	}
	return strings.TrimSuffix(addr, "/")
}

func (c *Client) buildTelemetry() {
	c.ring = telemetry.NewRing(telemetry.DefaultBudgets())
	c.policy = telemetry.NewPolicy()
	c.recorder = telemetry.NewRecorder(c.ring, c.policy)
	c.metrics = telemetry.NewMetrics(c.ring)
	c.logs = slog.New(telemetry.NewHandler(c.ring, telemetry.HandlerOptions{
		InstanceID: c.instanceID,
	}))
}

func (c *Client) buildChannels() {
	c.eventsCh = newChannel("events")
	c.jobsCh = newChannel("jobs")
	c.workflowCh = newChannel("workflows")
	c.telemCh = newChannel("telemetry")
	c.invokeCh = newChannel("rpc.proxy")
}

func (c *Client) buildConnection(addr string) error {
	watch, err := registry.NewWatch(registry.WatchConfig{
		Clients:          c,
		Request:          c.decls.BuildRegisterRequest,
		OnChange:         c.onRegistryChange,
		OnPolicyWarnings: c.onPolicyWarnings,
		OnError:          func(err error) { c.log.Warn("registry stream failed", "error", err) },
		Backoff:          c.backoff(),
		Logger:           c.log,
	})
	if err != nil {
		return err
	}
	c.watch = watch

	if !c.cfg.callerOnly {
		server, err := rpc.NewServer(rpc.ServerConfig{
			Host: c.cfg.advertiseHost,
			Port: c.cfg.advertisePort,
			Limits: rpc.ServerLimits{
				MaxConcurrentCalls:   c.cfg.maxConcurrentCalls,
				MaxConcurrentStreams: c.cfg.maxConcurrentStreams,
			},
			Dispatcher: c.dispatch,
			Policies:   watch.Cache(),
			Logger:     c.log,
		})
		if err != nil {
			return err
		}
		c.server = server
	}

	inbound := connection.InboundServer(nil)
	if c.server != nil {
		inbound = c.server
	}
	life, err := connection.NewLifecycle(connection.LifecycleConfig{
		Addr:        addr,
		CACert:      c.key.CACert,
		Provisioner: connection.BootstrapProvisioner{Addr: addr, Key: c.key},
		Credentials: c.creds,
		Inbound:     inbound,
		Registrars:  c,
		Observer:    (*observer)(c),
		Backoff:     c.backoff(),
		MaxAttempts: c.cfg.reconnectAttempts,
		Logger:      c.log,
	})
	if err != nil {
		return err
	}
	c.life = life
	return nil
}

func (c *Client) backoff() stream.Backoff {
	if len(c.cfg.reconnectLadder) == 0 {
		return stream.NewBackoff()
	}
	return stream.NewBackoff(stream.WithLadder(c.cfg.reconnectLadder...))
}

func (c *Client) buildRPC() error {
	c.direct = rpc.NewDirect(rpc.DirectConfig{Logger: c.log})
	proxy, err := rpc.NewProxy(c.invokeCh)
	if err != nil {
		return err
	}
	c.proxy = proxy
	c.balancer = rpc.NewBalancer()
	c.breaker = rpc.NewBreaker(rpc.DefaultBreakerConfig())

	retry := rpc.DefaultRetryPolicy()
	retry.MaxAttempts = c.cfg.callAttempts
	for _, t := range []Transport{TransportDirect, TransportProxy} {
		caller, err := rpc.NewClient(rpc.ClientConfig{
			Registry:  c.watch.Cache(),
			Direct:    c.direct,
			Proxy:     c.proxy,
			Balancer:  c.balancer,
			Breaker:   c.breaker,
			Recorder:  c.recorder,
			Retry:     retry,
			Transport: t.internal(),
			Logger:    c.log,
		})
		if err != nil {
			return err
		}
		c.callers[t] = caller
	}
	return nil
}

func (c *Client) buildDomains() error {
	sub, err := events.NewSubscriber(events.SubscriberConfig{
		Open:        c.openEventStream,
		Codec:       codec{},
		Identity:    c.eventsIdentity,
		MaxInFlight: c.cfg.maxInFlightEvents,
		Backoff:     c.backoff(),
		OnError:     func(err error) { c.log.Warn("event stream failed", "error", err) },
		Logger:      c.log,
	})
	if err != nil {
		return err
	}
	c.eventSub = sub

	jobSub, err := jobi.NewSubscriber(jobi.SubscriberConfig{
		Clients:  c.jobsCh,
		Identity: c.jobIdentity,
		Jobs:     c.jobDecls,
		Backoff:  c.backoff(),
		OnError:  func(err error) { c.log.Warn("job stream failed", "error", err) },
		Logger:   c.log,
	})
	if err != nil {
		return err
	}
	c.jobSub = jobSub

	caller, err := wfi.NewCaller(wfi.CallerConfig{Clients: c.workflowCh})
	if err != nil {
		return err
	}
	c.wfCall = caller

	checkpoints, err := wfi.NewCheckpoints(wfi.CheckpointConfig{
		Clients:  c.workflowCh,
		Identity: c.workflowIdentity,
	})
	if err != nil {
		return err
	}
	runner, err := wfi.NewRunner(wfi.RunnerConfig{
		Ops:      checkpoints,
		Executor: (*executor)(c),
		WrapStep: c.wrapStep,
		Logger:   c.log,
	})
	if err != nil {
		return err
	}
	wfSub, err := wfi.NewSubscriber(wfi.SubscriberConfig{
		Clients:  c.workflowCh,
		Identity: c.workflowIdentity,
		Graphs:   c,
		Runner:   runner,
		Ops:      checkpoints,
		Backoff:  c.backoff(),
		OnError:  func(err error) { c.log.Warn("workflow stream failed", "error", err) },
		Logger:   c.log,
	})
	if err != nil {
		return err
	}
	c.wfSub = wfSub

	tport, err := telemetry.NewTransport(telemetry.TransportConfig{
		Open:    c.openTelemetryStream,
		Ring:    c.ring,
		Metrics: c.metrics,
		Backoff: c.backoff(),
		OnError: func(err error) { c.log.Warn("telemetry stream failed", "error", err) },
		Logger:  c.log,
	})
	if err != nil {
		return err
	}
	c.tport = tport

	sampler, err := telemetry.NewSampler(telemetry.SamplerConfig{
		Metrics:    c.metrics,
		InstanceID: c.instanceID,
		Logger:     c.log,
	})
	if err != nil {
		return err
	}
	c.sampler = sampler

	c.Job = &JobDomain{c: c}
	c.Workflow = &WorkflowDomain{c: c}
	c.Telemetry = &TelemetryDomain{c: c}
	return nil
}

// Start brings the instance up in the one order that works:
//
//  0. check that every workflow call step names a declared dependency, while
//     the declarations are still the reader's to fix
//  1. seal the declarations — after this the mesh has been told what exists
//  2. open the local outbox, so a publish has somewhere durable to land
//  3. register every credential consumer, so the first lease reaches all of them
//  4. hand the lifecycle control: it provisions (or reuses the cached lease),
//     binds the inbound Call server, opens Control.Open and only then starts
//     the registry stream — the listener's address has to be inside the very
//     first RegisterRequest or the mesh dials an endpoint nobody listens on
//  5. wait for the first snapshot, which is the confirmation of registration
//  6. start the subscriptions that need a registered identity
func (c *Client) Start(ctx context.Context) error {
	const op = "Client.Start"
	c.lifeMu.Lock()
	switch {
	case c.stopped:
		c.lifeMu.Unlock()
		return newError(CodeState, op, "client has been stopped", nil)
	case c.started:
		c.lifeMu.Unlock()
		return newError(CodeState, op, "client is already started", nil)
	}
	if err := c.checkCallDependencies(); err != nil {
		c.lifeMu.Unlock()
		return err
	}
	c.started = true
	// The client outlives the call that started it, so the supervision context
	// keeps the caller's values and drops its cancellation.
	c.runCtx, c.cancel = context.WithCancel(context.WithoutCancel(ctx))
	c.lifeMu.Unlock()

	c.dispatch.Seal()
	if err := c.openOutbox(ctx); err != nil {
		return wrap(op, err)
	}
	if err := c.registerConsumers(ctx); err != nil {
		return wrap(op, err)
	}
	if err := c.life.Start(ctx); err != nil {
		return wrap(op, err)
	}
	if err := c.watch.Ready(ctx); err != nil {
		return wrap(op, err)
	}
	return wrap(op, c.startSubscriptions())
}

func (c *Client) openOutbox(ctx context.Context) error {
	storage, err := outbox.Open(ctx, outbox.Config{Dir: c.cfg.dataDir})
	if err != nil {
		return err
	}
	c.buffer = storage

	drainer, err := events.NewDrainer(events.DrainerConfig{
		Storage:           storage,
		Publish:           c.publishEnvelope,
		Identity:          c.eventsIdentity,
		BatchSize:         c.cfg.drainBatchSize,
		OnPolicyViolation: c.onDrainViolation,
		OnError:           func(err error) { c.log.Warn("event drain failed", "error", err) },
		Logger:            c.log,
	})
	if err != nil {
		return err
	}
	c.drainer = drainer

	publisher, err := events.NewPublisher(events.PublisherConfig{
		Storage:       storage,
		Codec:         codec{},
		Publish:       c.publishEnvelope,
		Identity:      c.eventsIdentity,
		Kick:          drainer.Kick,
		MaxOutboxRows: c.cfg.maxOutboxRows,
		Logger:        c.log,
	})
	if err != nil {
		return err
	}
	c.publisher = publisher
	return nil
}

// registerConsumers wires every holder of mTLS material into the credential
// registry. The list is not maintained by hand anywhere else: rotation makes
// one pass over the registry, and a consumer missing from it keeps serving on a
// certificate that is about to expire while the control channel still looks
// healthy.
func (c *Client) registerConsumers(ctx context.Context) error {
	if c.server != nil {
		if err := c.creds.Register(ctx, "rpc.server", c.server); err != nil {
			return err
		}
	}
	if err := c.creds.Register(ctx, "rpc.direct", c.direct); err != nil {
		return err
	}
	for _, ch := range []*channel{c.invokeCh, c.eventsCh, c.jobsCh, c.workflowCh, c.telemCh} {
		if err := c.creds.Register(ctx, ch.name, ch); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) startSubscriptions() error {
	if err := c.drainer.Start(c.runCtx); err != nil {
		return err
	}
	if err := c.tport.Start(c.runCtx); err != nil {
		return err
	}
	if err := c.sampler.Start(c.runCtx); err != nil {
		return err
	}
	if len(c.eventSub.Names()) > 0 {
		if err := c.eventSub.Start(c.runCtx); err != nil {
			return err
		}
	}
	if c.jobDecls.Len() > 0 {
		if err := c.jobSub.Start(c.runCtx); err != nil {
			return err
		}
	}
	if c.workflowCount() > 0 {
		if err := c.wfSub.Start(c.runCtx); err != nil {
			return err
		}
	}
	return nil
}

// Stop releases every resource the client owns, in the reverse order of Start.
// It is idempotent and reports the first failure without skipping the rest:
// a channel left open holds its own reconnect goroutines until the process
// dies.
func (c *Client) Stop(ctx context.Context) error {
	const op = "Client.Stop"
	c.lifeMu.Lock()
	if c.stopped || !c.started {
		c.stopped = true
		cancel := c.cancel
		c.lifeMu.Unlock()
		if cancel != nil {
			cancel()
		}
		return nil
	}
	c.stopped = true
	c.lifeMu.Unlock()

	c.wfSub.Stop()
	c.jobSub.Stop()
	c.eventSub.Stop()
	c.sampler.Stop()
	c.drainer.Stop()
	c.tport.Stop()
	c.watch.Stop()

	var errs []error
	errs = append(errs, c.life.Stop(ctx))
	errs = append(errs, c.direct.Close())
	for _, ch := range []*channel{c.invokeCh, c.eventsCh, c.jobsCh, c.workflowCh, c.telemCh} {
		errs = append(errs, ch.Close())
	}
	if c.buffer != nil {
		errs = append(errs, c.buffer.Close())
	}
	c.cancel()
	return wrap(op, errors.Join(errs...))
}

// Recorder, Declarations and RestartRegistry are what the HTTP integrations
// need from a client. They are declared as an interface there, at the consumer.
func (c *Client) Recorder() *telemetry.Recorder        { return c.recorder }
func (c *Client) Declarations() *registry.Declarations { return c.decls }

// RestartRegistry reopens the registry stream so declarations added after start
// — an HTTP route published by an integration — reach the runtime now rather
// than at the next reconnect.
func (c *Client) RestartRegistry() { c.watch.Restart() }

// Service declares this service's outgoing dependencies on another one. It is
// the coarse form: NewMethod declares the same edge and knows the schema too.
func (c *Client) Service(name string, deps ServiceDeps) error {
	const op = "Client.Service"
	if c.isStarted() {
		return newError(CodeState, op, "dependencies must be declared before Start", nil)
	}
	for _, method := range deps.RPC {
		if err := c.decls.AddOutgoing(name, method, pb.MethodType_METHOD_TYPE_RPC); err != nil {
			return wrap(op, err)
		}
	}
	for _, method := range deps.Workflows {
		if err := c.decls.AddOutgoing(name, method, pb.MethodType_METHOD_TYPE_WORKFLOW); err != nil {
			return wrap(op, err)
		}
	}
	for _, route := range deps.HTTP {
		if err := c.decls.AddOutgoing(name, route, pb.MethodType_METHOD_TYPE_HTTP); err != nil {
			return wrap(op, err)
		}
	}
	return nil
}

// ServiceDeps names the methods of one other service this one calls.
type ServiceDeps struct {
	RPC       []string
	Workflows []string
	HTTP      []string
}

// Identity reports the live session. It is read per use, never cached: every
// certificate rotation mints a fresh InstanceID for the same ServiceID.
func (c *Client) Identity() Identity {
	id := c.life.Identity()
	return Identity{
		SessionID:   id.SessionID,
		ServiceID:   id.ServiceID,
		ServiceName: id.ServiceName,
		InstanceID:  id.InstanceID,
	}
}

// Identity names the live session.
type Identity struct {
	SessionID   string
	ServiceID   string
	ServiceName string
	InstanceID  string
}

// ServiceMap is the mesh as the runtime last described it.
func (c *Client) ServiceMap() ServiceMap {
	cache := c.watch.Cache()
	var m ServiceMap
	cache.EachInstance(func(i *pb.ServiceInstanceInfo) bool {
		m.Instances = append(m.Instances, InstanceInfo{
			ServiceID:        i.GetServiceId(),
			ServiceName:      i.GetServiceName(),
			InstanceID:       i.GetInstanceId(),
			CallEndpoint:     i.GetCallEndpoint(),
			HTTPEndpoint:     i.GetHttpEndpoint(),
			Status:           i.GetStatus(),
			UnhealthySinceMs: i.GetIsUnhealthySinceUnixMs(),
		})
		return true
	})
	cache.EachMethod(func(d *pb.MethodDescriptor) bool {
		m.Methods = append(m.Methods, MethodInfo{
			ServiceName:  d.GetServiceName(),
			ServiceID:    d.GetServiceId(),
			InstanceID:   d.GetInstanceId(),
			Name:         d.GetName(),
			Type:         d.GetType().String(),
			ContractHash: d.GetContractHash(),
			Streaming:    d.GetStreaming(),
		})
		return true
	})
	return m
}

// ServiceMap is a point-in-time view of the mesh.
type ServiceMap struct {
	Instances []InstanceInfo
	Methods   []MethodInfo
}

// InstanceInfo is one instance the runtime knows about.
type InstanceInfo struct {
	ServiceID        string
	ServiceName      string
	InstanceID       string
	CallEndpoint     string
	HTTPEndpoint     string
	Status           string
	UnhealthySinceMs int64
}

// MethodInfo is one method some instance publishes.
type MethodInfo struct {
	ServiceName  string
	ServiceID    string
	InstanceID   string
	Name         string
	Type         string
	ContractHash string
	Streaming    bool
}

// PolicyEvaluation is the access policy the runtime last pushed. It is nil-safe
// before the first snapshot: no capabilities and no warnings.
func (c *Client) PolicyEvaluation() PolicyEvaluation {
	p := c.watch.Cache().Policy()
	out := PolicyEvaluation{Capabilities: p.GetCapabilities()}
	for _, w := range p.GetWarnings() {
		out.Warnings = append(out.Warnings, violationOf(w))
	}
	return out
}

// PolicyEvaluation is what this service is allowed to do and what it declared
// that the policy refused.
type PolicyEvaluation struct {
	Capabilities []string
	Warnings     []PolicyViolation
}

// PolicyViolation is one declaration the policy refused. The runtime registers
// the rest and reports this instead of failing registration, so on the wire a
// half-connected service looks like a fully successful one — this is the only
// signal that part of it is not wired up.
type PolicyViolation struct {
	Declaration string
	Value       string
	DenySide    string
	Reason      string
}

func violationOf(v *pb.PolicyViolation) PolicyViolation {
	return PolicyViolation{
		Declaration: v.GetDeclaration(),
		Value:       v.GetValue(),
		DenySide:    v.GetDenySide(),
		Reason:      v.GetReason(),
	}
}

// OnConnected registers a callback fired once per live session, after the
// runtime's Welcome. Callbacks run on the lifecycle's goroutines and must not
// block.
func (c *Client) OnConnected(fn func(Identity)) {
	c.obsMu.Lock()
	defer c.obsMu.Unlock()
	c.onConnected = append(c.onConnected, fn)
}

// OnReconnecting fires before each attempt to rebuild a lost session.
func (c *Client) OnReconnecting(fn func(attempt int, cause error)) {
	c.obsMu.Lock()
	defer c.obsMu.Unlock()
	c.onReconnect = append(c.onReconnect, fn)
}

// OnDraining fires when the runtime announces it is shutting down.
func (c *Client) OnDraining(fn func(reason string)) {
	c.obsMu.Lock()
	defer c.obsMu.Unlock()
	c.onDrain = append(c.onDrain, fn)
}

// OnDisconnected fires once, when the client has stopped trying to reconnect.
func (c *Client) OnDisconnected(fn func(cause error)) {
	c.obsMu.Lock()
	defer c.obsMu.Unlock()
	c.onDisconn = append(c.onDisconn, fn)
}

// OnPolicyViolation fires for every declaration the policy refused, whether it
// came in a registry snapshot or as a denied event publication.
func (c *Client) OnPolicyViolation(fn func(PolicyViolation)) {
	c.obsMu.Lock()
	defer c.obsMu.Unlock()
	c.onViolation = append(c.onViolation, fn)
}

// observer keeps the four lifecycle callbacks off the Client's own method set:
// Connected, Reconnecting, Draining and Disconnected are names an application
// would otherwise find on its client and try to call.
type observer Client

func (o *observer) Connected(id connection.SessionIdentity) {
	c := (*Client)(o)
	c.log.Info("connected", "service", id.ServiceName, "instance", id.InstanceID)
	identity := Identity{
		SessionID:   id.SessionID,
		ServiceID:   id.ServiceID,
		ServiceName: id.ServiceName,
		InstanceID:  id.InstanceID,
	}
	c.obsMu.RLock()
	defer c.obsMu.RUnlock()
	for _, fn := range c.onConnected {
		fn(identity)
	}
}

func (o *observer) Reconnecting(attempt int, cause error) {
	c := (*Client)(o)
	c.log.Warn("reconnecting", "attempt", attempt, "error", cause)
	c.obsMu.RLock()
	defer c.obsMu.RUnlock()
	for _, fn := range c.onReconnect {
		fn(attempt, cause)
	}
}

func (o *observer) Draining(reason string) {
	c := (*Client)(o)
	c.log.Warn("runtime is draining", "reason", reason)
	c.obsMu.RLock()
	defer c.obsMu.RUnlock()
	for _, fn := range c.onDrain {
		fn(reason)
	}
}

func (o *observer) Disconnected(cause error) {
	c := (*Client)(o)
	c.log.Error("disconnected", "error", cause)
	c.obsMu.RLock()
	defer c.obsMu.RUnlock()
	for _, fn := range c.onDisconn {
		fn(cause)
	}
}

// NewRegistrar builds the registry stream owner for one session. The stream
// belongs to the session that carries it, so it is built from that session's
// channel rather than from whatever channel the lifecycle currently holds — at
// this point the new session has not been adopted yet.
func (c *Client) NewRegistrar(conn grpc.ClientConnInterface) connection.Registrar {
	return &sessionRegistrar{c: c, conn: conn}
}

type sessionRegistrar struct {
	c    *Client
	conn grpc.ClientConnInterface
}

func (r *sessionRegistrar) Start(_ context.Context, endpoint string) error {
	if endpoint != "" {
		r.c.decls.SetCallEndpoint(endpoint)
	}
	r.c.session.Store(r.conn)
	if r.c.watchStarted.CompareAndSwap(false, true) {
		return r.c.watch.Start(r.c.runCtx)
	}
	// The watch outlives sessions: it holds the cache the call path reads, and
	// rebuilding it per session would hand every caller a stale mesh view.
	// Restart moves it onto the channel this session just published.
	r.c.watch.Restart()
	return nil
}

// Close is a no-op because the watch is not the session's to close. The stream
// under it dies with the session's channel and the supervisor reopens it on
// whichever channel the next session publishes.
func (r *sessionRegistrar) Close(context.Context) error { return nil }

// RegistryClient satisfies registry.ClientSource against the live session.
func (c *Client) RegistryClient(context.Context) (pb.RegistryClient, error) {
	conn, ok := c.session.Load().(grpc.ClientConnInterface)
	if !ok || conn == nil {
		return nil, newError(CodeConnection, "registry", "no live session", nil)
	}
	return pb.NewRegistryClient(conn), nil
}

// onRegistryChange applies the frame to everything that keys off the live mesh.
func (c *Client) onRegistryChange(ch registry.Change) {
	if ch.Capture != nil {
		c.policy.Set(telemetry.Modes{
			RPC:             telemetry.ModeFromProto(ch.Capture.RPC),
			HTTP:            telemetry.ModeFromProto(ch.Capture.HTTP),
			Event:           telemetry.ModeFromProto(ch.Capture.Event),
			Workflow:        telemetry.ModeFromProto(ch.Capture.Workflow),
			Enabled:         ch.Capture.TelemetryEnabled,
			PayloadMaxBytes: ch.Capture.PayloadMaxBytes,
		})
	}
	if ch.Snapshot || len(ch.RemovedInstances) > 0 || len(ch.RemovedPeers) > 0 {
		c.retainLive()
	}
}

// retainLive evicts the per-instance state of instances that left the mesh. The
// registry is the authority here; the idle sweeps inside the breaker and the
// channel pool only cover the window between two frames, so without this the
// two maps grow for the lifetime of the process on every rolling deploy.
func (c *Client) retainLive() {
	cache := c.watch.Cache()
	keys := map[rpc.BreakerKey]struct{}{}
	ids := map[string]struct{}{}
	cache.EachInstance(func(i *pb.ServiceInstanceInfo) bool {
		keys[rpc.BreakerKey{ServiceID: i.GetServiceId(), InstanceID: i.GetInstanceId()}] = struct{}{}
		ids[i.GetInstanceId()] = struct{}{}
		return true
	})
	c.breaker.Retain(keys)
	c.direct.RetainInstances(ids)
}

func (c *Client) onPolicyWarnings(warnings []*pb.PolicyViolation) {
	for _, w := range warnings {
		v := violationOf(w)
		c.log.Warn("policy violation",
			"declaration", v.Declaration, "value", v.Value, "side", v.DenySide, "reason", v.Reason)
		c.emitViolation(v)
	}
	if len(warnings) > 0 && c.cfg.failOnPolicyViolation {
		// The owner asked for a hard stop, and this callback runs on the watch
		// goroutine that Stop waits for.
		go func() {
			if err := c.Stop(context.WithoutCancel(c.runCtx)); err != nil {
				c.log.Error("stop after policy violation failed", "error", err)
			}
		}()
	}
}

func (c *Client) onDrainViolation(v events.PolicyViolation) {
	c.emitViolation(PolicyViolation{
		Declaration: "event.publish",
		Value:       v.EventName,
		DenySide:    "self_egress",
		Reason:      v.Reason,
	})
}

func (c *Client) emitViolation(v PolicyViolation) {
	c.obsMu.RLock()
	defer c.obsMu.RUnlock()
	for _, fn := range c.onViolation {
		fn(v)
	}
}

func (c *Client) isStarted() bool {
	c.lifeMu.Lock()
	defer c.lifeMu.Unlock()
	return c.started
}

func (c *Client) instanceID() string { return c.life.Identity().InstanceID }

func (c *Client) eventsIdentity() events.Identity {
	id := c.life.Identity()
	return events.Identity{ServiceID: id.ServiceID, InstanceID: id.InstanceID}
}

func (c *Client) jobIdentity() jobi.Identity {
	id := c.life.Identity()
	return jobi.Identity{ServiceID: id.ServiceID, InstanceID: id.InstanceID}
}

func (c *Client) workflowIdentity() wfi.Identity {
	id := c.life.Identity()
	return wfi.Identity{ServiceID: id.ServiceID, InstanceID: id.InstanceID}
}

func (c *Client) openEventStream(ctx context.Context) (events.SubscribeStream, error) {
	conn, err := c.eventsCh.conn()
	if err != nil {
		return nil, err
	}
	return pb.NewEventsClient(conn).Subscribe(ctx)
}

func (c *Client) openTelemetryStream(ctx context.Context) (telemetry.ReportStream, error) {
	conn, err := c.telemCh.conn()
	if err != nil {
		return nil, err
	}
	return pb.NewTelemetryClient(conn).Report(ctx)
}

func (c *Client) publishEnvelope(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	conn, err := c.eventsCh.conn()
	if err != nil {
		return nil, err
	}
	return pb.NewEventsClient(conn).Publish(ctx, req)
}

// Steps satisfies workflow.GraphSource: a run is executed from the graph
// declared in this process, because a Local step carries a Go closure no frozen
// plan can hold.
func (c *Client) Steps(name string) ([]wf.Step, bool) {
	c.graphMu.RLock()
	defer c.graphMu.RUnlock()
	steps, ok := c.graphs[name]
	return steps, ok
}

func (c *Client) workflowCount() int {
	c.graphMu.RLock()
	defer c.graphMu.RUnlock()
	return len(c.graphs)
}

// checkCallDependencies refuses a start whose graphs call methods this service
// never declared. A call step reaches a typed handler only through the schema
// its dependency was bound to, so the graph could not run anyway; reporting it
// here names the workflow and the step, while a step-level failure would name
// them one run and one lease later.
//
// Only literally named targets are covered. A target written as a Path is a
// value of the run, not of the graph, and it stays the responsibility of the
// step that resolves it.
func (c *Client) checkCallDependencies() error {
	const op = "Client.Start"
	c.graphMu.RLock()
	graphs := make(map[string][]wf.Step, len(c.graphs))
	maps.Copy(graphs, c.graphs)
	c.graphMu.RUnlock()

	// Sorted, so a service with two broken graphs is told about the same one on
	// every start rather than a different one each time.
	for _, name := range slices.Sorted(maps.Keys(graphs)) {
		for _, target := range wfi.StaticCallTargets(graphs[name]) {
			if _, bound := c.callSchemas.Lookup(target.Service, target.Method); bound {
				continue
			}
			return newError(CodeConfig, op, fmt.Sprintf("workflow %q step %q: %s",
				name, target.StepID, undeclaredDependency(target.Service, target.Method)), nil)
		}
	}
	return nil
}

// channel owns the one mTLS channel a data-plane domain talks over and rebuilds
// it on every rotation. It is a credential consumer rather than a borrower of
// the control session's channel so that a domain's streams break — and reopen —
// exactly when its certificate is replaced.
type channel struct {
	name string

	mu sync.Mutex
	cc *grpc.ClientConn
}

func newChannel(name string) *channel { return &channel{name: name} }

func (ch *channel) UseCredentials(ctx context.Context, creds connection.Credentials) error {
	next, err := connection.MTLSDialer{}.Dial(ctx, creds)
	if err != nil {
		return fmt.Errorf("servicebridge: dial %s channel: %w", ch.name, err)
	}
	ch.mu.Lock()
	prev := ch.cc
	ch.cc = next
	ch.mu.Unlock()
	if prev != nil {
		// The old channel carries the retiring leaf. Closing it breaks this
		// domain's streams, which is the point: they reopen on the new one.
		_ = prev.Close()
	}
	return nil
}

func (ch *channel) conn() (grpc.ClientConnInterface, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.cc == nil {
		return nil, newError(CodeConnection, ch.name, "no credentials published yet", nil)
	}
	return ch.cc, nil
}

func (ch *channel) Close() error {
	ch.mu.Lock()
	cc := ch.cc
	ch.cc = nil
	ch.mu.Unlock()
	if cc == nil {
		return nil
	}
	return cc.Close()
}

func (ch *channel) JobsClient(context.Context) (pb.JobsClient, error) {
	conn, err := ch.conn()
	if err != nil {
		return nil, err
	}
	return pb.NewJobsClient(conn), nil
}

func (ch *channel) WorkflowsClient(context.Context) (pb.WorkflowsClient, error) {
	conn, err := ch.conn()
	if err != nil {
		return nil, err
	}
	return pb.NewWorkflowsClient(conn), nil
}

func (ch *channel) InvokeClient(context.Context) (pb.InvokeClient, error) {
	conn, err := ch.conn()
	if err != nil {
		return nil, err
	}
	return pb.NewInvokeClient(conn), nil
}

// codec bridges the event domain to the serialization layer. It ignores the
// event name: the payload's own descriptor is the schema, so there is no side
// table to keep in sync with it.
type codec struct{}

func (codec) Encode(_ string, payload any) (events.Encoded, error) {
	p, err := serde.Encode(payload)
	if err != nil {
		return events.Encoded{}, err
	}
	return events.Encoded{Proto: p.Proto, JSON: p.JSON, ContractHash: p.ContractHash}, nil
}

func (codec) Decode(_ string, payload []byte, out any) error {
	return serde.Decode(payload, out)
}
