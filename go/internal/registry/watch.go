package registry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/stream"
)

// DefaultPayloadMaxBytes caps captured payloads until the runtime pushes its
// own limit.
const DefaultPayloadMaxBytes int32 = 65536

// ErrProtocol reports a registry stream that broke its own contract.
var ErrProtocol = errors.New("registry stream protocol violation")

// ErrInvalidConfig is returned by NewWatch when a required dependency is absent.
var ErrInvalidConfig = errors.New("invalid watch config")

// ClientSource yields the Registry stub to open the watch stream with. The
// connection layer owns the channel and swaps it when the identity rotates, so
// the client is asked for on every open rather than captured once.
type ClientSource interface {
	RegistryClient(ctx context.Context) (pb.RegistryClient, error)
}

// CaptureState is the runtime-pushed telemetry authority. Fail-safe before the
// first snapshot: nothing is captured, the cap is the default and telemetry is
// on — the SDK never captures a payload the runtime did not authorise.
type CaptureState struct {
	RPC              pb.CaptureMode
	HTTP             pb.CaptureMode
	Event            pb.CaptureMode
	Workflow         pb.CaptureMode
	TelemetryEnabled bool
	PayloadMaxBytes  int32
}

// DefaultCaptureState is what applies until the first RegistrySnapshot lands.
func DefaultCaptureState() CaptureState {
	return CaptureState{
		RPC:              pb.CaptureMode_CAPTURE_MODE_NONE,
		HTTP:             pb.CaptureMode_CAPTURE_MODE_NONE,
		Event:            pb.CaptureMode_CAPTURE_MODE_NONE,
		Workflow:         pb.CaptureMode_CAPTURE_MODE_NONE,
		TelemetryEnabled: true,
		PayloadMaxBytes:  DefaultPayloadMaxBytes,
	}
}

// ForChannel returns the effective capture mode for one op channel. Channels
// the runtime carries no mode for — job, user — never capture.
func (c CaptureState) ForChannel(ch pb.Channel) pb.CaptureMode {
	switch ch {
	case pb.Channel_RPC:
		return c.RPC
	case pb.Channel_HTTP:
		return c.HTTP
	case pb.Channel_EVENT:
		return c.Event
	case pb.Channel_WORKFLOW:
		return c.Workflow
	default:
		return pb.CaptureMode_CAPTURE_MODE_NONE
	}
}

// normalizeMode maps an unset or unknown wire value onto NONE.
func normalizeMode(m pb.CaptureMode) pb.CaptureMode {
	switch m {
	case pb.CaptureMode_CAPTURE_MODE_ALL, pb.CaptureMode_CAPTURE_MODE_ERRORS:
		return m
	default:
		return pb.CaptureMode_CAPTURE_MODE_NONE
	}
}

// nextCaptureState folds a pushed CaptureModes message onto the current state.
// Modes always come whole, so a missing message means "capture nothing". The
// telemetry globals are different: payload_max_bytes = 0 is proto3's "unset",
// and taking it literally would truncate every payload to zero.
func nextCaptureState(prev CaptureState, m *pb.CaptureModes) CaptureState {
	next := CaptureState{
		RPC:              normalizeMode(m.GetRpc()),
		HTTP:             normalizeMode(m.GetHttp()),
		Event:            normalizeMode(m.GetEvent()),
		Workflow:         normalizeMode(m.GetWorkflow()),
		TelemetryEnabled: prev.TelemetryEnabled,
		PayloadMaxBytes:  prev.PayloadMaxBytes,
	}
	if m.GetPayloadMaxBytes() > 0 {
		next.PayloadMaxBytes = m.GetPayloadMaxBytes()
		next.TelemetryEnabled = m.GetTelemetryEnabled()
		return next
	}
	next.PayloadMaxBytes = DefaultPayloadMaxBytes
	if m.GetTelemetryEnabled() {
		next.TelemetryEnabled = true
	}
	return next
}

// Change describes what one registry frame did to the cache.
type Change struct {
	// Snapshot marks the frame that replaced the whole world.
	Snapshot         bool
	AddedMethods     []*pb.MethodDescriptor
	RemovedMethods   []*pb.MethodDescriptor
	AddedInstances   []*pb.ServiceInstanceInfo
	RemovedInstances []*pb.ServiceInstanceInfo
	AddedPeers       []string
	RemovedPeers     []string
	// Policy is non-nil when a fresh evaluation arrived. It always comes whole.
	Policy *pb.PolicyEvaluation
	// Capture is non-nil when the pushed telemetry authority changed.
	Capture *CaptureState
}

type serviceMethod struct {
	service string
	method  string
}

// methodKey identifies one descriptor row. An instance publishes at most one
// descriptor per (type, name, published).
type methodKey struct {
	instanceID string
	typ        pb.MethodType
	name       string
	published  bool
}

func keyOfMethod(d *pb.MethodDescriptor) methodKey {
	return methodKey{
		instanceID: d.GetInstanceId(),
		typ:        d.GetType(),
		name:       d.GetName(),
		published:  d.GetPublished(),
	}
}

type eventSubKey struct {
	serviceID string
	pattern   string
}

type outgoingKey struct {
	callerServiceID string
	targetServiceID string
	targetMethod    string
	targetType      pb.MethodType
}

// methodBucket is the read view of one (service, method). Buckets are replaced
// wholesale on every change and never mutated in place, so a slice handed to a
// caller stays valid after the lock is dropped.
type methodBucket struct {
	all    []*pb.MethodDescriptor
	byHash map[string][]*pb.MethodDescriptor
}

// Cache is the live mesh view. Lookups on the call path go through the
// (service, method) index; nothing on that path scans the full descriptor set.
type Cache struct {
	mu sync.RWMutex

	methods map[methodKey]*pb.MethodDescriptor
	members map[serviceMethod]map[methodKey]*pb.MethodDescriptor
	buckets map[serviceMethod]*methodBucket

	instances   map[string]*pb.ServiceInstanceInfo
	instMembers map[string]map[string]*pb.ServiceInstanceInfo
	instBuckets map[string][]*pb.ServiceInstanceInfo

	eventSubs map[eventSubKey]*pb.EventSubscriptionDescriptor
	outgoing  map[outgoingKey]*pb.OutgoingCallDescriptor

	policy  *pb.PolicyEvaluation
	capture CaptureState
}

// NewCache builds an empty cache holding the fail-safe capture state.
func NewCache() *Cache {
	return &Cache{
		methods:     make(map[methodKey]*pb.MethodDescriptor),
		members:     make(map[serviceMethod]map[methodKey]*pb.MethodDescriptor),
		buckets:     make(map[serviceMethod]*methodBucket),
		instances:   make(map[string]*pb.ServiceInstanceInfo),
		instMembers: make(map[string]map[string]*pb.ServiceInstanceInfo),
		instBuckets: make(map[string][]*pb.ServiceInstanceInfo),
		eventSubs:   make(map[eventSubKey]*pb.EventSubscriptionDescriptor),
		outgoing:    make(map[outgoingKey]*pb.OutgoingCallDescriptor),
		capture:     DefaultCaptureState(),
	}
}

// Candidates returns every descriptor serving (service, method), narrowed to one
// contract hash when it is given. The result is a shared read-only slice — the
// caller must not mutate it. Nil when nothing serves the pair.
func (c *Cache) Candidates(service, method, contractHash string) []*pb.MethodDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	b := c.buckets[serviceMethod{service: service, method: method}]
	if b == nil {
		return nil
	}
	if contractHash == "" {
		return b.all
	}
	return b.byHash[contractHash]
}

// InstancesOf returns every known instance of a service as a shared read-only
// slice.
func (c *Cache) InstancesOf(service string) []*pb.ServiceInstanceInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.instBuckets[service]
}

// Instance looks one instance up by id.
func (c *Cache) Instance(instanceID string) (*pb.ServiceInstanceInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	inst, ok := c.instances[instanceID]
	return inst, ok
}

// Policy returns the last evaluation the runtime pushed, nil before the first
// snapshot.
func (c *Cache) Policy() *pb.PolicyEvaluation {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.policy
}

// Capture returns the runtime-pushed telemetry authority.
func (c *Cache) Capture() CaptureState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.capture
}

// EachMethod walks every descriptor. Returning false stops the walk. The
// callback runs under the read lock and must not call back into the cache.
func (c *Cache) EachMethod(fn func(*pb.MethodDescriptor) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, d := range c.methods {
		if !fn(d) {
			return
		}
	}
}

// EachInstance walks every instance under the same contract as EachMethod.
func (c *Cache) EachInstance(fn func(*pb.ServiceInstanceInfo) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, inst := range c.instances {
		if !fn(inst) {
			return
		}
	}
}

// EachEventSubscription walks the subscriptions the runtime reported.
func (c *Cache) EachEventSubscription(fn func(*pb.EventSubscriptionDescriptor) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, es := range c.eventSubs {
		if !fn(es) {
			return
		}
	}
}

// EachOutgoingCall walks the outgoing-call edges the runtime reported.
func (c *Cache) EachOutgoingCall(fn func(*pb.OutgoingCallDescriptor) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, oc := range c.outgoing {
		if !fn(oc) {
			return
		}
	}
}

// dirty collects the index buckets one frame invalidated so each is rebuilt
// once, no matter how many rows in the frame touched it.
type dirty struct {
	methods   map[serviceMethod]struct{}
	instances map[string]struct{}
}

func newDirty() *dirty {
	return &dirty{
		methods:   make(map[serviceMethod]struct{}),
		instances: make(map[string]struct{}),
	}
}

func (c *Cache) putMethod(d *pb.MethodDescriptor, dt *dirty) {
	k := keyOfMethod(d)
	sm := serviceMethod{service: d.GetServiceName(), method: d.GetName()}
	c.methods[k] = d
	members := c.members[sm]
	if members == nil {
		members = make(map[methodKey]*pb.MethodDescriptor)
		c.members[sm] = members
	}
	members[k] = d
	dt.methods[sm] = struct{}{}
}

// dropMethod removes a descriptor by key and returns the row that was actually
// held, which is the one listeners must be told about.
func (c *Cache) dropMethod(k methodKey, dt *dirty) (*pb.MethodDescriptor, bool) {
	old, ok := c.methods[k]
	if !ok {
		return nil, false
	}
	delete(c.methods, k)
	sm := serviceMethod{service: old.GetServiceName(), method: old.GetName()}
	if members := c.members[sm]; members != nil {
		delete(members, k)
		if len(members) == 0 {
			delete(c.members, sm)
		}
	}
	dt.methods[sm] = struct{}{}
	return old, true
}

func (c *Cache) putInstance(inst *pb.ServiceInstanceInfo, dt *dirty) {
	id := inst.GetInstanceId()
	if prev, ok := c.instances[id]; ok && prev.GetServiceName() != inst.GetServiceName() {
		c.detachInstance(prev, dt)
	}
	c.instances[id] = inst
	svc := inst.GetServiceName()
	members := c.instMembers[svc]
	if members == nil {
		members = make(map[string]*pb.ServiceInstanceInfo)
		c.instMembers[svc] = members
	}
	members[id] = inst
	dt.instances[svc] = struct{}{}
}

func (c *Cache) dropInstance(instanceID string, dt *dirty) (*pb.ServiceInstanceInfo, bool) {
	old, ok := c.instances[instanceID]
	if !ok {
		return nil, false
	}
	delete(c.instances, instanceID)
	c.detachInstance(old, dt)
	return old, true
}

func (c *Cache) detachInstance(inst *pb.ServiceInstanceInfo, dt *dirty) {
	svc := inst.GetServiceName()
	if members := c.instMembers[svc]; members != nil {
		delete(members, inst.GetInstanceId())
		if len(members) == 0 {
			delete(c.instMembers, svc)
		}
	}
	dt.instances[svc] = struct{}{}
}

// rebuild regenerates the invalidated read views. Buckets are sorted by instance
// id so candidate order is stable across rebuilds instead of following Go's map
// iteration order.
func (c *Cache) rebuild(dt *dirty) {
	for sm := range dt.methods {
		members := c.members[sm]
		if len(members) == 0 {
			delete(c.buckets, sm)
			continue
		}
		all := make([]*pb.MethodDescriptor, 0, len(members))
		for _, d := range members {
			all = append(all, d)
		}
		sort.Slice(all, func(i, j int) bool { return all[i].GetInstanceId() < all[j].GetInstanceId() })
		byHash := make(map[string][]*pb.MethodDescriptor)
		for _, d := range all {
			byHash[d.GetContractHash()] = append(byHash[d.GetContractHash()], d)
		}
		c.buckets[sm] = &methodBucket{all: all, byHash: byHash}
	}
	for svc := range dt.instances {
		members := c.instMembers[svc]
		if len(members) == 0 {
			delete(c.instBuckets, svc)
			continue
		}
		all := make([]*pb.ServiceInstanceInfo, 0, len(members))
		for _, inst := range members {
			all = append(all, inst)
		}
		sort.Slice(all, func(i, j int) bool { return all[i].GetInstanceId() < all[j].GetInstanceId() })
		c.instBuckets[svc] = all
	}
}

// ApplySnapshot replaces the whole cache. The first frame of every registry
// stream is a snapshot: after a reconnect the cache may be arbitrarily stale, so
// nothing but a full replacement is safe.
func (c *Cache) ApplySnapshot(s *pb.RegistrySnapshot) Change {
	c.mu.Lock()
	defer c.mu.Unlock()

	dt := newDirty()
	change := Change{Snapshot: true}

	next := make(map[methodKey]*pb.MethodDescriptor, len(s.GetMethods()))
	for _, d := range s.GetMethods() {
		next[keyOfMethod(d)] = d
	}
	for k, old := range c.methods {
		if _, ok := next[k]; !ok {
			change.RemovedMethods = append(change.RemovedMethods, old)
		}
	}
	for sm := range c.buckets {
		dt.methods[sm] = struct{}{}
	}
	c.methods = make(map[methodKey]*pb.MethodDescriptor, len(next))
	c.members = make(map[serviceMethod]map[methodKey]*pb.MethodDescriptor)
	for _, d := range s.GetMethods() {
		c.putMethod(d, dt)
	}
	change.AddedMethods = s.GetMethods()

	nextInstances := make(map[string]struct{}, len(s.GetInstances()))
	for _, inst := range s.GetInstances() {
		nextInstances[inst.GetInstanceId()] = struct{}{}
	}
	for id, old := range c.instances {
		if _, ok := nextInstances[id]; !ok {
			change.RemovedInstances = append(change.RemovedInstances, old)
		}
	}
	for svc := range c.instBuckets {
		dt.instances[svc] = struct{}{}
	}
	c.instances = make(map[string]*pb.ServiceInstanceInfo, len(s.GetInstances()))
	c.instMembers = make(map[string]map[string]*pb.ServiceInstanceInfo)
	for _, inst := range s.GetInstances() {
		c.putInstance(inst, dt)
	}
	change.AddedInstances = s.GetInstances()

	c.eventSubs = make(map[eventSubKey]*pb.EventSubscriptionDescriptor, len(s.GetEventSubscriptions()))
	for _, es := range s.GetEventSubscriptions() {
		c.eventSubs[eventSubKey{serviceID: es.GetServiceId(), pattern: es.GetPattern()}] = es
	}
	c.outgoing = make(map[outgoingKey]*pb.OutgoingCallDescriptor, len(s.GetOutgoingCalls()))
	for _, oc := range s.GetOutgoingCalls() {
		c.outgoing[keyOfOutgoing(oc)] = oc
	}

	if p := s.GetPolicy(); p != nil {
		c.policy = p
		change.Policy = p
	}
	if capture := nextCaptureState(c.capture, s.GetCaptureModes()); capture != c.capture {
		c.capture = capture
		change.Capture = &capture
	}

	c.rebuild(dt)
	return change
}

// ApplyUpdate folds one incremental frame onto the cache.
func (c *Cache) ApplyUpdate(u *pb.RegistryUpdate) Change {
	c.mu.Lock()
	defer c.mu.Unlock()

	dt := newDirty()
	change := Change{
		AddedMethods:   u.GetAdded(),
		AddedInstances: u.GetAddedInstances(),
		AddedPeers:     u.GetAddedPeers(),
		RemovedPeers:   u.GetRemovedPeers(),
	}

	for _, d := range u.GetAdded() {
		c.putMethod(d, dt)
	}
	for _, d := range u.GetRemoved() {
		if old, ok := c.dropMethod(keyOfMethod(d), dt); ok {
			change.RemovedMethods = append(change.RemovedMethods, old)
		}
	}
	for _, inst := range u.GetAddedInstances() {
		c.putInstance(inst, dt)
	}
	for _, inst := range u.GetRemovedInstances() {
		if old, ok := c.dropInstance(inst.GetInstanceId(), dt); ok {
			change.RemovedInstances = append(change.RemovedInstances, old)
		}
	}
	for _, es := range u.GetAddedEventSubscriptions() {
		c.eventSubs[eventSubKey{serviceID: es.GetServiceId(), pattern: es.GetPattern()}] = es
	}
	for _, es := range u.GetRemovedEventSubscriptions() {
		delete(c.eventSubs, eventSubKey{serviceID: es.GetServiceId(), pattern: es.GetPattern()})
	}
	for _, oc := range u.GetAddedOutgoingCalls() {
		c.outgoing[keyOfOutgoing(oc)] = oc
	}
	for _, oc := range u.GetRemovedOutgoingCalls() {
		delete(c.outgoing, keyOfOutgoing(oc))
	}

	if peers := u.GetRemovedPeers(); len(peers) > 0 {
		c.purgePeers(peers, dt, &change)
	}

	if p := u.GetPolicy(); p != nil {
		c.policy = p
		change.Policy = p
	}
	if capture := nextCaptureState(c.capture, u.GetCaptureModes()); capture != c.capture {
		c.capture = capture
		change.Capture = &capture
	}

	c.rebuild(dt)
	return change
}

// purgePeers drops everything tied to a peer that left the caller's policy
// scope. It walks the whole cache: peers are keyed by service id while the
// indexes are keyed by service name, and losing a peer is a rare policy event,
// not part of the call path.
func (c *Cache) purgePeers(peers []string, dt *dirty, change *Change) {
	gone := make(map[string]struct{}, len(peers))
	for _, id := range peers {
		gone[id] = struct{}{}
	}

	for k, d := range c.methods {
		if _, ok := gone[d.GetServiceId()]; !ok {
			continue
		}
		if old, dropped := c.dropMethod(k, dt); dropped {
			change.RemovedMethods = append(change.RemovedMethods, old)
		}
	}
	for id, inst := range c.instances {
		if _, ok := gone[inst.GetServiceId()]; !ok {
			continue
		}
		if old, dropped := c.dropInstance(id, dt); dropped {
			change.RemovedInstances = append(change.RemovedInstances, old)
		}
	}
	for k, es := range c.eventSubs {
		if _, ok := gone[es.GetServiceId()]; ok {
			delete(c.eventSubs, k)
		}
	}
	for k, oc := range c.outgoing {
		if _, ok := gone[oc.GetTargetServiceId()]; ok {
			delete(c.outgoing, k)
		}
	}
}

func keyOfOutgoing(oc *pb.OutgoingCallDescriptor) outgoingKey {
	return outgoingKey{
		callerServiceID: oc.GetCallerServiceId(),
		targetServiceID: oc.GetTargetServiceId(),
		targetMethod:    oc.GetTargetMethod(),
		targetType:      oc.GetTargetType(),
	}
}

// WatchConfig wires the registry watch. See ./README.md.
type WatchConfig struct {
	// Clients yields the Registry stub for each open.
	Clients ClientSource
	// Request is evaluated on every open so a declaration added after start —
	// an HTTP route published by an integration — reaches the runtime on the
	// next stream.
	Request func() *pb.RegisterRequest
	// OnChange reports what each frame did. It runs on the watch goroutine.
	OnChange func(Change)
	// OnPolicyWarnings surfaces the violations the runtime reports instead of
	// failing registration. Silence from the runtime is not success: a denied
	// capability makes it skip the handler and register the rest.
	OnPolicyWarnings func(warnings []*pb.PolicyViolation)
	// OnError reports stream failures. The watch reconnects regardless.
	OnError func(err error)
	// Backoff pins the reconnect ladder. Zero value falls back to the default.
	Backoff stream.Backoff
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// Watch keeps the registry stream alive and the cache current.
type Watch struct {
	cfg    WatchConfig
	cache  *Cache
	sup    *stream.Supervisor[*pb.RegistryEvent, pb.Registry_RegisterAndWatchClient]
	logger *slog.Logger

	ready     chan struct{}
	readyOnce sync.Once

	// Touched only from the supervisor goroutine, which runs OnData serially.
	curStream        pb.Registry_RegisterAndWatchClient
	awaitingSnapshot bool
}

// NewWatch builds the registry watch over a single stream supervisor.
func NewWatch(cfg WatchConfig) (*Watch, error) {
	if cfg.Clients == nil {
		return nil, fmt.Errorf("registry: new watch: missing client source: %w", ErrInvalidConfig)
	}
	if cfg.Request == nil {
		return nil, fmt.Errorf("registry: new watch: missing request builder: %w", ErrInvalidConfig)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	w := &Watch{
		cfg:    cfg,
		cache:  NewCache(),
		logger: cfg.Logger,
		ready:  make(chan struct{}),
	}

	sup, err := stream.NewSupervisor(stream.Config[*pb.RegistryEvent, pb.Registry_RegisterAndWatchClient]{
		Name:    "registry.watch",
		Open:    w.open,
		OnData:  w.onData,
		OnError: w.reportError,
		Backoff: cfg.Backoff,
		Logger:  cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("registry: new watch: %w", err)
	}
	w.sup = sup
	return w, nil
}

// Cache exposes the live mesh view.
func (w *Watch) Cache() *Cache { return w.cache }

// Start opens the stream and keeps it open until ctx is cancelled or Stop runs.
func (w *Watch) Start(ctx context.Context) error {
	if err := w.sup.Start(ctx); err != nil {
		return fmt.Errorf("registry: watch: start: %w", err)
	}
	return nil
}

// Stop closes the stream and releases the watch goroutines.
func (w *Watch) Stop() { w.sup.Stop() }

// Restart reopens the stream immediately. The connection layer calls it after a
// certificate rotation and after a declaration changes post-start.
func (w *Watch) Restart() { w.sup.Restart() }

// Ready blocks until the first snapshot lands, i.e. until registration is
// confirmed and the cache is usable.
func (w *Watch) Ready(ctx context.Context) error {
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("registry: watch: ready: %w", ctx.Err())
	}
}

func (w *Watch) open(ctx context.Context) (pb.Registry_RegisterAndWatchClient, error) {
	client, err := w.cfg.Clients.RegistryClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("registry: watch: registry client: %w", err)
	}
	st, err := client.RegisterAndWatch(ctx, w.cfg.Request())
	if err != nil {
		return nil, fmt.Errorf("registry: watch: register and watch: %w", err)
	}
	return st, nil
}

func (w *Watch) onData(_ context.Context, evt *pb.RegistryEvent, st pb.Registry_RegisterAndWatchClient) {
	if w.curStream != st {
		w.curStream = st
		w.awaitingSnapshot = true
	}

	switch kind := evt.GetKind().(type) {
	case *pb.RegistryEvent_Snapshot:
		w.awaitingSnapshot = false
		change := w.cache.ApplySnapshot(kind.Snapshot)
		w.readyOnce.Do(func() { close(w.ready) })
		w.emit(change)
	case *pb.RegistryEvent_Update:
		if w.awaitingSnapshot {
			// An incremental delta folded onto a cache from a previous session
			// would silently keep dead peers alive. The runtime always sends the
			// snapshot first, so this frame is a broken stream, not a state to
			// merge.
			w.reportError(fmt.Errorf("registry: watch: update before snapshot: %w", ErrProtocol))
			return
		}
		w.emit(w.cache.ApplyUpdate(kind.Update))
	default:
		w.reportError(fmt.Errorf("registry: watch: unknown event kind: %w", ErrProtocol))
	}
}

func (w *Watch) emit(change Change) {
	if change.Policy != nil {
		if warnings := change.Policy.GetWarnings(); len(warnings) > 0 {
			w.raisePolicyWarnings(warnings)
		}
	}
	if w.cfg.OnChange != nil {
		w.cfg.OnChange(change)
	}
}

// raisePolicyWarnings never stays silent: a skipped handler registration looks
// exactly like a successful one on the wire, and the warning is the only signal
// the caller gets.
func (w *Watch) raisePolicyWarnings(warnings []*pb.PolicyViolation) {
	for _, v := range warnings {
		w.logger.Warn("registry: policy warning",
			"declaration", v.GetDeclaration(),
			"value", v.GetValue(),
			"deny_side", v.GetDenySide(),
			"reason", v.GetReason())
	}
	if w.cfg.OnPolicyWarnings != nil {
		w.cfg.OnPolicyWarnings(warnings)
	}
}

func (w *Watch) reportError(err error) {
	if w.cfg.OnError != nil {
		w.cfg.OnError(err)
	}
}
