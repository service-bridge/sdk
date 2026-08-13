// Package registry owns what the service declares before start and the live
// view of the mesh the runtime pushes back.
package registry

import (
	"errors"
	"fmt"
	"sync"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
)

// Declaration rules the runtime enforces on RegisterRequest. Breaking any of
// them costs a round trip and an InvalidArgument, so they are checked here.
var (
	ErrEmptyName        = errors.New("declaration name must not be empty")
	ErrUnspecifiedType  = errors.New("declaration type must be set")
	ErrEventAsIncoming  = errors.New("event subscriptions travel via event_subscriptions, not incoming")
	ErrOutgoingType     = errors.New("outgoing dependency must be rpc, workflow or http")
	ErrOutputSchema     = errors.New("job and http methods carry no transport-level output")
	ErrEmptyJobSpec     = errors.New("job requires its canonical spec in input_schema_json")
	ErrEmptyHTTPPattern = errors.New("http route needs both a method and a pattern")
)

// IncomingSpec is one handler the service exposes. See ./README.md.
type IncomingSpec struct {
	Type             pb.MethodType
	Name             string
	InputSchemaJSON  []byte
	OutputSchemaJSON []byte
	Streaming        bool
	ContractHash     string
}

type outgoingDepKey struct {
	service string
	method  string
	typ     pb.MethodType
}

// Declarations accumulates the service's contract and assembles the
// RegisterRequest. Safe for concurrent use: handlers are registered from
// wherever the application wires them up.
type Declarations struct {
	mu sync.Mutex

	incoming  []*pb.IncomingMethod
	published []*pb.PublishedEvent

	outgoing     []*pb.OutgoingDep
	outgoingSeen map[outgoingDepKey]struct{}

	subs     []*pb.EventSubscription
	subsSeen map[string]struct{}

	callEndpoint string
	httpEndpoint string
}

// NewDeclarations builds an empty declaration set.
func NewDeclarations() *Declarations {
	return &Declarations{
		outgoingSeen: make(map[outgoingDepKey]struct{}),
		subsSeen:     make(map[string]struct{}),
	}
}

// AddIncoming records one exposed handler. It rejects the shapes the runtime
// answers with InvalidArgument, so a bad declaration fails where it is written
// instead of at start.
func (d *Declarations) AddIncoming(spec IncomingSpec) error {
	if spec.Name == "" {
		return fmt.Errorf("registry: add incoming: %w", ErrEmptyName)
	}
	switch spec.Type {
	case pb.MethodType_METHOD_TYPE_UNSPECIFIED:
		return fmt.Errorf("registry: add incoming %q: %w", spec.Name, ErrUnspecifiedType)
	case pb.MethodType_METHOD_TYPE_EVENT:
		return fmt.Errorf("registry: add incoming %q: %w", spec.Name, ErrEventAsIncoming)
	case pb.MethodType_METHOD_TYPE_JOB, pb.MethodType_METHOD_TYPE_HTTP:
		if len(spec.OutputSchemaJSON) > 0 {
			return fmt.Errorf("registry: add incoming %q (%s): %w", spec.Name, spec.Type, ErrOutputSchema)
		}
	}
	if spec.Type == pb.MethodType_METHOD_TYPE_JOB && len(spec.InputSchemaJSON) == 0 {
		return fmt.Errorf("registry: add incoming %q: %w", spec.Name, ErrEmptyJobSpec)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.incoming = append(d.incoming, &pb.IncomingMethod{
		Type:             spec.Type,
		Name:             spec.Name,
		InputSchemaJson:  spec.InputSchemaJSON,
		OutputSchemaJson: spec.OutputSchemaJSON,
		Streaming:        spec.Streaming,
		ContractHash:     spec.ContractHash,
	})
	return nil
}

// AddHTTPRoute declares one route of the application's own HTTP server. The
// runtime does not proxy it (ADR 0001); the declaration only feeds the service
// map and discovery, so it carries no schema and no contract hash.
func (d *Declarations) AddHTTPRoute(httpMethod, pattern string) error {
	if httpMethod == "" || pattern == "" {
		return fmt.Errorf("registry: add http route: %w", ErrEmptyHTTPPattern)
	}
	return d.AddIncoming(IncomingSpec{
		Type: pb.MethodType_METHOD_TYPE_HTTP,
		Name: httpMethod + " " + pattern,
	})
}

// PublishEvent declares an event this service emits.
func (d *Declarations) PublishEvent(name string, schemaJSON []byte, contractHash string) error {
	if name == "" {
		return fmt.Errorf("registry: publish event: %w", ErrEmptyName)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.published = append(d.published, &pb.PublishedEvent{
		Name:         name,
		SchemaJson:   schemaJSON,
		ContractHash: contractHash,
	})
	return nil
}

// SubscribeEvent declares an event subscription. Duplicate patterns collapse
// into one row: event_subscriptions has PRIMARY KEY (subscriber_id, pattern),
// so a duplicate rolls the whole registration back. In-process fan-out across
// several handlers for the same pattern is the SDK's own business.
func (d *Declarations) SubscribeEvent(pattern string, durable bool) error {
	if pattern == "" {
		return fmt.Errorf("registry: subscribe event: %w", ErrEmptyName)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, seen := d.subsSeen[pattern]; seen {
		return nil
	}
	d.subsSeen[pattern] = struct{}{}
	d.subs = append(d.subs, &pb.EventSubscription{Pattern: pattern, Durable: durable})
	return nil
}

// AddOutgoing declares a dependency on another service. Events and jobs are not
// dependencies — an event is delivered by the runtime and a job fires on its own
// schedule — and the runtime rejects them. Duplicates collapse: one client and
// one explicit dependency declaration for the same target both land here and
// only inflate the register frame.
func (d *Declarations) AddOutgoing(serviceName, methodName string, typ pb.MethodType) error {
	if serviceName == "" || methodName == "" {
		return fmt.Errorf("registry: add outgoing: %w", ErrEmptyName)
	}
	switch typ {
	case pb.MethodType_METHOD_TYPE_RPC, pb.MethodType_METHOD_TYPE_WORKFLOW, pb.MethodType_METHOD_TYPE_HTTP:
	default:
		return fmt.Errorf("registry: add outgoing %s/%s (%s): %w", serviceName, methodName, typ, ErrOutgoingType)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	key := outgoingDepKey{service: serviceName, method: methodName, typ: typ}
	if _, seen := d.outgoingSeen[key]; seen {
		return nil
	}
	d.outgoingSeen[key] = struct{}{}
	d.outgoing = append(d.outgoing, &pb.OutgoingDep{
		ServiceName: serviceName,
		MethodName:  methodName,
		Type:        typ,
	})
	return nil
}

// SetCallEndpoint advertises the address peers dial for direct RPC.
func (d *Declarations) SetCallEndpoint(endpoint string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.callEndpoint = endpoint
}

// SetHTTPEndpoint advertises the address of the application's own HTTP server.
func (d *Declarations) SetHTTPEndpoint(endpoint string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.httpEndpoint = endpoint
}

// BuildRegisterRequest assembles the current declaration set. Each call returns
// an independent request: the watch stream reopens with a freshly built one, and
// a declaration added meanwhile must not mutate a frame already in flight.
func (d *Declarations) BuildRegisterRequest() *pb.RegisterRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	return &pb.RegisterRequest{
		Incoming:           append([]*pb.IncomingMethod(nil), d.incoming...),
		Published:          append([]*pb.PublishedEvent(nil), d.published...),
		Outgoing:           append([]*pb.OutgoingDep(nil), d.outgoing...),
		CallEndpoint:       d.callEndpoint,
		EventSubscriptions: append([]*pb.EventSubscription(nil), d.subs...),
		HttpEndpoint:       d.httpEndpoint,
	}
}
