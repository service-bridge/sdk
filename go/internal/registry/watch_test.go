package registry_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/registry"
	"github.com/service-bridge/sdk/go/internal/stream"
)

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

func md(service, serviceID, instanceID, name, hash string) *pb.MethodDescriptor {
	return &pb.MethodDescriptor{
		ServiceName:  service,
		ServiceId:    serviceID,
		InstanceId:   instanceID,
		Type:         pb.MethodType_METHOD_TYPE_RPC,
		Name:         name,
		ContractHash: hash,
	}
}

func si(instanceID, serviceID, service string) *pb.ServiceInstanceInfo {
	return &pb.ServiceInstanceInfo{
		InstanceId:   instanceID,
		ServiceId:    serviceID,
		ServiceName:  service,
		CallEndpoint: instanceID + ":9000",
		Status:       "connected",
	}
}

func esd(serviceID, service, pattern string) *pb.EventSubscriptionDescriptor {
	return &pb.EventSubscriptionDescriptor{
		ServiceId:   serviceID,
		ServiceName: service,
		Pattern:     pattern,
		Durable:     true,
	}
}

func ocd(callerID, targetID, targetMethod string) *pb.OutgoingCallDescriptor {
	return &pb.OutgoingCallDescriptor{
		CallerServiceId: callerID,
		TargetServiceId: targetID,
		TargetMethod:    targetMethod,
		TargetType:      pb.MethodType_METHOD_TYPE_RPC,
	}
}

func instanceIDsOf(descs []*pb.MethodDescriptor) []string {
	ids := make([]string, 0, len(descs))
	for _, d := range descs {
		ids = append(ids, d.GetInstanceId())
	}
	return ids
}

func cacheMethodIDs(c *registry.Cache) []string {
	var got []string
	c.EachMethod(func(d *pb.MethodDescriptor) bool {
		got = append(got, d.GetInstanceId()+"/"+d.GetName())
		return true
	})
	sort.Strings(got)
	return got
}

func cacheInstanceIDs(c *registry.Cache) []string {
	var got []string
	c.EachInstance(func(inst *pb.ServiceInstanceInfo) bool {
		got = append(got, inst.GetInstanceId())
		return true
	})
	sort.Strings(got)
	return got
}

func cachePatterns(c *registry.Cache) []string {
	var got []string
	c.EachEventSubscription(func(es *pb.EventSubscriptionDescriptor) bool {
		got = append(got, es.GetServiceId()+"/"+es.GetPattern())
		return true
	})
	sort.Strings(got)
	return got
}

func cacheOutgoingEdges(c *registry.Cache) []string {
	var got []string
	c.EachOutgoingCall(func(oc *pb.OutgoingCallDescriptor) bool {
		got = append(got, oc.GetCallerServiceId()+"->"+oc.GetTargetServiceId()+"/"+oc.GetTargetMethod())
		return true
	})
	sort.Strings(got)
	return got
}

// ---------------------------------------------------------------------------
// cache: fail-safe defaults
// ---------------------------------------------------------------------------

// TestCacheIsFailSafeBeforeTheFirstSnapshot pins the state the SDK runs in
// between process start and the first frame: nothing is captured, because the
// runtime has not authorised any capture yet.
func TestCacheIsFailSafeBeforeTheFirstSnapshot(t *testing.T) {
	c := registry.NewCache()

	capture := c.Capture()
	if capture != registry.DefaultCaptureState() {
		t.Fatalf("fresh cache capture %+v, want %+v", capture, registry.DefaultCaptureState())
	}
	if capture.PayloadMaxBytes != 65536 {
		t.Fatalf("payload cap %d, want 65536", capture.PayloadMaxBytes)
	}
	if !capture.TelemetryEnabled {
		t.Fatal("telemetry must be on before the runtime says otherwise")
	}
	for _, ch := range []pb.Channel{
		pb.Channel_RPC, pb.Channel_HTTP, pb.Channel_EVENT,
		pb.Channel_WORKFLOW, pb.Channel_JOB, pb.Channel_USER,
	} {
		if got := capture.ForChannel(ch); got != pb.CaptureMode_CAPTURE_MODE_NONE {
			t.Fatalf("channel %s capture mode %s, want NONE", ch, got)
		}
	}

	if c.Policy() != nil {
		t.Fatal("policy is known only from the runtime")
	}
	if got := c.Candidates("billing", "charge", ""); got != nil {
		t.Fatalf("candidates before any frame: %v", got)
	}
	if got := c.InstancesOf("billing"); got != nil {
		t.Fatalf("instances before any frame: %v", got)
	}
	if _, ok := c.Instance("i1"); ok {
		t.Fatal("instance lookup hit on an empty cache")
	}
}

// TestCaptureModesNeverPartiallyMerge guards the channel that is *absent* from
// a pushed CaptureModes: modes come whole, so a missing channel means "stop
// capturing", not "keep the previous mode".
func TestCaptureModesNeverPartiallyMerge(t *testing.T) {
	c := registry.NewCache()

	c.ApplySnapshot(&pb.RegistrySnapshot{
		CaptureModes: &pb.CaptureModes{
			Rpc:              pb.CaptureMode_CAPTURE_MODE_ALL,
			Http:             pb.CaptureMode_CAPTURE_MODE_ERRORS,
			Event:            pb.CaptureMode_CAPTURE_MODE_ALL,
			Workflow:         pb.CaptureMode_CAPTURE_MODE_ALL,
			TelemetryEnabled: true,
			PayloadMaxBytes:  4096,
		},
	})
	got := c.Capture()
	want := registry.CaptureState{
		RPC:              pb.CaptureMode_CAPTURE_MODE_ALL,
		HTTP:             pb.CaptureMode_CAPTURE_MODE_ERRORS,
		Event:            pb.CaptureMode_CAPTURE_MODE_ALL,
		Workflow:         pb.CaptureMode_CAPTURE_MODE_ALL,
		TelemetryEnabled: true,
		PayloadMaxBytes:  4096,
	}
	if got != want {
		t.Fatalf("after snapshot: %+v, want %+v", got, want)
	}

	change := c.ApplyUpdate(&pb.RegistryUpdate{
		CaptureModes: &pb.CaptureModes{
			Rpc:              pb.CaptureMode_CAPTURE_MODE_ERRORS,
			TelemetryEnabled: true,
			PayloadMaxBytes:  4096,
		},
	})
	if change.Capture == nil {
		t.Fatal("a changed capture authority was not reported")
	}
	got = c.Capture()
	if got.RPC != pb.CaptureMode_CAPTURE_MODE_ERRORS {
		t.Fatalf("rpc mode %s, want ERRORS", got.RPC)
	}
	for name, mode := range map[string]pb.CaptureMode{
		"http": got.HTTP, "event": got.Event, "workflow": got.Workflow,
	} {
		if mode != pb.CaptureMode_CAPTURE_MODE_NONE {
			t.Fatalf("%s mode %s after a whole-message update, want NONE", name, mode)
		}
	}

	// An unchanged authority must not be reported as a change.
	if again := c.ApplyUpdate(&pb.RegistryUpdate{
		CaptureModes: &pb.CaptureModes{
			Rpc:              pb.CaptureMode_CAPTURE_MODE_ERRORS,
			TelemetryEnabled: true,
			PayloadMaxBytes:  4096,
		},
	}); again.Capture != nil {
		t.Fatalf("identical capture authority reported as a change: %+v", *again.Capture)
	}
}

// TestCaptureModesFallBackToTheDefaultCapWhenTheRuntimeSendsNone covers
// proto3's "unset is zero": taking payload_max_bytes = 0 literally would
// truncate every captured payload to nothing.
func TestCaptureModesFallBackToTheDefaultCapWhenTheRuntimeSendsNone(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		CaptureModes: &pb.CaptureModes{
			Rpc:              pb.CaptureMode_CAPTURE_MODE_ALL,
			TelemetryEnabled: true,
			PayloadMaxBytes:  1024,
		},
	})

	c.ApplyUpdate(&pb.RegistryUpdate{
		CaptureModes: &pb.CaptureModes{Rpc: pb.CaptureMode_CAPTURE_MODE_ALL},
	})
	got := c.Capture()
	if got.PayloadMaxBytes != 65536 {
		t.Fatalf("payload cap %d after an unset limit, want the 65536 default", got.PayloadMaxBytes)
	}
	if !got.TelemetryEnabled {
		t.Fatal("an unset limit must not silently switch telemetry off")
	}

	// A snapshot carrying no CaptureModes at all is the same "capture nothing,
	// keep the safe globals" case.
	c.ApplySnapshot(&pb.RegistrySnapshot{})
	if got := c.Capture(); got != registry.DefaultCaptureState() {
		t.Fatalf("snapshot without capture modes left %+v, want the fail-safe state", got)
	}
}

// TestCaptureAuthorityCanDisableTelemetry is the other half: when the runtime
// states a limit it also states the telemetry switch, and that answer wins.
func TestCaptureAuthorityCanDisableTelemetry(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		CaptureModes: &pb.CaptureModes{
			Rpc:              pb.CaptureMode_CAPTURE_MODE_ALL,
			TelemetryEnabled: false,
			PayloadMaxBytes:  2048,
		},
	})
	got := c.Capture()
	if got.TelemetryEnabled {
		t.Fatal("the runtime switched telemetry off and the SDK ignored it")
	}
	if got.PayloadMaxBytes != 2048 {
		t.Fatalf("payload cap %d, want 2048", got.PayloadMaxBytes)
	}

	// Switching it back on is a statement in its own right and survives an
	// otherwise unset limit.
	c.ApplyUpdate(&pb.RegistryUpdate{
		CaptureModes: &pb.CaptureModes{
			Rpc:              pb.CaptureMode_CAPTURE_MODE_ALL,
			TelemetryEnabled: true,
		},
	})
	if got := c.Capture(); !got.TelemetryEnabled || got.PayloadMaxBytes != 65536 {
		t.Fatalf("after re-enabling telemetry: %+v", got)
	}
}

// TestUnknownCaptureModeIsTreatedAsNone keeps a newer runtime from silently
// enabling capture the SDK cannot reason about.
func TestUnknownCaptureModeIsTreatedAsNone(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		CaptureModes: &pb.CaptureModes{
			Rpc:              pb.CaptureMode(99),
			Http:             pb.CaptureMode_CAPTURE_MODE_UNSPECIFIED,
			TelemetryEnabled: true,
			PayloadMaxBytes:  4096,
		},
	})
	got := c.Capture()
	if got.RPC != pb.CaptureMode_CAPTURE_MODE_NONE || got.HTTP != pb.CaptureMode_CAPTURE_MODE_NONE {
		t.Fatalf("unknown/unset modes became %s/%s, want NONE", got.RPC, got.HTTP)
	}
}

// ---------------------------------------------------------------------------
// cache: snapshot / update semantics
// ---------------------------------------------------------------------------

func TestSnapshotReplacesTheWholeCache(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods:            []*pb.MethodDescriptor{md("billing", "svc-b", "i1", "charge", "h1")},
		Instances:          []*pb.ServiceInstanceInfo{si("i1", "svc-b", "billing")},
		EventSubscriptions: []*pb.EventSubscriptionDescriptor{esd("svc-b", "billing", "order.*")},
		OutgoingCalls:      []*pb.OutgoingCallDescriptor{ocd("svc-a", "svc-b", "charge")},
	})

	change := c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods:            []*pb.MethodDescriptor{md("shipping", "svc-s", "i9", "ship", "h9")},
		Instances:          []*pb.ServiceInstanceInfo{si("i9", "svc-s", "shipping")},
		EventSubscriptions: []*pb.EventSubscriptionDescriptor{esd("svc-s", "shipping", "ship.*")},
		OutgoingCalls:      []*pb.OutgoingCallDescriptor{ocd("svc-s", "svc-b", "refund")},
	})

	if !change.Snapshot {
		t.Fatal("a snapshot frame must be marked as one")
	}
	if got := cacheMethodIDs(c); !reflect.DeepEqual(got, []string{"i9/ship"}) {
		t.Fatalf("methods after replacement: %v", got)
	}
	if got := cacheInstanceIDs(c); !reflect.DeepEqual(got, []string{"i9"}) {
		t.Fatalf("instances after replacement: %v", got)
	}
	if got := cachePatterns(c); !reflect.DeepEqual(got, []string{"svc-s/ship.*"}) {
		t.Fatalf("subscriptions after replacement: %v", got)
	}
	if got := cacheOutgoingEdges(c); !reflect.DeepEqual(got, []string{"svc-s->svc-b/refund"}) {
		t.Fatalf("outgoing edges after replacement: %v", got)
	}
	if got := c.Candidates("billing", "charge", ""); got != nil {
		t.Fatalf("the replaced world is still routable: %v", got)
	}
	if got := c.InstancesOf("billing"); got != nil {
		t.Fatalf("the replaced service still has instances: %v", got)
	}
}

// TestSnapshotReportsOnlyGoneRowsAsRemoved is the regression guard for the Node
// SDK bug: a snapshot listed every previously known instance as removed, so a
// consumer applying the delta literally dropped instances that were still live.
func TestSnapshotReportsOnlyGoneRowsAsRemoved(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods: []*pb.MethodDescriptor{
			md("billing", "svc-b", "i1", "charge", "h1"),
			md("billing", "svc-b", "i2", "charge", "h1"),
		},
		Instances: []*pb.ServiceInstanceInfo{
			si("i1", "svc-b", "billing"),
			si("i2", "svc-b", "billing"),
		},
	})

	// Scale-down: i2 is gone, i1 is still serving, i3 just joined.
	change := c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods: []*pb.MethodDescriptor{
			md("billing", "svc-b", "i1", "charge", "h1"),
			md("billing", "svc-b", "i3", "charge", "h1"),
		},
		Instances: []*pb.ServiceInstanceInfo{
			si("i1", "svc-b", "billing"),
			si("i3", "svc-b", "billing"),
		},
	})

	if got := instanceIDsOf(change.RemovedMethods); !reflect.DeepEqual(got, []string{"i2"}) {
		t.Fatalf("removed methods %v — a live instance was reported as gone", got)
	}
	if len(change.RemovedInstances) != 1 || change.RemovedInstances[0].GetInstanceId() != "i2" {
		t.Fatalf("removed instances %v — a live instance was reported as gone", change.RemovedInstances)
	}
}

func TestUpdateAppliesIncrementally(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods:   []*pb.MethodDescriptor{md("billing", "svc-b", "i1", "charge", "h1")},
		Instances: []*pb.ServiceInstanceInfo{si("i1", "svc-b", "billing")},
	})

	change := c.ApplyUpdate(&pb.RegistryUpdate{
		Added:                   []*pb.MethodDescriptor{md("billing", "svc-b", "i2", "charge", "h1")},
		AddedInstances:          []*pb.ServiceInstanceInfo{si("i2", "svc-b", "billing")},
		AddedEventSubscriptions: []*pb.EventSubscriptionDescriptor{esd("svc-b", "billing", "order.*")},
		AddedOutgoingCalls:      []*pb.OutgoingCallDescriptor{ocd("svc-b", "svc-s", "ship")},
		AddedPeers:              []string{"svc-s"},
	})

	if change.Snapshot {
		t.Fatal("an incremental frame must not claim to be a snapshot")
	}
	if !reflect.DeepEqual(change.AddedPeers, []string{"svc-s"}) {
		t.Fatalf("added peers %v", change.AddedPeers)
	}
	if got := instanceIDsOf(c.Candidates("billing", "charge", "")); !reflect.DeepEqual(got, []string{"i1", "i2"}) {
		t.Fatalf("candidates %v — the update did not fold onto the previous state", got)
	}

	c.ApplyUpdate(&pb.RegistryUpdate{
		Removed:                   []*pb.MethodDescriptor{md("billing", "svc-b", "i1", "charge", "h1")},
		RemovedInstances:          []*pb.ServiceInstanceInfo{si("i1", "svc-b", "billing")},
		RemovedEventSubscriptions: []*pb.EventSubscriptionDescriptor{esd("svc-b", "billing", "order.*")},
		RemovedOutgoingCalls:      []*pb.OutgoingCallDescriptor{ocd("svc-b", "svc-s", "ship")},
	})
	if got := instanceIDsOf(c.Candidates("billing", "charge", "")); !reflect.DeepEqual(got, []string{"i2"}) {
		t.Fatalf("candidates after removal: %v", got)
	}
	if got := cachePatterns(c); got != nil {
		t.Fatalf("subscriptions after removal: %v", got)
	}
	if got := cacheOutgoingEdges(c); got != nil {
		t.Fatalf("outgoing edges after removal: %v", got)
	}
}

// TestUpdateDoesNotInventRemovalsOfUnknownRows keeps a consumer from acting on
// a removal it never saw an addition for.
func TestUpdateDoesNotInventRemovalsOfUnknownRows(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{})

	change := c.ApplyUpdate(&pb.RegistryUpdate{
		Removed:          []*pb.MethodDescriptor{md("billing", "svc-b", "ghost", "charge", "h1")},
		RemovedInstances: []*pb.ServiceInstanceInfo{si("ghost", "svc-b", "billing")},
	})
	if len(change.RemovedMethods) != 0 || len(change.RemovedInstances) != 0 {
		t.Fatalf("phantom removals reported: %+v", change)
	}
}

// TestRemovedPeerPurgesEverythingTiedToIt covers the policy event: the peer
// left the caller's scope, so no trace of it may keep serving traffic.
func TestRemovedPeerPurgesEverythingTiedToIt(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods: []*pb.MethodDescriptor{
			md("billing", "svc-b", "i1", "charge", "h1"),
			md("billing", "svc-b", "i2", "charge", "h1"),
			md("shipping", "svc-s", "s1", "ship", "h2"),
		},
		Instances: []*pb.ServiceInstanceInfo{
			si("i1", "svc-b", "billing"),
			si("i2", "svc-b", "billing"),
			si("s1", "svc-s", "shipping"),
		},
		EventSubscriptions: []*pb.EventSubscriptionDescriptor{
			esd("svc-b", "billing", "order.*"),
			esd("svc-s", "shipping", "ship.*"),
		},
		OutgoingCalls: []*pb.OutgoingCallDescriptor{
			ocd("svc-a", "svc-b", "charge"),
			ocd("svc-a", "svc-s", "ship"),
		},
	})

	change := c.ApplyUpdate(&pb.RegistryUpdate{RemovedPeers: []string{"svc-b"}})

	if !reflect.DeepEqual(change.RemovedPeers, []string{"svc-b"}) {
		t.Fatalf("removed peers %v", change.RemovedPeers)
	}
	if got := instanceIDsOf(change.RemovedMethods); len(got) != 2 {
		t.Fatalf("removed methods %v, want both billing rows", got)
	}
	if len(change.RemovedInstances) != 2 {
		t.Fatalf("removed instances %v, want both billing instances", change.RemovedInstances)
	}
	if got := c.Candidates("billing", "charge", ""); got != nil {
		t.Fatalf("a purged peer is still routable: %v", got)
	}
	if got := c.InstancesOf("billing"); got != nil {
		t.Fatalf("a purged peer still has instances: %v", got)
	}
	if _, ok := c.Instance("i1"); ok {
		t.Fatal("a purged peer's instance is still resolvable by id")
	}
	if got := cachePatterns(c); !reflect.DeepEqual(got, []string{"svc-s/ship.*"}) {
		t.Fatalf("subscriptions after purge: %v", got)
	}
	if got := cacheOutgoingEdges(c); !reflect.DeepEqual(got, []string{"svc-a->svc-s/ship"}) {
		t.Fatalf("outgoing edges after purge: %v", got)
	}
	if got := cacheMethodIDs(c); !reflect.DeepEqual(got, []string{"s1/ship"}) {
		t.Fatalf("methods after purge: %v", got)
	}
}

// TestPolicyArrivesWholeAndReplacesThePrevious pins that a policy evaluation is
// never merged: a capability dropped from the new evaluation is revoked.
func TestPolicyArrivesWholeAndReplacesThePrevious(t *testing.T) {
	c := registry.NewCache()
	first := &pb.PolicyEvaluation{
		Capabilities: []string{"rpc.handle", "event.subscribe", "job.register"},
		Egress:       []*pb.PolicyRule{{Action: "allow", PeerServiceId: "svc-b"}},
	}
	change := c.ApplySnapshot(&pb.RegistrySnapshot{Policy: first})
	if change.Policy != first {
		t.Fatal("the snapshot's policy was not reported")
	}

	second := &pb.PolicyEvaluation{Capabilities: []string{"rpc.handle"}}
	change = c.ApplyUpdate(&pb.RegistryUpdate{Policy: second})
	if change.Policy != second {
		t.Fatal("the update's policy was not reported")
	}
	if got := c.Policy(); got != second {
		t.Fatalf("policy %v, want the whole new evaluation", got)
	}
	if got := c.Policy().GetCapabilities(); !reflect.DeepEqual(got, []string{"rpc.handle"}) {
		t.Fatalf("capabilities %v — the previous evaluation leaked through", got)
	}
	if got := c.Policy().GetEgress(); len(got) != 0 {
		t.Fatalf("egress rules %v — the previous evaluation leaked through", got)
	}

	// A frame carrying no policy leaves the last evaluation standing.
	if change := c.ApplyUpdate(&pb.RegistryUpdate{}); change.Policy != nil {
		t.Fatal("a frame without a policy reported one")
	}
	if got := c.Policy(); got != second {
		t.Fatalf("policy %v after a policy-less frame, want the last evaluation", got)
	}
}

// ---------------------------------------------------------------------------
// cache: the read path
// ---------------------------------------------------------------------------

func TestCandidatesNarrowByContractHash(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods: []*pb.MethodDescriptor{
			md("billing", "svc-b", "i1", "charge", "v1"),
			md("billing", "svc-b", "i2", "charge", "v2"),
			md("billing", "svc-b", "i3", "charge", "v1"),
			md("billing", "svc-b", "i1", "refund", "v1"),
		},
	})

	if got := instanceIDsOf(c.Candidates("billing", "charge", "")); !reflect.DeepEqual(got, []string{"i1", "i2", "i3"}) {
		t.Fatalf("unfiltered candidates %v", got)
	}
	if got := instanceIDsOf(c.Candidates("billing", "charge", "v1")); !reflect.DeepEqual(got, []string{"i1", "i3"}) {
		t.Fatalf("v1 candidates %v", got)
	}
	if got := c.Candidates("billing", "charge", "v3"); got != nil {
		t.Fatalf("candidates for an unknown contract: %v", got)
	}
	if got := c.Candidates("billing", "unknown", ""); got != nil {
		t.Fatalf("candidates for an unknown method: %v", got)
	}
}

var (
	sinkMethods   []*pb.MethodDescriptor
	sinkInstances []*pb.ServiceInstanceInfo
)

// TestReadsDoNotCopyTheMesh is the regression guard for the second Node SDK
// bug: the read path handed out a freshly built copy of the whole mesh, so
// every scale event allocated and threw away the entire descriptor set. Here
// two reads with no frame in between must return the very same backing array —
// which also proves the lookup is an index hit, not a scan that rebuilds a
// result.
func TestReadsDoNotCopyTheMesh(t *testing.T) {
	c := registry.NewCache()
	methods := make([]*pb.MethodDescriptor, 0, 2000)
	instances := make([]*pb.ServiceInstanceInfo, 0, 2000)
	for i := range 1000 {
		id := "i" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		methods = append(methods,
			md("billing", "svc-b", id, "charge", "v1"),
			md("shipping", "svc-s", id, "ship", "v1"),
		)
		instances = append(instances, si(id, "svc-b", "billing"))
	}
	c.ApplySnapshot(&pb.RegistrySnapshot{Methods: methods, Instances: instances})

	first := c.Candidates("billing", "charge", "v1")
	second := c.Candidates("billing", "charge", "v1")
	if len(first) != 1000 {
		t.Fatalf("got %d candidates, want 1000", len(first))
	}
	if &first[0] != &second[0] {
		t.Fatal("two reads with no frame in between rebuilt the candidate slice")
	}

	firstInst := c.InstancesOf("billing")
	secondInst := c.InstancesOf("billing")
	if len(firstInst) != 1000 {
		t.Fatalf("got %d instances, want 1000", len(firstInst))
	}
	if &firstInst[0] != &secondInst[0] {
		t.Fatal("two reads with no frame in between rebuilt the instance slice")
	}

	if allocs := testing.AllocsPerRun(50, func() {
		sinkMethods = c.Candidates("billing", "charge", "v1")
		sinkInstances = c.InstancesOf("billing")
	}); allocs != 0 {
		t.Fatalf("the read path allocated %v times per call over a 2000-descriptor mesh", allocs)
	}
}

// TestHandedOutCandidatesSurviveTheNextFrame backs the promise that makes the
// zero-copy read path safe: buckets are replaced, never mutated, so a slice a
// caller is iterating cannot change under it.
func TestHandedOutCandidatesSurviveTheNextFrame(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods: []*pb.MethodDescriptor{
			md("billing", "svc-b", "i1", "charge", "v1"),
			md("billing", "svc-b", "i2", "charge", "v1"),
		},
	})

	held := c.Candidates("billing", "charge", "")
	c.ApplyUpdate(&pb.RegistryUpdate{
		Removed: []*pb.MethodDescriptor{md("billing", "svc-b", "i1", "charge", "v1")},
	})

	if got := instanceIDsOf(held); !reflect.DeepEqual(got, []string{"i1", "i2"}) {
		t.Fatalf("the held slice mutated under the caller: %v", got)
	}
	if got := instanceIDsOf(c.Candidates("billing", "charge", "")); !reflect.DeepEqual(got, []string{"i2"}) {
		t.Fatalf("the next read did not see the removal: %v", got)
	}
}

// TestCandidateOrderIsStableAcrossRebuilds keeps load balancing reproducible:
// Go's map iteration order must never reach the call path.
func TestCandidateOrderIsStableAcrossRebuilds(t *testing.T) {
	build := func() []string {
		c := registry.NewCache()
		c.ApplySnapshot(&pb.RegistrySnapshot{
			Methods: []*pb.MethodDescriptor{
				md("billing", "svc-b", "i3", "charge", "v1"),
				md("billing", "svc-b", "i1", "charge", "v1"),
				md("billing", "svc-b", "i2", "charge", "v1"),
			},
		})
		return instanceIDsOf(c.Candidates("billing", "charge", ""))
	}
	want := []string{"i1", "i2", "i3"}
	for range 20 {
		if got := build(); !reflect.DeepEqual(got, want) {
			t.Fatalf("candidate order %v, want %v", got, want)
		}
	}
}

// TestInstanceMovedToAnotherServiceLeavesTheOldBucket covers the redeploy that
// reuses an instance id under a different service name.
func TestInstanceMovedToAnotherServiceLeavesTheOldBucket(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		Instances: []*pb.ServiceInstanceInfo{si("i1", "svc-b", "billing")},
	})

	c.ApplyUpdate(&pb.RegistryUpdate{
		AddedInstances: []*pb.ServiceInstanceInfo{si("i1", "svc-s", "shipping")},
	})

	if got := c.InstancesOf("billing"); got != nil {
		t.Fatalf("the instance is still listed under its old service: %v", got)
	}
	if got := c.InstancesOf("shipping"); len(got) != 1 || got[0].GetServiceId() != "svc-s" {
		t.Fatalf("instances of the new service: %v", got)
	}
	inst, ok := c.Instance("i1")
	if !ok || inst.GetServiceName() != "shipping" {
		t.Fatalf("lookup by id returned %v, %v", inst, ok)
	}
}

func TestEachWalkStopsOnFalse(t *testing.T) {
	c := registry.NewCache()
	c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods: []*pb.MethodDescriptor{
			md("billing", "svc-b", "i1", "charge", "v1"),
			md("billing", "svc-b", "i2", "charge", "v1"),
		},
		Instances: []*pb.ServiceInstanceInfo{
			si("i1", "svc-b", "billing"),
			si("i2", "svc-b", "billing"),
		},
		EventSubscriptions: []*pb.EventSubscriptionDescriptor{
			esd("svc-b", "billing", "a.*"),
			esd("svc-b", "billing", "b.*"),
		},
		OutgoingCalls: []*pb.OutgoingCallDescriptor{
			ocd("svc-a", "svc-b", "charge"),
			ocd("svc-a", "svc-b", "refund"),
		},
	})

	count := 0
	c.EachMethod(func(*pb.MethodDescriptor) bool { count++; return false })
	c.EachInstance(func(*pb.ServiceInstanceInfo) bool { count++; return false })
	c.EachEventSubscription(func(*pb.EventSubscriptionDescriptor) bool { count++; return false })
	c.EachOutgoingCall(func(*pb.OutgoingCallDescriptor) bool { count++; return false })
	if count != 4 {
		t.Fatalf("walks visited %d rows, want 1 per walk", count)
	}
}

// ---------------------------------------------------------------------------
// cache: deltas are literally applicable
// ---------------------------------------------------------------------------

type meshKey struct {
	instanceID string
	typ        pb.MethodType
	name       string
	published  bool
}

func meshKeyOf(d *pb.MethodDescriptor) meshKey {
	return meshKey{
		instanceID: d.GetInstanceId(),
		typ:        d.GetType(),
		name:       d.GetName(),
		published:  d.GetPublished(),
	}
}

// literalMesh is the consumer the Change contract promises is possible: it
// knows nothing about the cache and folds each frame exactly as handed over —
// a snapshot replaces the world, an update adds and then removes. The Node SDK
// broke precisely this consumer by listing live rows under "removed".
type literalMesh struct {
	methods   map[meshKey]*pb.MethodDescriptor
	instances map[string]*pb.ServiceInstanceInfo
}

func newLiteralMesh() *literalMesh {
	return &literalMesh{
		methods:   make(map[meshKey]*pb.MethodDescriptor),
		instances: make(map[string]*pb.ServiceInstanceInfo),
	}
}

func (m *literalMesh) apply(ch registry.Change) {
	if ch.Snapshot {
		m.methods = make(map[meshKey]*pb.MethodDescriptor)
		m.instances = make(map[string]*pb.ServiceInstanceInfo)
	}
	for _, d := range ch.AddedMethods {
		m.methods[meshKeyOf(d)] = d
	}
	for _, d := range ch.RemovedMethods {
		delete(m.methods, meshKeyOf(d))
	}
	for _, inst := range ch.AddedInstances {
		m.instances[inst.GetInstanceId()] = inst
	}
	for _, inst := range ch.RemovedInstances {
		delete(m.instances, inst.GetInstanceId())
	}
}

func (m *literalMesh) methodIDs() []string {
	got := make([]string, 0, len(m.methods))
	for k := range m.methods {
		got = append(got, k.instanceID+"/"+k.name)
	}
	sort.Strings(got)
	return got
}

func (m *literalMesh) instanceIDs() []string {
	got := make([]string, 0, len(m.instances))
	for id := range m.instances {
		got = append(got, id)
	}
	sort.Strings(got)
	return got
}

// TestDeltasApplyLiterallyAndConvergeWithTheCache walks a full mesh lifecycle —
// scale up, scale down, a peer leaving, a reconnect snapshot — and checks after
// every frame that a consumer folding the deltas verbatim holds exactly what
// the cache holds.
func TestDeltasApplyLiterallyAndConvergeWithTheCache(t *testing.T) {
	c := registry.NewCache()
	consumer := newLiteralMesh()

	converged := func(t *testing.T, step string) {
		t.Helper()
		if got, want := consumer.methodIDs(), cacheMethodIDs(c); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: consumer methods %v, cache %v", step, got, want)
		}
		if got, want := consumer.instanceIDs(), cacheInstanceIDs(c); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: consumer instances %v, cache %v", step, got, want)
		}
	}

	consumer.apply(c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods: []*pb.MethodDescriptor{
			md("billing", "svc-b", "i1", "charge", "v1"),
			md("billing", "svc-b", "i2", "charge", "v1"),
			md("shipping", "svc-s", "s1", "ship", "v1"),
		},
		Instances: []*pb.ServiceInstanceInfo{
			si("i1", "svc-b", "billing"),
			si("i2", "svc-b", "billing"),
			si("s1", "svc-s", "shipping"),
		},
	}))
	converged(t, "first snapshot")

	consumer.apply(c.ApplyUpdate(&pb.RegistryUpdate{
		Added:          []*pb.MethodDescriptor{md("billing", "svc-b", "i3", "charge", "v1")},
		AddedInstances: []*pb.ServiceInstanceInfo{si("i3", "svc-b", "billing")},
	}))
	converged(t, "scale up")

	consumer.apply(c.ApplyUpdate(&pb.RegistryUpdate{
		Removed:          []*pb.MethodDescriptor{md("billing", "svc-b", "i2", "charge", "v1")},
		RemovedInstances: []*pb.ServiceInstanceInfo{si("i2", "svc-b", "billing")},
	}))
	converged(t, "scale down")

	// A frame that adds rows for a peer and drops that peer in one go: the
	// removals the purge produced must cover the additions of the same frame.
	consumer.apply(c.ApplyUpdate(&pb.RegistryUpdate{
		Added:          []*pb.MethodDescriptor{md("shipping", "svc-s", "s2", "ship", "v1")},
		AddedInstances: []*pb.ServiceInstanceInfo{si("s2", "svc-s", "shipping")},
		RemovedPeers:   []string{"svc-s"},
	}))
	converged(t, "peer added and dropped in one frame")
	if got := c.Candidates("shipping", "ship", ""); got != nil {
		t.Fatalf("the dropped peer is still routable: %v", got)
	}

	// Reconnect: the snapshot keeps i1 alive and brings i4 in.
	consumer.apply(c.ApplySnapshot(&pb.RegistrySnapshot{
		Methods: []*pb.MethodDescriptor{
			md("billing", "svc-b", "i1", "charge", "v1"),
			md("billing", "svc-b", "i4", "charge", "v1"),
		},
		Instances: []*pb.ServiceInstanceInfo{
			si("i1", "svc-b", "billing"),
			si("i4", "svc-b", "billing"),
		},
	}))
	converged(t, "reconnect snapshot")
	if got := consumer.instanceIDs(); !reflect.DeepEqual(got, []string{"i1", "i4"}) {
		t.Fatalf("consumer instances after reconnect %v — a live instance was dropped", got)
	}
}

// ---------------------------------------------------------------------------
// watch: over a real gRPC stream on bufconn
// ---------------------------------------------------------------------------

// scriptedRegistry is a real Registry server: the watch reaches it through the
// generated stub over bufconn, so the lifecycle under test is the production
// one and not a hand-rolled stand-in.
type scriptedRegistry struct {
	pb.UnimplementedRegistryServer
	requests chan *pb.RegisterRequest
	handle   func(call int, srv pb.Registry_RegisterAndWatchServer) error
	calls    atomic.Int64
}

func (s *scriptedRegistry) RegisterAndWatch(req *pb.RegisterRequest, srv pb.Registry_RegisterAndWatchServer) error {
	n := int(s.calls.Add(1))
	select {
	case s.requests <- req:
	default:
	}
	return s.handle(n, srv)
}

func startRegistry(t *testing.T, handle func(call int, srv pb.Registry_RegisterAndWatchServer) error) (pb.RegistryClient, *scriptedRegistry) {
	t.Helper()

	srv := &scriptedRegistry{requests: make(chan *pb.RegisterRequest, 16), handle: handle}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterRegistryServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	})
	return pb.NewRegistryClient(conn), srv
}

type staticClients struct {
	client pb.RegistryClient
	fail   func() error
}

func (s staticClients) RegistryClient(context.Context) (pb.RegistryClient, error) {
	if s.fail != nil {
		if err := s.fail(); err != nil {
			return nil, err
		}
	}
	return s.client, nil
}

// switchingClients models the connection layer swapping the channel between
// opens, which is why the watch asks for a stub on every open.
type switchingClients struct {
	pick func() pb.RegistryClient
}

func (s switchingClients) RegistryClient(context.Context) (pb.RegistryClient, error) {
	return s.pick(), nil
}

func testBackoff() stream.Backoff {
	return stream.NewBackoff(
		stream.WithLadder(time.Millisecond, 2*time.Millisecond, 3*time.Millisecond),
		stream.WithJitterRatio(0),
	)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func recvWithin[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func snapshotEvent(s *pb.RegistrySnapshot) *pb.RegistryEvent {
	return &pb.RegistryEvent{Kind: &pb.RegistryEvent_Snapshot{Snapshot: s}}
}

func updateEvent(u *pb.RegistryUpdate) *pb.RegistryEvent {
	return &pb.RegistryEvent{Kind: &pb.RegistryEvent_Update{Update: u}}
}

// TestWatchRaisesPolicyWarningsWhileRegistrationSucceeds is the whole reason
// OnPolicyWarnings exists: the runtime skips a handler whose capability is
// denied and registers the rest, so the stream looks perfectly healthy while
// part of the service is silently not wired up.
func TestWatchRaisesPolicyWarningsWhileRegistrationSucceeds(t *testing.T) {
	violation := &pb.PolicyViolation{
		Declaration: "job:nightly-reconcile",
		Value:       "job.register",
		DenySide:    "acceptance",
		Reason:      "capability denied by access policy",
	}

	client, _ := startRegistry(t, func(_ int, srv pb.Registry_RegisterAndWatchServer) error {
		if err := srv.Send(snapshotEvent(&pb.RegistrySnapshot{
			Methods: []*pb.MethodDescriptor{md("billing", "svc-b", "i1", "charge", "v1")},
			Policy: &pb.PolicyEvaluation{
				Capabilities: []string{"rpc.handle"},
				Warnings:     []*pb.PolicyViolation{violation},
			},
		})); err != nil {
			return err
		}
		<-srv.Context().Done()
		return nil
	})

	warnings := make(chan []*pb.PolicyViolation, 4)
	changes := make(chan registry.Change, 8)
	w, err := registry.NewWatch(registry.WatchConfig{
		Clients:          staticClients{client: client},
		Request:          func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
		OnChange:         func(ch registry.Change) { changes <- ch },
		OnPolicyWarnings: func(v []*pb.PolicyViolation) { warnings <- v },
		Backoff:          testBackoff(),
		Logger:           quietLogger(),
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer w.Stop()

	got := recvWithin(t, warnings, "policy warnings")
	if len(got) != 1 {
		t.Fatalf("got %d warnings, want 1", len(got))
	}
	if got[0].GetDeclaration() != violation.GetDeclaration() || got[0].GetReason() != violation.GetReason() {
		t.Fatalf("warning %v does not carry what the runtime reported", got[0])
	}

	// Registration itself succeeded: a denied capability is a warning, not a
	// failure, and the rest of the mesh is usable.
	readyCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := w.Ready(readyCtx); err != nil {
		t.Fatalf("ready: %v — a policy warning must not fail registration", err)
	}
	if change := recvWithin(t, changes, "snapshot change"); change.Policy == nil {
		t.Fatal("the change handed to the consumer carries no policy")
	}
	if got := instanceIDsOf(w.Cache().Candidates("billing", "charge", "")); !reflect.DeepEqual(got, []string{"i1"}) {
		t.Fatalf("candidates %v — the accepted handlers did not land", got)
	}
}

// TestWatchWithoutPolicyWarningsStaysQuiet keeps the warning hook meaningful.
func TestWatchWithoutPolicyWarningsStaysQuiet(t *testing.T) {
	client, _ := startRegistry(t, func(_ int, srv pb.Registry_RegisterAndWatchServer) error {
		if err := srv.Send(snapshotEvent(&pb.RegistrySnapshot{
			Policy: &pb.PolicyEvaluation{Capabilities: []string{"rpc.handle"}},
		})); err != nil {
			return err
		}
		<-srv.Context().Done()
		return nil
	})

	warned := make(chan []*pb.PolicyViolation, 1)
	w, err := registry.NewWatch(registry.WatchConfig{
		Clients:          staticClients{client: client},
		Request:          func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
		OnPolicyWarnings: func(v []*pb.PolicyViolation) { warned <- v },
		Backoff:          testBackoff(),
		Logger:           quietLogger(),
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer w.Stop()

	readyCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := w.Ready(readyCtx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	select {
	case v := <-warned:
		t.Fatalf("a clean policy raised warnings: %v", v)
	default:
	}
}

// TestWatchRefusesAnUpdateBeforeTheFirstSnapshot: folding a delta onto a cache
// left over from a previous session would keep dead peers alive forever.
func TestWatchRefusesAnUpdateBeforeTheFirstSnapshot(t *testing.T) {
	client, _ := startRegistry(t, func(_ int, srv pb.Registry_RegisterAndWatchServer) error {
		if err := srv.Send(updateEvent(&pb.RegistryUpdate{
			Added: []*pb.MethodDescriptor{md("billing", "svc-b", "i1", "charge", "v1")},
		})); err != nil {
			return err
		}
		if err := srv.Send(snapshotEvent(&pb.RegistrySnapshot{
			Methods: []*pb.MethodDescriptor{md("billing", "svc-b", "i2", "charge", "v1")},
		})); err != nil {
			return err
		}
		// Once the snapshot has landed, deltas are the normal traffic.
		if err := srv.Send(updateEvent(&pb.RegistryUpdate{
			Added: []*pb.MethodDescriptor{md("billing", "svc-b", "i3", "charge", "v1")},
		})); err != nil {
			return err
		}
		<-srv.Context().Done()
		return nil
	})

	errs := make(chan error, 8)
	changes := make(chan registry.Change, 8)
	w, err := registry.NewWatch(registry.WatchConfig{
		Clients:  staticClients{client: client},
		Request:  func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
		OnChange: func(ch registry.Change) { changes <- ch },
		OnError:  func(err error) { errs <- err },
		Backoff:  testBackoff(),
		Logger:   quietLogger(),
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer w.Stop()

	reported := recvWithin(t, errs, "protocol violation")
	if !errors.Is(reported, registry.ErrProtocol) {
		t.Fatalf("reported %v, want ErrProtocol", reported)
	}

	readyCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := w.Ready(readyCtx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	if first := recvWithin(t, changes, "snapshot change"); !first.Snapshot {
		t.Fatalf("first delivered change is not a snapshot: %+v", first)
	}
	if second := recvWithin(t, changes, "update change"); second.Snapshot {
		t.Fatalf("the incremental frame was delivered as a snapshot: %+v", second)
	}
	if got := instanceIDsOf(w.Cache().Candidates("billing", "charge", "")); !reflect.DeepEqual(got, []string{"i2", "i3"}) {
		t.Fatalf("candidates %v — want the snapshot folded with the delta, and no trace of the rejected frame", got)
	}
}

func TestWatchRejectsAnUnknownEventKind(t *testing.T) {
	client, _ := startRegistry(t, func(_ int, srv pb.Registry_RegisterAndWatchServer) error {
		if err := srv.Send(&pb.RegistryEvent{}); err != nil {
			return err
		}
		<-srv.Context().Done()
		return nil
	})

	errs := make(chan error, 8)
	w, err := registry.NewWatch(registry.WatchConfig{
		Clients: staticClients{client: client},
		Request: func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
		OnError: func(err error) { errs <- err },
		Backoff: testBackoff(),
		Logger:  quietLogger(),
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer w.Stop()

	if reported := recvWithin(t, errs, "protocol violation"); !errors.Is(reported, registry.ErrProtocol) {
		t.Fatalf("reported %v, want ErrProtocol", reported)
	}
}

// TestWatchKeepsTheCacheAcrossABreakAndDemandsAFreshSnapshot covers the whole
// reconnect contract at once: the cache keeps serving while the stream is down,
// the reopened stream must lead with a snapshot, and that snapshot replaces the
// stale world.
func TestWatchKeepsTheCacheAcrossABreakAndDemandsAFreshSnapshot(t *testing.T) {
	reopened := make(chan struct{}, 1)
	sendUpdate := make(chan struct{})
	sendSnapshot := make(chan struct{})

	client, srv := startRegistry(t, func(call int, s pb.Registry_RegisterAndWatchServer) error {
		switch call {
		case 1:
			if err := s.Send(snapshotEvent(&pb.RegistrySnapshot{
				Methods:   []*pb.MethodDescriptor{md("billing", "svc-b", "i1", "charge", "v1")},
				Instances: []*pb.ServiceInstanceInfo{si("i1", "svc-b", "billing")},
			})); err != nil {
				return err
			}
			// A clean close: the supervisor reconnects on the ladder.
			return nil
		case 2:
			reopened <- struct{}{}
			<-sendUpdate
			// A delta on a stream that has not sent its snapshot yet is a broken
			// stream, no matter that the previous stream did send one.
			if err := s.Send(updateEvent(&pb.RegistryUpdate{
				Added: []*pb.MethodDescriptor{md("billing", "svc-b", "i9", "charge", "v1")},
			})); err != nil {
				return err
			}
			<-sendSnapshot
			if err := s.Send(snapshotEvent(&pb.RegistrySnapshot{
				Methods:   []*pb.MethodDescriptor{md("billing", "svc-b", "i2", "charge", "v1")},
				Instances: []*pb.ServiceInstanceInfo{si("i2", "svc-b", "billing")},
			})); err != nil {
				return err
			}
			<-s.Context().Done()
			return nil
		default:
			<-s.Context().Done()
			return nil
		}
	})

	declared := atomic.Int64{}
	errs := make(chan error, 8)
	changes := make(chan registry.Change, 16)
	w, err := registry.NewWatch(registry.WatchConfig{
		Clients: staticClients{client: client},
		Request: func() *pb.RegisterRequest {
			// A route declared by an HTTP integration after start must reach the
			// runtime on the next stream, so the frame is rebuilt on every open.
			n := declared.Add(1)
			req := &pb.RegisterRequest{}
			for i := int64(0); i < n; i++ {
				req.Incoming = append(req.Incoming, &pb.IncomingMethod{
					Type: pb.MethodType_METHOD_TYPE_HTTP,
					Name: "GET /route",
				})
			}
			return req
		},
		OnChange: func(ch registry.Change) { changes <- ch },
		OnError:  func(err error) { errs <- err },
		Backoff:  testBackoff(),
		Logger:   quietLogger(),
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer w.Stop()

	readyCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := w.Ready(readyCtx); err != nil {
		t.Fatalf("ready: %v", err)
	}
	recvWithin(t, changes, "first snapshot change")

	recvWithin(t, reopened, "stream reopen")
	// The stream is down and the new one has sent nothing: routing must keep
	// working off the last known world instead of falling into a hole.
	if got := instanceIDsOf(w.Cache().Candidates("billing", "charge", "")); !reflect.DeepEqual(got, []string{"i1"}) {
		t.Fatalf("candidates while reconnecting: %v, want the last known world", got)
	}

	close(sendUpdate)
	if reported := recvWithin(t, errs, "protocol violation on the reopened stream"); !errors.Is(reported, registry.ErrProtocol) {
		t.Fatalf("reported %v, want ErrProtocol", reported)
	}
	if got := instanceIDsOf(w.Cache().Candidates("billing", "charge", "")); !reflect.DeepEqual(got, []string{"i1"}) {
		t.Fatalf("candidates %v — the rejected delta reached the cache", got)
	}

	close(sendSnapshot)
	change := recvWithin(t, changes, "reconnect snapshot change")
	if !change.Snapshot {
		t.Fatalf("expected a snapshot change, got %+v", change)
	}
	if got := instanceIDsOf(w.Cache().Candidates("billing", "charge", "")); !reflect.DeepEqual(got, []string{"i2"}) {
		t.Fatalf("candidates after the reconnect snapshot: %v", got)
	}
	if _, ok := w.Cache().Instance("i1"); ok {
		t.Fatal("the stale instance survived the reconnect snapshot")
	}

	first := recvWithin(t, srv.requests, "first register request")
	second := recvWithin(t, srv.requests, "second register request")
	if len(first.GetIncoming()) != 1 || len(second.GetIncoming()) != 2 {
		t.Fatalf("register frames carried %d and %d declarations — the frame was not rebuilt on reopen",
			len(first.GetIncoming()), len(second.GetIncoming()))
	}
}

// TestWatchReportsOpenFailuresAndRecovers covers the normal start-up race: the
// identity is not provisioned yet, so the first opens fail.
func TestWatchReportsOpenFailuresAndRecovers(t *testing.T) {
	client, _ := startRegistry(t, func(_ int, srv pb.Registry_RegisterAndWatchServer) error {
		if err := srv.Send(snapshotEvent(&pb.RegistrySnapshot{})); err != nil {
			return err
		}
		<-srv.Context().Done()
		return nil
	})

	noIdentity := errors.New("no identity yet")
	var attempts atomic.Int64
	errs := make(chan error, 8)
	w, err := registry.NewWatch(registry.WatchConfig{
		Clients: staticClients{
			client: client,
			fail: func() error {
				if attempts.Add(1) <= 2 {
					return noIdentity
				}
				return nil
			},
		},
		Request: func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
		OnError: func(err error) { errs <- err },
		Backoff: testBackoff(),
		Logger:  quietLogger(),
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer w.Stop()

	for i := range 2 {
		if reported := recvWithin(t, errs, "open failure"); !errors.Is(reported, noIdentity) {
			t.Fatalf("open failure %d reported %v", i, reported)
		}
	}
	readyCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := w.Ready(readyCtx); err != nil {
		t.Fatalf("ready: %v — the watch never recovered", err)
	}
}

func TestWatchRestartOpensAFreshStream(t *testing.T) {
	opened := make(chan int, 8)
	client, _ := startRegistry(t, func(call int, srv pb.Registry_RegisterAndWatchServer) error {
		opened <- call
		if err := srv.Send(snapshotEvent(&pb.RegistrySnapshot{})); err != nil {
			return err
		}
		<-srv.Context().Done()
		return nil
	})

	w, err := registry.NewWatch(registry.WatchConfig{
		Clients: staticClients{client: client},
		Request: func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
		Backoff: testBackoff(),
		Logger:  quietLogger(),
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer w.Stop()

	if got := recvWithin(t, opened, "first open"); got != 1 {
		t.Fatalf("first call number %d", got)
	}
	w.Restart()
	if got := recvWithin(t, opened, "reopen after restart"); got != 2 {
		t.Fatalf("second call number %d", got)
	}
}

func TestWatchReadyRespectsItsContext(t *testing.T) {
	client, _ := startRegistry(t, func(_ int, srv pb.Registry_RegisterAndWatchServer) error {
		<-srv.Context().Done()
		return nil
	})

	w, err := registry.NewWatch(registry.WatchConfig{
		Clients: staticClients{client: client},
		Request: func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
		Backoff: testBackoff(),
		Logger:  quietLogger(),
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer w.Stop()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := w.Ready(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ready on a cancelled context: %v, want context.Canceled", err)
	}
}

func TestNewWatchRejectsIncompleteConfig(t *testing.T) {
	if _, err := registry.NewWatch(registry.WatchConfig{
		Request: func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
	}); !errors.Is(err, registry.ErrInvalidConfig) {
		t.Fatalf("missing client source: %v, want ErrInvalidConfig", err)
	}
	if _, err := registry.NewWatch(registry.WatchConfig{
		Clients: staticClients{},
	}); !errors.Is(err, registry.ErrInvalidConfig) {
		t.Fatalf("missing request builder: %v, want ErrInvalidConfig", err)
	}
}

// TestWatchStartIsSingleUse: the supervisor underneath is terminal, and a
// second Start must say so instead of silently running one more lifecycle.
func TestWatchStartIsSingleUse(t *testing.T) {
	client, _ := startRegistry(t, func(_ int, srv pb.Registry_RegisterAndWatchServer) error {
		<-srv.Context().Done()
		return nil
	})

	w, err := registry.NewWatch(registry.WatchConfig{
		Clients: staticClients{client: client},
		Request: func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
		Backoff: testBackoff(),
		Logger:  quietLogger(),
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer w.Stop()

	if err := w.Start(t.Context()); !errors.Is(err, stream.ErrAlreadyStarted) {
		t.Fatalf("second start: %v, want ErrAlreadyStarted", err)
	}
}

// TestWatchReportsAStreamThatCannotBeOpened covers the torn-down channel: the
// stub is there but the connection under it is gone, which is what a rotation
// racing a reopen looks like.
func TestWatchReportsAStreamThatCannotBeOpened(t *testing.T) {
	live, _ := startRegistry(t, func(_ int, srv pb.Registry_RegisterAndWatchServer) error {
		if err := srv.Send(snapshotEvent(&pb.RegistrySnapshot{})); err != nil {
			return err
		}
		<-srv.Context().Done()
		return nil
	})

	dead, err := grpc.NewClient("passthrough:///dead",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return nil, errors.New("no transport")
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("build dead client: %v", err)
	}
	if err := dead.Close(); err != nil {
		t.Fatalf("close dead client: %v", err)
	}

	var opens atomic.Int64
	errs := make(chan error, 8)
	w, err := registry.NewWatch(registry.WatchConfig{
		Clients: switchingClients{pick: func() pb.RegistryClient {
			if opens.Add(1) == 1 {
				return pb.NewRegistryClient(dead)
			}
			return live
		}},
		Request: func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
		OnError: func(err error) { errs <- err },
		Backoff: testBackoff(),
		Logger:  quietLogger(),
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if err := w.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer w.Stop()

	if reported := recvWithin(t, errs, "open failure"); reported == nil {
		t.Fatal("a stream that could not be opened was not reported")
	}
	readyCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := w.Ready(readyCtx); err != nil {
		t.Fatalf("ready: %v — the watch never recovered", err)
	}
}

// TestWatchCacheIsFailSafeBeforeItStarts pins that a watch hands out a usable,
// capture-nothing cache from construction. It also builds without a logger, the
// way the connection layer may wire it up.
func TestWatchCacheIsFailSafeBeforeItStarts(t *testing.T) {
	w, err := registry.NewWatch(registry.WatchConfig{
		Clients: staticClients{},
		Request: func() *pb.RegisterRequest { return &pb.RegisterRequest{} },
	})
	if err != nil {
		t.Fatalf("new watch: %v", err)
	}
	if got := w.Cache().Capture(); got != registry.DefaultCaptureState() {
		t.Fatalf("capture %+v before start, want the fail-safe state", got)
	}
	if w.Cache().Policy() != nil {
		t.Fatal("policy is known only from the runtime")
	}
}
