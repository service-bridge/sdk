package registry_test

import (
	"errors"
	"strconv"
	"sync"
	"testing"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/registry"
)

func TestCallSchemasBindAndLookup(t *testing.T) {
	schemas := registry.NewCallSchemas()
	want := registry.CallSchema{
		Input:        (*pb.OpReport)(nil),
		Output:       (*pb.TelemetryAck)(nil),
		ContractHash: "v2:abc",
	}
	if err := schemas.Bind("billing", "Charge", want); err != nil {
		t.Fatalf("bind: %v", err)
	}

	got, ok := schemas.Lookup("billing", "Charge")
	if !ok {
		t.Fatal("the binding just recorded is not there")
	}
	if got.ContractHash != want.ContractHash || got.Input != want.Input || got.Output != want.Output {
		t.Fatalf("lookup returned %+v, want %+v", got, want)
	}

	if _, ok := schemas.Lookup("billing", "Refund"); ok {
		t.Error("a method nobody bound resolved to a schema")
	}
	if _, ok := schemas.Lookup("shipping", "Charge"); ok {
		t.Error("the binding leaked across services")
	}
}

// TestCallSchemasAcceptTheSameBindingTwice covers the ordinary case of two
// handles on one method: the same pair of types, declared from two places.
func TestCallSchemasAcceptTheSameBindingTwice(t *testing.T) {
	schemas := registry.NewCallSchemas()
	schema := registry.CallSchema{
		Input:        (*pb.OpReport)(nil),
		Output:       (*pb.TelemetryAck)(nil),
		ContractHash: "v2:abc",
	}
	if err := schemas.Bind("billing", "Charge", schema); err != nil {
		t.Fatalf("first bind: %v", err)
	}
	if err := schemas.Bind("billing", "Charge", schema); err != nil {
		t.Fatalf("second bind of the same schema: %v", err)
	}
}

// TestCallSchemasRefuseTwoSchemasForOneMethod is the case a silent overwrite
// would turn into a routing bug: the two hashes name two deployed versions of
// the callee, and a step naming the method would reach whichever binding won.
func TestCallSchemasRefuseTwoSchemasForOneMethod(t *testing.T) {
	schemas := registry.NewCallSchemas()
	if err := schemas.Bind("billing", "Charge", registry.CallSchema{
		Input:        (*pb.OpReport)(nil),
		Output:       (*pb.TelemetryAck)(nil),
		ContractHash: "v2:abc",
	}); err != nil {
		t.Fatalf("first bind: %v", err)
	}
	err := schemas.Bind("billing", "Charge", registry.CallSchema{
		Input:        (*pb.Log)(nil),
		Output:       (*pb.TelemetryAck)(nil),
		ContractHash: "v2:def",
	})
	if !errors.Is(err, registry.ErrSchemaConflict) {
		t.Fatalf("error is %v, want ErrSchemaConflict", err)
	}

	kept, _ := schemas.Lookup("billing", "Charge")
	if kept.ContractHash != "v2:abc" {
		t.Errorf("the refused binding still replaced the first one: hash is %q", kept.ContractHash)
	}
}

func TestCallSchemasRefuseAnEmptyTarget(t *testing.T) {
	schemas := registry.NewCallSchemas()
	for _, tc := range []struct{ service, method string }{
		{"", "Charge"},
		{"billing", ""},
	} {
		if err := schemas.Bind(tc.service, tc.method, registry.CallSchema{}); !errors.Is(err, registry.ErrEmptyName) {
			t.Errorf("bind(%q, %q) returned %v, want ErrEmptyName", tc.service, tc.method, err)
		}
	}
}

// TestCallSchemasAreSafeUnderConcurrency matches how they are filled: NewMethod
// is called from wherever the application wires its dependencies up.
func TestCallSchemasAreSafeUnderConcurrency(t *testing.T) {
	schemas := registry.NewCallSchemas()
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			method := "Charge" + strconv.Itoa(i)
			if err := schemas.Bind("billing", method, registry.CallSchema{ContractHash: method}); err != nil {
				t.Errorf("bind %s: %v", method, err)
				return
			}
			if _, ok := schemas.Lookup("billing", method); !ok {
				t.Errorf("%s is missing right after it was bound", method)
			}
		}()
	}
	wg.Wait()
}
