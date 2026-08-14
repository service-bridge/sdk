package telemetry

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestTraceContextRoundTrip(t *testing.T) {
	want := TraceContext{
		TraceID:    uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcde"),
		ParentOpID: uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcdf"),
	}

	ctx := WithTraceContext(context.Background(), want)

	got, ok := FromContext(ctx)
	if !ok {
		t.Fatal("FromContext: no context found")
	}
	if got != want {
		t.Fatalf("FromContext = %+v, want %+v", got, want)
	}
}

func TestFromContextWithoutTrace(t *testing.T) {
	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("FromContext on a bare context reported a trace")
	}
}

func TestTraceContextChild(t *testing.T) {
	parent := TraceContext{
		TraceID:    uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcde"),
		ParentOpID: uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcdf"),
	}
	opID := uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abce0")

	child := parent.Child(opID)

	if child.TraceID != parent.TraceID {
		t.Fatalf("child trace = %s, want %s", child.TraceID, parent.TraceID)
	}
	if child.ParentOpID != opID {
		t.Fatalf("child parent = %s, want %s", child.ParentOpID, opID)
	}
	if child.Root() {
		t.Fatal("child reported itself as root")
	}
}

func TestNewRootContext(t *testing.T) {
	tc, err := NewRootContext()
	if err != nil {
		t.Fatalf("NewRootContext: %v", err)
	}
	if tc.TraceID == uuid.Nil {
		t.Fatal("root trace id is nil")
	}
	if tc.TraceID.Version() != 7 {
		t.Fatalf("trace id version = %d, want 7", tc.TraceID.Version())
	}
	if !tc.Root() {
		t.Fatal("fresh root reported a parent")
	}

	other, err := NewRootContext()
	if err != nil {
		t.Fatalf("NewRootContext: %v", err)
	}
	if other.TraceID == tc.TraceID {
		t.Fatal("two root contexts share a trace id")
	}
}
