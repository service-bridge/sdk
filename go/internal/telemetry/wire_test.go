package telemetry

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

func TestFormatParseRoundTrip(t *testing.T) {
	want := TraceContext{
		TraceID:    uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcde"),
		ParentOpID: uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcdf"),
	}

	value := FormatHeader(want)
	if len(value) != 73 {
		t.Fatalf("header length = %d, want 73: %q", len(value), value)
	}
	if value[36] != '-' {
		t.Fatalf("separator at 36 = %q, want '-'", value[36])
	}

	got, err := ParseHeader(value)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if got != want {
		t.Fatalf("ParseHeader = %+v, want %+v", got, want)
	}
}

func TestFormatRootHeader(t *testing.T) {
	tc := TraceContext{TraceID: uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcde")}

	value := FormatHeader(tc)

	if !strings.HasSuffix(value, "00000000-0000-0000-0000-000000000000") {
		t.Fatalf("root header = %q, want a zero parent half", value)
	}
	got, err := ParseHeader(value)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if !got.Root() {
		t.Fatalf("parsed root header carries parent %s", got.ParentOpID)
	}
}

// A malformed header is not an error — it means the caller sent no usable
// trace, so the operation starts a new tree (ADR-0006 §6).
func TestParseMalformedMintsNewRoot(t *testing.T) {
	valid := "018f3a2b-1c4d-7e8f-9012-3456789abcde-018f3a2b-1c4d-7e8f-9012-3456789abcdf"

	cases := map[string]string{
		"empty":              "",
		"garbage":            "not-a-trace",
		"too short":          valid[:72],
		"too long":           valid + "0",
		"separator shifted":  "018f3a2b-1c4d-7e8f-9012-3456789abcd-e018f3a2b-1c4d-7e8f-9012-3456789abcdf",
		"separator replaced": valid[:36] + "_" + valid[37:],
		"bad trace half":     "zzzzzzzz-1c4d-7e8f-9012-3456789abcde-018f3a2b-1c4d-7e8f-9012-3456789abcdf",
		"bad parent half":    "018f3a2b-1c4d-7e8f-9012-3456789abcde-zzzzzzzz-1c4d-7e8f-9012-3456789abcdf",
		"only trace":         valid[:36],
		"whitespace padded":  " " + valid[:72],
	}

	seen := make(map[uuid.UUID]struct{}, len(cases))
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseHeader(value)
			if err != nil {
				t.Fatalf("ParseHeader(%q) returned an error: %v", value, err)
			}
			if got.TraceID == uuid.Nil {
				t.Fatalf("ParseHeader(%q) produced a nil trace id", value)
			}
			if !got.Root() {
				t.Fatalf("ParseHeader(%q) produced parent %s, want a root", value, got.ParentOpID)
			}
			if _, dup := seen[got.TraceID]; dup {
				t.Fatalf("ParseHeader(%q) reused a trace id", value)
			}
			seen[got.TraceID] = struct{}{}
		})
	}
}

func TestParseAcceptsUppercase(t *testing.T) {
	want := TraceContext{
		TraceID:    uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcde"),
		ParentOpID: uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcdf"),
	}

	got, err := ParseHeader(strings.ToUpper(FormatHeader(want)))
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}
	if got != want {
		t.Fatalf("ParseHeader = %+v, want %+v", got, want)
	}
}

func TestMetadataRoundTrip(t *testing.T) {
	want := TraceContext{
		TraceID:    uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcde"),
		ParentOpID: uuid.MustParse("018f3a2b-1c4d-7e8f-9012-3456789abcdf"),
	}

	out := InjectMetadata(context.Background(), want)
	md, ok := metadata.FromOutgoingContext(out)
	if !ok {
		t.Fatal("InjectMetadata wrote no outgoing metadata")
	}
	got, err := TraceFromMetadata(metadata.NewIncomingContext(context.Background(), md))
	if err != nil {
		t.Fatalf("TraceFromMetadata: %v", err)
	}
	if got != want {
		t.Fatalf("TraceFromMetadata = %+v, want %+v", got, want)
	}
}

func TestTraceFromMetadataWithoutKey(t *testing.T) {
	got, err := TraceFromMetadata(context.Background())
	if err != nil {
		t.Fatalf("TraceFromMetadata: %v", err)
	}
	if got.TraceID == uuid.Nil || !got.Root() {
		t.Fatalf("TraceFromMetadata = %+v, want a fresh root", got)
	}

	empty := metadata.NewIncomingContext(context.Background(), metadata.New(nil))
	got, err = TraceFromMetadata(empty)
	if err != nil {
		t.Fatalf("TraceFromMetadata: %v", err)
	}
	if got.TraceID == uuid.Nil || !got.Root() {
		t.Fatalf("TraceFromMetadata = %+v, want a fresh root", got)
	}
}
