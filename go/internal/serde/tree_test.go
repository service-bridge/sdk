package serde_test

import (
	"encoding/json"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/serde"
)

// TestTreeRoundTripKeeps64BitIntegersAndEnums is the assertion the whole
// workflow call path rests on: a value that leaves a callee, lands in run state
// as a JSON tree and is fed back into the next call must be the same value. The
// two shapes that silently degrade are exactly these — a 64-bit integer read as
// a JSON number loses everything past 2^53, and an enum read as a number is a
// different symbol from the one the schema names.
func TestTreeRoundTripKeeps64BitIntegersAndEnums(t *testing.T) {
	// 2^53+1 is the first integer a float64 cannot hold; the next two are the
	// extremes of the type.
	for _, n := range []int64{9007199254740993, 9223372036854775807, -9223372036854775808} {
		src := &pb.OpReport{
			OpId:          "op-1",
			StartedAtMs:   n,
			Channel:       pb.Channel_WORKFLOW,
			Status:        pb.Status_ERROR,
			StatusMessage: "boom",
		}
		raw, err := proto.Marshal(src)
		if err != nil {
			t.Fatalf("marshal source: %v", err)
		}

		tree, err := serde.DecodeTree(raw, (*pb.OpReport)(nil))
		if err != nil {
			t.Fatalf("decode tree: %v", err)
		}

		// The tree survives the trip through run state, which is plain JSON with
		// no knowledge of the schema. Doing it for real is what would catch a
		// 64-bit field rendered as a number.
		blob, err := json.Marshal(tree)
		if err != nil {
			t.Fatalf("encode run state: %v", err)
		}
		var restored any
		if err := json.Unmarshal(blob, &restored); err != nil {
			t.Fatalf("decode run state: %v", err)
		}

		back, err := serde.EncodeTree(restored, (*pb.OpReport)(nil))
		if err != nil {
			t.Fatalf("encode tree: %v", err)
		}
		var got pb.OpReport
		if err := proto.Unmarshal(back, &got); err != nil {
			t.Fatalf("unmarshal result: %v", err)
		}
		if !proto.Equal(src, &got) {
			t.Fatalf("round trip changed the message:\n  sent: %v\n  got:  %v", src, &got)
		}
		if got.GetStartedAtMs() != n {
			t.Errorf("startedAtMs came back %d, want %d", got.GetStartedAtMs(), n)
		}
		if got.GetChannel() != pb.Channel_WORKFLOW {
			t.Errorf("channel came back %v, want WORKFLOW", got.GetChannel())
		}
	}
}

// TestDecodeTreeRendersTheJSONMirror pins the shape the tree takes, because the
// rest of the graph reads it with JSON paths: a 64-bit field is a string there
// and an enum is its value name, exactly as JSONSchema promises.
func TestDecodeTreeRendersTheJSONMirror(t *testing.T) {
	raw, err := proto.Marshal(&pb.OpReport{StartedAtMs: 42, Channel: pb.Channel_RPC})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	tree, err := serde.DecodeTree(raw, (*pb.OpReport)(nil))
	if err != nil {
		t.Fatalf("decode tree: %v", err)
	}
	fields, ok := tree.(map[string]any)
	if !ok {
		t.Fatalf("tree is %T, want a JSON object", tree)
	}
	if got := fields["startedAtMs"]; got != "42" {
		t.Errorf("startedAtMs is %#v, want the string \"42\"", got)
	}
	if got := fields["channel"]; got != "RPC" {
		t.Errorf("channel is %#v, want the string \"RPC\"", got)
	}
}

// TestEncodeTreeAcceptsBothFormsOf64BitIntegers covers the graph author who
// writes a plain number for a 64-bit field: it is the natural thing to write and
// it is unambiguous below 2^53.
func TestEncodeTreeAcceptsBothFormsOf64BitIntegers(t *testing.T) {
	for name, tree := range map[string]any{
		"number": map[string]any{"startedAtMs": 1700000000000},
		"string": map[string]any{"startedAtMs": "1700000000000"},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := serde.EncodeTree(tree, (*pb.OpReport)(nil))
			if err != nil {
				t.Fatalf("encode tree: %v", err)
			}
			var got pb.OpReport
			if err := proto.Unmarshal(raw, &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got.GetStartedAtMs() != 1700000000000 {
				t.Errorf("startedAtMs is %d, want 1700000000000", got.GetStartedAtMs())
			}
		})
	}
}

// TestEncodeTreeRefusesAFieldTheMessageHasNoRoomFor keeps a misspelled field from
// arriving at the callee as an absent one. Dropping it silently would mean the
// handler runs on an input the graph never described.
func TestEncodeTreeRefusesAFieldTheMessageHasNoRoomFor(t *testing.T) {
	_, err := serde.EncodeTree(map[string]any{"opId": "x", "opid": "x"}, (*pb.OpReport)(nil))
	if !errors.Is(err, serde.ErrTreeShape) {
		t.Fatalf("error is %v, want ErrTreeShape", err)
	}
}

// TestEncodeTreeRefusesAValueOfTheWrongKind covers the other authoring mistake:
// the field exists, the value does not fit it.
func TestEncodeTreeRefusesAValueOfTheWrongKind(t *testing.T) {
	_, err := serde.EncodeTree(map[string]any{"startedAtMs": "not-a-number"}, (*pb.OpReport)(nil))
	if !errors.Is(err, serde.ErrTreeShape) {
		t.Fatalf("error is %v, want ErrTreeShape", err)
	}
}

// TestEncodeTreeReadsAnAbsentInputAsTheEmptyMessage covers the step that calls a
// method taking nothing.
func TestEncodeTreeReadsAnAbsentInputAsTheEmptyMessage(t *testing.T) {
	raw, err := serde.EncodeTree(nil, (*pb.OpReport)(nil))
	if err != nil {
		t.Fatalf("encode tree: %v", err)
	}
	if len(raw) != 0 {
		t.Fatalf("empty message encoded to %d bytes, want 0", len(raw))
	}
	tree, err := serde.DecodeTree(nil, (*pb.OpReport)(nil))
	if err != nil {
		t.Fatalf("decode tree: %v", err)
	}
	fields, ok := tree.(map[string]any)
	if !ok || len(fields) != 0 {
		t.Fatalf("empty payload decoded to %#v, want an empty object", tree)
	}
}

// TestDecodeTreeRefusesAPayloadOfAnotherType proves a reply that is not the
// declared response type is reported rather than half-read.
func TestDecodeTreeRefusesAPayloadOfAnotherType(t *testing.T) {
	if _, err := serde.DecodeTree([]byte("not protobuf at all"), (*pb.OpReport)(nil)); err == nil {
		t.Fatal("a payload of another type decoded without error")
	}
}
