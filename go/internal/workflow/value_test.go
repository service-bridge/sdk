package workflow

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	wf "github.com/service-bridge/sdk/go/workflow"
)

type opaqueInput struct {
	Order string `json:"order"`
	Qty   int    `json:"qty"`
}

type namedString string

func TestCanonicalValueCarriesEveryScalarShape(t *testing.T) {
	kind := "eur"
	var absent *string

	got, err := canonicalValue(map[string]any{
		"int":        int(1),
		"int8":       int8(2),
		"int16":      int16(3),
		"int32":      int32(4),
		"int64":      int64(5),
		"uint":       uint(6),
		"uint8":      uint8(7),
		"uint16":     uint16(8),
		"uint32":     uint32(9),
		"uint64":     uint64(10),
		"float32":    float32(1.5),
		"float64":    float64(2.5),
		"bool":       true,
		"named":      namedString("plain"),
		"pointer":    &kind,
		"nil":        absent,
		"typedSlice": []string{"a", "b"},
		"typedMap":   map[string]int{"n": 1},
		"array":      [2]int{1, 2},
		"bytes":      []byte("raw"),
		"opaque":     opaqueInput{Order: "o-1", Qty: 2},
		"nested":     []any{map[string]any{"deep": wf.Path("$.x")}},
	})
	if err != nil {
		t.Fatalf("canonicalise: %v", err)
	}

	blob, err := encodeCanonical(got)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(blob, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}

	for key, want := range map[string]any{
		"int": float64(1), "int8": float64(2), "int16": float64(3), "int32": float64(4),
		"int64": float64(5), "uint": float64(6), "uint8": float64(7), "uint16": float64(8),
		"uint32": float64(9), "uint64": float64(10), "float32": 1.5, "float64": 2.5,
		"bool": true, "named": "plain", "pointer": "eur", "nil": nil,
		// A byte slice is a JSON string, not an array of numbers.
		"bytes": "cmF3",
	} {
		if decoded[key] != want {
			t.Errorf("%s = %#v, want %#v", key, decoded[key], want)
		}
	}
	if !reflect.DeepEqual(decoded["typedSlice"], []any{"a", "b"}) {
		t.Errorf("typedSlice = %#v", decoded["typedSlice"])
	}
	if !reflect.DeepEqual(decoded["typedMap"], map[string]any{"n": float64(1)}) {
		t.Errorf("typedMap = %#v", decoded["typedMap"])
	}
	if !reflect.DeepEqual(decoded["array"], []any{float64(1), float64(2)}) {
		t.Errorf("array = %#v", decoded["array"])
	}
	if !reflect.DeepEqual(decoded["opaque"], map[string]any{"order": "o-1", "qty": float64(2)}) {
		t.Errorf("opaque = %#v", decoded["opaque"])
	}
	if !reflect.DeepEqual(decoded["nested"], []any{map[string]any{"deep": "$.x"}}) {
		t.Errorf("nested = %#v", decoded["nested"])
	}
}

func TestCanonicalValueRefusesNumbersWithNoJSONForm(t *testing.T) {
	for _, v := range []any{math.NaN(), math.Inf(1), math.Inf(-1), float32(math.Inf(1))} {
		if _, err := canonicalValue(map[string]any{"n": v}); !errors.Is(err, ErrGraphValue) {
			t.Errorf("%v: error %v, want ErrGraphValue", v, err)
		}
	}
}

func TestCanonicalValueRefusesWhatHasNoJSONForm(t *testing.T) {
	cases := map[string]any{
		"a channel":     make(chan int),
		"a function":    func() {},
		"a complex":     complex(1, 2),
		"a keyed map":   map[float64]string{1: "x"},
		"a nested func": []any{func() {}},
	}
	for name, v := range cases {
		if _, err := canonicalValue(v); !errors.Is(err, ErrGraphValue) {
			t.Errorf("%s: error %v, want ErrGraphValue", name, err)
		}
	}
}

func TestEvalValueLeavesOpaqueDataAlone(t *testing.T) {
	input := opaqueInput{Order: "o-1", Qty: 2}
	got, err := EvalValue(map[string]any{"body": input}, evalState())
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !reflect.DeepEqual(got, map[string]any{"body": input}) {
		t.Errorf("got %#v, want the value as written", got)
	}
}

func TestJSONEqualComparesTheWayJSONDoes(t *testing.T) {
	cases := []struct {
		name  string
		a, b  any
		equal bool
	}{
		{"two nils", nil, nil, true},
		{"a nil against a value", nil, 1, false},
		{"integers across types", int(1), float64(1), true},
		{"different numbers", 1, 2, false},
		{"a number against a string", 1, "1", false},
		{"equal strings", "a", "a", true},
		{"a string against a bool", "a", true, false},
		{"equal booleans", true, true, true},
		{"opposite booleans", true, false, false},
		{"a bool against a number", true, 1, false},
		{"equal lists", []any{1, "a"}, []any{float64(1), "a"}, true},
		{"lists of different length", []any{1}, []any{1, 2}, false},
		{"lists differing in content", []any{1}, []any{2}, false},
		{"a list against a scalar", []any{1}, 1, false},
		{"equal maps", map[string]any{"a": 1}, map[string]any{"a": float64(1)}, true},
		{"maps of different size", map[string]any{"a": 1}, map[string]any{"a": 1, "b": 2}, false},
		{"maps with different keys", map[string]any{"a": 1}, map[string]any{"b": 1}, false},
		{"maps with different values", map[string]any{"a": 1}, map[string]any{"a": 2}, false},
		{"a map against a list", map[string]any{"a": 1}, []any{1}, false},
		{"equal structs", opaqueInput{"o", 1}, opaqueInput{"o", 1}, true},
		{"different structs", opaqueInput{"o", 1}, opaqueInput{"o", 2}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := jsonEqual(tc.a, tc.b); got != tc.equal {
				t.Errorf("jsonEqual(%#v, %#v) = %v, want %v", tc.a, tc.b, got, tc.equal)
			}
		})
	}
}

func TestTruthinessFollowsTheValue(t *testing.T) {
	cases := []struct {
		value any
		want  bool
	}{
		{nil, false},
		{false, false},
		{true, true},
		{"", false},
		{"x", true},
		{0, false},
		{1, true},
		{float64(0), false},
		{0.5, true},
		{int8(0), false},
		{uint(2), true},
		{[]any{}, true},
		{map[string]any{}, true},
	}
	for _, tc := range cases {
		if got := isTruthy(tc.value); got != tc.want {
			t.Errorf("isTruthy(%#v) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestCanonicalEncodesEveryOptionField(t *testing.T) {
	frozen, err := Freeze("wf", wf.Definition{Steps: []wf.Step{
		wf.Call{
			Control: wf.Control{
				ID: "charge",
				Compensate: &wf.Compensation{
					Kind:           wf.CompensatePublish,
					Event:          wf.Name("order.refunded"),
					Input:          map[string]any{"txId": wf.Path("$.charge.txId")},
					IdempotencyKey: "refund-1",
					Retry:          &wf.RetryPolicy{MaxAttempts: 2, MaxDelay: 4 * time.Second},
				},
			},
			Service: wf.Name("billing"),
			Method:  wf.Name("Charge"),
			Opts: &wf.CallOpts{
				Timeout:        time.Second,
				Transport:      wf.TransportProxy,
				IdempotencyKey: "key-1",
				RequestID:      wf.Path("$.input.requestId"),
				Retry:          &wf.RetryPolicy{MaxAttempts: 4, BaseDelay: 100 * time.Millisecond, Factor: 1.5, MaxDelay: time.Second, Jitter: 0.2},
			},
		},
		wf.Publish{
			Control: wf.Control{ID: "announce"},
			Event:   wf.Name("order.charged"),
			Opts: &wf.PublishOpts{
				IdempotencyKey: "pub-1",
				PartitionKey:   wf.Path("$.input.orderId"),
				FireAndForget:  true,
				Headers:        map[string]any{"tenant": wf.Path("$.input.tenant")},
				OccurredAtMs:   1700000000000,
			},
		},
		wf.SubWorkflow{
			Control:  wf.Control{ID: "invoice"},
			Workflow: wf.Name("billing.invoice"),
			Opts:     &wf.StartOpts{IdempotencyKey: "start-1", TimeoutSec: 30},
		},
	}})
	if err != nil {
		t.Fatalf("freeze: %v", err)
	}

	var graph runtimeCanonicalGraph
	if err := json.Unmarshal(frozen.JSON, &graph); err != nil {
		t.Fatalf("decode: %v", err)
	}

	var charge map[string]any
	if err := json.Unmarshal(graph.Graph[0], &charge); err != nil {
		t.Fatalf("decode call: %v", err)
	}
	opts := charge["opts"].(map[string]any)
	for key, want := range map[string]any{
		"timeoutMs": float64(1000),
		"transport": "proxy",
		// A request id resolved from state travels as the expression it is.
		"requestId":      "$.input.requestId",
		"idempotencyKey": "key-1",
	} {
		if opts[key] != want {
			t.Errorf("opts.%s = %#v, want %#v", key, opts[key], want)
		}
	}
	retry := opts["retry"].(map[string]any)
	for key, want := range map[string]any{
		"maxAttempts": float64(4), "baseDelayMs": float64(100),
		"factor": 1.5, "maxDelayMs": float64(1000), "jitter": 0.2,
	} {
		if retry[key] != want {
			t.Errorf("opts.retry.%s = %#v, want %#v", key, retry[key], want)
		}
	}
	comp := charge["compensate"].(map[string]any)
	if comp["type"] != "publish" || comp["event"] != "order.refunded" || comp["idempotencyKey"] != "refund-1" {
		t.Errorf("compensate = %#v", comp)
	}
	if _, ok := comp["retry"]; !ok {
		t.Error("compensate.retry is absent")
	}

	var announce map[string]any
	if err := json.Unmarshal(graph.Graph[1], &announce); err != nil {
		t.Fatalf("decode publish: %v", err)
	}
	pub := announce["opts"].(map[string]any)
	for key, want := range map[string]any{
		"idempotencyKey": "pub-1",
		"partitionKey":   "$.input.orderId",
		"fireAndForget":  true,
		"occurredAtMs":   float64(1700000000000),
	} {
		if pub[key] != want {
			t.Errorf("publish opts.%s = %#v, want %#v", key, pub[key], want)
		}
	}
	if !reflect.DeepEqual(pub["headers"], map[string]any{"tenant": "$.input.tenant"}) {
		t.Errorf("publish opts.headers = %#v", pub["headers"])
	}

	var invoice map[string]any
	if err := json.Unmarshal(graph.Graph[2], &invoice); err != nil {
		t.Fatalf("decode workflow: %v", err)
	}
	start := invoice["opts"].(map[string]any)
	if start["idempotencyKey"] != "start-1" || start["timeoutSec"] != float64(30) {
		t.Errorf("start opts = %#v", start)
	}
}

func TestCanonicalRefusesAValueThatCannotTravel(t *testing.T) {
	// Freeze validates first, so reaching the encoder with a bad value takes
	// calling it directly.
	if _, err := canonicalStep(wf.Call{
		Control: wf.Control{ID: "a"},
		Service: wf.Name("s"),
		Method:  wf.Name("M"),
		Input:   map[string]any{"c": make(chan int)},
	}); !errors.Is(err, ErrGraphValue) {
		t.Errorf("error %v, want ErrGraphValue", err)
	}

	if _, err := canonicalStep(wf.Call{
		Control: wf.Control{ID: "a", When: wf.Equals(make(chan int), 1)},
		Service: wf.Name("s"),
		Method:  wf.Name("M"),
	}); !errors.Is(err, ErrGraphValue) {
		t.Errorf("condition: error %v, want ErrGraphValue", err)
	}

	if _, err := canonicalStep(wf.Call{
		Control: wf.Control{ID: "a", Compensate: &wf.Compensation{Input: make(chan int)}},
		Service: wf.Name("s"),
		Method:  wf.Name("M"),
	}); !errors.Is(err, ErrGraphValue) {
		t.Errorf("compensation: error %v, want ErrGraphValue", err)
	}
}

func TestNumbersCompareAcrossEveryWidth(t *testing.T) {
	widths := []any{
		int(1), int8(1), int16(1), int32(1), int64(1),
		uint(1), uint8(1), uint16(1), uint32(1), uint64(1),
		float32(1), float64(1),
	}
	for _, value := range widths {
		if !jsonEqual(value, float64(1)) {
			t.Errorf("%T(%v) did not compare equal to 1", value, value)
		}
		if !isTruthy(value) {
			t.Errorf("%T(%v) is not truthy", value, value)
		}
	}
}

func TestMapsCompareThroughTheirDeclaredType(t *testing.T) {
	if !jsonEqual(map[string]int{"a": 1}, map[string]any{"a": float64(1)}) {
		t.Error("a typed map did not compare equal to its JSON form")
	}
	if jsonEqual(map[string]int{"a": 1}, map[string]int{"a": 2}) {
		t.Error("typed maps with different values compared equal")
	}
}

func TestCanonicalValueReportsABadElementInsideAnyContainer(t *testing.T) {
	cases := map[string]any{
		"a typed slice": []chan int{make(chan int)},
		"an array":      [1]chan int{make(chan int)},
		"a typed map":   map[string]chan int{"c": make(chan int)},
		"a plain map":   map[string]any{"c": make(chan int)},
		"a plain slice": []any{make(chan int)},
		"behind a pointer": func() any {
			c := make(chan int)
			return &c
		}(),
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := canonicalValue(v); !errors.Is(err, ErrGraphValue) {
				t.Errorf("error %v, want ErrGraphValue", err)
			}
		})
	}
}
