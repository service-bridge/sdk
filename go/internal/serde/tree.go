package serde

import (
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ErrTreeShape marks a JSON tree that does not fit the message it is read into.
// It is an authoring mistake — a workflow graph naming a field the callee has no
// room for, or putting a string where a number belongs — and no retry changes
// the answer.
var ErrTreeShape = errors.New("serde: value does not fit the message schema")

// EncodeTree reads a JSON tree into the template's message type and renders the
// canonical encoding.
//
// The tree crosses over through protojson rather than through the Go struct, so
// it goes in under exactly the rules JSONSchema describes: 64-bit integers are
// strings and enums are value names. That is the same form the runtime keeps in
// payload_json, so a value that left one step can enter the next unchanged.
//
// An absent tree is the empty message: a step that declares no input still
// calls a method, and protojson has no reading for a bare null.
func EncodeTree(tree any, template proto.Message) ([]byte, error) {
	msg := New(template)
	if tree != nil {
		js, err := json.Marshal(tree)
		if err != nil {
			return nil, fmt.Errorf("serde: encode tree into %T: %w: %w", template, ErrTreeShape, err)
		}
		if err := protojson.Unmarshal(js, msg); err != nil {
			return nil, fmt.Errorf("serde: encode tree into %T: %w: %w", template, ErrTreeShape, err)
		}
	}
	bin, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("serde: encode tree into %T: proto: %w", template, err)
	}
	return bin, nil
}

// DecodeTree renders the canonical encoding back into a JSON tree of the shape
// EncodeTree accepts. An empty payload is an empty message, which is what a
// method answering nothing sends.
func DecodeTree(raw []byte, template proto.Message) (any, error) {
	msg := New(template)
	if err := proto.Unmarshal(raw, msg); err != nil {
		return nil, fmt.Errorf("serde: decode tree from %T: %w", template, err)
	}
	js, err := protojson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("serde: decode tree from %T: json: %w", template, err)
	}
	var out any
	if err := json.Unmarshal(js, &out); err != nil {
		return nil, fmt.Errorf("serde: decode tree from %T: %w", template, err)
	}
	return out, nil
}
