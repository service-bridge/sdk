package serde

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ErrNotProto marks a value the SDK cannot put on the wire. Every typed
// operation is parameterised by a generated protobuf type, so this is a
// programming error rather than a runtime condition.
var ErrNotProto = errors.New("serde: value is not a protobuf message")

// Payload is the pair of wire forms one message takes plus the identity of its
// schema.
type Payload struct {
	// Proto is the canonical encoding the runtime stores and forwards opaquely.
	Proto []byte
	// JSON mirrors Proto so the runtime can evaluate JSON-path wait_event
	// filters without decoding protobuf (ADR-0002).
	JSON         []byte
	ContractHash string
}

// Encode renders a payload into both wire forms.
//
// A protobuf message carries a schema, so it gets the canonical encoding, a
// JSON mirror and a contract hash. Anything else is a schema-less payload — the
// values a workflow graph carries are JSON trees by construction, and the
// runtime stores them opaquely — so both forms are its JSON encoding and the
// contract hash is the empty string, which matches only the empty string.
func Encode(payload any) (Payload, error) {
	msg, ok := payload.(proto.Message)
	if !ok {
		js, err := json.Marshal(payload)
		if err != nil {
			return Payload{}, fmt.Errorf("serde: encode %T: %w: %w", payload, ErrNotProto, err)
		}
		return Payload{Proto: js, JSON: js}, nil
	}
	bin, err := proto.Marshal(msg)
	if err != nil {
		return Payload{}, fmt.Errorf("serde: encode %T: proto: %w", payload, err)
	}
	js, err := protojson.Marshal(msg)
	if err != nil {
		return Payload{}, fmt.Errorf("serde: encode %T: json: %w", payload, err)
	}
	return Payload{
		Proto:        bin,
		JSON:         js,
		ContractHash: EventContractHash(msg.ProtoReflect().Descriptor()),
	}, nil
}

// Decode fills out from the canonical encoding. out is either a protobuf
// message or a pointer to one — the generic subscription path hands over a
// pointer to its still-nil type parameter, which this allocates.
func Decode(payload []byte, out any) error {
	switch dst := out.(type) {
	case proto.Message:
		return unmarshal(payload, dst)
	case *[]byte:
		// The raw subscription hands the payload over undecoded, which is the
		// only way to serve a name whose payload type varies by publisher.
		*dst = append([]byte(nil), payload...)
		return nil
	}
	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("serde: decode into %T: %w", out, ErrNotProto)
	}
	elem := rv.Elem()
	if elem.Kind() != reflect.Pointer {
		return fmt.Errorf("serde: decode into %T: %w", out, ErrNotProto)
	}
	if elem.IsNil() {
		elem.Set(reflect.New(elem.Type().Elem()))
	}
	msg, ok := elem.Interface().(proto.Message)
	if !ok {
		return fmt.Errorf("serde: decode into %T: %w", out, ErrNotProto)
	}
	return unmarshal(payload, msg)
}

func unmarshal(payload []byte, msg proto.Message) error {
	if err := proto.Unmarshal(payload, msg); err != nil {
		return fmt.Errorf("serde: decode %T: %w", msg, err)
	}
	return nil
}

// New mints an empty message of the same type as the template. The template may
// be a typed nil, which is how a type parameter reaches this without a value.
func New[T proto.Message](template T) T {
	return template.ProtoReflect().New().Interface().(T)
}

// DescriptorOf reads the schema of a message type off a typed nil, so a generic
// operation can learn its contract without an instance.
func DescriptorOf[T proto.Message](template T) protoreflect.MessageDescriptor {
	return template.ProtoReflect().Descriptor()
}
