package serde

import (
	"google.golang.org/protobuf/reflect/protoreflect"
)

// JSONSchema describes the JSON mirror of a message — the form the runtime
// stores in input_schema_json and the console renders. It describes protojson
// output, not the Go struct: 64-bit integers are strings there and enums are
// their value names, and a schema that claimed otherwise would not match the
// payload_json the runtime evaluates wait_event filters against.
func JSONSchema(md protoreflect.MessageDescriptor) []byte {
	return encode(objectSchema(md, map[protoreflect.FullName]struct{}{}))
}

func objectSchema(md protoreflect.MessageDescriptor, open map[protoreflect.FullName]struct{}) map[string]any {
	name := md.FullName()
	if _, cycle := open[name]; cycle {
		// A self-referential message has no finite expansion. The node keeps
		// its type and loses its detail rather than recursing forever.
		return map[string]any{"type": "object", "title": string(name)}
	}
	open[name] = struct{}{}
	defer delete(open, name)

	fds := md.Fields()
	props := make(map[string]any, fds.Len())
	for i := range fds.Len() {
		fd := fds.Get(i)
		props[fd.JSONName()] = fieldSchema(fd, open)
	}
	return map[string]any{
		"type":       "object",
		"title":      string(name),
		"properties": props,
	}
}

func fieldSchema(fd protoreflect.FieldDescriptor, open map[protoreflect.FullName]struct{}) map[string]any {
	switch {
	case fd.IsMap():
		return map[string]any{
			"type":                 "object",
			"additionalProperties": scalarSchema(fd.MapValue(), open),
		}
	case fd.IsList():
		return map[string]any{"type": "array", "items": scalarSchema(fd, open)}
	default:
		return scalarSchema(fd, open)
	}
}

func scalarSchema(fd protoreflect.FieldDescriptor, open map[protoreflect.FullName]struct{}) map[string]any {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return map[string]any{"type": "boolean"}
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return map[string]any{"type": "number"}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return map[string]any{"type": "integer"}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		// protojson renders 64-bit integers as strings: JSON numbers lose
		// precision past 2^53.
		return map[string]any{"type": "string", "format": "int64"}
	case protoreflect.BytesKind:
		return map[string]any{"type": "string", "contentEncoding": "base64"}
	case protoreflect.EnumKind:
		vals := fd.Enum().Values()
		names := make([]any, 0, vals.Len())
		for i := range vals.Len() {
			names = append(names, string(vals.Get(i).Name()))
		}
		return map[string]any{"type": "string", "enum": names}
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return objectSchema(fd.Message(), open)
	default:
		return map[string]any{"type": "string"}
	}
}
