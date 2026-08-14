// Package serde derives the wire identity of a protobuf contract and moves
// application messages between their Go form and the two wire forms the runtime
// stores.
package serde

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
)

// HashPrefix tags the algorithm generation inside the hash string itself, so a
// mesh running mixed generations never matches a hash computed under different
// rules.
const HashPrefix = "v2"

// ContractHash is the routing identity of an (input, output) message pair:
//
//	"v2:" + hex(sha256(canon(input) + ":" + canon(output)))
//
// canon is the language-neutral wire descriptor built by Canonical: field
// numbers, cardinality and types only. Field names are excluded because
// renaming a field is wire-compatible in protobuf and must not reroute live
// traffic, while a number, type or cardinality change must.
func ContractHash(in, out protoreflect.MessageDescriptor) string {
	var buf bytes.Buffer
	buf.Write(Canonical(in))
	buf.WriteByte(':')
	buf.Write(Canonical(out))
	sum := sha256.Sum256(buf.Bytes())
	return HashPrefix + ":" + hex.EncodeToString(sum[:])
}

// EventContractHash is the identity of an event payload. An event has no reply,
// so google.protobuf.Empty stands in for the output half — the same shape a
// one-way method would have, rather than a second hashing rule to keep in sync.
func EventContractHash(md protoreflect.MessageDescriptor) string {
	return ContractHash(md, (&emptypb.Empty{}).ProtoReflect().Descriptor())
}

// Canonical renders one message as the canonical JSON descriptor that feeds the
// contract hash:
//
//	{"f":[{"c":"opt","n":1,"t":"string"}],
//	 "o":[{"f":[4,5]}],
//	 "r":{".pkg.Item":{"f":[...]},".pkg.Kind":{"e":[0,1]}}}
//
// "o" is absent when the message has no oneof group, "r" when it references no
// other type. Full type names carry a leading dot.
//
// @internal — см. ./README.md
func Canonical(md protoreflect.MessageDescriptor) []byte {
	refs := map[string]any{}
	doc := shape(md, refs)
	if len(refs) > 0 {
		doc["r"] = refs
	}
	return encode(doc)
}

// shape builds one message's descriptor, recording everything it reaches in
// refs. The map is shared across the whole traversal, so reachability is
// transitive and every type is described exactly once.
func shape(md protoreflect.MessageDescriptor, refs map[string]any) map[string]any {
	fds := md.Fields()
	fields := make([]any, 0, fds.Len())
	for i := range fds.Len() {
		fields = append(fields, field(fds.Get(i), refs))
	}
	sort.Slice(fields, func(a, b int) bool {
		return fields[a].(map[string]any)["n"].(int) < fields[b].(map[string]any)["n"].(int)
	})

	doc := map[string]any{"f": fields}
	if groups := oneofs(md); len(groups) > 0 {
		doc["o"] = groups
	}
	return doc
}

// oneofs collects the real oneof groups. A proto3 `optional` field is a
// single-member synthetic oneof in the descriptor model but a plain singular
// field on the wire, so it is skipped.
func oneofs(md protoreflect.MessageDescriptor) []any {
	ods := md.Oneofs()
	groups := make([]any, 0, ods.Len())
	for i := range ods.Len() {
		od := ods.Get(i)
		if od.IsSynthetic() {
			continue
		}
		fds := od.Fields()
		nums := make([]int, 0, fds.Len())
		for j := range fds.Len() {
			nums = append(nums, int(fds.Get(j).Number()))
		}
		if len(nums) == 0 {
			continue
		}
		sort.Ints(nums)
		groups = append(groups, map[string]any{"f": nums})
	}
	sort.Slice(groups, func(a, b int) bool {
		return groups[a].(map[string]any)["f"].([]int)[0] < groups[b].(map[string]any)["f"].([]int)[0]
	})
	return groups
}

func field(fd protoreflect.FieldDescriptor, refs map[string]any) map[string]any {
	if fd.IsMap() {
		return map[string]any{
			"n": int(fd.Number()),
			"c": "map",
			"k": typeRef(fd.MapKey(), refs),
			"t": typeRef(fd.MapValue(), refs),
		}
	}
	card := "opt"
	switch fd.Cardinality() {
	case protoreflect.Repeated:
		card = "rep"
	case protoreflect.Required:
		card = "req"
	}
	return map[string]any{
		"n": int(fd.Number()),
		"c": card,
		"t": typeRef(fd, refs),
	}
}

// typeRef names the field's type and records the referenced message or enum.
func typeRef(fd protoreflect.FieldDescriptor, refs map[string]any) string {
	switch fd.Kind() {
	case protoreflect.EnumKind:
		ed := fd.Enum()
		name := "." + string(ed.FullName())
		if _, seen := refs[name]; !seen {
			refs[name] = map[string]any{"e": enumNumbers(ed)}
		}
		return "e:" + name
	case protoreflect.MessageKind, protoreflect.GroupKind:
		nested := fd.Message()
		name := "." + string(nested.FullName())
		if _, seen := refs[name]; !seen {
			// Claim the slot before recursing so a cycle resolves by name
			// against this placeholder instead of recursing forever.
			refs[name] = map[string]any{"f": []any{}}
			refs[name] = shape(nested, refs)
		}
		return "m:" + name
	default:
		return fd.Kind().String()
	}
}

func enumNumbers(ed protoreflect.EnumDescriptor) []int {
	vals := ed.Values()
	seen := make(map[int]struct{}, vals.Len())
	nums := make([]int, 0, vals.Len())
	for i := range vals.Len() {
		n := int(vals.Get(i).Number())
		if _, dup := seen[n]; dup {
			continue
		}
		seen[n] = struct{}{}
		nums = append(nums, n)
	}
	sort.Ints(nums)
	return nums
}

// encode writes the deterministic JSON the hash is taken over: object keys
// sorted at every level (encoding/json sorts map keys), array order preserved,
// no whitespace and no HTML escaping — escaping would make `<` in a type name
// produce bytes no other SDK produces.
func encode(v any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		// The document is built here from maps of strings, ints and slices of
		// the same. A value outside that set is unreachable.
		panic(fmt.Sprintf("serde: encode canonical descriptor: %v", err))
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}
