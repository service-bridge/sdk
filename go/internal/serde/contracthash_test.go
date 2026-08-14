package serde_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/service-bridge/sdk/go/internal/serde"
)

// vectorFile is the cross-SDK golden set, read from the repository root rather
// than copied into testdata: every SDK hashes the same shapes to the same
// string or the mesh routes a caller to a callee it cannot talk to, and a copy
// would go stale exactly when the shared file changes.
const vectorFile = "../../../contract-hash-vectors.json"

type vectors struct {
	Vectors []struct {
		Name           string `json:"name"`
		Source         string `json:"source"`
		InputMessage   string `json:"inputMessage"`
		OutputMessage  string `json:"outputMessage"`
		CanonicalInput string `json:"canonicalInput"`
		CanonicalOut   string `json:"canonicalOutput"`
		ContractHash   string `json:"contractHash"`
	} `json:"vectors"`
	Properties []struct {
		Name    string   `json:"name"`
		Assert  string   `json:"assert"`
		Vectors []string `json:"vectors"`
	} `json:"properties"`
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	raw, err := os.ReadFile(vectorFile)
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	if len(v.Vectors) == 0 {
		t.Fatal("vector file carries no vectors")
	}
	return v
}

// descriptors loads the FileDescriptorSet protoc produced for one vector. Each
// vector gets its own registry: several vectors declare the same message name
// on purpose (a rename is one of the properties under test), and one shared
// registry could not hold both.
func descriptors(t *testing.T, vector string) *protoregistryFiles {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "desc", vector+".binpb"))
	if err != nil {
		t.Fatalf("%s: read descriptor set: %v", vector, err)
	}
	var set descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(raw, &set); err != nil {
		t.Fatalf("%s: parse descriptor set: %v", vector, err)
	}
	files, err := protodesc.NewFiles(&set)
	if err != nil {
		t.Fatalf("%s: build files: %v", vector, err)
	}
	return &protoregistryFiles{t: t, vector: vector, files: files}
}

type protoregistryFiles struct {
	t      *testing.T
	vector string
	files  interface {
		FindDescriptorByName(protoreflect.FullName) (protoreflect.Descriptor, error)
	}
}

func (p *protoregistryFiles) message(name string) protoreflect.MessageDescriptor {
	p.t.Helper()
	d, err := p.files.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		p.t.Fatalf("%s: find %s: %v", p.vector, name, err)
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		p.t.Fatalf("%s: %s is not a message", p.vector, name)
	}
	return md
}

// TestGoldenVectors is the whole point of the file: canonical form and hash
// must match the cross-SDK golden set byte for byte.
func TestGoldenVectors(t *testing.T) {
	v := loadVectors(t)
	proved := 0
	for _, vec := range v.Vectors {
		if vec.Source != "proto" {
			// .schema.json is a Node-only schema source: the Go SDK derives
			// every contract from a generated protobuf descriptor.
			continue
		}
		proved++
		t.Run(vec.Name, func(t *testing.T) {
			reg := descriptors(t, vec.Name)
			in := reg.message(vec.InputMessage)
			out := reg.message(vec.OutputMessage)

			if got := string(serde.Canonical(in)); got != vec.CanonicalInput {
				t.Errorf("canonical input:\n got %s\nwant %s", got, vec.CanonicalInput)
			}
			if got := string(serde.Canonical(out)); got != vec.CanonicalOut {
				t.Errorf("canonical output:\n got %s\nwant %s", got, vec.CanonicalOut)
			}
			if got := serde.ContractHash(in, out); got != vec.ContractHash {
				t.Errorf("contract hash: got %s want %s", got, vec.ContractHash)
			}
		})
	}
	if proved == 0 {
		t.Fatal("no proto-source vector ran")
	}
}

// TestVectorProperties checks the assertions the vector file states about pairs
// of vectors: what must keep an identity and what must change it.
func TestVectorProperties(t *testing.T) {
	v := loadVectors(t)
	hashes := map[string]string{}
	for _, vec := range v.Vectors {
		if vec.Source != "proto" {
			continue
		}
		reg := descriptors(t, vec.Name)
		hashes[vec.Name] = serde.ContractHash(reg.message(vec.InputMessage), reg.message(vec.OutputMessage))
	}

	ran := 0
	for _, prop := range v.Properties {
		if len(prop.Vectors) != 2 {
			t.Fatalf("property %q: expected two vectors, got %d", prop.Name, len(prop.Vectors))
		}
		a, okA := hashes[prop.Vectors[0]]
		b, okB := hashes[prop.Vectors[1]]
		if !okA || !okB {
			continue // property over .schema.json vectors
		}
		ran++
		t.Run(prop.Name, func(t *testing.T) {
			switch prop.Assert {
			case "equal":
				if a != b {
					t.Errorf("%s and %s must hash alike: %s vs %s", prop.Vectors[0], prop.Vectors[1], a, b)
				}
			case "different":
				if a == b {
					t.Errorf("%s and %s must hash apart, both %s", prop.Vectors[0], prop.Vectors[1], a)
				}
			default:
				t.Fatalf("unknown assertion %q", prop.Assert)
			}
		})
	}
	if ran == 0 {
		t.Fatal("no property ran")
	}
}

// TestEmptyHashMatchesOnlyItself pins the rule the runtime routes by: a handler
// registered without a schema declares the empty string, and the empty string
// matches nothing else.
func TestEmptyHashMatchesOnlyItself(t *testing.T) {
	reg := descriptors(t, "empty_message")
	in := reg.message("sb.vectors.empty.EmptyRequest")
	out := reg.message("sb.vectors.empty.EmptyResponse")
	if got := serde.ContractHash(in, out); got == "" {
		t.Fatal("a described contract must never hash to the empty string")
	}
}

// TestEventHashIsStableAndSchemaSensitive pins the one-way identity: an event
// has no reply, so the output half is the empty message. The hash still has to
// move when the payload shape moves.
func TestEventHashIsStableAndSchemaSensitive(t *testing.T) {
	plain := descriptors(t, "scalars").message("sb.vectors.scalars.ScalarsRequest")
	renamed := descriptors(t, "scalars_renamed").message("sb.vectors.scalars.ScalarsRequest")
	renumbered := descriptors(t, "scalars_renumbered").message("sb.vectors.scalars.ScalarsRequest")

	if serde.EventContractHash(plain) != serde.EventContractHash(renamed) {
		t.Error("renaming a field must not change the event identity")
	}
	if serde.EventContractHash(plain) == serde.EventContractHash(renumbered) {
		t.Error("renumbering a field must change the event identity")
	}
}

// TestJSONSchemaDescribesTheJSONMirror keeps the schema honest about protojson:
// 64-bit integers are strings there, and a wait_event filter written against an
// integer would never match.
func TestJSONSchemaDescribesTheJSONMirror(t *testing.T) {
	md := descriptors(t, "scalars").message("sb.vectors.scalars.ScalarsRequest")
	var doc struct {
		Type       string `json:"type"`
		Properties map[string]struct {
			Type   string `json:"type"`
			Format string `json:"format"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(serde.JSONSchema(md), &doc); err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	if doc.Type != "object" {
		t.Errorf("root type: got %q want object", doc.Type)
	}
	if got := doc.Properties["fInt64"].Type; got != "string" {
		t.Errorf("int64 property type: got %q want string", got)
	}
	if got := doc.Properties["fInt32"].Type; got != "integer" {
		t.Errorf("int32 property type: got %q want integer", got)
	}
	if got := doc.Properties["fBytes"].Type; got != "string" {
		t.Errorf("bytes property type: got %q want string", got)
	}
}

// TestJSONSchemaTerminatesOnACycle proves the recursion guard: a self
// referential message must produce a schema rather than a stack overflow.
func TestJSONSchemaTerminatesOnACycle(t *testing.T) {
	md := descriptors(t, "cyclic").message("sb.vectors.cyclic.Node")
	if len(serde.JSONSchema(md)) == 0 {
		t.Fatal("cyclic message produced no schema")
	}
}
