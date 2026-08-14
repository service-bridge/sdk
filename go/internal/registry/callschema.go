package registry

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
)

// ErrSchemaConflict marks a second binding of the same dependency to a
// different pair of types. The two pairs hash to different contracts, so they
// name different deployed versions of the callee, and a caller holding both has
// no way to say which one a step meant.
var ErrSchemaConflict = errors.New("outgoing dependency is already bound to a different schema")

// CallSchema is what one declared RPC dependency is bound to. See ./README.md.
type CallSchema struct {
	// Input and Output carry their descriptors and may be typed nils: that is
	// how a type parameter reaches here without a value.
	Input        proto.Message
	Output       proto.Message
	ContractHash string
}

type callSchemaKey struct{ service, method string }

// CallSchemas is the caller-side table of declared RPC dependencies. See
// ./README.md.
type CallSchemas struct {
	mu sync.RWMutex
	m  map[callSchemaKey]CallSchema
}

// NewCallSchemas builds an empty table.
func NewCallSchemas() *CallSchemas {
	return &CallSchemas{m: make(map[callSchemaKey]CallSchema)}
}

// Bind records the schema of one dependency. Binding the same pair twice is the
// ordinary case — two handles on the same method — and is accepted; binding a
// different pair is refused.
func (s *CallSchemas) Bind(serviceName, methodName string, schema CallSchema) error {
	if serviceName == "" || methodName == "" {
		return fmt.Errorf("registry: bind call schema: %w", ErrEmptyName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := callSchemaKey{service: serviceName, method: methodName}
	if prev, seen := s.m[key]; seen && prev.ContractHash != schema.ContractHash {
		return fmt.Errorf("registry: bind call schema %s/%s: %w: %s and %s",
			serviceName, methodName, ErrSchemaConflict, prev.ContractHash, schema.ContractHash)
	}
	s.m[key] = schema
	return nil
}

// Lookup reports the schema bound to a dependency.
func (s *CallSchemas) Lookup(serviceName, methodName string) (CallSchema, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	schema, ok := s.m[callSchemaKey{service: serviceName, method: methodName}]
	return schema, ok
}
