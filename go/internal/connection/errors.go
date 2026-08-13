// Package connection establishes the SDK's mTLS identity with the runtime:
// bootstrap-key parsing, certificate provisioning, pinned TLS configuration and
// SPIFFE identity handling.
package connection

import (
	"errors"
	"fmt"
)

// Kind classifies a connection-layer failure. The root package maps it onto its
// own Code taxonomy; this package cannot import the root package because the
// root package imports this one.
type Kind string

const (
	KindKey       Kind = "KEY"
	KindProvision Kind = "PROVISION"
	KindTLS       Kind = "TLS"
	KindIdentity  Kind = "IDENTITY"
)

// Error is the only error type this package returns.
type Error struct {
	Kind Kind
	Op   string
	Msg  string
	Err  error
}

// Sentinels for errors.Is. They carry a Kind only — matching ignores Op, Msg and
// the wrapped cause.
var (
	ErrKey       = &Error{Kind: KindKey}
	ErrProvision = &Error{Kind: KindProvision}
	ErrTLS       = &Error{Kind: KindTLS}
	ErrIdentity  = &Error{Kind: KindIdentity}
)

func (e *Error) Error() string {
	msg := e.Msg
	if msg == "" && e.Err != nil {
		msg = e.Err.Error()
	} else if msg != "" && e.Err != nil {
		msg = msg + ": " + e.Err.Error()
	}
	if msg == "" {
		return fmt.Sprintf("connection: %s: %s", e.Op, e.Kind)
	}
	return fmt.Sprintf("connection: %s: %s", e.Op, msg)
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return t.Kind == e.Kind
}

func newError(kind Kind, op, msg string, cause error) *Error {
	return &Error{Kind: kind, Op: op, Msg: msg, Err: cause}
}
