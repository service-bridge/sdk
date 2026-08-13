package servicebridge

import (
	"errors"
	"fmt"
)

// Code classifies a failure. It is the single axis callers switch on.
type Code string

const (
	CodeUnimplemented  Code = "UNIMPLEMENTED"
	CodeConfig         Code = "CONFIG"
	CodeConnection     Code = "CONNECTION"
	CodeAccessDenied   Code = "ACCESS_DENIED"
	CodeNotFound       Code = "NOT_FOUND"
	CodeValidation     Code = "VALIDATION"
	CodeTerminal       Code = "TERMINAL"
	CodeOutboxFull     Code = "OUTBOX_FULL"
	CodeNoLiveInstance Code = "NO_LIVE_INSTANCE"
)

// Error is the only error type this SDK returns. A single type keeps
// errors.As(err, &sbErr) exhaustive; the taxonomy lives in Code, not in a
// family of unrelated types.
type Error struct {
	Code Code
	Op   string
	Msg  string
	Err  error
}

// Sentinels for errors.Is. They carry a Code only — matching ignores Op, Msg
// and the wrapped cause.
var (
	ErrUnimplemented  = &Error{Code: CodeUnimplemented}
	ErrConfig         = &Error{Code: CodeConfig}
	ErrConnection     = &Error{Code: CodeConnection}
	ErrAccessDenied   = &Error{Code: CodeAccessDenied}
	ErrNotFound       = &Error{Code: CodeNotFound}
	ErrValidation     = &Error{Code: CodeValidation}
	ErrTerminal       = &Error{Code: CodeTerminal}
	ErrOutboxFull     = &Error{Code: CodeOutboxFull}
	ErrNoLiveInstance = &Error{Code: CodeNoLiveInstance}
)

func (e *Error) Error() string {
	msg := e.Msg
	if msg == "" && e.Err != nil {
		msg = e.Err.Error()
	}
	switch {
	case e.Op != "" && msg != "":
		return fmt.Sprintf("%s: %s: %s", e.Op, e.Code, msg)
	case e.Op != "":
		return fmt.Sprintf("%s: %s", e.Op, e.Code)
	case msg != "":
		return fmt.Sprintf("%s: %s", e.Code, msg)
	default:
		return string(e.Code)
	}
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return t.Code == e.Code
}

func newError(code Code, op, msg string, cause error) *Error {
	return &Error{Code: code, Op: op, Msg: msg, Err: cause}
}
