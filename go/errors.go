package servicebridge

import (
	"errors"
	"fmt"

	"github.com/service-bridge/sdk/go/internal/events"
	"github.com/service-bridge/sdk/go/internal/outbox"
	"github.com/service-bridge/sdk/go/internal/rpc"
	"github.com/service-bridge/sdk/go/internal/serde"
	wfi "github.com/service-bridge/sdk/go/internal/workflow"
)

// Code classifies a failure. It is the single axis callers switch on.
type Code string

const (
	// CodeConfig marks a configuration the SDK refuses to run with. It never
	// reaches the reconnect ladder: a bad bound is not a network condition and
	// no amount of retrying fixes it.
	CodeConfig Code = "CONFIG"
	// CodeState marks an operation attempted in the wrong lifecycle phase —
	// declaring a handler after Start, calling before Start, using a stopped
	// client.
	CodeState Code = "STATE"
	// CodeConnection marks a control-plane failure: provisioning, the session,
	// or a stream that will not open.
	CodeConnection Code = "CONNECTION"
	// CodeAccessDenied marks a refusal by the mesh access policy.
	CodeAccessDenied Code = "ACCESS_DENIED"
	// CodeNotFound marks a name the mesh has no definition for.
	CodeNotFound Code = "NOT_FOUND"
	// CodeValidation marks a declaration or an argument the runtime would
	// reject, caught locally where it was written.
	CodeValidation Code = "VALIDATION"
	// CodeTerminal marks a workflow run that has already finished.
	CodeTerminal Code = "TERMINAL"
	// CodeOutboxFull marks a local event buffer at its cap.
	CodeOutboxFull Code = "OUTBOX_FULL"
	// CodeNoLiveInstance marks a call with nowhere to go: nothing publishes the
	// contract, nothing advertises an address, or everything is shedding.
	CodeNoLiveInstance Code = "NO_LIVE_INSTANCE"
	// CodeInvalidEventName marks a name the runtime's event grammar rejects.
	CodeInvalidEventName Code = "INVALID_EVENT_NAME"
	// CodeHandler marks a failure the callee's handler returned. It is an
	// answer, not a transport condition, and repeating it changes nothing.
	CodeHandler Code = "HANDLER"
	// CodeInternal marks everything else.
	CodeInternal Code = "INTERNAL"
)

// Error is the only error type this SDK returns, so errors.As against it is
// exhaustive by construction. The taxonomy lives in Code rather than in a
// family of unrelated types: with seven unrelated classes the only way to catch
// "an SDK error" is to name all seven, and the eighth one added later is caught
// by nobody.
type Error struct {
	Code Code
	Op   string
	Msg  string
	Err  error
}

// Sentinels for errors.Is. They carry a Code only — matching ignores Op, Msg
// and the wrapped cause.
var (
	ErrConfig           = &Error{Code: CodeConfig}
	ErrState            = &Error{Code: CodeState}
	ErrConnection       = &Error{Code: CodeConnection}
	ErrAccessDenied     = &Error{Code: CodeAccessDenied}
	ErrNotFound         = &Error{Code: CodeNotFound}
	ErrValidation       = &Error{Code: CodeValidation}
	ErrTerminal         = &Error{Code: CodeTerminal}
	ErrOutboxFull       = &Error{Code: CodeOutboxFull}
	ErrNoLiveInstance   = &Error{Code: CodeNoLiveInstance}
	ErrInvalidEventName = &Error{Code: CodeInvalidEventName}
	ErrHandler          = &Error{Code: CodeHandler}
	ErrInternal         = &Error{Code: CodeInternal}
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

// configError is the one failure New reports, and it is deliberately its own
// code: the Node SDK classified a bad bound by gRPC status, got "unknown" and
// reconnected forever with a message about provisioning.
func configError(op, msg string) *Error {
	return newError(CodeConfig, op, msg, nil)
}

// wrap classifies an error raised inside the SDK and presents it as the one
// public type. Classification is by sentinel rather than by message, so a
// reworded error in an internal package cannot silently change what callers
// see.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	var already *Error
	if errors.As(err, &already) {
		return err
	}
	return newError(classify(err), op, "", err)
}

func classify(err error) Code {
	switch {
	case errors.Is(err, rpc.ErrNoCandidates),
		errors.Is(err, rpc.ErrNoEndpoint),
		errors.Is(err, rpc.ErrAllUnavailable):
		return CodeNoLiveInstance

	case errors.Is(err, rpc.ErrAcceptanceDenied),
		errors.Is(err, wfi.ErrAccessDenied):
		return CodeAccessDenied

	case errors.Is(err, wfi.ErrWorkflowNotFound):
		return CodeNotFound

	case errors.Is(err, wfi.ErrRunTerminal):
		return CodeTerminal

	case errors.Is(err, outbox.ErrFull), errors.Is(err, events.ErrOutboxFull):
		return CodeOutboxFull

	case errors.Is(err, events.ErrInvalidName):
		return CodeInvalidEventName

	case errors.Is(err, rpc.ErrServerConfig),
		errors.Is(err, rpc.ErrInvalidConfig),
		errors.Is(err, events.ErrInvalidConfig),
		errors.Is(err, wfi.ErrInvalidConfig),
		errors.Is(err, outbox.ErrInvalidConfig),
		errors.Is(err, serde.ErrNotProto):
		return CodeConfig

	case errors.Is(err, rpc.ErrDecode),
		errors.Is(err, rpc.ErrEmptyMethod),
		errors.Is(err, rpc.ErrNoFunc),
		errors.Is(err, rpc.ErrDuplicate),
		isValidation(err):
		return CodeValidation

	case errors.Is(err, rpc.ErrSealed),
		errors.Is(err, outbox.ErrClosed),
		errors.Is(err, rpc.ErrServerClosed),
		errors.Is(err, rpc.ErrDirectClosed):
		return CodeState

	case errors.Is(err, rpc.ErrNoLease),
		errors.Is(err, wfi.ErrNoIdentity),
		errors.Is(err, wfi.ErrLeaseLost):
		return CodeConnection
	}

	var handlerErr *rpc.HandlerError
	if errors.As(err, &handlerErr) {
		return CodeHandler
	}
	var validation *wfi.ValidationError
	if errors.As(err, &validation) {
		return CodeValidation
	}
	return CodeInternal
}

// isValidation covers the declaration errors the registry and the job layer
// raise. They all mean the same thing to a caller: the thing you wrote would be
// refused by the runtime, and it was caught where you wrote it.
func isValidation(err error) bool {
	var pathErr *wfi.PathError
	return errors.As(err, &pathErr)
}
