package servicebridge

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/service-bridge/sdk/go/internal/connection"
	"github.com/service-bridge/sdk/go/internal/events"
	jobi "github.com/service-bridge/sdk/go/internal/job"
	"github.com/service-bridge/sdk/go/internal/registry"
	"github.com/service-bridge/sdk/go/internal/rpc"
	"github.com/service-bridge/sdk/go/internal/serde"
	wfi "github.com/service-bridge/sdk/go/internal/workflow"
)

// Code classifies a failure. It is the single axis callers switch on, and the
// same strings name the same conditions in every ServiceBridge SDK.
type Code string

const (
	// CodeConfig marks a configuration the SDK refuses to run with. It never
	// reaches the reconnect ladder: a bad bound is not a network condition.
	CodeConfig Code = "CONFIG"
	// CodeState marks an operation attempted in the wrong lifecycle phase.
	CodeState Code = "STATE"
	// CodeConnection marks a failure to reach the runtime or the callee.
	CodeConnection Code = "CONNECTION"
	// CodeTimeout marks a deadline that passed with the outcome unknown.
	CodeTimeout Code = "TIMEOUT"
	// CodeCancelled marks an operation its caller cancelled.
	CodeCancelled Code = "CANCELLED"
	// CodeAccessDenied marks a refusal by the access policy or of the identity.
	CodeAccessDenied Code = "ACCESS_DENIED"
	// CodeNotFound marks a name the mesh has no definition for.
	CodeNotFound Code = "NOT_FOUND"
	// CodeValidation marks a declaration or an argument that is refused.
	CodeValidation Code = "VALIDATION"
	// CodeConflict marks an identity already used with other content.
	CodeConflict Code = "CONFLICT"
	// CodeTerminal marks a workflow run that has already finished.
	CodeTerminal Code = "TERMINAL"
	// CodeNoLiveInstance marks a call with nowhere to go.
	CodeNoLiveInstance Code = "NO_LIVE_INSTANCE"
	// CodeOverloaded marks a callee or runtime shedding load.
	CodeOverloaded Code = "OVERLOADED"
	// CodeQueueFull marks the publish queue at its cap.
	CodeQueueFull Code = "QUEUE_FULL"
	// CodeInvalidEventName marks a name the event grammar rejects.
	CodeInvalidEventName Code = "INVALID_EVENT_NAME"
	// CodeHandler marks a failure the callee's handler answered with. The
	// handler's own code is on the *HandlerError reachable with errors.As.
	CodeHandler Code = "HANDLER"
	// CodeInternal marks everything else.
	CodeInternal Code = "INTERNAL"
)

// Error is the only error type this SDK returns, so errors.As against it is
// exhaustive by construction.
type Error struct {
	Code Code
	Op   string
	Msg  string
	Err  error
}

// HandlerError is a business failure. Returned from a handler, its Code
// travels to the caller as the answer's error code; on the caller side it is
// what errors.As reaches inside an *Error with CodeHandler. Any other error a
// handler returns — and a panic — reaches the caller as code "INTERNAL".
type HandlerError = rpc.HandlerError

// Sentinels for errors.Is. They carry a Code only — matching ignores Op, Msg
// and the wrapped cause.
var (
	ErrConfig           = &Error{Code: CodeConfig}
	ErrState            = &Error{Code: CodeState}
	ErrConnection       = &Error{Code: CodeConnection}
	ErrTimeout          = &Error{Code: CodeTimeout}
	ErrCancelled        = &Error{Code: CodeCancelled}
	ErrAccessDenied     = &Error{Code: CodeAccessDenied}
	ErrNotFound         = &Error{Code: CodeNotFound}
	ErrValidation       = &Error{Code: CodeValidation}
	ErrConflict         = &Error{Code: CodeConflict}
	ErrTerminal         = &Error{Code: CodeTerminal}
	ErrNoLiveInstance   = &Error{Code: CodeNoLiveInstance}
	ErrOverloaded       = &Error{Code: CodeOverloaded}
	ErrQueueFull        = &Error{Code: CodeQueueFull}
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

// Retryable reports whether repeating the operation may succeed without
// risking a duplicate effect: the condition is transient and nothing was done.
// TIMEOUT is not retryable — the outcome is unknown, so a repeat is safe only
// with an idempotency key, which is the caller's decision.
func (e *Error) Retryable() bool {
	switch e.Code {
	case CodeConnection, CodeNoLiveInstance, CodeOverloaded, CodeQueueFull:
		return true
	default:
		return false
	}
}

func newError(code Code, op, msg string, cause error) *Error {
	return &Error{Code: code, Op: op, Msg: msg, Err: cause}
}

func configError(op, msg string) *Error {
	return newError(CodeConfig, op, msg, nil)
}

// wrap classifies an error raised inside the SDK and presents it as the one
// public type. Classification is by sentinel and by gRPC status, never by
// message text.
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
	if code, ok := classifySentinel(err); ok {
		return code
	}
	var handlerErr *rpc.HandlerError
	if errors.As(err, &handlerErr) {
		return CodeHandler
	}
	if st, ok := status.FromError(err); ok {
		return codeOfStatus(st.Code())
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return CodeTimeout
	case errors.Is(err, context.Canceled):
		return CodeCancelled
	}
	return CodeInternal
}

func classifySentinel(err error) (Code, bool) {
	switch {
	case errors.Is(err, rpc.ErrNoCandidates),
		errors.Is(err, rpc.ErrNoEndpoint),
		errors.Is(err, rpc.ErrAllUnavailable):
		return CodeNoLiveInstance, true

	case errors.Is(err, rpc.ErrPeerUnreachable),
		errors.Is(err, rpc.ErrNoLease),
		errors.Is(err, events.ErrStopped),
		errors.Is(err, wfi.ErrNoIdentity):
		return CodeConnection, true

	case errors.Is(err, events.ErrNotSent),
		errors.Is(err, events.ErrOutcomeUnknown):
		return CodeTimeout, true

	case errors.Is(err, rpc.ErrAcceptanceDenied),
		errors.Is(err, events.ErrForbidden),
		errors.Is(err, wfi.ErrAccessDenied):
		return CodeAccessDenied, true

	case errors.Is(err, wfi.ErrWorkflowNotFound):
		return CodeNotFound, true

	case errors.Is(err, wfi.ErrRunTerminal), errors.Is(err, wfi.ErrRunFailed):
		return CodeTerminal, true

	case errors.Is(err, events.ErrQueueFull):
		return CodeQueueFull, true

	case errors.Is(err, events.ErrConflict):
		return CodeConflict, true

	case errors.Is(err, events.ErrInvalidName):
		return CodeInvalidEventName, true

	case errors.Is(err, connection.ErrProtocol),
		errors.Is(err, rpc.ErrServerConfig),
		errors.Is(err, rpc.ErrInvalidConfig),
		errors.Is(err, events.ErrInvalidConfig),
		errors.Is(err, wfi.ErrInvalidConfig),
		errors.Is(err, serde.ErrNotProto):
		return CodeConfig, true

	case errors.Is(err, rpc.ErrDecode),
		errors.Is(err, rpc.ErrEmptyMethod),
		errors.Is(err, rpc.ErrNoFunc),
		errors.Is(err, rpc.ErrDuplicate),
		errors.Is(err, events.ErrDuplicatePattern),
		errors.Is(err, serde.ErrTreeShape),
		errors.Is(err, registry.ErrSchemaConflict),
		isJobDeclaration(err):
		return CodeValidation, true

	case errors.Is(err, rpc.ErrSealed),
		errors.Is(err, rpc.ErrServerClosed),
		errors.Is(err, rpc.ErrDirectClosed):
		return CodeState, true
	}
	return "", false
}

// isJobDeclaration covers the reasons a job declaration is refused where it is
// written.
func isJobDeclaration(err error) bool {
	for _, s := range []error{
		jobi.ErrVersion, jobi.ErrNoTrigger, jobi.ErrCronFieldCount, jobi.ErrCronExpr, jobi.ErrCronTZ,
		jobi.ErrInterval, jobi.ErrRunAt, jobi.ErrCatchupPolicy, jobi.ErrOverlapPolicy, jobi.ErrDepKind,
		jobi.ErrDepTarget, jobi.ErrRetryInitial, jobi.ErrNegativeLimit, jobi.ErrEmptyName,
		jobi.ErrNoHandler, jobi.ErrDuplicateName,
	} {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}

// codeOfStatus maps a gRPC status the far side answered with onto the error
// model. The table is the same in every ServiceBridge SDK.
func codeOfStatus(c codes.Code) Code {
	switch c {
	case codes.Canceled:
		return CodeCancelled
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return CodeValidation
	case codes.DeadlineExceeded:
		return CodeTimeout
	case codes.NotFound, codes.Unimplemented:
		return CodeNotFound
	case codes.AlreadyExists:
		return CodeConflict
	case codes.PermissionDenied, codes.Unauthenticated:
		return CodeAccessDenied
	case codes.ResourceExhausted:
		return CodeOverloaded
	case codes.Unavailable:
		return CodeConnection
	default:
		// Unknown, Aborted, Internal, DataLoss.
		return CodeInternal
	}
}

// NonRetryable marks a job handler failure the runtime must not retry: the
// execution goes to the dead-letter queue at once instead of burning every
// remaining attempt on input that will never work.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", jobi.ErrPermanent, err)
}
