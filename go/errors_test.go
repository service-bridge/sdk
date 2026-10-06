package servicebridge

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/service-bridge/sdk/go/internal/events"
	jobi "github.com/service-bridge/sdk/go/internal/job"
	"github.com/service-bridge/sdk/go/internal/rpc"
)

// The status table is part of the cross-SDK contract: every SDK maps a remote
// answer onto the same code.
func TestRemoteStatusesMapOntoTheErrorModel(t *testing.T) {
	want := map[codes.Code]Code{
		codes.Canceled:           CodeCancelled,
		codes.Unknown:            CodeInternal,
		codes.InvalidArgument:    CodeValidation,
		codes.DeadlineExceeded:   CodeTimeout,
		codes.NotFound:           CodeNotFound,
		codes.AlreadyExists:      CodeConflict,
		codes.PermissionDenied:   CodeAccessDenied,
		codes.Unauthenticated:    CodeAccessDenied,
		codes.ResourceExhausted:  CodeOverloaded,
		codes.FailedPrecondition: CodeValidation,
		codes.Aborted:            CodeInternal,
		codes.OutOfRange:         CodeValidation,
		codes.Unimplemented:      CodeNotFound,
		codes.Internal:           CodeInternal,
		codes.Unavailable:        CodeConnection,
		codes.DataLoss:           CodeInternal,
	}
	for c, code := range want {
		err := fmt.Errorf("rpc: direct unary X: %w", status.Error(c, "remote"))
		if got := classify(err); got != code {
			t.Errorf("%v → %s, want %s", c, got, code)
		}
	}
}

func TestSDKFailuresMapOntoTheErrorModel(t *testing.T) {
	cases := map[error]Code{
		&rpc.SelectionError{Reason: rpc.ErrNoCandidates}:                   CodeNoLiveInstance,
		&rpc.NotDispatchedError{Err: rpc.ErrPeerUnreachable, Direct: true}: CodeConnection,
		events.ErrQueueFull:        CodeQueueFull,
		events.ErrNotSent:          CodeTimeout,
		events.ErrOutcomeUnknown:   CodeTimeout,
		events.ErrConflict:         CodeConflict,
		events.ErrForbidden:        CodeAccessDenied,
		events.ErrInvalidName:      CodeInvalidEventName,
		events.ErrStopped:          CodeConnection,
		events.ErrDuplicatePattern: CodeValidation,
		jobi.ErrCronExpr:           CodeValidation,
		context.DeadlineExceeded:   CodeTimeout,
		context.Canceled:           CodeCancelled,
		errors.New("anything"):     CodeInternal,
	}
	for err, code := range cases {
		if got := classify(err); got != code {
			t.Errorf("%v → %s, want %s", err, got, code)
		}
	}
}

func TestHandlerAnswerIsReachableWithErrorsAs(t *testing.T) {
	err := wrap("servicebridge.Call", &rpc.HandlerError{Code: "OUT_OF_STOCK", Message: "sku 42"})
	var sbErr *Error
	if !errors.As(err, &sbErr) || sbErr.Code != CodeHandler {
		t.Fatalf("got %v, want CodeHandler", err)
	}
	var he *HandlerError
	if !errors.As(err, &he) || he.Code != "OUT_OF_STOCK" || he.Message != "sku 42" {
		t.Fatalf("handler error not reachable: %#v", he)
	}
	if sbErr.Retryable() || !errors.Is(err, ErrHandler) {
		t.Fatal("a handler answer is not retryable")
	}
}

func TestRetryableFollowsTheCode(t *testing.T) {
	retryable := map[Code]bool{
		CodeConnection: true, CodeNoLiveInstance: true, CodeOverloaded: true, CodeQueueFull: true,
		CodeTimeout: false, CodeHandler: false, CodeValidation: false, CodeInternal: false, CodeCancelled: false,
	}
	for code, want := range retryable {
		if got := (&Error{Code: code}).Retryable(); got != want {
			t.Errorf("%s retryable = %v, want %v", code, got, want)
		}
	}
}

// A handler's own HandlerError answers with its code; an SDK error it merely
// passes through answers INTERNAL instead of impersonating a downstream code.
func TestHandlerFailureKeepsOnlyTheHandlersOwnCode(t *testing.T) {
	own := handlerFailure(&HandlerError{Code: "OUT_OF_STOCK"})
	var he *HandlerError
	if !errors.As(own, &he) || he.Code != "OUT_OF_STOCK" {
		t.Fatalf("own code lost: %v", own)
	}
	nested := handlerFailure(wrap("servicebridge.Call", &rpc.HandlerError{Code: "DOWNSTREAM"}))
	if errors.As(nested, &he) {
		t.Fatalf("a downstream code leaked through: %v", nested)
	}
	if handlerFailure(nil) != nil {
		t.Fatal("nil stays nil")
	}
}

func TestNonRetryableMarksAPermanentJobFailure(t *testing.T) {
	cause := errors.New("poisoned input")
	err := NonRetryable(cause)
	if !errors.Is(err, jobi.ErrPermanent) || !errors.Is(err, cause) {
		t.Fatalf("NonRetryable lost its marker or its cause: %v", err)
	}
	if NonRetryable(nil) != nil {
		t.Fatal("nil stays nil")
	}
}
