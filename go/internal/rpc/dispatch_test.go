package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
)

func inboundTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func echoUnaryHandler(_ context.Context, payload []byte) ([]byte, error) {
	return payload, nil
}

func TestRegisterRejectsBadInput(t *testing.T) {
	t.Parallel()

	d := NewDispatcher(inboundTestLogger())

	if err := d.RegisterUnary("", echoUnaryHandler); !errors.Is(err, ErrEmptyMethod) {
		t.Fatalf("empty name: got %v, want ErrEmptyMethod", err)
	}
	if err := d.RegisterUnary("nil", nil); !errors.Is(err, ErrNoFunc) {
		t.Fatalf("nil func: got %v, want ErrNoFunc", err)
	}
	if err := d.RegisterStream("nil", nil); !errors.Is(err, ErrNoFunc) {
		t.Fatalf("nil stream func: got %v, want ErrNoFunc", err)
	}
	if err := d.RegisterUnary("echo", echoUnaryHandler); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := d.RegisterUnary("echo", echoUnaryHandler); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate: got %v, want ErrDuplicate", err)
	}
	if err := d.RegisterStream("echo", func(context.Context, []byte, Sender) error { return nil }); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate across kinds: got %v, want ErrDuplicate", err)
	}
}

func TestRegisterAfterSealIsRefused(t *testing.T) {
	t.Parallel()

	d := NewDispatcher(inboundTestLogger())
	if d.Sealed() {
		t.Fatal("a fresh dispatcher must accept registrations")
	}
	d.Seal()
	if !d.Sealed() {
		t.Fatal("Seal did not close registration")
	}

	if err := d.RegisterUnary("late", echoUnaryHandler); !errors.Is(err, ErrSealed) {
		t.Fatalf("late unary: got %v, want ErrSealed", err)
	}
	if err := d.RegisterStream("late", func(context.Context, []byte, Sender) error { return nil }); !errors.Is(err, ErrSealed) {
		t.Fatalf("late stream: got %v, want ErrSealed", err)
	}
}

func TestMethodsAreListedInOrder(t *testing.T) {
	t.Parallel()

	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "zulu", echoUnaryHandler)
	mustAddUnary(t, d, "alpha", echoUnaryHandler)
	mustAddStream(t, d, "mike", func(context.Context, []byte, Sender) error { return nil })

	want := []MethodInfo{
		{Name: "alpha"},
		{Name: "mike", Streaming: true},
		{Name: "zulu"},
	}
	if got := d.Methods(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Methods() = %v, want %v", got, want)
	}
}

func TestUnknownMethodIsNotFound(t *testing.T) {
	t.Parallel()

	d := NewDispatcher(inboundTestLogger())

	if got := d.Unary(context.Background(), "nope", nil); got.Status != codes.NotFound {
		t.Fatalf("unary status = %v, want NotFound", got.Status)
	}
	if got := d.Stream(context.Background(), "nope", nil, discardChunks); got.Status != codes.NotFound {
		t.Fatalf("stream status = %v, want NotFound", got.Status)
	}
}

func TestWrongCallKindIsFailedPrecondition(t *testing.T) {
	t.Parallel()

	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "unary", echoUnaryHandler)
	mustAddStream(t, d, "stream", func(context.Context, []byte, Sender) error { return nil })

	if got := d.Unary(context.Background(), "stream", nil); got.Status != codes.FailedPrecondition {
		t.Fatalf("streaming method called as unary: status = %v, want FailedPrecondition", got.Status)
	}
	if got := d.Stream(context.Background(), "unary", nil, discardChunks); got.Status != codes.FailedPrecondition {
		t.Fatalf("unary method called as stream: status = %v, want FailedPrecondition", got.Status)
	}
}

func TestDecodeFailureIsInvalidArgument(t *testing.T) {
	t.Parallel()

	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "typed", func(context.Context, []byte) ([]byte, error) {
		return nil, fmt.Errorf("unmarshal request: %w", ErrDecode)
	})
	mustAddStream(t, d, "typed-stream", func(context.Context, []byte, Sender) error {
		return fmt.Errorf("unmarshal request: %w", ErrDecode)
	})

	got := d.Unary(context.Background(), "typed", nil)
	if got.Status != codes.InvalidArgument {
		t.Fatalf("status = %v, want InvalidArgument", got.Status)
	}
	if got.ErrorCode != "" {
		t.Fatalf("a decode failure must not also fill the response body, got error_code %q", got.ErrorCode)
	}

	if got := d.Stream(context.Background(), "typed-stream", nil, discardChunks); got.Status != codes.InvalidArgument {
		t.Fatalf("stream status = %v, want InvalidArgument", got.Status)
	}
}

func TestHandlerErrorTravelsInTheBody(t *testing.T) {
	t.Parallel()

	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "fails", func(context.Context, []byte) ([]byte, error) {
		return nil, errors.New("insufficient funds")
	})

	got := d.Unary(context.Background(), "fails", nil)
	if got.Status != codes.OK {
		t.Fatalf("status = %v, want OK: a handler error is an answer, not a transport fault", got.Status)
	}
	if got.ErrorCode != errorCodeInternal {
		t.Fatalf("error_code = %q, want %q", got.ErrorCode, errorCodeInternal)
	}
	if got.ErrorMessage != "insufficient funds" {
		t.Fatalf("error_message = %q", got.ErrorMessage)
	}
}

func TestPanicBecomesABodyError(t *testing.T) {
	t.Parallel()

	d := NewDispatcher(inboundTestLogger())
	mustAddUnary(t, d, "boom", func(context.Context, []byte) ([]byte, error) {
		panic("handler exploded")
	})
	mustAddStream(t, d, "boom-stream", func(context.Context, []byte, Sender) error {
		panic("stream exploded")
	})

	got := d.Unary(context.Background(), "boom", nil)
	if got.Status != codes.OK || got.ErrorCode != errorCodeInternal {
		t.Fatalf("unary panic: status = %v, error_code = %q", got.Status, got.ErrorCode)
	}
	if got.ErrorMessage == "" {
		t.Fatal("unary panic: error_message is empty")
	}

	got = d.Stream(context.Background(), "boom-stream", nil, discardChunks)
	if got.Status != codes.OK || got.ErrorCode != errorCodeInternal {
		t.Fatalf("stream panic: status = %v, error_code = %q", got.Status, got.ErrorCode)
	}

	// The dispatcher stays usable: a panic costs its own call and nothing else.
	if out := d.Unary(context.Background(), "boom", nil); out.ErrorCode != errorCodeInternal {
		t.Fatalf("second call after a panic: error_code = %q", out.ErrorCode)
	}
}

func TestStreamFeedsChunksThroughSender(t *testing.T) {
	t.Parallel()

	d := NewDispatcher(inboundTestLogger())
	mustAddStream(t, d, "count", func(_ context.Context, _ []byte, send Sender) error {
		for i := range 3 {
			if err := send([]byte{byte(i)}); err != nil {
				return err
			}
		}
		return nil
	})

	var got [][]byte
	out := d.Stream(context.Background(), "count", nil, func(chunk []byte) error {
		got = append(got, chunk)
		return nil
	})
	if out.Status != codes.OK || out.ErrorCode != "" {
		t.Fatalf("outcome = %+v", out)
	}
	if len(got) != 3 {
		t.Fatalf("got %d chunks, want 3", len(got))
	}
}

func TestConcurrentRegistrationAndLookup(t *testing.T) {
	t.Parallel()

	d := NewDispatcher(inboundTestLogger())
	var wg sync.WaitGroup

	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mustAddUnary(t, d, fmt.Sprintf("m%02d", i), echoUnaryHandler)
		}()
	}
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Unary(context.Background(), "m00", nil)
		}()
	}
	wg.Wait()

	if got := len(d.Methods()); got != 32 {
		t.Fatalf("registered %d methods, want 32", got)
	}
}

func discardChunks([]byte) error { return nil }

func mustAddUnary(t *testing.T, d *Dispatcher, name string, fn UnaryFunc) {
	t.Helper()
	if err := d.RegisterUnary(name, fn); err != nil {
		t.Fatalf("register unary %q: %v", name, err)
	}
}

func mustAddStream(t *testing.T, d *Dispatcher, name string, fn StreamFunc) {
	t.Helper()
	if err := d.RegisterStream(name, fn); err != nil {
		t.Fatalf("register stream %q: %v", name, err)
	}
}
