package sbtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	servicebridge "github.com/service-bridge/sdk/go"
	"github.com/service-bridge/sdk/go/internal/rpc"
	"github.com/service-bridge/sdk/go/internal/serde"
)

// InvokeOption describes the caller of an inbound call.
type InvokeOption func(*rpc.CallInfo)

// WithCaller names the calling service and instance (CallInfo.CallerServiceID,
// CallerInstanceID).
func WithCaller(serviceID, instanceID string) InvokeOption {
	return func(i *rpc.CallInfo) {
		i.CallerServiceID = serviceID
		i.CallerInstanceID = instanceID
	}
}

// WithRequestID sets CallInfo.RequestID. A fresh UUID otherwise.
func WithRequestID(id string) InvokeOption {
	return func(i *rpc.CallInfo) { i.RequestID = id }
}

// WithIdempotencyKey sets CallInfo.IdempotencyKey.
func WithIdempotencyKey(key string) InvokeOption {
	return func(i *rpc.CallInfo) { i.IdempotencyKey = key }
}

func (h *Harness) inbound(ctx context.Context, opts []InvokeOption) context.Context {
	info := rpc.CallInfo{RequestID: uuid.NewString()}
	if d, ok := ctx.Deadline(); ok {
		info.Deadline = d
	}
	for _, opt := range opts {
		opt(&info)
	}
	return rpc.WithCallInfo(ctx, info)
}

// Invoke calls the unary handler registered with servicebridge.Handle under
// method, the way a peer would: req is encoded, the client's dispatcher
// decodes it, runs the handler and encodes the answer, which is decoded into
// Resp. A failure comes back as the caller would see it — a HandlerError the
// handler returned as *servicebridge.Error{Code: CodeHandler} wrapping it, any
// other error as code INTERNAL, an unknown method as NOT_FOUND, a request that
// does not decode as VALIDATION. The handler's ctx carries the CallInfo and is
// cancelled with ctx.
func Invoke[Req, Resp proto.Message](ctx context.Context, h *Harness, method string, req Req, opts ...InvokeOption) (Resp, error) {
	const op = "sbtest.Invoke"
	var zero Resp
	payload, err := proto.Marshal(req)
	if err != nil {
		return zero, fmt.Errorf("sbtest: invoke %s: encode request: %w", method, err)
	}
	out := h.mem.Unary(h.inbound(ctx, opts), method, payload)
	if out.Status != codes.OK || out.ErrorCode != "" {
		return zero, h.wireError(op, out)
	}
	resp := serde.New(zero)
	if err := serde.Decode(out.Payload, resp); err != nil {
		return zero, fmt.Errorf("sbtest: invoke %s: decode response: %w", method, err)
	}
	return resp, nil
}

// InvokeStream calls the streaming handler registered with
// servicebridge.HandleStream and collects its chunks. The error follows the
// rules of Invoke; the chunks sent before a failure are returned with it.
func InvokeStream[Req, Chunk proto.Message](ctx context.Context, h *Harness, method string, req Req, opts ...InvokeOption) ([]Chunk, error) {
	const op = "sbtest.InvokeStream"
	var zero Chunk
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("sbtest: invoke stream %s: encode request: %w", method, err)
	}
	var mu sync.Mutex
	var chunks []Chunk
	var decodeErr error
	out := h.mem.ServeStream(h.inbound(ctx, opts), method, payload, func(raw []byte) error {
		chunk := serde.New(zero)
		if err := serde.Decode(raw, chunk); err != nil {
			decodeErr = err
			return err
		}
		mu.Lock()
		chunks = append(chunks, chunk)
		mu.Unlock()
		return nil
	})
	if decodeErr != nil {
		return chunks, fmt.Errorf("sbtest: invoke stream %s: decode chunk: %w", method, decodeErr)
	}
	if out.Status != codes.OK || out.ErrorCode != "" {
		return chunks, h.wireError(op, out)
	}
	return chunks, nil
}

func statusError(out rpc.Outcome) error {
	return status.Error(out.Status, out.StatusMessage)
}

// CallRecord is one outbound call the handler under test made, in order.
type CallRecord struct {
	Service        string
	Method         string
	Payload        []byte
	IdempotencyKey string
	BusinessKey    string
	Transport      servicebridge.Transport
}

// DecodeCall reads the request of one recorded call back as T.
func DecodeCall[T proto.Message](rec CallRecord) (T, error) {
	var zero T
	out := serde.New(zero)
	if err := serde.Decode(rec.Payload, out); err != nil {
		return zero, fmt.Errorf("sbtest: decode call %s/%s: %w", rec.Service, rec.Method, err)
	}
	return out, nil
}

// Calls returns the outbound calls in the order they happened.
func (h *Harness) Calls() []CallRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]CallRecord(nil), h.calls...)
}

type responder func(ctx context.Context, payload []byte) ([]byte, error)

type streamResponder func(ctx context.Context, payload []byte) ([][]byte, error)

// Respond arranges the answer to every outbound call to service/method:
// servicebridge.Call, a declared method's Call and a workflow call step all
// land here. The request is decoded into Req and the answer encoded from Resp,
// so a type mismatch fails as it would on the wire. Returning a
// *servicebridge.HandlerError answers with that business code; any other
// error answers INTERNAL. Arranging again replaces the previous answer.
func Respond[Req, Resp proto.Message](h *Harness, service, method string, fn func(ctx context.Context, req Req) (Resp, error)) error {
	if h == nil || service == "" || method == "" || fn == nil {
		return fmt.Errorf("sbtest: respond %s/%s: %w", service, method, ErrInvalidArg)
	}
	var reqZero Req
	h.mu.Lock()
	defer h.mu.Unlock()
	h.responders[service+"/"+method] = func(ctx context.Context, payload []byte) ([]byte, error) {
		req := serde.New(reqZero)
		if err := serde.Decode(payload, req); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		resp, err := fn(ctx, req)
		if err != nil {
			return nil, remoteFailure(err)
		}
		return proto.Marshal(resp)
	}
	return nil
}

// RespondStream arranges the chunks of every outbound servicebridge.Stream to
// service/method.
func RespondStream[Req, Chunk proto.Message](h *Harness, service, method string, fn func(ctx context.Context, req Req) ([]Chunk, error)) error {
	if h == nil || service == "" || method == "" || fn == nil {
		return fmt.Errorf("sbtest: respond stream %s/%s: %w", service, method, ErrInvalidArg)
	}
	var reqZero Req
	h.mu.Lock()
	defer h.mu.Unlock()
	h.streams[service+"/"+method] = func(ctx context.Context, payload []byte) ([][]byte, error) {
		req := serde.New(reqZero)
		if err := serde.Decode(payload, req); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		chunks, err := fn(ctx, req)
		if err != nil {
			return nil, remoteFailure(err)
		}
		out := make([][]byte, 0, len(chunks))
		for _, c := range chunks {
			raw, err := proto.Marshal(c)
			if err != nil {
				return nil, fmt.Errorf("sbtest: encode chunk: %w", err)
			}
			out = append(out, raw)
		}
		return out, nil
	}
	return nil
}

// remoteFailure is what a responder's error looks like after the wire: a
// HandlerError keeps its code, anything else is the callee's INTERNAL.
func remoteFailure(err error) error {
	var he *servicebridge.HandlerError
	if errors.As(err, &he) {
		return &rpc.HandlerError{Code: he.Code, Message: he.Message}
	}
	return &rpc.HandlerError{Code: "INTERNAL", Message: err.Error()}
}

func (h *Harness) record(req rpc.Request) {
	h.calls = append(h.calls, CallRecord{
		Service:        req.Service,
		Method:         req.Method,
		Payload:        append([]byte(nil), req.Payload...),
		IdempotencyKey: req.IdempotencyKey,
		BusinessKey:    req.BusinessKey,
		Transport:      transportOf(req.Transport),
	})
}

func (h *Harness) call(ctx context.Context, req rpc.Request) ([]byte, error) {
	h.mu.Lock()
	h.record(req)
	fn, ok := h.responders[req.Service+"/"+req.Method]
	h.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s — arrange it with sbtest.Respond", ErrNoResponse, req.Service, req.Method)
	}
	return fn(ctx, req.Payload)
}

func (h *Harness) stream(ctx context.Context, req rpc.Request) (*rpc.Stream, error) {
	h.mu.Lock()
	h.record(req)
	fn, ok := h.streams[req.Service+"/"+req.Method]
	h.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s — arrange it with sbtest.RespondStream", ErrNoResponse, req.Service, req.Method)
	}
	chunks, err := fn(ctx, req.Payload)
	if err != nil {
		return nil, err
	}
	next := 0
	return rpc.NewStream(func() ([]byte, error) {
		if next >= len(chunks) {
			return nil, io.EOF
		}
		next++
		return chunks[next-1], nil
	}, func() {}), nil
}

func transportOf(t rpc.Transport) servicebridge.Transport {
	switch t {
	case rpc.TransportDirect:
		return servicebridge.TransportDirect
	case rpc.TransportProxy:
		return servicebridge.TransportProxy
	default:
		return servicebridge.TransportAuto
	}
}
