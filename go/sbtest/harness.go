// Package sbtest runs a service's handlers inside a real *servicebridge.Client
// whose network edges are in memory: no runtime, no listener, no TLS. Requests
// and responses go through the same proto encoding, the same handler wrapping,
// the same publish queue and the same event routing as in production; what is
// doubled is only what the runtime and the peers would answer.
package sbtest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	servicebridge "github.com/service-bridge/sdk/go"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/internal/rpc"
	"github.com/service-bridge/sdk/go/internal/testkit"
)

// Failures the harness itself reports. Match them with errors.Is.
var (
	// ErrNoResponse: the handler under test called a method nothing was
	// arranged for. A forgotten Respond is a bug in the test, not a zero value.
	ErrNoResponse = errors.New("sbtest: no response arranged for this call")
	// ErrInvalidArg: a nil harness, an empty name or a nil function.
	ErrInvalidArg = errors.New("sbtest: invalid argument")
)

// TB is the part of testing.TB the harness uses.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Cleanup(func())
}

// Harness owns one client and the in-memory world around it. See ./README.md.
type Harness struct {
	// Client is a real client: declare handlers, subscriptions, dependencies
	// and events on it exactly as in production, then call Start.
	Client *servicebridge.Client

	mem *testkit.Memory

	mu         sync.Mutex
	responders map[string]responder
	streams    map[string]streamResponder
	calls      []CallRecord
	published  []PublishedEvent
}

// New builds a harness around a fresh client. opts are ordinary client
// options; the harness adds its in-memory transport. The client is stopped
// when the test ends.
func New(t TB, opts ...servicebridge.Option) *Harness {
	t.Helper()
	h := &Harness{
		responders: make(map[string]responder),
		streams:    make(map[string]streamResponder),
	}
	h.mem = &testkit.Memory{Call: h.call, Stream: h.stream, Publish: h.publish}
	memOpt, ok := testkit.NewOption(h.mem).(servicebridge.Option)
	if !ok {
		t.Fatalf("sbtest: the client offers no in-memory transport")
	}
	key, err := testKey()
	if err != nil {
		t.Fatalf("sbtest: %v", err)
	}
	c, err := servicebridge.New("sbtest.invalid:0", key, append(opts, memOpt)...)
	if err != nil {
		t.Fatalf("sbtest: new client: %v", err)
	}
	h.Client = c
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Stop(ctx)
	})
	return h
}

// Start seals the declarations and makes the client ready, as Client.Start
// does against a runtime.
func (h *Harness) Start(ctx context.Context) error { return h.Client.Start(ctx) }

// Reset forgets every arranged answer and every record.
func (h *Harness) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	clear(h.responders)
	clear(h.streams)
	h.calls = nil
	h.published = nil
}

// testKey mints a syntactically valid bootstrap key. The memory transport never
// provisions, so the key authenticates nothing; it only has to parse.
func testKey() (string, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", fmt.Errorf("generate key: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "sbtest"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return "", fmt.Errorf("create certificate: %w", err)
	}
	blob, err := proto.Marshal(&pb.BootstrapKeyPayload{KeyId: make([]byte, 8), Secret: make([]byte, 32), CaCertDer: der})
	if err != nil {
		return "", fmt.Errorf("encode key: %w", err)
	}
	return "sb." + base64.RawURLEncoding.EncodeToString(blob), nil
}

// wireError turns an outcome the client's dispatcher produced into the error
// a caller on the other side of the wire would see.
func (h *Harness) wireError(op string, out rpc.Outcome) error {
	if out.ErrorCode != "" {
		return h.mem.Wrap(op, &rpc.HandlerError{Code: out.ErrorCode, Message: out.ErrorMessage})
	}
	return h.mem.Wrap(op, statusError(out))
}
