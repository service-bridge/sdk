package servicebridge_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"math/big"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	sb "github.com/service-bridge/sdk/go"
	internaljob "github.com/service-bridge/sdk/go/internal/job"
	"github.com/service-bridge/sdk/go/internal/outbox"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"github.com/service-bridge/sdk/go/job"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A real TLS runtime fixture exercises public construction/start/stop, including
// bootstrap, mTLS data-plane streams, SQLite ownership and a running job.
type lifecycleRuntime struct {
	pb.UnimplementedBootstrapServer
	pb.UnimplementedControlServer
	pb.UnimplementedRegistryServer
	pb.UnimplementedTelemetryServer
	pb.UnimplementedJobsServer
	ca                 *x509.Certificate
	key                *ecdsa.PrivateKey
	provisionEntered   chan struct{}
	provisionCancelled chan struct{}
	blockProvision     bool
	rejectProvision    bool
	jobs               chan *pb.JobExecution
	staleResults       atomic.Int64
}

func (f *lifecycleRuntime) Provision(ctx context.Context, req *pb.ProvisionRequest) (*pb.ProvisionResponse, error) {
	select {
	case f.provisionEntered <- struct{}{}:
	default:
	}
	if f.blockProvision {
		<-ctx.Done()
		close(f.provisionCancelled)
		return nil, ctx.Err()
	}
	if f.rejectProvision {
		return nil, status.Error(codes.Unauthenticated, "bootstrap key revoked")
	}
	csr, err := x509.ParseCertificateRequest(req.CsrDer)
	if err != nil {
		return nil, err
	}
	if err = csr.CheckSignature(); err != nil {
		return nil, err
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "instance"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, URIs: []*url.URL{{Scheme: "spiffe", Host: "service-bridge", Path: "/service/service/instance/instance"}}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, f.ca, csr.PublicKey, f.key)
	if err != nil {
		return nil, err
	}
	return &pb.ProvisionResponse{CertDer: der, CaChainDer: f.ca.Raw, ServiceId: "service", ServiceName: "test", InstanceId: "instance", NotAfterUnixMs: leaf.NotAfter.UnixMilli()}, nil
}
func (f *lifecycleRuntime) Open(_ *pb.OpenRequest, st pb.Control_OpenServer) error {
	if err := st.Send(&pb.ServerControl{Kind: &pb.ServerControl_Welcome{Welcome: &pb.Welcome{ServiceId: "service", ServiceName: "test", SessionId: "session"}}}); err != nil {
		return err
	}
	<-st.Context().Done()
	return nil
}
func (f *lifecycleRuntime) RegisterAndWatch(_ *pb.RegisterRequest, st pb.Registry_RegisterAndWatchServer) error {
	if err := st.Send(&pb.RegistryEvent{Kind: &pb.RegistryEvent_Snapshot{Snapshot: &pb.RegistrySnapshot{}}}); err != nil {
		return err
	}
	<-st.Context().Done()
	return nil
}
func (f *lifecycleRuntime) Report(st pb.Telemetry_ReportServer) error {
	for {
		b, err := st.Recv()
		if err != nil {
			return nil
		}
		if err = st.Send(&pb.TelemetryAck{AcknowledgedSequence: b.Sequence}); err != nil {
			return err
		}
	}
}
func (f *lifecycleRuntime) Subscribe(_ *pb.JobsSubscribeRequest, st pb.Jobs_SubscribeServer) error {
	for {
		select {
		case <-st.Context().Done():
			return nil
		case m := <-f.jobs:
			if err := st.Send(m); err != nil {
				return err
			}
		}
	}
}
func (f *lifecycleRuntime) JobResult(context.Context, *pb.JobResultRequest) (*pb.JobResultResponse, error) {
	f.staleResults.Add(1)
	return &pb.JobResultResponse{Accepted: true}, nil
}

func startLifecycleRuntime(t *testing.T) (*lifecycleRuntime, string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caT := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, caT, caT, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	runtimeT := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, URIs: []*url.URL{{Scheme: "spiffe", Host: "service-bridge", Path: "/runtime"}}}
	runtimeDER, err := x509.CreateCertificate(rand.Reader, runtimeT, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{{Certificate: [][]byte{runtimeDER, der}, PrivateKey: key}}, ClientCAs: roots, ClientAuth: tls.VerifyClientCertIfGiven})))
	f := &lifecycleRuntime{ca: ca, key: key, provisionEntered: make(chan struct{}, 1), provisionCancelled: make(chan struct{}), jobs: make(chan *pb.JobExecution, 1)}
	pb.RegisterBootstrapServer(gs, f)
	pb.RegisterControlServer(gs, f)
	pb.RegisterRegistryServer(gs, f)
	pb.RegisterTelemetryServer(gs, f)
	pb.RegisterJobsServer(gs, f)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(gs.Stop)
	blob, err := proto.Marshal(&pb.BootstrapKeyPayload{KeyId: make([]byte, 8), Secret: make([]byte, 32), CaCertDer: der})
	if err != nil {
		t.Fatal(err)
	}
	return f, ln.Addr().String(), "sb." + base64.RawURLEncoding.EncodeToString(blob)
}

func TestPublicStartRollsBackFailedProvision(t *testing.T) {
	f, addr, key := startLifecycleRuntime(t)
	f.rejectProvision = true
	c, err := sb.New(addr, key, sb.WithCallerOnly(), sb.WithDataDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = c.Start(ctx); err == nil {
		t.Fatal("revoked bootstrap unexpectedly started")
	}
	if _, err = c.FailedOutboxEvents(context.Background(), 1, ""); !errors.Is(err, outbox.ErrClosed) {
		t.Fatalf("failed startup must close SQLite: %v", err)
	}
	if err = c.Start(ctx); err == nil {
		t.Fatal("terminal failed client restarted")
	}
	if err = c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPublicStopInterruptsInitialProvision(t *testing.T) {
	f, addr, key := startLifecycleRuntime(t)
	f.blockProvision = true
	c, err := sb.New(addr, key, sb.WithCallerOnly(), sb.WithDataDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- c.Start(context.Background()) }()
	select {
	case <-f.provisionEntered:
	case <-time.After(time.Second):
		t.Fatal("provision did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-started:
		if err == nil {
			t.Fatal("Start succeeded after Stop")
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not cancel")
	}
	select {
	case <-f.provisionCancelled:
	case <-time.After(time.Second):
		t.Fatal("bootstrap RPC context was not cancelled")
	}
}

func TestPublicStopCancelsAndBoundsUncooperativeJob(t *testing.T) {
	f, addr, key := startLifecycleRuntime(t)
	c, err := sb.New(addr, key, sb.WithCallerOnly(), sb.WithDataDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := job.Interval(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	spec := job.NewSpec(trigger, job.WithVersion("v1"))
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	if err = c.Job.Handle("long", spec, func(ctx context.Context, _ job.Execution) error {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	raw, err := spec.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	f.jobs <- &pb.JobExecution{JobName: "long", ExecutionId: "execution", Fingerprint: internaljob.ContractHash(raw), LeaseEpoch: 1}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("job handler not entered")
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer stopCancel()
	if err = c.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop must be bounded for uncooperative handler: %v", err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("Stop did not cancel active execution before waiting")
	}
	if _, err = c.FailedOutboxEvents(context.Background(), 1, ""); !errors.Is(err, outbox.ErrClosed) {
		t.Fatalf("bounded Stop must close owned SQLite before waiting on handler: %v", err)
	}
	if f.staleResults.Load() != 0 {
		t.Fatal("cancelled execution reported stale success")
	}
}
