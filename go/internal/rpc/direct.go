package rpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	gcreds "google.golang.org/grpc/credentials"
)

// Channel cache tuning.
const (
	// DefaultTTLLeadMs retires a channel this long before the leaf certificate
	// behind it expires, so no call starts on a certificate about to die.
	DefaultTTLLeadMs int64 = 5 * 60 * 1000
	// DefaultTTLFloorMs keeps a channel usable for at least this long even when
	// the certificate is already inside the lead window; the rotation that
	// replaces it evicts the whole cache anyway.
	DefaultTTLFloorMs int64 = 60_000
	// DefaultIdleTTLMs closes a channel nothing has used for this long.
	DefaultIdleTTLMs int64 = 5 * 60 * 1000
)

// ErrPeerIdentity is the SPIFFE pinning failure. It is the only authentication
// on the direct path: every leaf in the mesh is signed by the same CA, so chain
// verification alone lets any service impersonate any other.
var ErrPeerIdentity = errors.New("rpc: peer SPIFFE identity mismatch")

// ErrNoLease means no mTLS lease has been published to this transport yet.
var ErrNoLease = errors.New("rpc: no mTLS credentials published yet")

// ErrDirectClosed means the transport has been shut down.
var ErrDirectClosed = errors.New("rpc: direct transport is closed")

// PeerDialer opens a channel to one peer instance. The default builds an mTLS
// channel; tests substitute their own.
type PeerDialer interface {
	DialPeer(ctx context.Context, endpoint string, cfg *tls.Config) (*grpc.ClientConn, error)
}

// GRPCPeerDialer dials a peer over TLS.
//
// Hostname verification is deliberately not in play: advertised endpoints are
// usually raw IPs and the peer leaf carries no DNS or IP SAN. Trust comes from
// the pinned CA plus the SPIFFE URI SAN check in cfg.VerifyConnection.
type GRPCPeerDialer struct{}

// DialPeer builds the channel. grpc.NewClient connects lazily, so a peer that
// cannot come up surfaces on the first RPC, where the retry ladder handles it.
func (GRPCPeerDialer) DialPeer(_ context.Context, endpoint string, cfg *tls.Config) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(gcreds.NewTLS(cfg)))
	if err != nil {
		return nil, fmt.Errorf("rpc: dial peer %s: %w", endpoint, err)
	}
	return conn, nil
}

// DirectConfig tunes the direct transport.
type DirectConfig struct {
	Dialer     PeerDialer
	TTLLeadMs  int64
	TTLFloorMs int64
	IdleTTLMs  int64
	Now        func() int64
	Logger     *slog.Logger
}

func (c DirectConfig) normalized() DirectConfig {
	if c.Dialer == nil {
		c.Dialer = GRPCPeerDialer{}
	}
	if c.TTLLeadMs <= 0 {
		c.TTLLeadMs = DefaultTTLLeadMs
	}
	if c.TTLFloorMs <= 0 {
		c.TTLFloorMs = DefaultTTLFloorMs
	}
	if c.IdleTTLMs <= 0 {
		c.IdleTTLMs = DefaultIdleTTLMs
	}
	if c.Now == nil {
		c.Now = nowUnixMs
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// connKey identifies a cached channel.
//
// The peer identity is part of the key, not just the endpoint. Kubernetes
// recycles pod IPs: a channel keyed by endpoint alone is pinned to the previous
// owner's SPIFFE URI, and every handshake with the new owner fails for a reason
// that looks like an attack. The Node SDK shipped that bug.
type connKey struct {
	endpoint   string
	serviceID  string
	instanceID string
}

type pooledPeer struct {
	conn        *grpc.ClientConn
	expiresAtMs int64
	lastUsedMs  int64
	refs        int
	evicted     bool
}

// Direct takes its certificate from the credential registry like every other
// consumer. Rotation must reach it, or it keeps dialling peers with a leaf that
// is about to expire while the control channel still looks healthy.
var _ connection.CredentialConsumer = (*Direct)(nil)

// Direct calls a callee instance straight over mTLS, bypassing the runtime.
type Direct struct {
	cfg DirectConfig

	mu          sync.Mutex
	creds       connection.Credentials
	haveCreds   bool
	conns       map[connKey]*pooledPeer
	lastSweepMs int64
	closed      bool
}

// NewDirect builds the transport. It holds no credentials until the credential
// registry publishes a lease.
func NewDirect(cfg DirectConfig) *Direct {
	return &Direct{
		cfg:   cfg.normalized(),
		conns: make(map[connKey]*pooledPeer),
	}
}

// UseCredentials adopts a new lease and drops every cached channel: each one
// carries the previous leaf certificate as its client identity.
func (d *Direct) UseCredentials(_ context.Context, creds connection.Credentials) error {
	if creds.TLS == nil {
		return fmt.Errorf("rpc: adopt credentials: %w", ErrNoLease)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDirectClosed
	}
	d.creds = creds
	d.haveCreds = true
	d.dropAllLocked()
	return nil
}

// Close releases every channel. In-flight calls keep theirs until they finish.
func (d *Direct) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	d.dropAllLocked()
	return nil
}

// RetainInstances closes the channels of instances that left the mesh.
func (d *Direct) RetainInstances(live map[string]struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for k, p := range d.conns {
		if _, ok := live[k.instanceID]; !ok {
			delete(d.conns, k)
			d.retire(p)
		}
	}
}

// Cached reports how many channels the pool holds.
func (d *Direct) Cached() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.conns)
}

// Unary calls the callee and returns its response payload.
func (d *Direct) Unary(ctx context.Context, target Candidate, req *pb.CallRequest) ([]byte, error) {
	l, err := d.lease(ctx, target)
	if err != nil {
		return nil, err
	}
	defer l.release()

	resp, err := pb.NewCallClient(l.conn).Unary(ctx, req)
	if err != nil {
		d.evictOnTransportError(l.key, err)
		return nil, fmt.Errorf("rpc: direct unary %s to %s: %w", req.GetMethod(), target.Endpoint, err)
	}
	if resp.GetErrorCode() != "" {
		return nil, handlerError(resp.GetErrorCode(), resp.GetErrorMessage())
	}
	return resp.GetPayload(), nil
}

// Stream opens a server-side stream to the callee. The returned Stream owns the
// channel reservation and releases it on Close or on the terminal chunk.
func (d *Direct) Stream(ctx context.Context, target Candidate, req *pb.CallRequest) (*Stream, error) {
	l, err := d.lease(ctx, target)
	if err != nil {
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	cs, err := pb.NewCallClient(l.conn).Stream(streamCtx, req)
	if err != nil {
		cancel()
		l.release()
		d.evictOnTransportError(l.key, err)
		return nil, fmt.Errorf("rpc: direct stream %s to %s: %w", req.GetMethod(), target.Endpoint, err)
	}

	key := l.key
	return newStream(
		func() ([]byte, error) {
			chunk, rerr := cs.Recv()
			if rerr != nil {
				d.evictOnTransportError(key, rerr)
				return nil, rerr
			}
			if chunk.GetErrorCode() != "" {
				return nil, handlerError(chunk.GetErrorCode(), chunk.GetErrorMessage())
			}
			return chunk.GetPayload(), nil
		},
		func() {
			cancel()
			l.release()
		},
	), nil
}

// peerLease is one reservation on a cached channel. Reference counting is what
// lets an eviction close the channel without cutting a call that is still using
// it: the last holder closes.
type peerLease struct {
	direct *Direct
	key    connKey
	peer   *pooledPeer
	conn   *grpc.ClientConn
	once   sync.Once
}

func (l *peerLease) release() {
	l.once.Do(func() { l.direct.release(l.peer) })
}

func (d *Direct) lease(_ context.Context, target Candidate) (*peerLease, error) {
	if target.Endpoint == "" {
		return nil, fmt.Errorf("rpc: dial %s/%s: %w", target.ServiceName, target.InstanceID, ErrNoEndpoint)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, ErrDirectClosed
	}
	if !d.haveCreds {
		return nil, ErrNoLease
	}

	now := d.cfg.Now()
	d.sweepLocked(now)

	key := connKey{endpoint: target.Endpoint, serviceID: target.ServiceID, instanceID: target.InstanceID}
	p := d.conns[key]
	if p != nil && now >= p.expiresAtMs {
		delete(d.conns, key)
		d.retire(p)
		p = nil
	}
	if p == nil {
		cfg, err := peerTLSConfig(d.creds.TLS, connection.Identity{ServiceID: target.ServiceID, InstanceID: target.InstanceID})
		if err != nil {
			return nil, err
		}
		conn, err := d.cfg.Dialer.DialPeer(context.Background(), target.Endpoint, cfg)
		if err != nil {
			return nil, err
		}
		p = &pooledPeer{conn: conn, expiresAtMs: now + d.channelTTLMs(now)}
		d.conns[key] = p
	}
	p.lastUsedMs = now
	p.refs++
	return &peerLease{direct: d, key: key, peer: p, conn: p.conn}, nil
}

func (d *Direct) release(p *pooledPeer) {
	d.mu.Lock()
	defer d.mu.Unlock()

	p.refs--
	if p.evicted && p.refs <= 0 {
		d.closeConn(p.conn)
	}
}

func (d *Direct) evictOnTransportError(key connKey, err error) {
	// Unavailable is what gRPC reports for a refused connection, a reset peer
	// and a failed handshake alike, and all three mean the cached channel is
	// worthless. A handler error travels in error_code and leaves it alone.
	if code, ok := callCode(err); !ok || code != codes.Unavailable {
		return
	}
	d.evict(key)
}

func (d *Direct) evict(key connKey) {
	d.mu.Lock()
	defer d.mu.Unlock()

	p, ok := d.conns[key]
	if !ok {
		return
	}
	delete(d.conns, key)
	d.retire(p)
}

// retire marks a channel for closing and closes it once no call holds it.
// Callers hold d.mu.
func (d *Direct) retire(p *pooledPeer) {
	p.evicted = true
	if p.refs <= 0 {
		d.closeConn(p.conn)
	}
}

func (d *Direct) dropAllLocked() {
	for k, p := range d.conns {
		delete(d.conns, k)
		d.retire(p)
	}
}

func (d *Direct) sweepLocked(now int64) {
	if now-d.lastSweepMs < d.cfg.IdleTTLMs {
		return
	}
	d.lastSweepMs = now
	for k, p := range d.conns {
		if p.refs <= 0 && now-p.lastUsedMs >= d.cfg.IdleTTLMs {
			delete(d.conns, k)
			d.retire(p)
		}
	}
}

// channelTTLMs derives the channel lifetime from the local leaf certificate:
// once it expires the peer stops accepting our client identity, so the channel
// dies with it.
func (d *Direct) channelTTLMs(now int64) int64 {
	notAfter := d.creds.Lease.NotAfter
	if notAfter.IsZero() {
		return d.cfg.TTLFloorMs
	}
	ttl := notAfter.UnixMilli() - now - d.cfg.TTLLeadMs
	if ttl < d.cfg.TTLFloorMs {
		return d.cfg.TTLFloorMs
	}
	return ttl
}

func (d *Direct) closeConn(conn *grpc.ClientConn) {
	if err := conn.Close(); err != nil {
		d.cfg.Logger.Warn("rpc: closing a peer channel", "error", err)
	}
}

// peerTLSConfig derives the per-peer client config: the session's own chain
// verification plus the SPIFFE pin for exactly this instance.
func peerTLSConfig(base *tls.Config, want connection.Identity) (*tls.Config, error) {
	if base == nil {
		return nil, fmt.Errorf("rpc: peer TLS config: %w", ErrNoLease)
	}
	chain := base.VerifyConnection
	if base.InsecureSkipVerify && chain == nil {
		// The session config switches off the built-in check because peer leaves
		// carry no hostname; something must replace it. Dialling without either
		// would trust any certificate at all.
		return nil, fmt.Errorf("rpc: peer TLS config: base config verifies nothing")
	}

	cfg := base.Clone()
	expect := connection.FormatSPIFFE(want)
	cfg.VerifyConnection = func(cs tls.ConnectionState) error {
		if chain != nil {
			if err := chain(cs); err != nil {
				return err
			}
		}
		return verifyPeerIdentity(cs, expect)
	}
	return cfg, nil
}

// verifyPeerIdentity requires the peer leaf to carry exactly the expected
// SPIFFE URI SAN.
func verifyPeerIdentity(cs tls.ConnectionState, expect string) error {
	if len(cs.PeerCertificates) == 0 {
		return fmt.Errorf("rpc: verify peer identity: no peer certificate: %w", ErrPeerIdentity)
	}
	for _, u := range cs.PeerCertificates[0].URIs {
		if u.String() == expect {
			return nil
		}
	}
	return fmt.Errorf("rpc: verify peer identity: want %s, got %s: %w",
		expect, joinURIs(cs.PeerCertificates[0].URIs), ErrPeerIdentity)
}

func joinURIs(uris []*url.URL) string {
	if len(uris) == 0 {
		return "<none>"
	}
	parts := make([]string, 0, len(uris))
	for _, u := range uris {
		parts = append(parts, u.String())
	}
	return strings.Join(parts, ", ")
}
