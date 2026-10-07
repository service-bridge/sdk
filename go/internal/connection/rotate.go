package connection

import (
	"context"
	"crypto/x509"
	"time"

	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/grpc"
)

// Refresher issues the next leaf certificate over a live mTLS channel.
//
// Renewal never goes back through Bootstrap.Provision: the identity is already
// proven by the certificate on the wire, so no argon2id hash is involved.
type Refresher interface {
	Refresh(ctx context.Context, conn grpc.ClientConnInterface, prev Lease) (Lease, error)
}

// ControlRefresher renews through Control.RefreshCert.
type ControlRefresher struct{}

// Refresh sends a fresh CSR down the current channel and assembles the next
// lease from the answer.
//
// The instance identity is stable across a renewal: only the TLS material
// changes. A leaf naming another instance would leave the control session, the
// registrations and the data-plane streams on one identity and every new
// connection on another, so it is refused and the renewal retried.
func (ControlRefresher) Refresh(ctx context.Context, conn grpc.ClientConnInterface, prev Lease) (Lease, error) {
	const op = "refresh certificate"

	priv, csrDER, err := NewCSR()
	if err != nil {
		return Lease{}, err
	}

	resp, err := pb.NewControlClient(conn).RefreshCert(ctx, &pb.RefreshCertRequest{CsrDer: csrDER})
	if err != nil {
		return Lease{}, newError(KindRotate, op, "Control.RefreshCert", err)
	}

	tlsCert, err := NewTLSCertificate(resp.GetCertDer(), resp.GetCaChainDer(), priv)
	if err != nil {
		return Lease{}, err
	}

	id, err := leafIdentity(tlsCert.Leaf)
	if err != nil {
		return Lease{}, err
	}
	if id.ServiceID != prev.Identity.ServiceID {
		return Lease{}, newError(KindRotate, op,
			"renewed leaf carries serviceID "+id.ServiceID+", was "+prev.Identity.ServiceID, nil)
	}
	if got := resp.GetInstanceId(); got != "" && got != id.InstanceID {
		return Lease{}, newError(KindRotate, op,
			"instance_id "+got+" contradicts the SPIFFE SAN "+id.InstanceID, nil)
	}
	if id.InstanceID != prev.Identity.InstanceID {
		return Lease{}, newError(KindRotate, op,
			"renewed leaf names instance "+id.InstanceID+", the session runs as "+prev.Identity.InstanceID, nil)
	}

	return Lease{
		Identity:       id,
		ServiceName:    prev.ServiceName,
		CertDER:        resp.GetCertDer(),
		CAChainDER:     resp.GetCaChainDer(),
		PrivateKey:     priv,
		TLSCert:        tlsCert,
		NotAfterUnixMs: resp.GetNotAfterUnixMs(),
	}, nil
}

// leafIdentity reads the identity the runtime stamped into the leaf. The
// certificate, not the response body, is the authority: it is what every peer
// authenticates against.
func leafIdentity(leaf *x509.Certificate) (Identity, error) {
	const op = "read leaf identity"

	if leaf == nil || len(leaf.URIs) != 1 {
		return Identity{}, newError(KindIdentity, op, "renewed leaf carries no SPIFFE URI SAN", nil)
	}
	return ParseSPIFFE(leaf.URIs[0].String())
}

// rotateOnce renews the certificate in place. It reports false when the
// lifecycle must stop.
//
// Nothing is rebuilt: the renewed leaf is published to the credential holders
// and every TLS configuration reads it on its next handshake. The control
// session is not reopened — the runtime treats a second Control.Open of the
// same instance as a replacement and aborts the first one.
func (l *Lifecycle) rotateOnce(ctx context.Context, cur *session, timer *time.Timer) bool {
	err := l.rotate(ctx, cur)
	if err == nil {
		l.armRotation(timer)
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	if isTerminal(err) {
		l.giveUp(ctx, err)
		return false
	}
	l.cfg.Logger.Warn("connection: rotation failed, staying on the current certificate",
		"retry_in_ms", l.cfg.RotateRetry.Milliseconds(), "error", err)
	resetTimer(timer, l.cfg.RotateRetry)
	return true
}

func (l *Lifecycle) rotate(ctx context.Context, cur *session) error {
	const op = "rotate certificate"

	prev, ok := l.cachedLease()
	if !ok {
		return newError(KindRotate, op, "no lease to renew", nil)
	}
	lease, err := l.cfg.Refresher.Refresh(ctx, cur.conn, prev)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.st.lease = lease
	l.mu.Unlock()
	l.cert.Store(&lease.TLSCert)
	if err := l.cfg.Credentials.Update(ctx, l.credentials(lease)); err != nil {
		return newError(KindRotate, op, "publish the renewed leaf", err)
	}
	l.cfg.Logger.Info("connection: certificate renewed",
		"instance_id", lease.Identity.InstanceID, "not_after_unix_ms", lease.NotAfterUnixMs)
	return nil
}

// armRotation schedules the next renewal off the lease currently in use.
func (l *Lifecycle) armRotation(timer *time.Timer) {
	lease, ok := l.cachedLease()
	if !ok {
		stopTimer(timer)
		return
	}
	delay := l.rotateDelay(lease)
	l.cfg.Logger.Debug("connection: renewal scheduled",
		"delay_ms", delay.Milliseconds(), "not_after_unix_ms", lease.NotAfterUnixMs)
	resetTimer(timer, delay)
}

// rotateDelay renews a lead ahead of expiry, minus a random slice of the jitter
// window. Without the jitter a thousand instances provisioned in the same minute
// renew in the same second and the runtime meets the whole fleet at once.
func (l *Lifecycle) rotateDelay(lease Lease) time.Duration {
	jitter := time.Duration(l.cfg.Random() * float64(l.cfg.RotateJitter))
	renewAt := time.UnixMilli(lease.NotAfterUnixMs).Add(-l.cfg.RotateLead).Add(-jitter)
	delay := renewAt.Sub(l.cfg.Now())
	if delay < l.cfg.MinRotateDelay {
		return l.cfg.MinRotateDelay
	}
	return delay
}

func resetTimer(timer *time.Timer, d time.Duration) {
	stopTimer(timer)
	timer.Reset(d)
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
