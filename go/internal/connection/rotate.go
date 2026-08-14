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
// The runtime answers with a NEW instanceID under the same serviceID. That is
// what makes the overlap safe: the old and the new session are distinguishable,
// so closing the old one cannot mark the new one disconnected.
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
	if id.InstanceID == prev.Identity.InstanceID {
		return Lease{}, newError(KindRotate, op,
			"renewed leaf reuses instanceID "+id.InstanceID+": the overlapping sessions would be indistinguishable", nil)
	}

	return Lease{
		Identity:    id,
		ServiceName: prev.ServiceName,
		CertDER:     resp.GetCertDer(),
		CAChainDER:  resp.GetCaChainDer(),
		PrivateKey:  priv,
		TLSCert:     tlsCert,
		// not_after_unix is SECONDS here and in ProvisionResponse, unlike every
		// other time field on the wire, which is unix milliseconds (ADR-0006).
		// Read as milliseconds the expiry lands in 1970 and the client renews in
		// a hot loop.
		NotAfter: NotAfterFromUnixSeconds(resp.GetNotAfterUnix()),
	}, nil
}

// leafIdentity reads the identity the runtime stamped into the leaf. The
// certificate, not the response body, is the authority: it is what every peer
// authenticates against.
func leafIdentity(leaf *x509.Certificate) (Identity, error) {
	const op = "read leaf identity"

	if leaf == nil || len(leaf.URIs) == 0 {
		return Identity{}, newError(KindIdentity, op, "renewed leaf carries no SPIFFE URI SAN", nil)
	}
	return ParseSPIFFE(leaf.URIs[0].String())
}

// rotateOnce renews the certificate with an overlap: the new session must be
// welcomed before the old one is closed. It reports false when the lifecycle
// must stop.
//
// On failure nothing was swapped — connect closed the channels it opened and
// left the current session serving — so the renewal simply retries on the
// ladder while the old certificate is still valid.
func (l *Lifecycle) rotateOnce(ctx context.Context, cur *session, timer *time.Timer, attempt *int) bool {
	err := l.rotate(ctx, cur)
	if err == nil {
		*attempt = 0
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

	delay := l.cfg.Backoff.Delay(*attempt)
	*attempt++
	l.cfg.Logger.Warn("connection: rotation failed, staying on the current certificate",
		"attempt", *attempt, "delay_ms", delay.Milliseconds(), "error", err)
	resetTimer(timer, delay)
	return true
}

func (l *Lifecycle) rotate(ctx context.Context, cur *session) error {
	const op = "rotate certificate"

	lease, err := l.cfg.Refresher.Refresh(ctx, cur.conn, cur.lease)
	if err != nil {
		return err
	}

	// The renewed session is opened by the one connect path, so it inherits
	// supervision, credential publication and the lease cache. A rotation that
	// built its own path is how a rotated session ends up unsupervised: it dies
	// in silence and the service never reconnects.
	if err := l.connect(ctx, ctx, func(context.Context) (Lease, error) { return lease, nil }); err != nil {
		return newError(KindRotate, op, "open the renewed session", err)
	}
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
		"delay_ms", delay.Milliseconds(), "not_after", lease.NotAfter)
	resetTimer(timer, delay)
}

// rotateDelay renews a lead ahead of expiry, minus a random slice of the jitter
// window. Without the jitter a thousand instances provisioned in the same minute
// renew in the same second and the runtime meets the whole fleet at once.
func (l *Lifecycle) rotateDelay(lease Lease) time.Duration {
	jitter := time.Duration(l.cfg.Random() * float64(l.cfg.RotateJitter))
	delay := lease.NotAfter.Add(-l.cfg.RotateLead).Add(-jitter).Sub(l.cfg.Now())
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
