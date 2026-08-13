package connection_test

import (
	"errors"
	"testing"

	"github.com/service-bridge/sdk/go/internal/connection"
)

func TestParseSPIFFEValid(t *testing.T) {
	t.Parallel()
	const (
		serviceID  = "3f2b1c8e-9d4a-4f11-8a7b-0c5d6e7f8a9b"
		instanceID = "0123456789ab"
	)
	id, err := connection.ParseSPIFFE("spiffe://service-bridge/service/" + serviceID + "/instance/" + instanceID)
	if err != nil {
		t.Fatalf("ParseSPIFFE: %v", err)
	}
	if id.ServiceID != serviceID {
		t.Errorf("ServiceID: got %q want %q", id.ServiceID, serviceID)
	}
	if id.InstanceID != instanceID {
		t.Errorf("InstanceID: got %q want %q", id.InstanceID, instanceID)
	}
}

func TestFormatSPIFFERoundTrip(t *testing.T) {
	t.Parallel()
	want := connection.Identity{
		ServiceID:  "3f2b1c8e-9d4a-4f11-8a7b-0c5d6e7f8a9b",
		InstanceID: "vwxyz0123456",
	}
	uri := connection.FormatSPIFFE(want)
	if uri != "spiffe://service-bridge/service/"+want.ServiceID+"/instance/"+want.InstanceID {
		t.Fatalf("FormatSPIFFE: got %q", uri)
	}
	got, err := connection.ParseSPIFFE(uri)
	if err != nil {
		t.Fatalf("ParseSPIFFE: %v", err)
	}
	if got != want {
		t.Errorf("round trip: got %+v want %+v", got, want)
	}
}

func TestParseSPIFFERejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"foreign scheme", "https://service-bridge/service/svc/instance/inst"},
		{"no scheme", "//service-bridge/service/svc/instance/inst"},
		{"foreign trust domain", "spiffe://servicebridge/service/svc/instance/inst"},
		{"empty trust domain", "spiffe:///service/svc/instance/inst"},
		{"too few segments", "spiffe://service-bridge/service/svc"},
		{"too many segments", "spiffe://service-bridge/service/svc/instance/inst/extra"},
		{"wrong first segment", "spiffe://service-bridge/svc/svc/instance/inst"},
		{"wrong third segment", "spiffe://service-bridge/service/svc/node/inst"},
		{"empty service id", "spiffe://service-bridge/service//instance/inst"},
		{"empty instance id", "spiffe://service-bridge/service/svc/instance/"},
		{"no path", "spiffe://service-bridge"},
		{"malformed uri", "spiffe://service-bridge/service/svc/instance/%zz"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := connection.ParseSPIFFE(tc.raw)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !errors.Is(err, connection.ErrIdentity) {
				t.Errorf("kind: got %v want ErrIdentity", err)
			}
		})
	}
}

func TestSPIFFETrustDomainMatchesRuntime(t *testing.T) {
	t.Parallel()
	// The hyphenated form is what runtime/internal/connection/identity.go and the
	// Node SDK both use; a silent drift here rejects every peer certificate.
	if connection.SPIFFETrustDomain != "service-bridge" {
		t.Fatalf("trust domain drifted: %q", connection.SPIFFETrustDomain)
	}
}
