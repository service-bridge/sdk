package connection_test

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"math/big"
	"testing"
	"testing/quick"

	"github.com/service-bridge/sdk/go/internal/connection"
	pb "github.com/service-bridge/sdk/go/internal/pb/servicebridge/v1"
	"google.golang.org/protobuf/proto"
)

// encodeKey mirrors connection.EncodeBootstrapKey in the runtime — the SDK only
// ever reads keys, so the encoder lives in the test.
func encodeKey(t *testing.T, keyID, secret, caDER []byte) string {
	t.Helper()
	raw, err := proto.Marshal(&pb.BootstrapKeyPayload{KeyId: keyID, Secret: secret, CaCertDer: caDER})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return "sb." + base64.RawURLEncoding.EncodeToString(raw)
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

func TestParseBootstrapKeyRoundTrip(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	keyID := randomBytes(t, 8)
	secret := randomBytes(t, 32)

	key, err := connection.ParseBootstrapKey(encodeKey(t, keyID, secret, ca.der))
	if err != nil {
		t.Fatalf("ParseBootstrapKey: %v", err)
	}
	if !bytes.Equal(key.KeyID, keyID) {
		t.Errorf("KeyID: got %x want %x", key.KeyID, keyID)
	}
	if !bytes.Equal(key.Secret, secret) {
		t.Errorf("Secret: got %x want %x", key.Secret, secret)
	}
	if !bytes.Equal(key.CACertDER, ca.der) {
		t.Error("CACertDER mismatch")
	}
	if key.CACert == nil || !key.CACert.Equal(ca.cert) {
		t.Error("CACert is not the embedded CA")
	}
}

func TestParseBootstrapKeyPropertyRoundTrip(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)

	roundTrip := func(keyID [8]byte, secret [32]byte) bool {
		key, err := connection.ParseBootstrapKey(encodeKey(t, keyID[:], secret[:], ca.der))
		if err != nil {
			t.Logf("ParseBootstrapKey: %v", err)
			return false
		}
		return bytes.Equal(key.KeyID, keyID[:]) &&
			bytes.Equal(key.Secret, secret[:]) &&
			bytes.Equal(key.CACertDER, ca.der)
	}
	if err := quick.Check(roundTrip, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatalf("round-trip property failed: %v", err)
	}
}

func TestParseBootstrapKeyRejects(t *testing.T) {
	t.Parallel()
	ca := newTestCA(t)
	valid := encodeKey(t, randomBytes(t, 8), randomBytes(t, 32), ca.der)

	tests := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"missing prefix", valid[len("sb."):]},
		{"wrong prefix", "sbx" + valid[len("sb."):]},
		{"broken base64", "sb.!!!not-base64!!!"},
		{"padded base64", "sb." + base64.StdEncoding.EncodeToString([]byte{0xff, 0xfe})},
		{"not a proto message", "sb." + base64.RawURLEncoding.EncodeToString([]byte{0xff, 0xff, 0xff, 0xff})},
		{"empty payload", encodeKey(t, nil, nil, nil)},
		{"key_id too short", encodeKey(t, randomBytes(t, 7), randomBytes(t, 32), ca.der)},
		{"key_id too long", encodeKey(t, randomBytes(t, 9), randomBytes(t, 32), ca.der)},
		{"secret too short", encodeKey(t, randomBytes(t, 8), randomBytes(t, 31), ca.der)},
		{"secret too long", encodeKey(t, randomBytes(t, 8), randomBytes(t, 33), ca.der)},
		{"empty ca", encodeKey(t, randomBytes(t, 8), randomBytes(t, 32), nil)},
		{"broken ca der", encodeKey(t, randomBytes(t, 8), randomBytes(t, 32), []byte{0x30, 0x03, 0x02, 0x01, 0x01})},
		{"ca der truncated", encodeKey(t, randomBytes(t, 8), randomBytes(t, 32), ca.der[:len(ca.der)/2])},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := connection.ParseBootstrapKey(tc.raw)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !errors.Is(err, connection.ErrKey) {
				t.Errorf("kind: got %v want ErrKey", err)
			}
		})
	}
}

func TestParseBootstrapKeyRejectsRandomJunk(t *testing.T) {
	t.Parallel()
	// A random blob must never decode into a usable key. proto.Unmarshal accepts
	// some junk as an empty message, so the length checks carry the weight here.
	for range 200 {
		n, err := rand.Int(rand.Reader, big.NewInt(64))
		if err != nil {
			t.Fatalf("rand: %v", err)
		}
		blob := randomBytes(t, int(n.Int64()))
		raw := "sb." + base64.RawURLEncoding.EncodeToString(blob)
		if _, err := connection.ParseBootstrapKey(raw); err == nil {
			t.Fatalf("random blob parsed as a valid key: %x", blob)
		}
	}
}
