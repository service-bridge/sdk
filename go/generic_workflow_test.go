package servicebridge

import (
	"errors"
	"fmt"
	"testing"

	"github.com/service-bridge/sdk/go/internal/rpc"
)

func TestTaskErrorCode(t *testing.T) {
	cases := map[string]error{
		"CARD_DECLINED": fmt.Errorf("call: %w", &rpc.HandlerError{Code: "CARD_DECLINED", Message: "no"}),
		"CONFIG":        newError(CodeConfig, "op", "bad", nil),
		"":              errors.New("plain"),
	}
	for want, err := range cases {
		if got := taskErrorCode(err); got != want {
			t.Errorf("%v: %q, want %q", err, got, want)
		}
	}
}
