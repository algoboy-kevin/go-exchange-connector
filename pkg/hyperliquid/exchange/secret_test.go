package exchange

import (
	"context"
	"strings"
	"testing"
)

// S16: configuration errors about the agent key must never disclose key
// material (not even the single offending character hex reports).
func TestAgentKeyErrorsDoNotDiscloseKey(t *testing.T) {
	for _, key := range []string{
		"0x" + strings.Repeat("ab", 31) + "qz", // invalid hex near the end
		strings.Repeat("ab", 31) + "a",         // odd length
		"0x" + strings.Repeat("00", 32),        // zero scalar: not a valid key
	} {
		_, err := New(context.Background(), Config{BaseURL: "http://127.0.0.1:1", AgentKey: key, Account: testAccount})
		if err == nil {
			t.Fatalf("New accepted invalid key")
		}
		msg := err.Error()
		if strings.Contains(msg, "'q'") || strings.Contains(msg, "U+0071") || strings.Contains(msg, strings.TrimPrefix(key, "0x")[:8]) {
			t.Errorf("error discloses key material: %q", msg)
		}
	}
}
