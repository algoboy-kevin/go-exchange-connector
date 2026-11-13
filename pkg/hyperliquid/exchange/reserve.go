package exchange

import (
	"context"
	"errors"
	"fmt"
	"strings"

	hl "github.com/algoboy-kevin/go-exchange-connector/internal/hlsdk"
	"github.com/ethereum/go-ethereum/crypto"
)

// ErrNotMaster is returned when the signing key does not control the account.
// reserveRequestWeight must be signed by the master account's own key; an API
// (agent) wallet cannot sign it.
var ErrNotMaster = errors.New("hyperliquid: signing key does not derive to the account address")

// ReserveRequestWeight buys address-based request capacity: weight requests for
// 0.0005 USDC each, paid from the perps balance (docs: "Reserve Additional
// Actions"). It is signed by the key it is given, which must derive to the
// account address. Nothing is sent until the signature check passes.
//
// Build the client with Config{AgentKey: <master key>, Account: <master address>}.
// The key is only used for this call and is never logged.
func (c *Client) ReserveRequestWeight(ctx context.Context, weight int) (*hl.ReserveRequestWeightResponse, error) {
	if weight <= 0 {
		return nil, fmt.Errorf("hyperliquid: reserve weight must be > 0, got %d", weight)
	}
	if c.key == nil {
		return nil, ErrNotMaster
	}
	derived := strings.ToLower(crypto.PubkeyToAddress(c.key.PublicKey).Hex())
	if derived != strings.ToLower(c.account) {
		return nil, ErrNotMaster
	}
	return c.writeExchange().Reserve(ctx, weight)
}
