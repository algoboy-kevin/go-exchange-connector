// Package asfutures adapts the Aster perpetuals client to the shared contract.
// Aster's order, batch, cancel, countdown and position semantics match Binance
// USDⓈ-M (batch caps place 5 / cancel 10, post-only is GTX, countdownTime 0
// stops the timer), so this reuses the bnfutures adapter over the Aster client.
package asfutures

import (
	"github.com/algoboy-kevin/go-exchange-connector/pkg/aster/futures"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/contract"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/contract/bnfutures"
)

// Adapter is the Binance futures adapter bound to an Aster client.
type Adapter = bnfutures.Adapter

var (
	_ contract.Trader         = (*Adapter)(nil)
	_ contract.BatchLimiter   = (*Adapter)(nil)
	_ contract.CancelAller    = (*Adapter)(nil)
	_ contract.DeadManSwitch  = (*Adapter)(nil)
	_ contract.PositionReader = (*Adapter)(nil)
	_ contract.LeverageSetter = (*Adapter)(nil)
)

// New wraps a configured Aster futures client.
func New(c *futures.Client) *Adapter { return bnfutures.New(c) }
