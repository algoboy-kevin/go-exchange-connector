package main

import (
	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance/spot"
	"testing"
)

func TestSmokeUserStreamDoesNotRequireServerEventFilter(t *testing.T) {
	cfg := smokeUserStreamConfig(&app{network: spot.Testnet})
	if len(cfg.EventTypes) != 0 {
		t.Fatal("Demo Mode rejects optional subscription filter with -1104; subscribe to all and dispatch executionReport locally")
	}
	if cfg.Network != spot.Testnet {
		t.Fatal("wrong network")
	}
}
