package bnnet

import "testing"

func TestResolve(t *testing.T) {
	t.Setenv("BINANCE_SPOT_TESTNET_API_KEY", "k")
	t.Setenv("BINANCE_SPOT_TESTNET_API_SECRET", "")
	e, err := Resolve("spot", "testnet")
	if err != nil {
		t.Fatal(err)
	}
	if e.RESTURL != "https://testnet.binance.vision" || e.KeyVar != "BINANCE_SPOT_TESTNET_API_KEY" || e.Key != "k" {
		t.Errorf("env = %+v", e)
	}
	if e.MissingVar() != "BINANCE_SPOT_TESTNET_API_SECRET" {
		t.Errorf("MissingVar = %q", e.MissingVar())
	}
	f, _ := Resolve("futures", "mainnet")
	if f.RESTURL != "https://fapi.binance.com" || f.KeyVar != "BINANCE_FUTURES_MAINNET_API_KEY" || f.Key != "" {
		t.Errorf("futures mainnet = %+v", f)
	}
	if _, err := Resolve("spot", "devnet"); err == nil {
		t.Error("unknown network accepted")
	}
	if _, err := Resolve("margin", "testnet"); err == nil {
		t.Error("unknown market accepted")
	}
}
