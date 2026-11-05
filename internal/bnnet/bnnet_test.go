package bnnet

import "testing"

func TestResolve(t *testing.T) {
	t.Setenv("BINANCE_SPOT_TESTNET_API_KEY", "k")
	t.Setenv("BINANCE_SPOT_TESTNET_API_SECRET", "")
	e, err := Resolve("spot", "testnet")
	if err != nil {
		t.Fatal(err)
	}
	if e.RESTURL != "https://demo-api.binance.com" || e.KeyVar != "BINANCE_SPOT_TESTNET_API_KEY" || e.Key != "k" {
		t.Errorf("env = %+v", e)
	}
	if e.WSAPIURL != "wss://demo-ws-api.binance.com/ws-api/v3" {
		t.Errorf("spot testnet WSAPIURL = %q", e.WSAPIURL)
	}
	if e.WSURL != "wss://demo-stream.binance.com" {
		t.Errorf("spot testnet WSURL = %q", e.WSURL)
	}
	if e.SecretVar != "BINANCE_SPOT_TESTNET_API_SECRET" {
		t.Errorf("SecretVar = %q", e.SecretVar)
	}
	if e.MissingVar() != "BINANCE_SPOT_TESTNET_API_SECRET" {
		t.Errorf("MissingVar = %q", e.MissingVar())
	}
	f, _ := Resolve("futures", "mainnet")
	if f.RESTURL != "https://fapi.binance.com" || f.KeyVar != "BINANCE_FUTURES_MAINNET_API_KEY" || f.Key != "" {
		t.Errorf("futures mainnet = %+v", f)
	}
	sm, _ := Resolve("spot", "mainnet")
	if sm.RESTURL != "https://api.binance.com" || sm.WSAPIURL != "wss://ws-api.binance.com:443/ws-api/v3" || sm.WSURL != "wss://stream.binance.com:9443" {
		t.Errorf("spot mainnet = %+v", sm)
	}
	ft, _ := Resolve("futures", "testnet")
	if ft.RESTURL != "https://demo-fapi.binance.com" || ft.WSAPIURL != "wss://testnet.binancefuture.com/ws-fapi/v1" {
		t.Errorf("futures testnet = %+v", ft)
	}
	if _, err := Resolve("spot", "devnet"); err == nil {
		t.Error("unknown network accepted")
	}
	if _, err := Resolve("margin", "testnet"); err == nil {
		t.Error("unknown market accepted")
	}
}
