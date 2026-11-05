// Package bnnet resolves the Binance endpoints and the .env variable names for a
// market and network. Live test commands use it so that -network testnet|mainnet
// changes the URLs and the keys together, and a key from one network or market is
// never used on another. It never prints.
//
// Variable names (values are never printed; the tools read the process
// environment, so `set -a; source .env; set +a` first):
//
//	BINANCE_FUTURES_TESTNET_API_KEY / _API_SECRET   USDⓈ-M futures testnet (demo-fapi)
//	BINANCE_FUTURES_MAINNET_API_KEY / _API_SECRET
//	BINANCE_SPOT_TESTNET_API_KEY / _API_SECRET      spot "testnet" = Spot Demo Mode (demo-api.binance.com)
//	BINANCE_SPOT_MAINNET_API_KEY / _API_SECRET
package bnnet

import (
	"fmt"
	"os"
	"strings"
)

// Market selects the product.
const (
	Futures = "futures"
	Spot    = "spot"
)

// Env is the resolved configuration for one market and network.
type Env struct {
	Market    string
	Network   string // "testnet" or "mainnet"
	RESTURL   string // signed REST base
	WSAPIURL  string // WebSocket API (orders over WS); may be empty
	WSURL     string // market/user stream base
	Key       string // may be empty
	Secret    string // may be empty
	KeyVar    string // names, for messages
	SecretVar string
}

// Resolve picks endpoints and reads the keys. Unknown market or network is an error.
func Resolve(market, network string) (Env, error) {
	m := strings.ToLower(strings.TrimSpace(market))
	n := strings.ToLower(strings.TrimSpace(network))
	if n != "testnet" && n != "mainnet" {
		return Env{}, fmt.Errorf("bnnet: unknown network %q (want testnet or mainnet)", network)
	}
	e := Env{Market: m, Network: n}
	switch {
	case m == Futures && n == "testnet":
		e.RESTURL, e.WSURL = "https://demo-fapi.binance.com", "wss://demo-fstream.binance.com"
		e.WSAPIURL = "wss://testnet.binancefuture.com/ws-fapi/v1"
	case m == Futures && n == "mainnet":
		e.RESTURL, e.WSURL = "https://fapi.binance.com", "wss://fstream.binance.com"
		e.WSAPIURL = "wss://ws-fapi.binance.com/ws-fapi/v1"
	case m == Spot && n == "testnet":
		// "testnet" is the project alias for Binance Spot Demo Mode (official
		// doc: demo-mode/general-info). Binance's legacy Spot Testnet is not used.
		e.RESTURL, e.WSURL = "https://demo-api.binance.com", "wss://demo-stream.binance.com"
		e.WSAPIURL = "wss://demo-ws-api.binance.com/ws-api/v3"
	case m == Spot && n == "mainnet":
		e.RESTURL, e.WSURL = "https://api.binance.com", "wss://stream.binance.com:9443"
		e.WSAPIURL = "wss://ws-api.binance.com:443/ws-api/v3"
	default:
		return Env{}, fmt.Errorf("bnnet: unknown market %q (want futures or spot)", market)
	}
	prefix := "BINANCE_" + strings.ToUpper(m) + "_" + strings.ToUpper(n) + "_"
	e.KeyVar, e.SecretVar = prefix+"API_KEY", prefix+"API_SECRET"
	e.Key = strings.TrimSpace(os.Getenv(e.KeyVar))
	e.Secret = strings.TrimSpace(os.Getenv(e.SecretVar))
	return e, nil
}

// MissingVar returns the name of the first unset key variable, or "".
func (e Env) MissingVar() string {
	if e.Key == "" {
		return e.KeyVar
	}
	if e.Secret == "" {
		return e.SecretVar
	}
	return ""
}
