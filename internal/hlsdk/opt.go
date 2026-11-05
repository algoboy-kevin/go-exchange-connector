package hyperliquid

import (
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sonirico/vago/lol"
)

type Opt[T any] func(*T)

func (o Opt[T]) Apply(opt *T) {
	o(opt)
}

type (
	ClientOpt   = Opt[client]
	ExchangeOpt = Opt[Exchange]
	InfoOpt     = Opt[Info]
	WsOpt       = Opt[WebsocketClient]
)

func WsOptDebugMode() WsOpt {
	return func(w *WebsocketClient) {
		w.debug = true
		w.logger = lol.NewZerolog(
			lol.WithLevel(lol.LevelTrace),
			lol.WithWriter(os.Stdout),
			lol.WithEnv(lol.EnvDev),
		)
	}
}

func InfoOptDebugMode() InfoOpt {
	return func(i *Info) {
		i.debug = true
	}
}

func ExchangeOptDebugMode() ExchangeOpt {
	return func(e *Exchange) {
		e.debug = true
	}
}

func clientOptDebugMode() ClientOpt {
	return func(c *client) {
		c.debug = true
		c.logger = lol.NewZerolog(
			lol.WithLevel(lol.LevelTrace),
			lol.WithWriter(os.Stderr),
			lol.WithEnv(lol.EnvDev),
		)
	}
}

// ExchangeOptClientOptions allows passing of ClientOpt to Client
func ExchangeOptClientOptions(opts ...ClientOpt) ExchangeOpt {
	return func(e *Exchange) {
		e.clientOpts = append(e.clientOpts, opts...)
	}
}

// ExchangeOptInfoOptions allows passing of InfoOpt to Info
func ExchangeOptInfoOptions(opts ...InfoOpt) ExchangeOpt {
	return func(e *Exchange) {
		e.infoOpts = append(e.infoOpts, opts...)
	}
}

func ExchangeOptPerpDex(dex string) ExchangeOpt {
	return func(e *Exchange) {
		e.dex = dex
		if dex != "" {
			e.infoOpts = append(e.infoOpts, InfoOptPerpDexName(dex))
		}
	}
}

func InfoOptPerpDexName(dex string) InfoOpt {
	return func(i *Info) {
		i.perpDexName = dex
	}
}

// InfoOptPerpDexs makes the Info span SEVERAL perp DEXes at once (Python SDK
// `Info(perp_dexs=[...])` parity). "" names the core DEX; any other name must
// be present in the /info perpDexs registry. Every listed DEX is mapped to the
// same global asset ids a per-DEX Info (InfoOptPerpDexName) computes, so one
// Exchange can address them all in a single signed bulk action. The core DEX
// uses the `meta` argument of NewInfo; other metas are fetched unless supplied
// through InfoOptPerpDexMetas. It supersedes InfoOptPerpDexName; the existing
// single-DEX behavior is unchanged when this option is not used.
func InfoOptPerpDexs(dexes ...string) InfoOpt {
	return func(i *Info) {
		i.perpDexNames = append([]string(nil), dexes...)
	}
}

// InfoOptPerpDexMetas pre-supplies per-DEX metas (keyed by DEX name, "" for
// core) for InfoOptPerpDexs so construction does not fetch them.
func InfoOptPerpDexMetas(metas map[string]*Meta) InfoOpt {
	return func(i *Info) {
		i.perpDexMetas = make(map[string]*Meta, len(metas))
		for k, v := range metas {
			i.perpDexMetas[k] = v
		}
	}
}

// ExchangeOptPerpDexs builds the Exchange's Info over several perp DEXes (see
// InfoOptPerpDexs). The Exchange's own PerpDex() stays "" (core), which only
// affects its read helpers (SlippagePrice/MarketClose); bulk order and cancel
// actions resolve every listed DEX's coins to their global asset ids.
func ExchangeOptPerpDexs(dexes ...string) ExchangeOpt {
	return func(e *Exchange) {
		e.infoOpts = append(e.infoOpts, InfoOptPerpDexs(dexes...))
	}
}

// ExchangeOptPerpDexMetas pre-supplies per-DEX metas for ExchangeOptPerpDexs.
func ExchangeOptPerpDexMetas(metas map[string]*Meta) ExchangeOpt {
	return func(e *Exchange) {
		e.infoOpts = append(e.infoOpts, InfoOptPerpDexMetas(metas))
	}
}

// ExchangeOptL1Signer injects an L1ActionSigner. When nil, the default ECDSA implementation with privateKey is used.
func ExchangeOptL1Signer(s L1ActionSigner) ExchangeOpt {
	return func(e *Exchange) {
		e.l1Signer = s
	}
}

// ExchangeOptUserSignedSigner injects a UserSignedActionSigner. When nil, the default ECDSA implementation with privateKey is used.
func ExchangeOptUserSignedSigner(s UserSignedActionSigner) ExchangeOpt {
	return func(e *Exchange) {
		e.userSignedSigner = s
	}
}

// ExchangeOptAgentSigner injects an AgentSigner. When nil, the default ECDSA implementation with privateKey is used.
func ExchangeOptAgentSigner(s AgentSigner) ExchangeOpt {
	return func(e *Exchange) {
		e.agentSigner = s
	}
}

// InfoOptClientOptions allows passing of ClientOpt to Info
func InfoOptClientOptions(opts ...ClientOpt) InfoOpt {
	return func(i *Info) {
		i.clientOpts = append(i.clientOpts, opts...)
	}
}

// WsOptReadTimeout sets the maximum duration to wait for a single read from the
// server. If no message is received within the timeout the connection is closed
// and a reconnection is attempted. Must exceed the internal ping interval (50 s).
// Defaults to 90 s.
func WsOptReadTimeout(timeout time.Duration) WsOpt {
	return func(w *WebsocketClient) {
		w.readTimeout = timeout
	}
}

// WsOptDialer allows setting a custom websocket.Dialer
func WsOptDialer(dialer *websocket.Dialer) WsOpt {
	return func(w *WebsocketClient) {
		w.dialer = dialer
	}
}

// ClientOptHTTPClient allows setting a custom http.Client
func ClientOptHTTPClient(httpClient *http.Client) ClientOpt {
	return func(c *client) {
		c.httpClient = httpClient
	}
}
