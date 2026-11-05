package main

import (
	"github.com/algoboy-kevin/go-exchange-connector/internal/bnnet"
	"testing"
)

func TestUserStreamConfigUsesDocumentedPathLayout(t *testing.T) {
	for _, network := range []string{"testnet", "mainnet"} {
		env, err := bnnet.Resolve(bnnet.Futures, network)
		if err != nil {
			t.Fatal(err)
		}
		cfg := userStreamConfig(&app{env: env})
		if !cfg.KeyInPath {
			t.Errorf("%s: query-style /private/ws?listenKey= returns HTTP 404; require /private/ws/<listenKey>", network)
		}
		if cfg.WSURL != env.WSURL+"/private" {
			t.Errorf("%s: WSURL=%q", network, cfg.WSURL)
		}
	}
}
