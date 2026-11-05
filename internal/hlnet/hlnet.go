// Package hlnet resolves the Hyperliquid endpoints and the .env variable names for
// a network. Test commands use it so that -network testnet|mainnet changes the
// URLs and the keys together, and a key from one network is never used on the
// other.
//
// Variable names (values are never printed):
//
//	testnet: HL_TESTNET_AGENT_KEY (API wallet), HL_TESTNET_MASTER_KEY (main wallet,
//	         reserve only), HL_TESTNET_ACCOUNT (master address)
//	mainnet: HL_MAINNET_AGENT_KEY (API wallet), HL_MAINNET_MASTER_KEY (main wallet,
//	         reserve only), HL_MAINNET_MASTER_ADDRESS (master address)
package hlnet

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Env is the resolved configuration for one network.
type Env struct {
	Network    string // "testnet" or "mainnet" (value passed to exchange.Config.Network)
	InfoURL    string // REST /info
	WSURL      string // WebSocket
	AgentKey   string // API wallet key, may be empty
	MasterKey  string // main wallet key, may be empty (needed only for reserve)
	Account    string // master address, lowercased, may be empty
	AgentVar   string // name of the agent key variable (for messages)
	MasterVar  string // name of the master key variable (for messages)
	AccountVar string // name of the account variable (for messages)
}

var addrRE = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// Resolve picks the endpoints and reads the keys for network. It does not print.
func Resolve(network string) (Env, error) {
	switch strings.ToLower(strings.TrimSpace(network)) {
	case "testnet":
		return Env{
			Network:    "testnet",
			InfoURL:    "https://api.hyperliquid-testnet.xyz/info",
			WSURL:      "wss://api.hyperliquid-testnet.xyz/ws",
			AgentKey:   strings.TrimSpace(os.Getenv("HL_TESTNET_AGENT_KEY")),
			MasterKey:  strings.TrimSpace(os.Getenv("HL_TESTNET_MASTER_KEY")),
			Account:    normAddr(os.Getenv("HL_TESTNET_ACCOUNT")),
			AgentVar:   "HL_TESTNET_AGENT_KEY",
			MasterVar:  "HL_TESTNET_MASTER_KEY",
			AccountVar: "HL_TESTNET_ACCOUNT",
		}, nil
	case "mainnet":
		return Env{
			Network:    "mainnet",
			InfoURL:    "https://api.hyperliquid.xyz/info",
			WSURL:      "wss://api.hyperliquid.xyz/ws",
			AgentKey:   strings.TrimSpace(os.Getenv("HL_MAINNET_AGENT_KEY")),
			MasterKey:  strings.TrimSpace(os.Getenv("HL_MAINNET_MASTER_KEY")),
			Account:    normAddr(os.Getenv("HL_MAINNET_MASTER_ADDRESS")),
			AgentVar:   "HL_MAINNET_AGENT_KEY",
			MasterVar:  "HL_MAINNET_MASTER_KEY",
			AccountVar: "HL_MAINNET_MASTER_ADDRESS",
		}, nil
	}
	return Env{}, fmt.Errorf("unknown network %q (want testnet or mainnet)", network)
}

// ValidAddr reports whether a is a 0x + 40 hex address.
func ValidAddr(a string) bool { return addrRE.MatchString(a) }

func normAddr(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}
