// Command derive_key derives Polymarket CLOB API credentials from a wallet key.
//
// Usage (auto-detects .env from current directory):
//
//	cd /path/to/project  # where .env lives
//	go run ./cmd/derive_key/
//
//	# Or with explicit flags:
//	go run ./cmd/derive_key/ --private-key 0x... --funder 0x...
//
// Output:
//
//	POLYMARKET_KEY="<uuid>"
//	POLYMARKET_SECRET="<base64>"
//	POLYMARKET_PASSPHRASE="<passphrase>"
package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/polymarket"
	"github.com/ethereum/go-ethereum/crypto"
)

func main() {
	privateKeyHex := flag.String("private-key", "", "Wallet private key (hex, with or without 0x)")
	funder := flag.String("funder", "", "Funder/proxy address")
	clobURL := flag.String("clob-url", "", "CLOB API URL (default: https://clob.polymarket.com)")
	flag.Parse()

	// Load .env file from current directory (won't override existing env vars).
	loadDotEnv()

	// Fall back to env vars (now including .env values).
	if *privateKeyHex == "" {
		*privateKeyHex = os.Getenv("PRIVATE_KEY")
	}
	if *funder == "" {
		*funder = os.Getenv("FUNDER")
	}
	if *clobURL == "" {
		*clobURL = os.Getenv("POLY_CLOB_URL")
	}

	if *privateKeyHex == "" {
		fmt.Fprintln(os.Stderr, "❌ missing PRIVATE_KEY (set env or --private-key)")
		os.Exit(1)
	}
	if *funder == "" {
		fmt.Fprintln(os.Stderr, "❌ missing FUNDER (set env or --funder)")
		os.Exit(1)
	}

	// Parse private key.
	hexStr := strings.TrimPrefix(*privateKeyHex, "0x")
	keyBytes, err := hex.DecodeString(hexStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ invalid PRIVATE_KEY hex: %v\n", err)
		os.Exit(1)
	}
	key, err := crypto.ToECDSA(keyBytes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ invalid PRIVATE_KEY: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("🔑 Deriving API credentials...")
	fmt.Printf("   Address: %s\n", *funder)
	fmt.Printf("   CLOB:    %s\n", clobURLString(*clobURL))

	creds, err := polymarket.DeriveCredentials(*clobURL, key, *funder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ DeriveCredentials failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n✅ Success! Add these to your .env:")
	fmt.Printf("POLYMARKET_KEY=\"%s\"\n", creds.APIKey)
	fmt.Printf("POLYMARKET_SECRET=\"%s\"\n", creds.Secret)
	fmt.Printf("POLYMARKET_PASSPHRASE=\"%s\"\n", creds.Passphrase)
}

func clobURLString(url string) string {
	if url == "" {
		return "https://clob.polymarket.com (default)"
	}
	return url
}

// loadDotEnv reads .env from the working directory and populates
// any env vars that aren't already set.
func loadDotEnv() {
	f, err := os.Open(".env")
	if err != nil {
		return // .env doesn't exist, that's fine
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.IndexByte(line, '=')
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		// Strip surrounding quotes.
		val = strings.Trim(val, `"'`)
		// Only set if not already in environment.
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
}
