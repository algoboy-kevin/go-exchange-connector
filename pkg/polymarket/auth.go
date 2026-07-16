package polymarket

import (
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// ─────────────────────────────────────────────────────────────
// L1 Authentication — Derive API credentials from wallet key
//
// The CLOB API uses two-level auth:
//   L1 (Private Key) — proves wallet ownership via EIP-712
//   L2 (API Key)     — HMAC-SHA256 for trading endpoints
//
// This file implements L1 auth to derive L2 credentials.
//
// EIP-712 domain: { name: "ClobAuthDomain", version: "1", chainId: 137 }
// EIP-712 type:
//   ClobAuth(address address,string timestamp,uint256 nonce,string message)
// ─────────────────────────────────────────────────────────────

// APIKeyCredentials holds the derived CLOB API credentials.
type APIKeyCredentials struct {
	APIKey     string `json:"apiKey"`
	Secret     string `json:"secret"`
	Passphrase string `json:"passphrase"`
}

// ClobAuthMessage is the EIP-712 struct for L1 authentication.
type ClobAuthMessage struct {
	Address   string // signer address
	Timestamp string // UNIX timestamp as string
	Nonce     int64  // default 0
	Message   string // "This message attests that I control the given wallet"
}

const clobAuthMessageStr = "This message attests that I control the given wallet"

// DeriveCredentials derives CLOB API credentials from a wallet private key.
//
// It performs L1 authentication:
//  1. Builds and signs the ClobAuth EIP-712 message
//  2. Calls GET /auth/derive-api-key with L1 headers
//  3. Returns apiKey + secret + passphrase
//
// These credentials can then be used for L2 authenticated trading.
func DeriveCredentials(clobURL string, privateKey *ecdsa.PrivateKey, funderAddress string) (*APIKeyCredentials, error) {
	if clobURL == "" {
		clobURL = defaultClobURL
	}
	clobURL = strings.TrimRight(clobURL, "/")

	// Use current time as timestamp (seconds).
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := int64(0)

	// Build and sign the ClobAuth EIP-712 message.
	sig, err := signClobAuth(privateKey, funderAddress, ts, nonce)
	if err != nil {
		return nil, fmt.Errorf("sign clob auth: %w", err)
	}

	// Build L1 headers.
	url := clobURL + "/auth/derive-api-key"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("POLY_ADDRESS", funderAddress)
	req.Header.Set("POLY_SIGNATURE", sig)
	req.Header.Set("POLY_TIMESTAMP", ts)
	req.Header.Set("POLY_NONCE", strconv.FormatInt(nonce, 10))

	// Send request.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("derive request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("derive api key: %s returned %d: %s", url, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var creds APIKeyCredentials
	if err := json.NewDecoder(resp.Body).Decode(&creds); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if creds.APIKey == "" || creds.Secret == "" || creds.Passphrase == "" {
		return nil, fmt.Errorf("derive api key: incomplete response: %+v", creds)
	}

	return &creds, nil
}

// ─────────────────────────────────────────────────────────────
// EIP-712 signing for ClobAuth
// ─────────────────────────────────────────────────────────────

// clobAuthFields returns the EIP-712 type fields for ClobAuth.
func clobAuthFields() []eip712Field {
	return []eip712Field{
		{Name: "address", Type: "address"},
		{Name: "timestamp", Type: "string"},
		{Name: "nonce", Type: "uint256"},
		{Name: "message", Type: "string"},
	}
}

// signClobAuth builds and signs the ClobAuth EIP-712 typed data.
// Returns the hex-encoded signature (0x-prefixed).
func signClobAuth(privateKey *ecdsa.PrivateKey, address, timestamp string, nonce int64) (string, error) {
	fields := clobAuthFields()

	// Compute domain separator hash.
	// ClobAuth domain: { name: "ClobAuthDomain", version: "1", chainId: 137 }
	domainFields := []eip712Field{
		{Name: "name", Type: "string"},
		{Name: "version", Type: "string"},
		{Name: "chainId", Type: "uint256"},
	}
	domainTypeStr := eip712TypeEncoding("EIP712Domain", domainFields)
	domainTypeHash := crypto.Keccak256Hash([]byte(domainTypeStr))

	domainEncoded := make([]byte, 0, 32*4) // typeHash + 3 fields
	domainEncoded = append(domainEncoded, domainTypeHash.Bytes()...)
	domainEncoded = append(domainEncoded, crypto.Keccak256Hash([]byte("ClobAuthDomain")).Bytes()...)
	domainEncoded = append(domainEncoded, crypto.Keccak256Hash([]byte("1")).Bytes()...)
	domainEncoded = append(domainEncoded, uint256Pad(new(big.Int).SetInt64(polymarketChainID))...)
	domainSeparator := crypto.Keccak256Hash(domainEncoded)

	// Compute ClobAuth struct hash.
	authTypeStr := eip712TypeEncoding("ClobAuth", fields)
	typeHash := crypto.Keccak256Hash([]byte(authTypeStr))

	encoded := make([]byte, 0, 32*(1+len(fields)))
	encoded = append(encoded, typeHash.Bytes()...)

	// address
	encoded = append(encoded, addressToBytes(address)...)

	// timestamp (string → keccak256)
	encoded = append(encoded, crypto.Keccak256Hash([]byte(timestamp)).Bytes()...)

	// nonce (uint256)
	encoded = append(encoded, uint256Pad(bigInt(nonce))...)

	// message (string → keccak256)
	encoded = append(encoded, crypto.Keccak256Hash([]byte(clobAuthMessageStr)).Bytes()...)

	structHash := crypto.Keccak256Hash(encoded)

	// Final: keccak256("\x19\x01" + domainSeparator + structHash)
	data := make([]byte, 0, 2+32+32)
	data = append(data, 0x19, 0x01)
	data = append(data, domainSeparator.Bytes()...)
	data = append(data, structHash.Bytes()...)
	finalHash := crypto.Keccak256Hash(data)

	// Sign.
	sig, err := crypto.Sign(finalHash.Bytes(), privateKey)
	if err != nil {
		return "", fmt.Errorf("crypto.Sign: %w", err)
	}

	// Fix v (crypto.Sign returns 0/1, Ethereum expects 27/28).
	if sig[64] < 27 {
		sig[64] += 27
	}

	return "0x" + common.Bytes2Hex(sig), nil
}

// bigInt returns a *big.Int for an int64.
func bigInt(v int64) *big.Int {
	return new(big.Int).SetInt64(v)
}
