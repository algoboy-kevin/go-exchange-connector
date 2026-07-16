package polymarket

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// ─────────────────────────────────────────────────────────────
// EIP-712 signing for Polymarket CLOB V2 orders
//
// Domain (CTF Exchange V2):
//
//	EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)
//	- name:    "Polymarket CTF Exchange"
//	- version: "2"
//	- chainId: 137 (Polygon mainnet)
//	- verifyingContract: 0xE111180000d2663C0091e4f400237545B87B996B
//
// Order EIP-712 type:
//
//	Order(uint256 salt,address maker,address signer,uint256 tokenId,
//	      uint256 makerAmount,uint256 takerAmount,uint8 side,
//	      uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)
// ─────────────────────────────────────────────────────────────

const (
	polymarketChainID        = 137 // Polygon mainnet
	ctfExchangeV2Address     = "0xE111180000d2663C0091e4f400237545B87B996B"
	negRiskExchangeV2Address = "0xe2222d279d744050d28e00520010520000310F59"
)

// ─────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────

// SignOrder signs a CLOB Order using EIP-712 typed data signing (CTF Exchange V3).
// Returns the hex-encoded signature (0x-prefixed, 132 chars).
//
// After signing, set the result on order.Signature before submitting to the API.
func SignOrder(order *Order, privateKey *ecdsa.PrivateKey, isNegRisk bool, _ int64) (string, error) {
	// Default to CTF Exchange V2 on Polygon mainnet.
	// V2: 0xE111180000d2663C0091e4f400237545B87B996B
	contractAddr := ctfExchangeV2Address
	if isNegRisk {
		contractAddr = negRiskExchangeV2Address
	}

	hash, err := hashOrderTypedData(order, contractAddr)
	if err != nil {
		return "", fmt.Errorf("eip712 hash: %w", err)
	}

	sig, err := crypto.Sign(hash.Bytes(), privateKey)
	if err != nil {
		return "", fmt.Errorf("crypto.Sign: %w", err)
	}

	// crypto.Sign returns v as 0/1, Ethereum expects 27/28.
	if sig[64] < 27 {
		sig[64] += 27
	}

	return "0x" + common.Bytes2Hex(sig), nil
}

// SignAndSetOrder signs an Order and sets the Signature field.
func SignAndSetOrder(order *Order, privateKey *ecdsa.PrivateKey, isNegRisk bool, feeRateBps int64) error {
	sig, err := SignOrder(order, privateKey, isNegRisk, feeRateBps)
	if err != nil {
		return err
	}
	order.Signature = sig
	return nil
}

// ─────────────────────────────────────────────────────────────
// EIP-712 hash computation — CLOB V2
// ─────────────────────────────────────────────────────────────

type eip712Field struct {
	Name string
	Type string
}

func eip712TypeEncoding(primaryType string, fields []eip712Field) string {
	var b strings.Builder
	b.WriteString(primaryType)
	b.WriteByte('(')
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(f.Type)
		b.WriteByte(' ')
		b.WriteString(f.Name)
	}
	b.WriteByte(')')
	return b.String()
}

// v2DomainFields returns EIP-712 domain fields for CTF Exchange V2.
func v2DomainFields() []eip712Field {
	return []eip712Field{
		{Name: "name", Type: "string"},
		{Name: "version", Type: "string"},
		{Name: "chainId", Type: "uint256"},
		{Name: "verifyingContract", Type: "address"},
	}
}

// v2OrderFields returns the V2 Order EIP-712 type fields.
func v2OrderFields() []eip712Field {
	return []eip712Field{
		{Name: "salt", Type: "uint256"},
		{Name: "maker", Type: "address"},
		{Name: "signer", Type: "address"},
		{Name: "tokenId", Type: "uint256"},
		{Name: "makerAmount", Type: "uint256"},
		{Name: "takerAmount", Type: "uint256"},
		{Name: "side", Type: "uint8"},
		{Name: "signatureType", Type: "uint8"},
		{Name: "timestamp", Type: "uint256"},
		{Name: "metadata", Type: "bytes32"},
		{Name: "builder", Type: "bytes32"},
	}
}

// hashOrderTypedData computes the full EIP-712 hash:
//
//	keccak256("\x19\x01" + domainSeparator + hashStruct(order))
func hashOrderTypedData(order *Order, contractAddr string) (common.Hash, error) {
	// ── Domain separator ────────────────────────────────────
	domainFields := v2DomainFields()
	domainTypeStr := eip712TypeEncoding("EIP712Domain", domainFields)
	domainTypeHash := crypto.Keccak256Hash([]byte(domainTypeStr))

	encoded := make([]byte, 0, 32*5) // typeHash + 4 fields
	encoded = append(encoded, domainTypeHash.Bytes()...)
	encoded = append(encoded, crypto.Keccak256Hash([]byte("Polymarket CTF Exchange")).Bytes()...)
	encoded = append(encoded, crypto.Keccak256Hash([]byte("2")).Bytes()...)
	encoded = append(encoded, uint256Pad(new(big.Int).SetInt64(polymarketChainID))...)
	encoded = append(encoded, addressToBytes(contractAddr)...)
	domainSeparator := crypto.Keccak256Hash(encoded)

	// ── Order struct hash ───────────────────────────────────
	orderFields := v2OrderFields()
	orderTypeStr := eip712TypeEncoding("Order", orderFields)
	orderTypeHash := crypto.Keccak256Hash([]byte(orderTypeStr))

	oEnc := make([]byte, 0, 32*(1+len(orderFields))) // typeHash + 11 fields
	oEnc = append(oEnc, orderTypeHash.Bytes()...)
	oEnc = append(oEnc, uint256Pad(mustParseBigInt(order.Salt))...)        // salt
	oEnc = append(oEnc, addressToBytes(order.Maker)...)                    // maker
	oEnc = append(oEnc, addressToBytes(order.Signer)...)                   // signer
	oEnc = append(oEnc, uint256Pad(mustParseBigInt(order.TokenID))...)     // tokenId
	oEnc = append(oEnc, uint256Pad(mustParseBigInt(order.MakerAmount))...) // makerAmount
	oEnc = append(oEnc, uint256Pad(mustParseBigInt(order.TakerAmount))...) // takerAmount
	oEnc = append(oEnc, sideToUint(order.Side)...)                         // side
	oEnc = append(oEnc, uint64Pad(uint64(order.SignatureType), 8)...)      // signatureType
	oEnc = append(oEnc, uint256Pad(mustParseBigInt(order.Timestamp))...)   // timestamp (ms)
	oEnc = append(oEnc, bytes32Pad(order.Metadata)...)                     // metadata
	oEnc = append(oEnc, bytes32Pad(order.Builder)...)                      // builder
	structHash := crypto.Keccak256Hash(oEnc)

	// ── Final ───────────────────────────────────────────────
	data := make([]byte, 0, 2+32+32)
	data = append(data, 0x19, 0x01)
	data = append(data, domainSeparator.Bytes()...)
	data = append(data, structHash.Bytes()...)
	return crypto.Keccak256Hash(data), nil
}

// ─────────────────────────────────────────────────────────────
// Encoding helpers
// ─────────────────────────────────────────────────────────────

func addressToBytes(addr string) []byte {
	addr = strings.TrimPrefix(addr, "0x")
	addr = strings.TrimPrefix(addr, "0X")
	b := common.HexToAddress("0x" + addr).Bytes()
	if len(b) != 20 {
		b = make([]byte, 20)
	}
	out := make([]byte, 32)
	copy(out[12:], b)
	return out
}

func uint256Pad(v *big.Int) []byte {
	b := make([]byte, 32)
	if v != nil && v.Sign() != 0 {
		vBytes := v.Bytes()
		copy(b[32-len(vBytes):], vBytes)
	}
	return b
}

func uint64Pad(v uint64, _ int) []byte {
	return uint256Pad(new(big.Int).SetUint64(v))
}

// bytes32Pad converts a 0x-prefixed hex string to a 32-byte array for EIP-712.
func bytes32Pad(s string) []byte {
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	if s == "" {
		return make([]byte, 32)
	}
	b, err := hex.DecodeString(s)
	if err != nil || len(b) > 32 {
		b = make([]byte, 32)
	}
	out := make([]byte, 32)
	copy(out[32-len(b):], b)
	return out
}

func sideToUint(side string) []byte {
	switch strings.ToUpper(side) {
	case "BUY":
		return uint64Pad(0, 8)
	case "SELL":
		return uint64Pad(1, 8)
	default:
		return uint64Pad(0, 8)
	}
}

func mustParseBigInt(s string) *big.Int {
	s = strings.TrimSpace(s)
	val, ok := new(big.Int).SetString(s, 0)
	if !ok {
		return new(big.Int)
	}
	return val
}
