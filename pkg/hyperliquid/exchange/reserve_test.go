package exchange

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
)

// Test key only: a well-known throwaway secp256k1 scalar. It holds no funds and
// is used purely to derive an address for the mismatch check.
const testKeyHex = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"

func TestReserveRefusesKeyNotMatchingAccount(t *testing.T) {
	k, err := crypto.HexToECDSA(testKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	other := "0x0000000000000000000000000000000000000001"
	c := &Client{key: k, account: other}
	_, err = c.ReserveRequestWeight(context.Background(), 1)
	if !errors.Is(err, ErrNotMaster) {
		t.Fatalf("mismatched key must be refused with ErrNotMaster, got %v", err)
	}
}

func TestReserveRejectsNonPositiveWeight(t *testing.T) {
	c := &Client{}
	if _, err := c.ReserveRequestWeight(context.Background(), 0); err == nil {
		t.Fatal("weight 0 must be rejected")
	}
}

func TestReserveRefusesNilKey(t *testing.T) {
	c := &Client{account: "0x0000000000000000000000000000000000000001"}
	if _, err := c.ReserveRequestWeight(context.Background(), 1); !errors.Is(err, ErrNotMaster) {
		t.Fatalf("nil key must be refused, got %v", err)
	}
}
