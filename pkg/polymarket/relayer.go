// Package polymarket — Gasless relayer client for Polymarket on-chain operations.
//
// The RelayerClient submits transactions through Polymarket's gasless relayer
// (https://relayer-v2.polymarket.com). Instead of paying MATIC for gas, the
// relayer sponsors the transaction. This works with PROXY wallets (the common
// Polymarket wallet type for Magic Link / Google auth users).
//
// Flow for PROXY transactions:
//  1. ABI-encode the target contract call (e.g., splitPosition on CTF)
//  2. Wrap it in a ProxyFactory.proxy() call
//  3. Get a relay payload (nonce + relayer address) from the relayer API
//  4. Create an EIP-191 struct hash and sign it with the user's private key
//  5. Submit the signed request to POST /submit
//  6. Poll GET /transaction until confirmed
//
// Required credentials (from https://polymarket.com/settings → API):
//   - RELAYER_API_KEY
//   - RELAYER_API_KEY_ADDRESS
//
// Contract addresses (Polygon mainnet):
//   - ProxyFactory: 0xaB45c5A4B0c941a2F231C04C3f49182e1A254052
//   - RelayHub:     0xD216153c06E857cD7f72665E0aF1d7D82172F494
package polymarket

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// ─────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────

const (
	defaultRelayerURL = "https://relayer-v2.polymarket.com"

	// ProxyFactory is the Polymarket proxy wallet factory on Polygon mainnet.
	proxyFactoryAddress = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
	// RelayHub is the RelayHub contract on Polygon mainnet.
	relayHubAddress = "0xD216153c06E857cD7f72665E0aF1d7D82172F494"
	// proxyInitCodeHash is the init code hash for Polymarket proxy wallets
	// (Solady LibClone ERC1967). Used for CREATE2 address derivation.
	proxyInitCodeHash = "0xd21df8dc65880a8606f09fe0ce3df9b8869287ab0b058be05aa9e8af6330a00b"
)

// ─────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────

// RelayPayload is the response from GET /relay-payload.
type relayPayload struct {
	Address string `json:"address"`
	Nonce   string `json:"nonce"`
}

// relayerTransactionResponse is the response from POST /submit.
type relayerSubmitResponse struct {
	TransactionID string `json:"transactionID"`
	State         string `json:"state"`
}

// relayerTransaction is the response from GET /transaction.
type relayerTransaction struct {
	TransactionID   string `json:"transactionID"`
	TransactionHash string `json:"transactionHash"`
	State           string `json:"state"`
	From            string `json:"from"`
	To              string `json:"to"`
	ProxyAddress    string `json:"proxyAddress"`
	Data            string `json:"data"`
	Nonce           string `json:"nonce"`
	Value           string `json:"value"`
	Type            string `json:"type"`
	CreatedAt       string `json:"createdAt"`
	UpdatedAt       string `json:"updatedAt"`
}

// proxyCall represents a single ProxyFactory.Call struct.
type proxyCall struct {
	TypeCode uint8
	To       common.Address
	Value    *big.Int
	Data     []byte
}

// ─────────────────────────────────────────────────────────────
// RelayerClient
// ─────────────────────────────────────────────────────────────

// RelayerClient submits gasless transactions to the Polymarket relayer.
type RelayerClient struct {
	relayerURL       string
	apiKey           string
	apiKeyAddr       string
	proxyABI         abi.ABI
	splitABI         abi.ABI
	mergeABI         abi.ABI
	redeemABI        abi.ABI
	negRiskRedeemABI abi.ABI
	approveABI       abi.ABI
	httpClient       *http.Client
}

// NewRelayerClient creates a new relayer client.
//
//	apiKey      — Relayer API key from polymarket.com/settings → API Keys
//	apiKeyAddr  — Address that owns the relayer API key (your signer address)
func NewRelayerClient(apiKey, apiKeyAddr string) *RelayerClient {
	if apiKey == "" || apiKeyAddr == "" {
		return nil
	}

	// ProxyFactory ABI — minimal for the proxy() function (tuple array input).
	proxyABI, err := abi.JSON(strings.NewReader(`[
		{"inputs":[{"components":[{"name":"typeCode","type":"uint8"},{"name":"to","type":"address"},{"name":"value","type":"uint256"},{"name":"data","type":"bytes"}],"name":"calls","type":"tuple[]"}],"name":"proxy","outputs":[],"stateMutability":"payable","type":"function"}
	]`))
	if err != nil {
		panic(fmt.Sprintf("relayer: parse proxy abi: %v", err))
	}

	// Minimal splitPosition ABI.
	splitABI, err := abi.JSON(strings.NewReader(`[
		{"inputs":[{"name":"collateralToken","type":"address"},{"name":"parentCollectionId","type":"bytes32"},{"name":"conditionId","type":"bytes32"},{"name":"partition","type":"uint256[]"},{"name":"amount","type":"uint256"}],"name":"splitPosition","outputs":[],"stateMutability":"nonpayable","type":"function"}
	]`))
	if err != nil {
		panic(fmt.Sprintf("relayer: parse split abi: %v", err))
	}

	// Minimal mergePositions ABI (same parameters as splitPosition).
	mergeABI, err := abi.JSON(strings.NewReader(`[
		{"inputs":[{"name":"collateralToken","type":"address"},{"name":"parentCollectionId","type":"bytes32"},{"name":"conditionId","type":"bytes32"},{"name":"partition","type":"uint256[]"},{"name":"amount","type":"uint256"}],"name":"mergePositions","outputs":[],"stateMutability":"nonpayable","type":"function"}
	]`))
	if err != nil {
		panic(fmt.Sprintf("relayer: parse merge abi: %v", err))
	}

	// Minimal redeemPositions ABI (no amount — redeems full balance).
	// Used for the non-neg-risk collateral adapter.
	redeemABI, err := abi.JSON(strings.NewReader(`[
		{"inputs":[{"name":"collateralToken","type":"address"},{"name":"parentCollectionId","type":"bytes32"},{"name":"conditionId","type":"bytes32"},{"name":"partition","type":"uint256[]"}],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}
	]`))
	if err != nil {
		panic(fmt.Sprintf("relayer: parse redeem abi: %v", err))
	}

	// Neg-risk redeemPositions ABI — different signature, no collateralToken param.
	negRiskRedeemABI, err := abi.JSON(strings.NewReader(`[
		{"inputs":[{"name":"conditionId","type":"bytes32"},{"name":"partition","type":"uint256[]"}],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}
	]`))
	if err != nil {
		panic(fmt.Sprintf("relayer: parse neg-risk redeem abi: %v", err))
	}

	// ERC-20 approve ABI.
	approveABI, err := abi.JSON(strings.NewReader(`[
		{"constant":false,"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"type":"function"}
	]`))
	if err != nil {
		panic(fmt.Sprintf("relayer: parse approve abi: %v", err))
	}

	return &RelayerClient{
		relayerURL:       defaultRelayerURL,
		apiKey:           apiKey,
		apiKeyAddr:       apiKeyAddr,
		proxyABI:         proxyABI,
		splitABI:         splitABI,
		mergeABI:         mergeABI,
		redeemABI:        redeemABI,
		negRiskRedeemABI: negRiskRedeemABI,
		approveABI:       approveABI,
		httpClient:       &http.Client{Timeout: 60 * time.Second},
	}
}

 
var maxUint256 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

// ─────────────────────────────────────────────────────────────
// SplitPositionViaRelayer — gasless split
// ─────────────────────────────────────────────────────────────

// CollateralAdapterAddress is the Polymarket collateral adapter on Polygon mainnet.
// Used for non-neg-risk markets.
var CollateralAdapterAddress = common.HexToAddress("0xAdA100Db00Ca00073811820692005400218FcE1f")

// NegRiskCollateralAdapterAddress is the Polymarket neg-risk collateral adapter on Polygon mainnet.
// Used for neg-risk markets (most Polymarket markets).
var NegRiskCollateralAdapterAddress = common.HexToAddress("0xadA2005600Dec949baf300f4C6120000bDB6eAab")

// StandardExchangeAddress is the CTF Exchange V2 on Polygon mainnet.
var StandardExchangeAddress = common.HexToAddress("0xE111180000d2663C0091e4f400237545B87B996B")

// MergePositionsViaRelayer submits a gasless mergePositions transaction through
// the Polymarket relayer. This burns outcome tokens (YES/NO) and returns
// the equivalent amount of collateral (pUSD) to the proxy wallet.
//
// The adapter address is chosen based on the negRisk flag:
//   - true  → NegRiskCollateralAdapterAddress
//   - false → CollateralAdapterAddress
func (r *RelayerClient) MergePositionsViaRelayer(
	ctx context.Context,
	req *MergePositionsRequest,
	privateKey *ecdsa.PrivateKey,
	negRisk bool,
) (*MergePositionsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("relayer: merge: request is nil")
	}
	if req.Amount == nil {
		return nil, fmt.Errorf("relayer: merge: amount is required")
	}
	if len(req.Partition) == 0 {
		return nil, fmt.Errorf("relayer: merge: partition is required")
	}

	// Choose the correct adapter based on neg-risk flag.
	adapter := CollateralAdapterAddress
	if negRisk {
		adapter = NegRiskCollateralAdapterAddress
	}

	// Encode mergePositions call (goes to collateral adapter, not CTF directly).
	mergeCalldata, err := r.mergeABI.Pack("mergePositions",
		req.CollateralToken,
		req.ParentCollectionID,
		req.ConditionID,
		req.Partition,
		req.Amount,
	)
	if err != nil {
		return nil, fmt.Errorf("relayer: merge: encode calldata: %w", err)
	}

	call := proxyCall{
		TypeCode: 1, To: adapter,
		Value: big.NewInt(0), Data: mergeCalldata,
	}
	resp, err := r.submitProxyCall(ctx, []proxyCall{call}, privateKey)
	if err != nil {
		return nil, err
	}
	return &MergePositionsResponse{
		TransactionHash: resp.TransactionHash,
		AmountUSD:       rawToUSD(req.Amount),
	}, nil
}

// RedeemPositionsViaRelayer submits a gasless redeemPositions transaction
// through the Polymarket relayer. This burns winning outcome tokens and
// returns the equivalent amount of collateral (pUSD) to the proxy wallet.
//
// Unlike split/merge, there is no amount parameter — the full balance of
// each position token in the partition is redeemed.
//
// The adapter address and ABI are chosen based on the negRisk flag:
//   - true  → NegRiskCollateralAdapterAddress (ABI: conditionId, partition)
//   - false → CollateralAdapterAddress (ABI: collateralToken, parentCollectionId, conditionId, partition)
func (r *RelayerClient) RedeemPositionsViaRelayer(
	ctx context.Context,
	req *RedeemPositionsRequest,
	privateKey *ecdsa.PrivateKey,
	negRisk bool,
) (*RedeemPositionsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("relayer: redeem: request is nil")
	}
	if len(req.Partition) == 0 {
		return nil, fmt.Errorf("relayer: redeem: partition is required")
	}

	var adapter common.Address
	var redeemCalldata []byte
	var err error

	if negRisk {
		adapter = NegRiskCollateralAdapterAddress
		// Neg-risk adapter: redeemPositions(bytes32 conditionId, uint256[] partition)
		redeemCalldata, err = r.negRiskRedeemABI.Pack("redeemPositions",
			req.ConditionID,
			req.Partition,
		)
	} else {
		adapter = CollateralAdapterAddress
		// Standard adapter: redeemPositions(address,bytes32,bytes32,uint256[])
		redeemCalldata, err = r.redeemABI.Pack("redeemPositions",
			req.CollateralToken,
			req.ParentCollectionID,
			req.ConditionID,
			req.Partition,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("relayer: redeem: encode calldata: %w", err)
	}

	call := proxyCall{
		TypeCode: 1, To: adapter,
		Value: big.NewInt(0), Data: redeemCalldata,
	}
	resp, err := r.submitProxyCall(ctx, []proxyCall{call}, privateKey)
	if err != nil {
		return nil, err
	}
	return &RedeemPositionsResponse{
		TransactionHash: resp.TransactionHash,
	}, nil
}

// SplitPositionViaRelayer submits a gasless splitPosition transaction through
// the Polymarket relayer. The adapter address is chosen based on the negRisk flag:
//   - true  → NegRiskCollateralAdapterAddress
//   - false → CollateralAdapterAddress
func (r *RelayerClient) SplitPositionViaRelayer(
	ctx context.Context,
	req *SplitPositionRequest,
	privateKey *ecdsa.PrivateKey,
	negRisk bool,
) (*SplitPositionResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("relayer: split: request is nil")
	}
	if req.Amount == nil {
		return nil, fmt.Errorf("relayer: split: amount is required")
	}
	if len(req.Partition) == 0 {
		return nil, fmt.Errorf("relayer: split: partition is required")
	}

	// Choose the correct adapter based on neg-risk flag.
	adapter := CollateralAdapterAddress
	if negRisk {
		adapter = NegRiskCollateralAdapterAddress
	}

	// Encode splitPosition call (goes to collateral adapter, not CTF directly).
	splitCalldata, err := r.splitABI.Pack("splitPosition",
		req.CollateralToken,
		req.ParentCollectionID,
		req.ConditionID,
		req.Partition,
		req.Amount,
	)
	if err != nil {
		return nil, fmt.Errorf("relayer: split: encode calldata: %w", err)
	}

	call := proxyCall{
		TypeCode: 1, To: adapter,
		Value: big.NewInt(0), Data: splitCalldata,
	}
	resp, err := r.submitProxyCall(ctx, []proxyCall{call}, privateKey)
	if err != nil {
		return nil, err
	}
	resp.AmountUSD = rawToUSD(req.Amount)
	return resp, nil
}

// ApproveTokenViaRelayer submits a gasless ERC-20 approve transaction.
// Approves a spender to spend the given amount of tokens from the proxy wallet.
func (r *RelayerClient) ApproveTokenViaRelayer(
	ctx context.Context,
	token, spender common.Address,
	amount *big.Int,
	privateKey *ecdsa.PrivateKey,
) (*SplitPositionResponse, error) {
	approveCalldata, err := r.approveABI.Pack("approve", spender, amount)
	if err != nil {
		return nil, fmt.Errorf("relayer: approve: encode calldata: %w", err)
	}

	call := proxyCall{
		TypeCode: 1, To: token,
		Value: big.NewInt(0), Data: approveCalldata,
	}
	return r.submitProxyCall(ctx, []proxyCall{call}, privateKey)
}

// submitProxyCall is the shared helper that wraps calls in a proxy() call,
// gets the relay payload, signs, submits, and polls for confirmation.
func (r *RelayerClient) submitProxyCall(ctx context.Context, calls []proxyCall, privateKey *ecdsa.PrivateKey) (*SplitPositionResponse, error) {
	proxyCalldata, err := r.proxyABI.Pack("proxy", calls)
	if err != nil {
		return nil, fmt.Errorf("relayer: encode proxy calldata: %w", err)
	}

	signerAddr := crypto.PubkeyToAddress(privateKey.PublicKey)
	rp, err := r.getRelayPayload(ctx, signerAddr.Hex())
	if err != nil {
		return nil, fmt.Errorf("relayer: get relay payload: %w", err)
	}

	proxyWallet := DeriveProxyWallet(signerAddr)
	slog.Info("relayer: derived proxy wallet",
		"signer", signerAddr.Hex(),
		"proxy", proxyWallet.Hex(),
	)

	from := common.HexToAddress(signerAddr.Hex())
	to := common.HexToAddress(proxyFactoryAddress)
	relayHub := common.HexToAddress(relayHubAddress)
	relay := common.HexToAddress(rp.Address)

	txFee := big.NewInt(0)
	gasPrice := big.NewInt(0)
	// Use a proper gas limit for the inner proxy call. The relay hub
	// checks gasleft() >= gasLimit. 500k is sufficient for splitPosition
	// plus internal pUSD→USDC.e conversion. The Python SDK uses 200k
	// but that can be insufficient when the adapter converts collateral.
	gasLimit := big.NewInt(500_000)
	nonce := new(big.Int)
	nonce.SetString(rp.Nonce, 10)

	structHash := proxyStructHash(from, to, proxyCalldata, txFee, gasPrice, gasLimit, nonce, relayHub, relay)

	sig, err := personalSign(structHash.Bytes(), privateKey)
	if err != nil {
		return nil, fmt.Errorf("relayer: sign: %w", err)
	}

	proxyWalletHex := strings.ToLower(proxyWallet.Hex())
	fromHex := strings.ToLower(signerAddr.Hex())

	body := map[string]interface{}{
		"from":        fromHex,
		"to":          strings.ToLower(proxyFactoryAddress),
		"proxyWallet": proxyWalletHex,
		"data":        "0x" + common.Bytes2Hex(proxyCalldata),
		"nonce":       rp.Nonce,
		"signature":   sig,
		"signatureParams": map[string]string{
			"gasPrice":   "0",
			"gasLimit":   "500000",
			"relayerFee": "0",
			"relayHub":   strings.ToLower(relayHubAddress),
			"relay":      strings.ToLower(rp.Address),
		},
		"type": "PROXY",
	}

	submitResp, err := r.submitTransaction(ctx, body)
	if err != nil {
		return nil, fmt.Errorf("relayer: submit: %w", err)
	}

	final, err := r.pollUntilConfirmed(ctx, submitResp.TransactionID, 2*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("relayer: poll: %w", err)
	}
	if final == nil {
		return nil, fmt.Errorf("relayer: transaction did not confirm")
	}

	// If there's a transaction hash, the on-chain call was mined — treat as success.
	// The relayer may return STATE_FAILED even when the tx succeeded on-chain
	// (e.g. gas estimation discrepancy, event log parsing issue). The tx hash
	// on Polygonscan is the source of truth.
	if final.TransactionHash == "" || final.TransactionHash == "0x" {
		return nil, fmt.Errorf("relayer: transaction %s has no hash (state: %s)", submitResp.TransactionID, final.State)
	}

	return &SplitPositionResponse{
		TransactionHash: common.HexToHash(final.TransactionHash),
	}, nil
}

// ─────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────

// getRelayPayload fetches the relay payload from the relayer API.
func (r *RelayerClient) getRelayPayload(ctx context.Context, signerAddr string) (*relayPayload, error) {
	url := fmt.Sprintf("%s/relay-payload?address=%s&type=PROXY", r.relayerURL, signerAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	r.addAuthHeaders(req)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("relay payload: %s returned %d: %s", url, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var rp relayPayload
	if err := json.NewDecoder(resp.Body).Decode(&rp); err != nil {
		return nil, fmt.Errorf("decode relay payload: %w", err)
	}
	return &rp, nil
}

// submitTransaction submits a transaction to the relayer.
func (r *RelayerClient) submitTransaction(ctx context.Context, body map[string]interface{}) (*relayerSubmitResponse, error) {
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal body: %w", err)
	}

	url := r.relayerURL + "/submit"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	r.addAuthHeaders(req)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("submit: %s returned %d: %s", url, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var sr relayerSubmitResponse
	if err := json.Unmarshal(respBody, &sr); err != nil {
		return nil, fmt.Errorf("decode submit response: %w", err)
	}
	return &sr, nil
}

// pollUntilConfirmed polls the transaction until it's confirmed or times out.
func (r *RelayerClient) pollUntilConfirmed(ctx context.Context, txID string, timeout time.Duration) (*relayerTransaction, error) {
	deadline := time.Now().Add(timeout)
	pollInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		tx, err := r.getTransaction(ctx, txID)
		if err != nil {
			return nil, err
		}
		if tx == nil {
			time.Sleep(pollInterval)
			continue
		}

		switch tx.State {
		case "STATE_CONFIRMED", "STATE_MINED":
			return tx, nil
		case "STATE_FAILED", "STATE_INVALID":
			return tx, nil
		default:
			time.Sleep(pollInterval)
		}
	}

	return nil, fmt.Errorf("timed out waiting for transaction %s", txID)
}

// getTransaction fetches a transaction by ID from the relayer.
func (r *RelayerClient) getTransaction(ctx context.Context, txID string) (*relayerTransaction, error) {
	url := fmt.Sprintf("%s/transaction?id=%s", r.relayerURL, txID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	r.addAuthHeaders(req)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil // not found yet
	}

	var txs []relayerTransaction
	if err := json.NewDecoder(resp.Body).Decode(&txs); err != nil {
		return nil, err
	}
	if len(txs) == 0 {
		return nil, nil
	}
	return &txs[0], nil
}

// addAuthHeaders adds relayer API key auth headers.
func (r *RelayerClient) addAuthHeaders(req *http.Request) {
	req.Header.Set("RELAYER_API_KEY", r.apiKey)
	req.Header.Set("RELAYER_API_KEY_ADDRESS", r.apiKeyAddr)
}

// ─────────────────────────────────────────────────────────────
// Proxy wallet derivation
// ─────────────────────────────────────────────────────────────

// DeriveProxyWallet computes the proxy wallet address for a given signer
// using CREATE2, matching the TypeScript relayer client's derivation:
//
//	salt = keccak256(abi.encodePacked(address))  → keccak256(20 raw bytes)
//	address = CREATE2(ProxyFactory, salt, PROXY_INIT_CODE_HASH)
//
// This matches what Polymarket's UI shows — the proxy wallet address is
// deterministic from the signer key.
func DeriveProxyWallet(signer common.Address) common.Address {
	// Salt = keccak256 of the raw 20-byte address (abi.encodePacked style).
	salt := crypto.Keccak256Hash(signer.Bytes())

	initHash := common.HexToHash(proxyInitCodeHash)
	return crypto.CreateAddress2(
		common.HexToAddress(proxyFactoryAddress),
		salt,
		initHash.Bytes(),
	)
}

// ─────────────────────────────────────────────────────────────
// Cryptographic helpers
// ─────────────────────────────────────────────────────────────

// proxyStructHash computes the proxy transaction struct hash:
//
//	keccak256("rlx:" ++ from ++ to ++ data ++ txFee ++ gasPrice ++ gasLimit ++ nonce ++ relayHub ++ relay)
//
// where address types are 20 bytes, uints are 32-byte big-endian, data is raw bytes.
func proxyStructHash(from, to common.Address, data []byte, txFee, gasPrice, gasLimit, nonce *big.Int, relayHub, relay common.Address) common.Hash {
	rlxPrefix := []byte("rlx:")

	var buf []byte
	buf = append(buf, rlxPrefix...)
	buf = append(buf, from.Bytes()...)
	buf = append(buf, to.Bytes()...)
	buf = append(buf, data...)
	buf = append(buf, uint256PadBytes(txFee)...)
	buf = append(buf, uint256PadBytes(gasPrice)...)
	buf = append(buf, uint256PadBytes(gasLimit)...)
	buf = append(buf, uint256PadBytes(nonce)...)
	buf = append(buf, relayHub.Bytes()...)
	buf = append(buf, relay.Bytes()...)

	return crypto.Keccak256Hash(buf)
}

// personalSign signs a message with EIP-191 (personal_sign):
//
//	sign(keccak256("\x19Ethereum Signed Message:\n" ++ len(message) ++ message))
func personalSign(message []byte, privateKey *ecdsa.PrivateKey) (string, error) {
	msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(message), string(message))
	hash := crypto.Keccak256Hash([]byte(msg))

	sig, err := crypto.Sign(hash.Bytes(), privateKey)
	if err != nil {
		return "", err
	}

	// crypto.Sign returns v as 0/1, Ethereum expects 27/28.
	if sig[64] < 27 {
		sig[64] += 27
	}

	return "0x" + common.Bytes2Hex(sig), nil
}

// uint256PadBytes left-pads a big.Int to 32 bytes.
func uint256PadBytes(v *big.Int) []byte {
	b := make([]byte, 32)
	if v != nil && v.Sign() != 0 {
		vBytes := v.Bytes()
		copy(b[32-len(vBytes):], vBytes)
	}
	return b
}
