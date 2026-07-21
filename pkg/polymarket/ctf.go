// Package polymarket — CTF (Conditional Token Framework) on-chain client.
//
// The CTFClient wraps the Polymarket Conditional Tokens smart contract,
// providing methods to split, merge, and redeem outcome tokens directly
// on-chain. This is useful for operations that cannot be performed via
// the CLOB API alone, such as splitting USDC into YES/NO position tokens.
//
// Contract (Polygon mainnet): 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
package polymarket

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// ─────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────

const (
	// PolygonCTFExchangeAddress is the Conditional Tokens contract on Polygon mainnet.
	PolygonCTFExchangeAddress = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
	// AmoyCTFExchangeAddress is the Conditional Tokens contract on Amoy testnet.
	AmoyCTFExchangeAddress = "0x69308FB512518e39F9b16112fA8d994F4e2Bf8bB"

	// PolygonCTFChainID is the Polygon mainnet chain ID.
	PolygonCTFChainID int64 = 137
	// AmoyCTFChainID is the Amoy testnet chain ID.
	AmoyCTFChainID int64 = 80002
)

// Default collateral address on Polygon — pUSD (Polymarket migrated from USDC
// to pUSD on April 28, 2026). Your proxy wallet holds pUSD, not USDC.
var defaultCollateralAddress = common.HexToAddress("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB")

// ─────────────────────────────────────────────────────────────
// Request / Response types
// ─────────────────────────────────────────────────────────────

// SplitPositionRequest holds parameters for the on-chain splitPosition call.
type SplitPositionRequest struct {
	// CollateralToken is the ERC-20 token to split (e.g., USDC).
	CollateralToken common.Address
	// ParentCollectionID is the parent collection ID (zero hash for top-level).
	ParentCollectionID common.Hash
	// ConditionID is the market's condition ID from the Gamma API.
	ConditionID common.Hash
	// Partition defines how to split. For binary markets: [1, 2] (YES/NO).
	Partition []*big.Int
	// Amount is the amount of collateral to split (in wei / 6-decimals for USDC).
	Amount *big.Int
}

// SplitPositionResponse holds the result of a split transaction.
type SplitPositionResponse struct {
	TransactionHash common.Hash
	BlockNumber     uint64
	// AmountUSD is the amount of collateral that was split (e.g. 10.00 = $10).
	AmountUSD float64
}

// MergePositionsRequest holds parameters for the on-chain mergePositions call.
type MergePositionsRequest struct {
	// CollateralToken is the ERC-20 token to receive after merging (e.g., USDC).
	CollateralToken common.Address
	// ParentCollectionID is the parent collection ID (zero hash for top-level).
	ParentCollectionID common.Hash
	// ConditionID is the market's condition ID from the Gamma API.
	ConditionID common.Hash
	// Partition defines which outcome tokens to merge. For binary markets: [1, 2] (YES/NO).
	Partition []*big.Int
	// Amount is the amount of each outcome token to merge (in 6-decimals for USDC).
	Amount *big.Int
}

// MergePositionsResponse holds the result of a merge transaction.
type MergePositionsResponse struct {
	TransactionHash common.Hash
	BlockNumber     uint64
	// AmountUSD is the amount of collateral received after merging (e.g. 5.00 = $5).
	AmountUSD float64
}

// RedeemPositionsRequest holds parameters for the on-chain redeemPositions call.
type RedeemPositionsRequest struct {
	// CollateralToken is the ERC-20 token to receive after redeeming (e.g., USDC).
	CollateralToken common.Address
	// ParentCollectionID is the parent collection ID (zero hash for top-level).
	ParentCollectionID common.Hash
	// ConditionID is the market's condition ID from the Gamma API.
	ConditionID common.Hash
	// Partition defines which outcome tokens to redeem.
	// For binary markets: [1] for YES, [2] for NO, or [1, 2] for both.
	// Only winning outcome(s) will yield collateral.
	Partition []*big.Int
}

// RedeemPositionsResponse holds the result of a redeem transaction.
type RedeemPositionsResponse struct {
	TransactionHash common.Hash
	BlockNumber     uint64
	// AmountUSD is the amount of collateral received after redeeming (e.g. 5.00 = $5).
	AmountUSD float64
}

// ─────────────────────────────────────────────────────────────
// CTFClient
// ─────────────────────────────────────────────────────────────

// CTFClient handles on-chain interactions with the Polymarket Conditional
// Tokens smart contract.
type CTFClient struct {
	client            *ethclient.Client
	conditionalTokens *bind.BoundContract
	chainID           int64
}

// NewCTFClient creates a new CTF client connected to the given RPC endpoint.
//
//	rpcURL  — Polygon RPC URL (e.g. "https://polygon-mainnet.g.alchemy.com/v2/...")
//	chainID — 137 for Polygon mainnet, 80002 for Amoy testnet
func NewCTFClient(rpcURL string, chainID int64) (*CTFClient, error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("ctf: dial rpc: %w", err)
	}

	// Resolve contract address based on chain ID.
	var contractAddrStr string
	switch chainID {
	case PolygonCTFChainID:
		contractAddrStr = PolygonCTFExchangeAddress
	case AmoyCTFChainID:
		contractAddrStr = AmoyCTFExchangeAddress
	default:
		client.Close()
		return nil, fmt.Errorf("ctf: unsupported chain ID %d", chainID)
	}

	// Minimal ABI containing only the functions we need.
	// Full ABI reference: https://github.com/Polymarket/conditional-tokens-contracts
	const conditionalTokensABI = `[
		{"inputs":[{"internalType":"address","name":"collateralToken","type":"address"},{"internalType":"bytes32","name":"parentCollectionId","type":"bytes32"},{"internalType":"bytes32","name":"conditionId","type":"bytes32"},{"internalType":"uint256[]","name":"partition","type":"uint256[]"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"splitPosition","outputs":[],"stateMutability":"nonpayable","type":"function"},
		{"inputs":[{"internalType":"address","name":"collateralToken","type":"address"},{"internalType":"bytes32","name":"parentCollectionId","type":"bytes32"},{"internalType":"bytes32","name":"conditionId","type":"bytes32"},{"internalType":"uint256[]","name":"partition","type":"uint256[]"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"mergePositions","outputs":[],"stateMutability":"nonpayable","type":"function"},
		{"inputs":[{"internalType":"address","name":"collateralToken","type":"address"},{"internalType":"bytes32","name":"parentCollectionId","type":"bytes32"},{"internalType":"bytes32","name":"conditionId","type":"bytes32"},{"internalType":"uint256[]","name":"partition","type":"uint256[]"}],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}
	]`

	parsedABI, err := abi.JSON(strings.NewReader(conditionalTokensABI))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("ctf: parse abi: %w", err)
	}

	contractAddr := common.HexToAddress(contractAddrStr)
	contract := bind.NewBoundContract(contractAddr, parsedABI, client, client, client)

	return &CTFClient{
		client:            client,
		conditionalTokens: contract,
		chainID:           chainID,
	}, nil
}

// Close shuts down the underlying RPC connection.
func (c *CTFClient) Close() {
	c.client.Close()
}

// SplitPosition calls the splitPosition function on the Conditional Tokens
// contract. This deposits the specified amount of collateral and mints
// outcome tokens according to the partition.
//
// For a binary market (partition=[1,2]), this splits USDC into YES and NO
// position tokens, which can then be traded on the CLOB.
//
// The privateKey must control the wallet that holds the collateral tokens
// and has approved the CTF contract to spend them (via ERC-20 approve).
func (c *CTFClient) SplitPosition(ctx context.Context, req *SplitPositionRequest, privateKey *ecdsa.PrivateKey) (*SplitPositionResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("ctf: split: request is nil")
	}
	if req.Amount == nil {
		return nil, fmt.Errorf("ctf: split: amount is required")
	}
	if len(req.Partition) == 0 {
		return nil, fmt.Errorf("ctf: split: partition is required")
	}

	txOpts, err := bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(c.chainID))
	if err != nil {
		return nil, fmt.Errorf("ctf: split: create transactor: %w", err)
	}
	txOpts.Context = ctx

	tx, err := c.conditionalTokens.Transact(txOpts, "splitPosition",
		req.CollateralToken,
		req.ParentCollectionID,
		req.ConditionID,
		req.Partition,
		req.Amount,
	)
	if err != nil {
		return nil, fmt.Errorf("ctf: split: send tx: %w", err)
	}

	// Wait for the transaction to be mined.
	receipt, err := bind.WaitMined(ctx, c.client, tx)
	if err != nil {
		return nil, fmt.Errorf("ctf: split: wait receipt: %w", err)
	}
	if receipt == nil || receipt.BlockNumber == nil {
		return nil, fmt.Errorf("ctf: split: receipt missing block number")
	}

	return &SplitPositionResponse{
		TransactionHash: tx.Hash(),
		BlockNumber:     receipt.BlockNumber.Uint64(),
		AmountUSD:       rawToUSD(req.Amount),
	}, nil
}

// MergePositions calls the mergePositions function on the Conditional Tokens
// contract. This burns outcome tokens (YES/NO) and returns the equivalent
// amount of collateral (pUSD).
//
// For a binary market (partition=[1,2]), this merges YES and NO position
// tokens back into USDC, which can then be withdrawn or traded.
//
// The privateKey must control the wallet that holds the outcome tokens
// and has approved the CTF contract (or collateral adapter) to manage them.
func (c *CTFClient) MergePositions(ctx context.Context, req *MergePositionsRequest, privateKey *ecdsa.PrivateKey) (*MergePositionsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("ctf: merge: request is nil")
	}
	if req.Amount == nil {
		return nil, fmt.Errorf("ctf: merge: amount is required")
	}
	if len(req.Partition) == 0 {
		return nil, fmt.Errorf("ctf: merge: partition is required")
	}

	txOpts, err := bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(c.chainID))
	if err != nil {
		return nil, fmt.Errorf("ctf: merge: create transactor: %w", err)
	}
	txOpts.Context = ctx

	tx, err := c.conditionalTokens.Transact(txOpts, "mergePositions",
		req.CollateralToken,
		req.ParentCollectionID,
		req.ConditionID,
		req.Partition,
		req.Amount,
	)
	if err != nil {
		return nil, fmt.Errorf("ctf: merge: send tx: %w", err)
	}

	// Wait for the transaction to be mined.
	receipt, err := bind.WaitMined(ctx, c.client, tx)
	if err != nil {
		return nil, fmt.Errorf("ctf: merge: wait receipt: %w", err)
	}
	if receipt == nil || receipt.BlockNumber == nil {
		return nil, fmt.Errorf("ctf: merge: receipt missing block number")
	}

	return &MergePositionsResponse{
		TransactionHash: tx.Hash(),
		BlockNumber:     receipt.BlockNumber.Uint64(),
		AmountUSD:       rawToUSD(req.Amount),
	}, nil
}

// StandardPartition returns the standard binary partition [1, 2] for YES/NO markets.
func StandardPartition() []*big.Int {
	return []*big.Int{big.NewInt(1), big.NewInt(2)}
}

// RedeemPositions calls the redeemPositions function on the Conditional Tokens
// contract (or collateral adapter). This burns winning outcome tokens and
// returns the equivalent amount of collateral (pUSD).
//
// For a resolved market where the outcome is YES, calling with partition=[1,2]
// redeems only the winning position (YES). The losing position (NO) simply
// burns with no value returned.
//
// Unlike split/merge, there is no amount parameter — the full balance of each
// position token in the partition is redeemed.
//
// The privateKey must control the wallet that holds the outcome tokens.
func (c *CTFClient) RedeemPositions(ctx context.Context, req *RedeemPositionsRequest, privateKey *ecdsa.PrivateKey) (*RedeemPositionsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("ctf: redeem: request is nil")
	}
	if len(req.Partition) == 0 {
		return nil, fmt.Errorf("ctf: redeem: partition is required")
	}

	txOpts, err := bind.NewKeyedTransactorWithChainID(privateKey, big.NewInt(c.chainID))
	if err != nil {
		return nil, fmt.Errorf("ctf: redeem: create transactor: %w", err)
	}
	txOpts.Context = ctx

	tx, err := c.conditionalTokens.Transact(txOpts, "redeemPositions",
		req.CollateralToken,
		req.ParentCollectionID,
		req.ConditionID,
		req.Partition,
	)
	if err != nil {
		return nil, fmt.Errorf("ctf: redeem: send tx: %w", err)
	}

	// Wait for the transaction to be mined.
	receipt, err := bind.WaitMined(ctx, c.client, tx)
	if err != nil {
		return nil, fmt.Errorf("ctf: redeem: wait receipt: %w", err)
	}
	if receipt == nil || receipt.BlockNumber == nil {
		return nil, fmt.Errorf("ctf: redeem: receipt missing block number")
	}

	return &RedeemPositionsResponse{
		TransactionHash: tx.Hash(),
		BlockNumber:     receipt.BlockNumber.Uint64(),
	}, nil
}

// CollateralAddress returns the canonical USDC address for CTF operations.
func CollateralAddress(chainID int64) common.Address {
	switch chainID {
	case PolygonCTFChainID:
		return defaultCollateralAddress
	default:
		return defaultCollateralAddress
	}
}

// rawToUSD converts a raw 6-decimal USDC amount (e.g. 5000000) to dollars (5.0).
func rawToUSD(raw *big.Int) float64 {
	if raw == nil {
		return 0
	}
	f, _ := new(big.Float).Quo(
		new(big.Float).SetInt(raw),
		new(big.Float).SetInt64(1_000_000),
	).Float64()
	return f
}
