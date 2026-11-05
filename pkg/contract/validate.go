package contract

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Sentinel errors. Adapters wrap them, so callers test with errors.Is.
var (
	// ErrInvalid: the request is malformed. Nothing was sent.
	ErrInvalid = errors.New("contract: invalid request")
	// ErrUnsupported: the venue does not offer this operation.
	ErrUnsupported = errors.New("contract: venue does not offer this operation")
	// ErrNotImplemented: the venue offers it, but the adapter does not implement it
	// yet. This is a gap to close, not a venue limit.
	ErrNotImplemented = errors.New("contract: venue offers this operation; adapter not implemented yet")
	// ErrUncertain: the mutation may or may not have reached the venue. Reconcile
	// by ClientID or OrderID before retrying. Never resend blindly.
	ErrUncertain = errors.New("contract: request outcome unknown; reconcile before resending")
)

var decimalRE = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

// ValidPositiveDecimal reports whether s is a plain positive decimal, with no sign,
// exponent or spaces. "0", "0.0" and "" are not valid.
func ValidPositiveDecimal(s string) bool {
	return decimalRE.MatchString(s) && !IsZero(s)
}

// IsZero reports whether a valid decimal string is zero. Non-decimal input is not zero.
func IsZero(s string) bool {
	if !decimalRE.MatchString(s) {
		return false
	}
	return strings.Trim(s, "0.") == ""
}

// Validate checks an order request before any adapter sends it.
func (r OrderRequest) Validate() error {
	if r.Symbol == "" {
		return fmt.Errorf("%w: symbol is empty", ErrInvalid)
	}
	if r.Side != Buy && r.Side != Sell {
		return fmt.Errorf("%w: side %q", ErrInvalid, r.Side)
	}
	if !ValidPositiveDecimal(r.Price) {
		return fmt.Errorf("%w: price %q must be a positive decimal", ErrInvalid, r.Price)
	}
	if !ValidPositiveDecimal(r.Qty) {
		return fmt.Errorf("%w: qty %q must be a positive decimal", ErrInvalid, r.Qty)
	}
	switch r.TIF {
	case GTC, IOC, PostOnly:
	default:
		return fmt.Errorf("%w: tif %q", ErrInvalid, r.TIF)
	}
	return nil
}

// Validate checks a cancel request. It needs a symbol and at least one id.
func (r CancelRequest) Validate() error {
	if r.Symbol == "" {
		return fmt.Errorf("%w: symbol is empty", ErrInvalid)
	}
	if r.OrderID == "" && r.ClientID == "" {
		return fmt.Errorf("%w: cancel needs OrderID or ClientID", ErrInvalid)
	}
	return nil
}

// CheckBatch rejects an empty batch and a batch over the venue limit.
func CheckBatch(n, max int) error {
	if n == 0 {
		return fmt.Errorf("%w: empty batch", ErrInvalid)
	}
	if n > max {
		return fmt.Errorf("%w: batch of %d exceeds venue limit %d", ErrInvalid, n, max)
	}
	return nil
}
