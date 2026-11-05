package spot

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"
)

// Exact decimal helpers. Binance prices, quantities and balances are decimal
// strings; rounding them to tickSize / stepSize with float64 can land one step
// off, and a one-step error is a rejected order (PRICE_FILTER / LOT_SIZE,
// filters doc line 23909 and 24000 of the llms-full text). Everything here is
// big.Rat arithmetic on plain positive decimals.

var decimalRE = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

// ValidDecimal reports whether s is a plain non-negative decimal: digits with
// an optional fractional part. No sign, exponent, spaces or empty string.
func ValidDecimal(s string) bool { return decimalRE.MatchString(s) }

// ValidPositiveDecimal is ValidDecimal and greater than zero.
func ValidPositiveDecimal(s string) bool {
	if !ValidDecimal(s) {
		return false
	}
	return strings.Trim(s, "0.") != ""
}

func ratOf(s string) (*big.Rat, error) {
	if !ValidDecimal(s) {
		return nil, fmt.Errorf("spot: %q is not a plain decimal", s)
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("spot: %q is not a plain decimal", s)
	}
	return r, nil
}

// decimalsOf returns the number of significant fractional digits of a venue
// step string such as "0.00100000" (3) or "1.00000000" (0).
func decimalsOf(s string) int {
	i := strings.IndexByte(s, '.')
	if i < 0 {
		return 0
	}
	return len(strings.TrimRight(s[i+1:], "0"))
}

// writtenDecimals is the number of fractional digits s is written with,
// trailing zeros included.
func writtenDecimals(s string) int {
	i := strings.IndexByte(s, '.')
	if i < 0 {
		return 0
	}
	return len(s) - i - 1
}

// formatRat renders r with exactly dec fractional digits (dec == 0: integer).
func formatRat(r *big.Rat, dec int) string {
	return r.FloatString(dec)
}

// RoundDown returns v rounded down to a multiple of step, formatted with the
// number of decimals step carries. v and step are plain decimals and step must
// be positive. Use it for a price (buy: further from the book) or a quantity.
func RoundDown(v, step string) (string, error) { return roundToStep(v, step, false) }

// RoundUp returns v rounded up to a multiple of step (see RoundDown). Use it
// when a minimum must be met, e.g. quantity so that price*qty >= minNotional.
func RoundUp(v, step string) (string, error) { return roundToStep(v, step, true) }

func roundToStep(v, step string, up bool) (string, error) {
	vr, err := ratOf(v)
	if err != nil {
		return "", err
	}
	sr, err := ratOf(step)
	if err != nil {
		return "", err
	}
	if sr.Sign() <= 0 {
		return "", fmt.Errorf("spot: step %q must be positive", step)
	}
	q := new(big.Rat).Quo(vr, sr)
	n := new(big.Int).Quo(q.Num(), q.Denom()) // truncates toward zero; q >= 0
	if up && new(big.Rat).SetInt(n).Cmp(q) != 0 {
		n.Add(n, big.NewInt(1))
	}
	out := new(big.Rat).Mul(new(big.Rat).SetInt(n), sr)
	return formatRat(out, decimalsOf(step)), nil
}

// IsMultipleOf reports whether v is an exact multiple of step.
func IsMultipleOf(v, step string) (bool, error) {
	vr, err := ratOf(v)
	if err != nil {
		return false, err
	}
	sr, err := ratOf(step)
	if err != nil {
		return false, err
	}
	if sr.Sign() <= 0 {
		return false, fmt.Errorf("spot: step %q must be positive", step)
	}
	q := new(big.Rat).Quo(vr, sr)
	return q.IsInt(), nil
}

// CmpDecimal compares two plain decimals: -1, 0 or +1.
func CmpDecimal(a, b string) (int, error) {
	ar, err := ratOf(a)
	if err != nil {
		return 0, err
	}
	br, err := ratOf(b)
	if err != nil {
		return 0, err
	}
	return ar.Cmp(br), nil
}

// MulDecimal returns a*b exactly, with the smallest decimal count that
// represents it without loss (decimals(a)+decimals(b)).
func MulDecimal(a, b string) (string, error) {
	ar, err := ratOf(a)
	if err != nil {
		return "", err
	}
	br, err := ratOf(b)
	if err != nil {
		return "", err
	}
	return formatRat(new(big.Rat).Mul(ar, br), decimalsOf(a)+decimalsOf(b)), nil
}

// AddDecimal returns a+b exactly, with as many decimals as the longer operand
// is written with (trailing zeros included, so "100.25000000"+"20.5" keeps the
// venue's 8-digit form).
func AddDecimal(a, b string) (string, error) {
	ar, err := ratOf(a)
	if err != nil {
		return "", err
	}
	br, err := ratOf(b)
	if err != nil {
		return "", err
	}
	return formatRat(new(big.Rat).Add(ar, br), max(writtenDecimals(a), writtenDecimals(b))), nil
}

// DivDecimal returns a/b rounded half-up to dec decimals. b must be positive.
func DivDecimal(a, b string, dec int) (string, error) {
	ar, err := ratOf(a)
	if err != nil {
		return "", err
	}
	br, err := ratOf(b)
	if err != nil {
		return "", err
	}
	if br.Sign() <= 0 {
		return "", fmt.Errorf("spot: divisor %q must be positive", b)
	}
	return formatRat(new(big.Rat).Quo(ar, br), dec), nil
}

// MidPrice returns (bid+ask)/2 exactly as a rational rendered with dec
// decimals.
func MidPrice(bid, ask string, dec int) (string, error) {
	s, err := AddDecimal(bid, ask)
	if err != nil {
		return "", err
	}
	return DivDecimal(s, "2", dec)
}

// ScaleDecimal returns v*num/den rendered with dec decimals (half-up). It is
// the exact way to take "20% below mid": ScaleDecimal(mid, 80, 100, dec).
func ScaleDecimal(v string, num, den int64, dec int) (string, error) {
	vr, err := ratOf(v)
	if err != nil {
		return "", err
	}
	if den <= 0 || num < 0 {
		return "", fmt.Errorf("spot: bad scale %d/%d", num, den)
	}
	r := new(big.Rat).Mul(vr, big.NewRat(num, den))
	return formatRat(r, dec), nil
}

// AvgPrice is cummulativeQuoteQty / executedQty rendered with 8 decimals, or
// "" when nothing executed. Spot order responses carry no average price field;
// the user-data doc says "Average price can be found by doing Z divided by z"
// (llms-full line 21203).
func AvgPrice(cumQuote, executedQty string) string {
	if !ValidDecimal(cumQuote) || !ValidDecimal(executedQty) || !ValidPositiveDecimal(executedQty) {
		return ""
	}
	s, err := DivDecimal(cumQuote, executedQty, 8)
	if err != nil {
		return ""
	}
	return s
}
