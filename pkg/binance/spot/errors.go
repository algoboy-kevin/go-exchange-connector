package spot

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// APIError is a Binance spot error response: the HTTP status, the parsed
// {"code","msg"} body (llms-full line 21527: "Sample Payload ... code -1121")
// and, for 418/429, the Retry-After header ("number of seconds", line 21604).
// Code is 0 when the body was not a Binance error object (e.g. an HTML gateway
// error).
type APIError struct {
	HTTPStatus int
	Code       int
	Msg        string
	// RetryAfter is the Retry-After header of a 418/429 response; 0 if absent.
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	s := fmt.Sprintf("binance spot: http %d: %s", e.HTTPStatus, e.Msg)
	if e.Code != 0 {
		s = fmt.Sprintf("binance spot: http %d: code %d: %s", e.HTTPStatus, e.Code, e.Msg)
	}
	if e.RetryAfter > 0 {
		s += fmt.Sprintf(" (retry after %s)", e.RetryAfter)
	}
	return s
}

// ConfigError is a local client-configuration error.
type ConfigError struct{ Reason string }

func (e *ConfigError) Error() string { return "binance spot: " + e.Reason }

// ValidationError is a request that failed local validation. Nothing was sent.
type ValidationError struct{ Reason string }

func (e *ValidationError) Error() string { return "binance spot: invalid request: " + e.Reason }

func invalidf(format string, args ...any) error {
	return &ValidationError{Reason: fmt.Sprintf(format, args...)}
}

// IsValidation reports whether err is a local validation failure (nothing sent).
func IsValidation(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}

// errNoServerTime is returned by SyncTime when the server clock is missing.
var errNoServerTime = errors.New("server time missing from response")

// transportError wraps a networking failure (dial error, connection reset,
// context deadline/cancel during the request, unreadable body). The outcome of
// a mutation is then unknown.
type transportError struct{ err error }

func (e *transportError) Error() string { return "binance spot: transport error: " + e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// IsUncertain reports whether a failed request may nevertheless have executed:
// a transport error, a context deadline/cancel during the request, HTTP 5xx
// ("It is important to NOT treat this as a failure operation; the execution
// status is UNKNOWN and could have been a success", line 21520), or code
// -1007 TIMEOUT / -1006 UNEXPECTED_RESP ("execution status unknown", lines
// 24381-24387). Reconcile by clientOrderId (GetOrder / OpenOrders); never
// resend blindly.
func IsUncertain(err error) bool {
	if err == nil {
		return false
	}
	var te *transportError
	if errors.As(err, &te) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var ae *APIError
	if errors.As(err, &ae) {
		if ae.HTTPStatus >= 500 {
			return true
		}
		return ae.Code == -1007 || ae.Code == -1006
	}
	return false
}

// IsRateLimited reports a request rate limit: HTTP 429, or code -1003 / -1015
// on a non-418 response (lines 24373 and 24403). Back off; 429s that are
// ignored become a 418 IP ban. A 418 is IsBanned, not IsRateLimited.
func IsRateLimited(err error) bool {
	var ae *APIError
	if !errors.As(err, &ae) {
		return false
	}
	if ae.HTTPStatus == 418 {
		return false
	}
	return ae.HTTPStatus == 429 || ae.Code == -1003 || ae.Code == -1015
}

// IsBanned reports an automated IP ban: HTTP 418, lasting 2 minutes to 3 days
// (line 21603). Stop sending anything, including cleanup, until RetryAfter has
// passed.
func IsBanned(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.HTTPStatus == 418
}

// IsWAFBlocked reports HTTP 403: a Web Application Firewall rule was violated
// (line 21511). It can be a rate-limit violation or a security block.
func IsWAFBlocked(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.HTTPStatus == 403
}

// IsUnknownOrder reports that the order does not exist (already filled,
// cancelled or never placed): -2013 NO_SUCH_ORDER, or -2011 CANCEL_REJECTED
// with the message "Unknown order sent." (lines 24777-24783, 24829).
func IsUnknownOrder(err error) bool {
	var ae *APIError
	if !errors.As(err, &ae) {
		return false
	}
	if ae.Code == -2013 {
		return true
	}
	return ae.Code == -2011 && strings.Contains(ae.Msg, "Unknown order")
}

// IsTimestampError reports -1021 INVALID_TIMESTAMP: the signed timestamp was
// outside recvWindow or 1000 ms ahead of the server (line 24416). Call
// SyncTime and consider a larger RecvWindow.
func IsTimestampError(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Code == -1021
}

// IsInvalidSignature reports -1022 INVALID_SIGNATURE (line 24421).
func IsInvalidSignature(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Code == -1022
}
