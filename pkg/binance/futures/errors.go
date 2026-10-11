package futures

import (
	"context"
	"errors"
	"fmt"
)

// APIError is a Binance error response: the HTTP status plus the parsed
// {"code","msg"} body. Code is 0 when the body was not a Binance error object
// (e.g. an HTML gateway error).
type APIError struct {
	HTTPStatus int
	Code       int
	Msg        string
}

func (e *APIError) Error() string {
	if e.Code != 0 {
		return fmt.Sprintf("binance futures: http %d: code %d: %s", e.HTTPStatus, e.Code, e.Msg)
	}
	return fmt.Sprintf("binance futures: http %d: %s", e.HTTPStatus, e.Msg)
}

// ConfigError is a local client-configuration error.
type ConfigError struct{ Reason string }

func (e *ConfigError) Error() string { return "binance futures: " + e.Reason }

// errNoServerTime is returned by SyncTime when the server clock could not be
// read (treated as an uncertain/transport condition).
var errNoServerTime = errors.New("server time missing from response")

// transportError wraps a networking failure (dial error, connection reset,
// context deadline/cancel during the request). It is always uncertain.
type transportError struct{ err error }

func (e *transportError) Error() string { return "binance futures: transport error: " + e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// IsUncertain reports whether a failed request may nevertheless have executed
// on the server: a transport error, a context deadline/cancel during the
// request, an HTTP 5xx, or Binance code −1007 ("execution status unknown").
// Callers must reconcile state (e.g. query the order) rather than blindly
// retry.
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
		return ae.Code == -1007
	}
	return false
}

// IsRateLimited reports whether the request was rejected by a rate limit: an
// HTTP 429/418 or Binance code −1003 / −1015. The caller should back off.
func IsRateLimited(err error) bool {
	var ae *APIError
	if !errors.As(err, &ae) {
		return false
	}
	if ae.HTTPStatus == httpStatusTooManyRequests || ae.HTTPStatus == httpStatusTeapot {
		return true
	}
	return ae.Code == -1003 || ae.Code == -1015
}

// IsUnknownOrder reports whether the error is a Binance −2011 / −2013: the
// order being cancelled/replaced does not exist (already gone).
func IsUnknownOrder(err error) bool {
	var ae *APIError
	if !errors.As(err, &ae) {
		return false
	}
	return ae.Code == -2011 || ae.Code == -2013
}

const (
	httpStatusTooManyRequests = 429
	httpStatusTeapot          = 418
)

// codeOf returns the Binance error code, or 0 when err is not an *APIError.
// Exported indirectly through the Is* helpers; kept unexported on purpose.
func codeOf(err error) int {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae.Code
	}
	return 0
}
