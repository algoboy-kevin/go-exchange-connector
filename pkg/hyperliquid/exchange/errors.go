package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"

	hlinfo "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid"
	hl "github.com/sonirico/go-hyperliquid"
)

// ActionError is a business rejection returned by the venue in a TOP-LEVEL
// envelope: {"status":"err","response":"…"} rather than a per-item status.
type ActionError struct {
	Msg string
}

func (e *ActionError) Error() string { return "hyperliquid: " + e.Msg }

// ConfigError is a local client-configuration error.
type ConfigError struct {
	Reason string
}

func (e *ConfigError) Error() string { return "hyperliquid: " + e.Reason }

// ErrScheduleCancelUnavailable is returned by ScheduleCancel when the venue
// refuses to arm the dead-man switch because the account's traded volume is
// below the venue's threshold. Test with errors.Is.
//
// It is reported ONLY for a cleanly decoded 2xx {"status":"err"} envelope whose
// message explicitly names traded volume (see isVolumeRefusal). Transport
// failures, HTTP errors, unreadable bodies and any other rejection text are
// never mapped to it: a caller treats this error as "the venue does not offer
// the feature to this account", so a false positive would silently disable the
// dead-man switch.
var ErrScheduleCancelUnavailable = errors.New("hyperliquid: schedule cancel unavailable (account traded volume below the venue's threshold)")

// IsUncertain reports whether a failed action MAY still have executed on the
// venue: a transport error, a context deadline/cancel, an HTTP 5xx, or a
// response body that is not JSON (so the outcome cannot be read). Callers must
// reconcile state (query the order/position) rather than blindly retrying.
//
// A JSON error body — sonirico's hl.APIError, or a top-level ActionError — is a
// business rejection and is never uncertain.
func IsUncertain(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	// /exchange with a captured HTTP status (ScheduleCancel): the status is
	// authoritative even when the body happened to be JSON.
	var se *StatusError
	if errors.As(err, &se) {
		return se.Code >= 500
	}
	// A 2xx exchange whose outcome could not be read (oversize body, unknown
	// envelope status).
	if isUncertainMarker(err) {
		return true
	}

	// /info: the repo's client reports status and non-JSON bodies as *HTTPError.
	// A 2xx status on that error means the body was not JSON.
	var he *hlinfo.HTTPError
	if errors.As(err, &he) {
		if he.StatusCode >= 500 {
			return true
		}
		return he.StatusCode >= 200 && he.StatusCode < 300
	}

	// Business rejections.
	var actionErr *ActionError
	if errors.As(err, &actionErr) {
		return false
	}
	var apiErr hl.APIError
	if errors.As(err, &apiErr) {
		return false
	}

	// /exchange: a non-JSON body is the one case where the HTTP status survives
	// sonirico's client as text ("status %d: %s"); a JSON body arrives as an
	// hl.APIError above, which carries no status at all.
	if code, ok := httpStatusFromError(err); ok {
		return code >= 500
	}

	// A 2xx whose body could not be parsed at all: the action's fate is unknown.
	// (A body that parsed but did not match the expected shape is deliberately
	// NOT uncertain — for a query that is just a decode error, and on the action
	// path sonirico reports those through the markers below.)
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return true
	}
	if hasUncertainBodyMarker(err) {
		return true
	}

	// Transport failures: dial errors, resets, a truncated body, timeouts.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	return false
}

// IsRateLimited reports whether the venue rejected the request by rate limit:
// HTTP 429, or an error message mentioning "Too many" / "rate limit" (the venue
// reports some limits inside a 200/400 envelope, where no status is available).
func IsRateLimited(err error) bool {
	if err == nil {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) && se.Code == 429 {
		return true
	}
	var he *hlinfo.HTTPError
	if errors.As(err, &he) {
		if he.StatusCode == 429 {
			return true
		}
	} else if code, ok := httpStatusFromError(err); ok && code == 429 {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "too many") || strings.Contains(msg, "rate limit")
}

// statusPrefixRE matches sonirico's client.post error for a non-JSON error body:
// fmt.Errorf("status %d: %s", code, body).
var statusPrefixRE = regexp.MustCompile(`status (\d{3}):`)

func httpStatusFromError(err error) (int, bool) {
	m := statusPrefixRE.FindStringSubmatch(err.Error())
	if m == nil {
		return 0, false
	}
	code, cerr := strconv.Atoi(m[1])
	if cerr != nil {
		return 0, false
	}
	return code, true
}

// uncertainBodyMarkers are the (stable, unexported) prefixes sonirico uses when
// a 2xx body could not be interpreted: hl.APIResponse's generated decoder uses
// fastjson, so these are plain errors rather than *json.SyntaxError.
var uncertainBodyMarkers = []string{
	"failed to parse JSON response",
	"failed to unmarshal response data",
}

func hasUncertainBodyMarker(err error) bool {
	msg := err.Error()
	for _, m := range uncertainBodyMarkers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}
