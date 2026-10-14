package exchange

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// maxCapturedBody bounds a captured /exchange response body. A larger body is
// treated as unreadable: the read fails with errResponseTooLarge, which
// IsUncertain reports as uncertain (the action's fate cannot be read).
const maxCapturedBody = 64 << 10

// errResponseTooLarge is the read error a capturing body returns once the
// response exceeds maxCapturedBody.
var errResponseTooLarge = fmt.Errorf("hyperliquid: response body exceeds %d bytes", maxCapturedBody)

// captureKey is the context key carrying a per-request *responseCapture.
type captureKey struct{}

// responseCapture holds ONE request's raw HTTP status and (bounded) body.
//
// sonirico decodes some action responses into structs that drop fields the
// caller needs (ScheduleCancelResponse keeps only status/error, losing the
// top-level "response" rejection string). The capture is attached to the
// request's context, so concurrent calls each see only their own response —
// there is no shared "last response" state anywhere.
type responseCapture struct {
	mu     sync.Mutex
	seen   bool
	status int
	body   []byte
	over   bool
}

// captured is an immutable snapshot of a responseCapture.
type captured struct {
	seen   bool
	status int
	body   []byte
	over   bool
}

func withCapture(ctx context.Context) (context.Context, *responseCapture) {
	rc := &responseCapture{}
	return context.WithValue(ctx, captureKey{}, rc), rc
}

func (rc *responseCapture) reset(status int) {
	rc.mu.Lock()
	rc.seen, rc.status, rc.body, rc.over = true, status, rc.body[:0], false
	rc.mu.Unlock()
}

func (rc *responseCapture) snapshot() captured {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return captured{seen: rc.seen, status: rc.status, body: append([]byte(nil), rc.body...), over: rc.over}
}

// captureTransport wraps the /exchange HTTP transport. Requests whose context
// carries a responseCapture get their status recorded and their body teed
// (bounded) into it; every other request passes through untouched.
type captureTransport struct {
	base http.RoundTripper
}

func (t *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp == nil {
		return resp, err
	}
	rc, _ := req.Context().Value(captureKey{}).(*responseCapture)
	if rc == nil {
		return resp, nil
	}
	rc.reset(resp.StatusCode)
	if resp.Body != nil {
		resp.Body = &captureBody{rc: rc, rc0: resp.Body}
	}
	return resp, nil
}

// captureBody tees what the reader consumes into the capture and fails the
// read once more than maxCapturedBody bytes arrive.
type captureBody struct {
	rc  *responseCapture
	rc0 io.ReadCloser
	n   int
}

func (b *captureBody) Read(p []byte) (int, error) {
	n, err := b.rc0.Read(p)
	if n > 0 {
		room := maxCapturedBody - b.n
		b.n += n
		b.rc.mu.Lock()
		if room > 0 {
			k := n
			if k > room {
				k = room
			}
			b.rc.body = append(b.rc.body, p[:k]...)
		}
		if b.n > maxCapturedBody {
			b.rc.over = true
		}
		b.rc.mu.Unlock()
		if b.n > maxCapturedBody {
			return n, errResponseTooLarge
		}
	}
	return n, err
}

func (b *captureBody) Close() error { return b.rc0.Close() }

// withCaptureTransport returns a shallow copy of hc whose transport captures
// tagged requests. The caller's client is not modified; its transport (or
// http.DefaultTransport) still performs every request.
func withCaptureTransport(hc *http.Client) *http.Client {
	cp := *hc
	base := hc.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	cp.Transport = &captureTransport{base: base}
	return &cp
}

// StatusError is an /exchange failure whose HTTP status is known from the
// captured response. 5xx is uncertain and 429 rate-limited (IsUncertain /
// IsRateLimited); the body never turns it into a business classification.
type StatusError struct {
	Code int
	Err  error
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("hyperliquid: HTTP %d: %v", e.Code, e.Err)
}

func (e *StatusError) Unwrap() error { return e.Err }

// uncertainError marks an action whose outcome could not be read from an
// otherwise successful HTTP exchange (e.g. a 2xx body with no known status).
type uncertainError struct{ msg string }

func (e *uncertainError) Error() string { return "hyperliquid: " + e.msg }

func isUncertainMarker(err error) bool {
	var ue *uncertainError
	return errors.As(err, &ue) || errors.Is(err, errResponseTooLarge)
}
