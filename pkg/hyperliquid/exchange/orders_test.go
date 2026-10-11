package exchange

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	hl "github.com/sonirico/go-hyperliquid"
)

// placeResponse builds an /exchange order envelope with the given statuses.
func placeResponse(statuses ...string) string {
	return `{"status":"ok","response":{"type":"order","data":{"statuses":[` +
		strings.Join(statuses, ",") + `]}}}`
}

// TestPlaceMixedStatuses is the core case: sonirico returns a non-nil result AND
// an error when any item failed, and that error is not a call failure. One
// resting, one filled and one rejected order must map per item with no error.
func TestPlaceMixedStatuses(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(placeResponse(
		`{"resting":{"oid":7001,"cloid":"`+testCloid1+`","status":"resting"}}`,
		`{"filled":{"totalSz":"0.002","avgPx":"97123.5","oid":7002}}`,
		`{"error":"Post only order would have immediately matched"}`,
	), http.StatusOK)

	statuses, err := c.Place(context.Background(), []Order{
		{Coin: "BTC", IsBuy: true, Px: "97123", Sz: "0.001", Tif: TifAlo, Cloid: testCloid1},
		{Coin: "BTC", IsBuy: false, Px: "97124", Sz: "0.002", Tif: TifIoc},
		{Coin: "ETH", IsBuy: true, Px: "3000", Sz: "0.5", Tif: TifAlo},
	})
	if err != nil {
		t.Fatalf("Place returned a call error for a per-item rejection: %v", err)
	}
	if len(statuses) != 3 {
		t.Fatalf("statuses = %d, want 3", len(statuses))
	}
	if s := statuses[0]; !s.Resting || s.Filled || s.Oid != 7001 || s.Error != "" {
		t.Errorf("resting status = %+v", s)
	}
	if s := statuses[1]; !s.Filled || s.Resting || s.Oid != 7002 || s.TotalSz != "0.002" || s.AvgPx != "97123.5" || s.Error != "" {
		t.Errorf("filled status = %+v", s)
	}
	if s := statuses[2]; s.Resting || s.Filled || s.Error != "Post only order would have immediately matched" {
		t.Errorf("rejected status = %+v", s)
	}

	body := lastExchangeBody(t, v)
	if !strings.Contains(string(body), `"grouping":"na"`) {
		t.Errorf("order action missing grouping na: %s", body)
	}
	if n := bytes.Count(body, []byte(`"a":`)); n != 3 {
		t.Errorf("posted %d order wires, want 3: %s", n, body)
	}
}

// TestCancelByCloidMixed covers the MixedArray statuses: "success" plus the
// already-cancelled error the venue returns for a gone order.
func TestCancelByCloidMixed(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(`{"status":"ok","response":{"type":"cancelByCloid","data":{"statuses":["success",{"error":"Order was never placed, already canceled, or filled."}]}}}`, http.StatusOK)

	res, err := c.CancelByCloid(context.Background(), []CancelReq{
		{Coin: "BTC", Cloid: testCloid1},
		{Coin: "BTC", Cloid: testCloid2},
	})
	if err != nil {
		t.Fatalf("CancelByCloid returned a call error for a per-item failure: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("statuses = %d, want 2", len(res))
	}
	if !res[0].OK || res[0].Error != "" {
		t.Errorf("status[0] = %+v, want OK", res[0])
	}
	if res[1].OK || res[1].Error != "Order was never placed, already canceled, or filled." {
		t.Errorf("status[1] = %+v", res[1])
	}

	var env struct {
		Action hl.CancelByCloidAction `json:"action"`
	}
	if err := json.Unmarshal(lastExchangeBody(t, v), &env); err != nil {
		t.Fatalf("posted body: %v", err)
	}
	if env.Action.Type != "cancelByCloid" || len(env.Action.Cancels) != 2 {
		t.Fatalf("posted action = %+v", env.Action)
	}
	if env.Action.Cancels[0].Asset != 0 || env.Action.Cancels[0].ClientID != testCloid1 {
		t.Errorf("posted cancel[0] = %+v", env.Action.Cancels[0])
	}
}

// TestCancelByOidMixed covers the oid cancel path.
func TestCancelByOidMixed(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(`{"status":"ok","response":{"type":"cancel","data":{"statuses":["success",{"error":"Order was never placed, already canceled, or filled."}]}}}`, http.StatusOK)

	res, err := c.CancelByOid(context.Background(), []OidCancel{
		{Coin: "BTC", Oid: 7001},
		{Coin: "ETH", Oid: 7002},
	})
	if err != nil {
		t.Fatalf("CancelByOid: %v", err)
	}
	if len(res) != 2 || !res[0].OK || res[1].Error == "" {
		t.Fatalf("statuses = %+v", res)
	}

	var env struct {
		Action hl.CancelAction `json:"action"`
	}
	if err := json.Unmarshal(lastExchangeBody(t, v), &env); err != nil {
		t.Fatalf("posted body: %v", err)
	}
	if env.Action.Type != "cancel" || len(env.Action.Cancels) != 2 {
		t.Fatalf("posted action = %+v", env.Action)
	}
	if env.Action.Cancels[0].Asset != 0 || env.Action.Cancels[0].OrderID != 7001 {
		t.Errorf("posted cancel[0] = %+v", env.Action.Cancels[0])
	}
	if env.Action.Cancels[1].Asset != 1 || env.Action.Cancels[1].OrderID != 7002 {
		t.Errorf("posted cancel[1] = %+v", env.Action.Cancels[1])
	}
}

// TestTopLevelErrorIsActionError checks the {"status":"err","response":"…"}
// envelope becomes a typed business error — for placements and for cancels.
func TestTopLevelErrorIsActionError(t *testing.T) {
	const msg = "Must deposit before performing actions. User: 0x1111111111111111111111111111111111111111"
	envelope := `{"status":"err","response":"` + msg + `"}`

	t.Run("place", func(t *testing.T) {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setExchange(envelope, http.StatusOK)

		statuses, err := c.Place(context.Background(), []Order{{Coin: "BTC", IsBuy: true, Px: "1", Sz: "1", Tif: TifGtc}})
		if err == nil {
			t.Fatal("Place = nil error, want *ActionError")
		}
		if statuses != nil {
			t.Errorf("statuses = %+v, want nil", statuses)
		}
		var ae *ActionError
		if !errors.As(err, &ae) {
			t.Fatalf("error %T is not *ActionError", err)
		}
		if ae.Msg != msg {
			t.Errorf("Msg = %q, want %q", ae.Msg, msg)
		}
		if IsUncertain(err) || IsRateLimited(err) {
			t.Errorf("business rejection misclassified: uncertain=%v rateLimited=%v",
				IsUncertain(err), IsRateLimited(err))
		}
	})

	t.Run("cancel", func(t *testing.T) {
		v, url := newVenue(t)
		c := newTestClient(t, url)
		v.setExchange(envelope, http.StatusOK)

		res, err := c.CancelByCloid(context.Background(), []CancelReq{{Coin: "BTC", Cloid: testCloid1}})
		if err == nil || res != nil {
			t.Fatalf("CancelByCloid = (%v, %v), want (nil, *ActionError)", res, err)
		}
		var ae *ActionError
		if !errors.As(err, &ae) || ae.Msg != msg {
			t.Fatalf("error = %v (%T), want *ActionError{%q}", err, err, msg)
		}
	})
}

// ─────────────────────────────────────────────────────────────
// Posted body + signing
// ─────────────────────────────────────────────────────────────

// Golden signature for the single-order action below, nonce 4102444800001,
// signed as TESTNET (r/s/v). Derived with sonirico's own SignL1Action; it pins
// the whole signing path — action shape, msgpack hash, phantom agent, domain.
const (
	goldenR = "0xcbd22c4216e20d31eda6f900423b5f55eb64e7aa5ce42daf0bf1d9aacd2a2b84"
	goldenS = "0x4e6684ac87d05e6dbee062f1363022a1fd2994bd25e4d486a654bd31d03fdea"
	goldenV = 27

	// pinnedNonce is far in the future so sonirico's nextNonce() (max(now,
	// last+1)) returns exactly this value whatever the wall clock does.
	pinnedNonce = int64(4102444800001)
)

// TestPlacePostsSignedOrderAction pins the posted /exchange body: decimal
// strings preserved verbatim, cloid, tif "Alo", grouping "na", and a signature
// whose r/s/v are the golden vector. It also proves determinism — the same
// action and nonce produce a byte-identical request — and that mainnet signing
// (selected only by an exact base-URL match) yields a different signature.
func TestPlacePostsSignedOrderAction(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(placeResponse(`{"resting":{"oid":7001,"status":"resting"}}`), http.StatusOK)

	order := Order{Coin: "BTC", IsBuy: true, Px: "97123", Sz: "0.000123", Tif: TifAlo, Cloid: testCloid1}

	// White-box: pin the nonce (the public API has no nonce control, and a
	// wall-clock nonce cannot be asserted).
	c.ex.SetLastNonce(pinnedNonce - 1)
	if _, err := c.Place(context.Background(), []Order{order}); err != nil {
		t.Fatalf("Place: %v", err)
	}
	body1 := append([]byte(nil), lastExchangeBody(t, v)...)

	c.ex.SetLastNonce(pinnedNonce - 1)
	if _, err := c.Place(context.Background(), []Order{order}); err != nil {
		t.Fatalf("Place (second): %v", err)
	}
	body2 := lastExchangeBody(t, v)
	if !bytes.Equal(body1, body2) {
		t.Fatalf("signing is not deterministic for a fixed nonce:\n%s\n%s", body1, body2)
	}

	// The raw request must carry the wire shape the venue expects.
	for _, want := range []string{
		`"type":"order"`,
		`"p":"97123"`,
		`"s":"0.000123"`,
		`"t":{"limit":{"tif":"Alo"}}`,
		`"c":"` + testCloid1 + `"`,
		`"grouping":"na"`,
		`"r":false`,
		`"signature":{"r":"` + goldenR + `","s":"` + goldenS + `","v":27}`,
	} {
		if !strings.Contains(string(body1), want) {
			t.Errorf("posted body missing %s\nbody: %s", want, body1)
		}
	}

	var env struct {
		Action    json.RawMessage    `json:"action"`
		Nonce     int64              `json:"nonce"`
		Signature hl.SignatureResult `json:"signature"`
	}
	if err := json.Unmarshal(body1, &env); err != nil {
		t.Fatalf("posted body is not JSON: %v", err)
	}
	if env.Nonce != pinnedNonce {
		t.Fatalf("nonce = %d, want %d", env.Nonce, pinnedNonce)
	}
	if env.Signature.R != goldenR || env.Signature.S != goldenS || env.Signature.V != goldenV {
		t.Errorf("signature = %+v, want the golden vector", env.Signature)
	}

	// Re-signing the posted action must reproduce the posted signature: the
	// action in the body is exactly what was signed.
	var action hl.OrderAction
	if err := json.Unmarshal(env.Action, &action); err != nil {
		t.Fatalf("decode posted action: %v", err)
	}
	key, err := crypto.HexToECDSA(testAgentKey)
	if err != nil {
		t.Fatalf("HexToECDSA: %v", err)
	}
	re, err := hl.SignL1Action(key, action, "", env.Nonce, nil, false)
	if err != nil {
		t.Fatalf("re-sign: %v", err)
	}
	if re != env.Signature {
		t.Errorf("re-signed %+v != posted %+v", re, env.Signature)
	}

	// The base URL is the httptest one, not hl.MainnetAPIURL, so the client
	// signed as testnet; the mainnet domain would give a different signature.
	mainnet, err := hl.SignL1Action(key, action, "", env.Nonce, nil, true)
	if err != nil {
		t.Fatalf("mainnet re-sign: %v", err)
	}
	if mainnet == env.Signature {
		t.Error("mainnet and testnet signatures are identical: the signing domain is not being applied")
	}

	// The action itself is what the venue is told to do.
	if action.Type != "order" || action.Grouping != "na" || len(action.Orders) != 1 {
		t.Fatalf("posted action = %+v", action)
	}
	w := action.Orders[0]
	if w.Asset != 0 || !w.IsBuy || w.LimitPx != "97123" || w.Size != "0.000123" || w.ReduceOnly {
		t.Errorf("posted order wire = %+v", w)
	}
	if w.OrderType.Limit == nil || w.OrderType.Limit.Tif != hl.TifAlo {
		t.Errorf("posted tif = %+v", w.OrderType)
	}
	if w.Cloid == nil || *w.Cloid != testCloid1 {
		t.Errorf("posted cloid = %v", w.Cloid)
	}
}

// TestPlaceReducesWireDecimalsExactly pins floatToWire fidelity for the three
// shapes the driver cares about: a tiny size, an integer price and a 4-dp price.
func TestPlaceReducesWireDecimalsExactly(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(placeResponse(
		`{"resting":{"oid":1,"status":"resting"}}`,
		`{"resting":{"oid":2,"status":"resting"}}`,
		`{"resting":{"oid":3,"status":"resting"}}`,
	), http.StatusOK)

	if _, err := c.Place(context.Background(), []Order{
		{Coin: "BTC", IsBuy: true, Px: "97123", Sz: "0.000123", Tif: TifAlo},
		{Coin: "BTC", IsBuy: false, Px: "3.1415", Sz: "1.5", Tif: TifGtc},
		{Coin: "ETH", IsBuy: true, Px: "3000.25", Sz: "0.0001", Tif: TifIoc, ReduceOnly: true},
	}); err != nil {
		t.Fatalf("Place: %v", err)
	}

	var env struct {
		Action hl.OrderAction `json:"action"`
	}
	if err := json.Unmarshal(lastExchangeBody(t, v), &env); err != nil {
		t.Fatalf("posted body: %v", err)
	}
	want := []struct{ px, sz string }{
		{"97123", "0.000123"},
		{"3.1415", "1.5"},
		{"3000.25", "0.0001"},
	}
	if len(env.Action.Orders) != len(want) {
		t.Fatalf("orders = %d, want %d", len(env.Action.Orders), len(want))
	}
	for i, w := range env.Action.Orders {
		if w.LimitPx != want[i].px || w.Size != want[i].sz {
			t.Errorf("order %d wired as p=%q s=%q, want p=%q s=%q", i, w.LimitPx, w.Size, want[i].px, want[i].sz)
		}
	}
	if !env.Action.Orders[2].ReduceOnly {
		t.Error("reduce-only flag lost")
	}
}

// TestPlaceLocalValidation rejects malformed input before any request.
func TestPlaceLocalValidation(t *testing.T) {
	cases := []struct {
		name  string
		order Order
		want  string
	}{
		{"empty price", Order{Coin: "BTC", Px: "", Sz: "1", Tif: TifGtc}, "missing price"},
		{"bad size", Order{Coin: "BTC", Px: "1", Sz: "abc", Tif: TifGtc}, "invalid size"},
		{"nan price", Order{Coin: "BTC", Px: "NaN", Sz: "1", Tif: TifGtc}, "not finite"},
		{"negative size", Order{Coin: "BTC", Px: "1", Sz: "-1", Tif: TifGtc}, "must be positive"},
		{"zero price", Order{Coin: "BTC", Px: "0", Sz: "1", Tif: TifGtc}, "must be positive"},
		{"bad tif", Order{Coin: "BTC", Px: "1", Sz: "1", Tif: "Fok"}, "unsupported time-in-force"},
		{"empty tif", Order{Coin: "BTC", Px: "1", Sz: "1"}, "unsupported time-in-force"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, url := newVenue(t)
			c := newTestClient(t, url)
			if _, err := c.Place(context.Background(), []Order{tc.order}); err == nil {
				t.Fatalf("Place = nil error, want %q", tc.want)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err, tc.want)
			}
			if n := len(v.exchangeRequests()); n != 0 {
				t.Errorf("%d /exchange requests were made for invalid input", n)
			}
		})
	}

	// An unknown coin is sonirico's business error (no asset id), not a panic.
	v, url := newVenue(t)
	c := newTestClient(t, url)
	_, err := c.Place(context.Background(), []Order{{Coin: "NOPE", IsBuy: true, Px: "1", Sz: "1", Tif: TifGtc}})
	if err == nil || !strings.Contains(err.Error(), "not found in info") {
		t.Fatalf("unknown coin error = %v", err)
	}
	if IsUncertain(err) {
		t.Errorf("unknown coin classified as uncertain: %v", err)
	}
	if len(v.exchangeRequests()) != 0 {
		t.Errorf("an unknown coin still reached /exchange")
	}
}

// TestPlaceEmptyIsNoOp keeps the zero-order case off the network.
func TestPlaceEmptyIsNoOp(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	statuses, err := c.Place(context.Background(), nil)
	if err != nil || statuses != nil {
		t.Fatalf("Place(nil) = (%v, %v)", statuses, err)
	}
	if res, err := c.CancelByCloid(context.Background(), nil); err != nil || res != nil {
		t.Fatalf("CancelByCloid(nil) = (%v, %v)", res, err)
	}
	if res, err := c.CancelByOid(context.Background(), nil); err != nil || res != nil {
		t.Fatalf("CancelByOid(nil) = (%v, %v)", res, err)
	}
	if n := len(v.exchangeRequests()); n != 0 {
		t.Errorf("%d /exchange requests for empty input", n)
	}
}

// ─────────────────────────────────────────────────────────────
// Schedule cancel
// ─────────────────────────────────────────────────────────────

func TestScheduleCancelArmAndUnset(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(`{"status":"ok"}`, http.StatusOK)

	// nil disarms: the action must carry no time field at all.
	if err := c.ScheduleCancel(context.Background(), nil); err != nil {
		t.Fatalf("ScheduleCancel(nil): %v", err)
	}
	body := lastExchangeBody(t, v)
	if !strings.Contains(string(body), `"type":"scheduleCancel"`) {
		t.Fatalf("posted action is not scheduleCancel: %s", body)
	}
	if strings.Contains(string(body), `"time"`) {
		t.Errorf("disarm posted a time field: %s", body)
	}

	// A future time is posted in ms.
	at := time.Now().Add(30 * time.Second)
	if err := c.ScheduleCancel(context.Background(), &at); err != nil {
		t.Fatalf("ScheduleCancel(future): %v", err)
	}
	body = lastExchangeBody(t, v)
	want := `"time":` + strconv.FormatInt(at.UnixMilli(), 10)
	if !strings.Contains(string(body), want) {
		t.Errorf("posted body missing %s: %s", want, body)
	}
}

func TestScheduleCancelRejectsTooSoonWithoutRequest(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)

	for _, at := range []time.Time{
		time.Now(),                      // now
		time.Now().Add(2 * time.Second), // inside the 5s floor
		time.Now().Add(-time.Minute),    // past
	} {
		if err := c.ScheduleCancel(context.Background(), &at); err == nil {
			t.Fatalf("ScheduleCancel(%s) = nil error, want the 5s floor error", at)
		} else if !strings.Contains(err.Error(), "less than 5s ahead") {
			t.Errorf("error = %v, want the 5s floor", err)
		}
	}
	if n := len(v.exchangeRequests()); n != 0 {
		t.Errorf("%d /exchange requests for too-soon schedule cancels", n)
	}

	// Just past the floor is accepted.
	v.setExchange(`{"status":"ok"}`, http.StatusOK)
	ok := time.Now().Add(6 * time.Second)
	if err := c.ScheduleCancel(context.Background(), &ok); err != nil {
		t.Fatalf("ScheduleCancel(6s): %v", err)
	}
}

func TestScheduleCancelVolumeRefusal(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(`{"status":"err","error":"Cannot schedule cancel: account has insufficient trading volume"}`, http.StatusOK)

	at := time.Now().Add(time.Minute)
	err := c.ScheduleCancel(context.Background(), &at)
	if err == nil {
		t.Fatal("ScheduleCancel = nil error, want ErrScheduleCancelUnavailable")
	}
	if !errors.Is(err, ErrScheduleCancelUnavailable) {
		t.Fatalf("error %v is not ErrScheduleCancelUnavailable", err)
	}
	if IsUncertain(err) || IsRateLimited(err) {
		t.Errorf("volume refusal misclassified: %v", err)
	}

	// A textless rejection still surfaces (as the generic status error).
	v.setExchange(`{"status":"err"}`, http.StatusOK)
	err = c.ScheduleCancel(context.Background(), &at)
	if err == nil {
		t.Fatal("textless rejection returned nil")
	}
	if errors.Is(err, ErrScheduleCancelUnavailable) {
		t.Errorf("textless rejection mapped to ErrScheduleCancelUnavailable: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────
// Leverage
// ─────────────────────────────────────────────────────────────

func TestUpdateLeverage(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(`{}`, http.StatusOK)
	// UpdateLeverage verifies via activeAssetData: ETH must come back cross/10.
	v.setInfo("activeAssetData", `{"leverage":{"type":"cross","value":10}}`, 0)

	if err := c.UpdateLeverage(context.Background(), "ETH", 10, true); err != nil {
		t.Fatalf("UpdateLeverage: %v", err)
	}
	body := string(lastExchangeBody(t, v))
	for _, want := range []string{`"type":"updateLeverage"`, `"asset":1`, `"isCross":true`, `"leverage":10`} {
		if !strings.Contains(body, want) {
			t.Errorf("posted body missing %s: %s", want, body)
		}
	}
	// The verification query must target the master account for the same coin.
	reqs := v.queryRequests()
	if len(reqs) != 1 {
		t.Fatalf("/info queries = %d, want 1 (the activeAssetData verification)", len(reqs))
	}
	assertUsesMasterAccount(t, reqs[0])
	if !bytes.Contains(reqs[0], []byte(`"type":"activeAssetData"`)) || !bytes.Contains(reqs[0], []byte(`"coin":"ETH"`)) {
		t.Errorf("verification query = %s, want activeAssetData for ETH", reqs[0])
	}

	if err := c.UpdateLeverage(context.Background(), "ETH", 0, false); err == nil {
		t.Error("UpdateLeverage(0) = nil error, want a validation error")
	}
	if n := len(v.exchangeRequests()); n != 1 {
		t.Errorf("%d /exchange requests, want 1 (the invalid call must not post)", n)
	}
}

// TestClientUsesStableNoncesAcrossCalls documents rule 8: one *hl.Exchange per
// client keeps the nonce monotonic, so two actions never share one.
func TestClientUsesStableNoncesAcrossCalls(t *testing.T) {
	v, url := newVenue(t)
	c := newTestClient(t, url)
	v.setExchange(placeResponse(`{"resting":{"oid":1,"status":"resting"}}`), http.StatusOK)

	order := Order{Coin: "BTC", IsBuy: true, Px: "1", Sz: "1", Tif: TifGtc}
	for i := 0; i < 3; i++ {
		if _, err := c.Place(context.Background(), []Order{order}); err != nil {
			t.Fatalf("Place: %v", err)
		}
	}
	seen := map[int64]bool{}
	for _, body := range v.exchangeRequests() {
		var env struct {
			Nonce int64 `json:"nonce"`
		}
		if err := json.Unmarshal(body, &env); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if seen[env.Nonce] {
			t.Fatalf("nonce %d reused", env.Nonce)
		}
		seen[env.Nonce] = true
	}
	if len(seen) != 3 {
		t.Fatalf("distinct nonces = %d, want 3", len(seen))
	}
}
