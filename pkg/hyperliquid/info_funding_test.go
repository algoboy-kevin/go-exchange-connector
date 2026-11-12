package hyperliquid

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Fixtures are trimmed copies of real responses captured from
// api.hyperliquid.xyz on 2026-10-30.
const (
	realMetaXYZ = `{"universe":[{"szDecimals":4,"name":"xyz:XYZ100","maxLeverage":30,"marginTableId":30,"growthMode":"enabled","lastFeeScaleChangeTime":"2025-11-23T17:37:10.033211662","deployerFeeScale":"1.0"},{"szDecimals":3,"name":"xyz:TSLA","maxLeverage":20,"marginTableId":20,"growthMode":"enabled","lastFeeScaleChangeTime":"2025-11-23T17:37:10.033211662","deployerFeeScale":"1.0"}],"marginTables":[],"collateralToken":0}`

	realFundingHistory = `[{"coin":"SOL","fundingRate":"0.0000163603","premium":"0.0006308822","time":1790002800016},{"coin":"SOL","fundingRate":"0.0000125","premium":"0.0005078893","time":1790006400108},{"coin":"SOL","fundingRate":"0.0000125","premium":"0.0003242794","time":1790010000083}]`

	// BinPerp for the first coin has no fundingIntervalHours (seen in the real
	// response), and a venue may be null.
	realPredicted = `[["0G",[["BinPerp",{"fundingRate":"0.00005","nextFundingTime":1793376000000,"fundingIntervalHours":4}],["HlPerp",{"fundingRate":"0.0000125","nextFundingTime":1793361600000,"fundingIntervalHours":1}],["BybitPerp",{"fundingRate":"0.00005","nextFundingTime":1793376000000,"fundingIntervalHours":4}]]],["ABC",[["BinPerp",{"fundingRate":"0.0001","nextFundingTime":1793376000000}],["HlPerp",{"fundingRate":"0.0000125","nextFundingTime":1793361600000,"fundingIntervalHours":1}],["BybitPerp",null]]]]`
)

func TestMetaDexSendsDexAndDecodesHIP3Fields(t *testing.T) {
	c, bodies, _ := newTestInfoServer(t, http.StatusOK, realMetaXYZ)
	meta, err := c.MetaDex(context.Background(), "xyz")
	if err != nil {
		t.Fatal(err)
	}
	if b := (*bodies)[0]; b["type"] != "meta" || b["dex"] != "xyz" || len(b) != 2 {
		t.Errorf("request body = %+v, want exactly {type:meta, dex:xyz}", b)
	}
	if len(meta.Universe) != 2 || meta.Universe[1].Name != "xyz:TSLA" {
		t.Fatalf("universe = %+v", meta.Universe)
	}
	a := meta.Universe[0]
	if a.GrowthMode != "enabled" || a.DeployerFeeScale != "1.0" || a.LastFeeScaleChangeTime != "2025-11-23T17:37:10.033211662" || a.MaxLeverage != 30 {
		t.Errorf("HIP-3 fields lost: %+v", a)
	}
}

func TestMetaDexEmptyIsDefaultDex(t *testing.T) {
	c, bodies, _ := newTestInfoServer(t, http.StatusOK, `{"universe":[],"marginTables":[],"collateralToken":0}`)
	if _, err := c.MetaDex(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if b := (*bodies)[0]; b["type"] != "meta" || len(b) != 1 {
		t.Errorf("body = %+v, want only {type:meta} (the default dex is selected by omitting dex)", b)
	}
}

func TestMetaDexRejectsDangerousNames(t *testing.T) {
	c, bodies, _ := newTestInfoServer(t, http.StatusOK, `{}`)
	for _, bad := range []string{`x"y`, `x\y`, "x y", "x\ny", "x\t"} {
		if _, err := c.MetaDex(context.Background(), bad); err == nil {
			t.Errorf("MetaDex(%q) = nil error", bad)
		}
		if _, err := c.MetaDexRaw(context.Background(), bad); err == nil {
			t.Errorf("MetaDexRaw(%q) = nil error", bad)
		}
		if _, err := c.MetaAndAssetCtxsDex(context.Background(), bad); err == nil {
			t.Errorf("MetaAndAssetCtxsDex(%q) = nil error", bad)
		}
	}
	if len(*bodies) != 0 {
		t.Errorf("%d requests reached the server for invalid dex names", len(*bodies))
	}
}

func TestMetaDexRawIsVerbatim(t *testing.T) {
	c, _, _ := newTestInfoServer(t, http.StatusOK, realMetaXYZ)
	raw, err := c.MetaDexRaw(context.Background(), "xyz")
	if err != nil || string(raw) != realMetaXYZ {
		t.Errorf("raw = %q, err = %v; want the body byte for byte", raw, err)
	}
}

func TestMetaAndAssetCtxsDex(t *testing.T) {
	body := `[` + realMetaXYZ + `,[{"funding":"0.0000125","openInterest":"10","prevDayPx":"1","dayNtlVlm":"2","premium":"0","oraclePx":"100","markPx":"100","midPx":"100","impactPxs":["99","101"],"dayBaseVlm":"3"},{"funding":"0.00001","openInterest":"5","prevDayPx":"1","dayNtlVlm":"2","premium":"0","oraclePx":"50","markPx":"50","midPx":null,"impactPxs":null,"dayBaseVlm":"3"}]]`
	c, bodies, _ := newTestInfoServer(t, http.StatusOK, body)
	out, err := c.MetaAndAssetCtxsDex(context.Background(), "xyz")
	if err != nil {
		t.Fatal(err)
	}
	if b := (*bodies)[0]; b["type"] != "metaAndAssetCtxs" || b["dex"] != "xyz" {
		t.Errorf("body = %+v", b)
	}
	if f, ok := out.FundingRate("xyz:TSLA"); !ok || f != "0.00001" {
		t.Errorf("FundingRate(xyz:TSLA) = %q, %v", f, ok)
	}
}

func TestFundingHistoryRequestAndDecode(t *testing.T) {
	c, bodies, _ := newTestInfoServer(t, http.StatusOK, realFundingHistory)
	pts, err := c.FundingHistory(context.Background(), "SOL", 1790000000000, 1790010800000)
	if err != nil {
		t.Fatal(err)
	}
	b := (*bodies)[0]
	if b["type"] != "fundingHistory" || b["coin"] != "SOL" || b["startTime"] != float64(1790000000000) || b["endTime"] != float64(1790010800000) {
		t.Errorf("body = %+v", b)
	}
	if len(pts) != 3 || pts[0].FundingRate != "0.0000163603" || pts[0].Premium != "0.0006308822" || pts[0].Time != 1790002800016 || pts[2].Coin != "SOL" {
		t.Errorf("points = %+v", pts)
	}
}

func TestFundingHistoryOmitsEndTimeWhenZeroAndAcceptsHIP3Coin(t *testing.T) {
	c, bodies, _ := newTestInfoServer(t, http.StatusOK, `[]`)
	if _, err := c.FundingHistory(context.Background(), "xyz:TSLA", 5, 0); err != nil {
		t.Fatal(err)
	}
	b := (*bodies)[0]
	if _, has := b["endTime"]; has || b["coin"] != "xyz:TSLA" {
		t.Errorf("body = %+v, want no endTime and the dex-prefixed coin intact", b)
	}
}

func TestFundingHistoryValidatesInput(t *testing.T) {
	c, bodies, _ := newTestInfoServer(t, http.StatusOK, `[]`)
	for name, call := range map[string]func() error{
		"empty coin":     func() error { _, e := c.FundingHistory(context.Background(), "", 1, 2); return e },
		"quote in coin":  func() error { _, e := c.FundingHistory(context.Background(), `S"OL`, 1, 2); return e },
		"negative start": func() error { _, e := c.FundingHistory(context.Background(), "SOL", -1, 2); return e },
		"end before":     func() error { _, e := c.FundingHistory(context.Background(), "SOL", 10, 5); return e },
	} {
		if call() == nil {
			t.Errorf("%s: nil error", name)
		}
	}
	if len(*bodies) != 0 {
		t.Errorf("%d requests sent for invalid input", len(*bodies))
	}
}

// pagedFundingServer returns full pages (500 entries) for the first two
// requests and a short page for the third, recording each startTime.
func TestFundingHistoryAllPagesForward(t *testing.T) {
	var starts []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req struct {
			StartTime int64 `json:"startTime"`
		}
		_ = json.Unmarshal(raw, &req)
		starts = append(starts, req.StartTime)
		n := FundingHistoryPageSize
		if len(starts) == 3 {
			n = 7
		}
		var sb strings.Builder
		sb.WriteString("[")
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			ts := req.StartTime + int64(i)*3_600_000 + 16
			sb.WriteString(`{"coin":"SOL","fundingRate":"0.0000125","premium":"0","time":` + itoa(ts) + `}`)
		}
		sb.WriteString("]")
		_, _ = w.Write([]byte(sb.String()))
	}))
	defer srv.Close()

	c := NewInfoClient(srv.URL)
	pts, err := c.FundingHistoryAll(context.Background(), "SOL", 1_000, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 500+500+7 || len(starts) != 3 {
		t.Fatalf("got %d points in %d requests (starts %v)", len(pts), len(starts), starts)
	}
	last0 := int64(1_000) + 499*3_600_000 + 16
	if starts[0] != 1_000 || starts[1] != last0+1 {
		t.Errorf("startTimes = %v, want the next page to start 1 ms after the last entry (%d)", starts, last0+1)
	}
	for i := 1; i < len(pts); i++ {
		if pts[i].Time <= pts[i-1].Time {
			t.Fatalf("points not strictly increasing at %d: %d then %d", i, pts[i-1].Time, pts[i].Time)
		}
	}
}

func TestFundingHistoryAllStopsWithoutProgress(t *testing.T) {
	// A server that keeps returning the same full page must not loop forever.
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n++
		var sb strings.Builder
		sb.WriteString("[")
		for i := 0; i < FundingHistoryPageSize; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(`{"coin":"SOL","fundingRate":"0","premium":"0","time":5}`)
		}
		sb.WriteString("]")
		_, _ = w.Write([]byte(sb.String()))
	}))
	defer srv.Close()
	pts, err := NewInfoClient(srv.URL).FundingHistoryAll(context.Background(), "SOL", 100, 0)
	if err != nil || n > 2 {
		t.Errorf("err = %v after %d requests (%d points); want a clean stop after at most 2", err, n, len(pts))
	}
}

func itoa(v int64) string { b, _ := json.Marshal(v); return string(b) }

func TestPredictedFundingsDecodesRealShape(t *testing.T) {
	c, bodies, _ := newTestInfoServer(t, http.StatusOK, realPredicted)
	out, err := c.PredictedFundings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b := (*bodies)[0]; b["type"] != "predictedFundings" || len(b) != 1 {
		t.Errorf("body = %+v", b)
	}
	if len(out) != 2 || out[0].Coin != "0G" || len(out[0].Venues) != 3 {
		t.Fatalf("out = %+v", out)
	}
	hl, ok := out[0].Venue(PredictedVenueHyperliquid)
	if !ok || hl.FundingIntervalHours != 1 || hl.NextFundingTime != 1793361600000 || hl.FundingRate != "0.0000125" {
		t.Errorf("HlPerp = %+v, %v", hl, ok)
	}
	if bin, _ := out[0].Venue(PredictedVenueBinance); bin.FundingIntervalHours != 4 {
		t.Errorf("BinPerp interval = %d, want 4", bin.FundingIntervalHours)
	}
	// Second coin: interval absent -> 0, null venue dropped.
	if bin, ok := out[1].Venue(PredictedVenueBinance); !ok || bin.FundingIntervalHours != 0 || bin.FundingRate != "0.0001" {
		t.Errorf("BinPerp without interval = %+v, %v", bin, ok)
	}
	if _, ok := out[1].Venue(PredictedVenueBybit); ok || len(out[1].Venues) != 2 {
		t.Errorf("null venue should be dropped: %+v", out[1].Venues)
	}
}

func TestPredictedFundingsRejectsMalformedEntries(t *testing.T) {
	for name, body := range map[string]string{
		"not a pair":     `[["BTC"]]`,
		"venue not pair": `[["BTC",[["HlPerp"]]]]`,
		"bad coin":       `[[5,[]]]`,
	} {
		c, _, _ := newTestInfoServer(t, http.StatusOK, body)
		if _, err := c.PredictedFundings(context.Background()); err == nil {
			t.Errorf("%s: nil error", name)
		}
	}
}
