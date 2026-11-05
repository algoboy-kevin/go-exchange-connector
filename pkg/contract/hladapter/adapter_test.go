package hladapter

import (
	"errors"
	"testing"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/contract"
	hl "github.com/algoboy-kevin/go-exchange-connector/pkg/hyperliquid/exchange"
)

const goodCloid = "0x0123456789abcdef0123456789abcdef"

func TestTifMappingPostOnlyIsAlo(t *testing.T) {
	cases := map[contract.TIF]string{contract.GTC: "Gtc", contract.IOC: "Ioc", contract.PostOnly: "Alo"}
	for in, want := range cases {
		got, err := tifValue(in)
		if err != nil || got != want {
			t.Errorf("tifValue(%s) = %q, %v; want %q", in, got, err, want)
		}
	}
}

func TestToOrderBuildsNativeOrder(t *testing.T) {
	o, err := toOrder(contract.OrderRequest{
		Symbol: "BTC", Side: contract.Sell, Price: "60000", Qty: "0.01",
		TIF: contract.PostOnly, ReduceOnly: true, ClientID: goodCloid,
	})
	if err != nil {
		t.Fatal(err)
	}
	if o.Coin != "BTC" || o.IsBuy || o.Px != "60000" || o.Sz != "0.01" || o.Tif != "Alo" || !o.ReduceOnly || o.Cloid != goodCloid {
		t.Errorf("unexpected native order: %+v", o)
	}
}

func TestToOrderRejectsBadCloidBeforeSending(t *testing.T) {
	for _, bad := range []string{"cid-1", "0x123", "0xZZZZ456789abcdef0123456789abcdef"} {
		_, err := toOrder(contract.OrderRequest{Symbol: "BTC", Side: contract.Buy, Price: "1", Qty: "1", TIF: contract.GTC, ClientID: bad})
		if !errors.Is(err, contract.ErrInvalid) {
			t.Errorf("cloid %q: want ErrInvalid, got %v", bad, err)
		}
	}
}

func TestResultFromStatusMapping(t *testing.T) {
	req := contract.OrderRequest{Symbol: "BTC", ClientID: goodCloid}
	now := time.Now()
	cases := []struct {
		name string
		in   hl.OrderStatus
		want contract.Status
	}{
		{"resting", hl.OrderStatus{Resting: true, Oid: 9}, contract.Resting},
		{"filled", hl.OrderStatus{Filled: true, Oid: 9, TotalSz: "1", AvgPx: "2"}, contract.Filled},
		{"per-item error", hl.OrderStatus{Error: "Post only order would have immediately matched"}, contract.Rejected},
		{"empty", hl.OrderStatus{}, contract.Unknown},
	}
	for _, c := range cases {
		got := resultFromStatus(c.in, req, now, now)
		if got.Status != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got.Status, c.want)
		}
	}
	got := resultFromStatus(hl.OrderStatus{Resting: true, Oid: 42}, req, now, now)
	if got.OrderID != "42" || got.ClientID != goodCloid {
		t.Errorf("ids not carried: %+v", got)
	}
}

func TestPerItemErrorIsNotCallError(t *testing.T) {
	// A per-item rejection maps to a Rejected result; the adapter returns no error.
	req := contract.OrderRequest{Symbol: "BTC"}
	now := time.Now()
	res := resultFromStatus(hl.OrderStatus{Error: "Insufficient margin"}, req, now, now)
	if res.Status != contract.Rejected || res.Reason != "Insufficient margin" {
		t.Errorf("got %+v", res)
	}
}

func TestCancelBatchRejectsMixedIDKinds(t *testing.T) {
	// The adapter must refuse a batch that mixes oid and cloid before any venue call.
	a := &Adapter{} // no client: any venue call would panic, so this also proves nothing is sent
	_, err := a.CancelOrders(nil, []contract.CancelRequest{
		{Symbol: "BTC", OrderID: "1"},
		{Symbol: "BTC", ClientID: goodCloid},
	})
	if !errors.Is(err, contract.ErrInvalid) {
		t.Errorf("mixed batch: want ErrInvalid, got %v", err)
	}
}

func TestCancelRejectsNonNumericOid(t *testing.T) {
	a := &Adapter{}
	_, err := a.CancelOrders(nil, []contract.CancelRequest{{Symbol: "BTC", OrderID: "abc"}})
	if !errors.Is(err, contract.ErrInvalid) {
		t.Errorf("non-numeric oid: want ErrInvalid, got %v", err)
	}
}

func TestPlaceBatchOverLimitSendsNothing(t *testing.T) {
	a := &Adapter{}
	reqs := make([]contract.OrderRequest, maxBatch+1)
	for i := range reqs {
		reqs[i] = contract.OrderRequest{Symbol: "BTC", Side: contract.Buy, Price: "1", Qty: "1", TIF: contract.GTC}
	}
	_, err := a.PlaceOrders(nil, reqs)
	if !errors.Is(err, contract.ErrInvalid) {
		t.Errorf("over limit: want ErrInvalid, got %v", err)
	}
}
