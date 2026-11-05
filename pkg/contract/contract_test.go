package contract

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestValidPositiveDecimal(t *testing.T) {
	cases := map[string]bool{
		"1": true, "0.5": true, "10.25": true, "100": true,
		"0": false, "0.0": false, "00.00": false, "": false,
		"-1": false, "+1": false, "1e3": false, "1,5": false,
		" 1": false, "abc": false, ".5": false, "1.": false,
	}
	for in, want := range cases {
		if got := ValidPositiveDecimal(in); got != want {
			t.Errorf("ValidPositiveDecimal(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestOrderRequestValidate(t *testing.T) {
	good := OrderRequest{Symbol: "BTC", Side: Buy, Price: "100.5", Qty: "0.1", TIF: PostOnly}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	bad := map[string]OrderRequest{
		"no symbol":   {Side: Buy, Price: "1", Qty: "1", TIF: GTC},
		"bad side":    {Symbol: "BTC", Side: "HOLD", Price: "1", Qty: "1", TIF: GTC},
		"zero price":  {Symbol: "BTC", Side: Buy, Price: "0", Qty: "1", TIF: GTC},
		"float price": {Symbol: "BTC", Side: Buy, Price: "1e3", Qty: "1", TIF: GTC},
		"neg qty":     {Symbol: "BTC", Side: Buy, Price: "1", Qty: "-1", TIF: GTC},
		"bad tif":     {Symbol: "BTC", Side: Buy, Price: "1", Qty: "1", TIF: "GTD"},
	}
	for name, r := range bad {
		err := r.Validate()
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: want ErrInvalid, got %v", name, err)
		}
	}
}

func TestCancelRequestValidate(t *testing.T) {
	if err := (CancelRequest{Symbol: "BTC", OrderID: "1"}).Validate(); err != nil {
		t.Errorf("order id only: %v", err)
	}
	if err := (CancelRequest{Symbol: "BTC", ClientID: "x"}).Validate(); err != nil {
		t.Errorf("client id only: %v", err)
	}
	if err := (CancelRequest{Symbol: "BTC"}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Errorf("no id: want ErrInvalid, got %v", err)
	}
	if err := (CancelRequest{OrderID: "1"}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Errorf("no symbol: want ErrInvalid, got %v", err)
	}
}

func TestCheckBatch(t *testing.T) {
	if err := CheckBatch(0, 5); !errors.Is(err, ErrInvalid) {
		t.Errorf("empty: %v", err)
	}
	if err := CheckBatch(6, 5); !errors.Is(err, ErrInvalid) {
		t.Errorf("over limit: %v", err)
	}
	if err := CheckBatch(5, 5); err != nil {
		t.Errorf("at limit: %v", err)
	}
}

// fakeTrader proves the interface shape compiles and that optional capabilities
// are discovered by type assertion, not by a shared required method.
type fakeTrader struct{}

func (fakeTrader) PlaceOrder(context.Context, OrderRequest) (OrderResult, error) {
	return OrderResult{Status: Resting, SentAt: time.Now()}, nil
}
func (fakeTrader) PlaceOrders(_ context.Context, reqs []OrderRequest) ([]OrderResult, error) {
	return make([]OrderResult, len(reqs)), nil
}
func (fakeTrader) CancelOrder(context.Context, CancelRequest) (CancelResult, error) {
	return CancelResult{Canceled: true}, nil
}
func (fakeTrader) CancelOrders(_ context.Context, reqs []CancelRequest) ([]CancelResult, error) {
	return make([]CancelResult, len(reqs)), nil
}
func (fakeTrader) OpenOrders(context.Context, string) ([]Order, error) { return nil, nil }

func TestOptionalCapabilityByAssertion(t *testing.T) {
	var tr Trader = fakeTrader{}
	if _, ok := tr.(CancelAller); ok {
		t.Error("fake does not implement CancelAll, assertion must fail")
	}
	if _, ok := tr.(DeadManSwitch); ok {
		t.Error("fake does not implement DeadManSwitch, assertion must fail")
	}
}
