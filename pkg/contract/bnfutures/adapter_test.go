package bnfutures

import (
	"errors"
	"testing"
	"time"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/binance/futures"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/contract"
)

func TestTifMapping(t *testing.T) {
	cases := map[contract.TIF]string{contract.GTC: "GTC", contract.IOC: "IOC", contract.PostOnly: "GTX"}
	for in, want := range cases {
		got, err := tifValue(in)
		if err != nil || got != want {
			t.Errorf("tifValue(%s) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := tifValue("GTD"); !errors.Is(err, contract.ErrInvalid) {
		t.Errorf("unknown tif: want ErrInvalid, got %v", err)
	}
}

func TestToOrderReqPostOnlyIsGTX(t *testing.T) {
	n, err := toOrderReq(contract.OrderRequest{
		Symbol: "BTCUSDT", Side: contract.Buy, Price: "60000.5", Qty: "0.01",
		TIF: contract.PostOnly, ClientID: "cid-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if n.Type != "LIMIT" || n.TimeInForce != "GTX" || n.Side != "BUY" || n.NewClientOrderID != "cid-1" {
		t.Errorf("unexpected native request: %+v", n)
	}
	if n.NewOrderRespType != "RESULT" {
		t.Errorf("want RESULT response type, got %q", n.NewOrderRespType)
	}
}

func TestToOrderReqRejectsBeforeSending(t *testing.T) {
	_, err := toOrderReq(contract.OrderRequest{Symbol: "BTCUSDT", Side: contract.Buy, Price: "0", Qty: "1", TIF: contract.GTC})
	if !errors.Is(err, contract.ErrInvalid) {
		t.Errorf("zero price must be ErrInvalid, got %v", err)
	}
}

func TestMapStatus(t *testing.T) {
	cases := map[string]contract.Status{
		"NEW": contract.Resting, "PARTIALLY_FILLED": contract.Resting,
		"FILLED": contract.Filled, "REJECTED": contract.Rejected,
		"EXPIRED": contract.Rejected, "CANCELED": contract.Rejected,
		"WEIRD": contract.Unknown,
	}
	for in, want := range cases {
		if got := mapStatus(in); got != want {
			t.Errorf("mapStatus(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestResultFromErrorUncertainIsUnknown(t *testing.T) {
	// HTTP 5xx is classified uncertain by futures.IsUncertain: the order may have executed.
	err := &futures.APIError{HTTPStatus: 503, Msg: "service unavailable"}
	if !futures.IsUncertain(err) {
		t.Fatal("precondition: 503 must be uncertain for the native classifier")
	}
	res, callErr := resultFromError(err, "cid", time.Now(), time.Now())
	if res.Status != contract.Unknown {
		t.Errorf("uncertain must map to Unknown, got %s", res.Status)
	}
	if !errors.Is(callErr, contract.ErrUncertain) {
		t.Errorf("uncertain must wrap ErrUncertain, got %v", callErr)
	}
}

func TestResultFromErrorAPIRejectionIsRejectedNotError(t *testing.T) {
	res, callErr := resultFromError(&futures.APIError{HTTPStatus: 400, Code: -2010, Msg: "Insufficient balance"}, "cid", time.Now(), time.Now())
	if callErr != nil {
		t.Fatalf("definite rejection must not be a call error: %v", callErr)
	}
	if res.Status != contract.Rejected || res.Reason != "Insufficient balance" {
		t.Errorf("got %+v", res)
	}
}
