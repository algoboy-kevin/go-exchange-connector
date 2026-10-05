package binance

import (
	"errors"
	"testing"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

func TestSetOnDisconnectReceivesCloseReason(t *testing.T) {
	b := New(connector.New(false, nil))

	var gotMkt MarketType
	var gotErr error
	b.SetOnDisconnect(func(mkt MarketType, err error) {
		gotMkt, gotErr = mkt, err
	})

	want := errors.New("Too many requests")
	b.conns[MarketPerp][classPublic].onDisconnect(want)

	if gotMkt != MarketPerp {
		t.Errorf("hook market = %v, want %v", gotMkt, MarketPerp)
	}
	if !errors.Is(gotErr, want) {
		t.Fatalf("hook err = %v, want %v", gotErr, want)
	}
}
