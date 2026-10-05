package polymarket

import (
	"errors"
	"testing"

	connector "github.com/algoboy-kevin/go-exchange-connector"
	ws "github.com/algoboy-kevin/go-exchange-connector/pkg/websocket"
)

func TestMarketSetOnDisconnectReceivesCloseReason(t *testing.T) {
	base := connector.New(false, nil)
	m := NewWSPolymarketMarket(base, Config{})

	var gotErr error
	var gotStatus ws.ConnectionStatus
	m.SetOnDisconnect(func(err error) { gotErr = err })
	m.SetOnStatusChange(func(s ws.ConnectionStatus) { gotStatus = s })

	want := errors.New("book frame exceeded read limit")
	m.onDisconnect(want)

	if !errors.Is(gotErr, want) {
		t.Fatalf("on-disconnect hook got %v, want %v", gotErr, want)
	}
	if gotStatus != ws.StatusDisconnected {
		t.Fatalf("status hook got %v, want %v", gotStatus, ws.StatusDisconnected)
	}
}

func TestRTDSSetOnDisconnectReceivesCloseReason(t *testing.T) {
	base := connector.New(false, nil)
	r := NewWSPolymarketRTDS(base)

	var gotErr error
	r.SetOnDisconnect(func(err error) { gotErr = err })

	want := errors.New("slow consumer")
	r.onDisconnect(want)

	if !errors.Is(gotErr, want) {
		t.Fatalf("on-disconnect hook got %v, want %v", gotErr, want)
	}
}
