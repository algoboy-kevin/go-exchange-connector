package hyperliquid

import (
	"errors"
	"testing"

	connector "github.com/algoboy-kevin/go-exchange-connector"
)

// The close reason must reach the consumer, not just the log: distinguishing a
// blip from a refused subscription or a per-IP connection cap is the difference
// between waiting and backing off.
func TestSetOnDisconnectReceivesCloseReason(t *testing.T) {
	h := New(connector.New(false, nil))

	var gotErr error
	h.SetOnDisconnect(func(err error) { gotErr = err })

	want := errors.New("Cannot open more than 15 connections.")
	h.conn.onDisconnect(want)

	if !errors.Is(gotErr, want) {
		t.Fatalf("on-disconnect hook got %v, want %v", gotErr, want)
	}
}

// A deliberate Disconnect() reports a nil error; the hook must still fire so a
// consumer can tell "we closed it" from "we never heard about it".
func TestSetOnDisconnectFiresWithNilError(t *testing.T) {
	h := New(connector.New(false, nil))

	fired := false
	h.SetOnDisconnect(func(err error) {
		fired = true
		if err != nil {
			t.Errorf("hook err = %v, want nil", err)
		}
	})

	h.conn.onDisconnect(nil)

	if !fired {
		t.Fatal("on-disconnect hook did not fire for a nil-error disconnect")
	}
}
