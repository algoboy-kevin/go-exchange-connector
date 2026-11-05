package asfutures

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/algoboy-kevin/go-exchange-connector/pkg/aster/futures"
	"github.com/algoboy-kevin/go-exchange-connector/pkg/contract"
)

// The adapter must validate before any client call and use Aster's route for
// positions.
func TestAdapterOverAsterClient(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	c, err := futures.New(futures.Config{BaseURL: srv.URL, APIKey: "k", APISecret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	a := New(c)
	if a.MaxPlaceBatch() != 5 || a.MaxCancelBatch() != 10 {
		t.Errorf("caps = %d/%d, want 5/10", a.MaxPlaceBatch(), a.MaxCancelBatch())
	}
	_, err = a.PlaceOrder(context.Background(), contract.OrderRequest{Symbol: "BTCUSDT", Side: contract.Buy, Qty: "0", Price: "1", TIF: contract.PostOnly})
	if !errors.Is(err, contract.ErrInvalid) {
		t.Errorf("zero qty: err = %v, want ErrInvalid", err)
	}
	if len(paths) != 0 {
		t.Errorf("invalid request reached the server: %v", paths)
	}
	if _, err := a.Positions(context.Background(), ""); err != nil {
		t.Fatalf("Positions: %v", err)
	}
	if len(paths) != 1 || paths[0] != "/fapi/v2/positionRisk" {
		t.Errorf("paths = %v, want [/fapi/v2/positionRisk]", paths)
	}
}
