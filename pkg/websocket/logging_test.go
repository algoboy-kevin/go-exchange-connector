package websocket

import (
	"bytes"
	"context"
	coderws "github.com/coder/websocket"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestConnectedLogDoesNotExposeCredentialURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		_, _, _ = c.Read(r.Context())
	}))
	defer srv.Close()
	var logs lockedLogBuffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(old)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	b := &BaseWebSocket{}
	opts := DefaultWSOptions()
	opts.DisableDataStaleWatchdog = true
	token := "sensitive-test-listen-key"
	if err := b.Connect(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/private/ws/"+token, opts); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if strings.Contains(logs.String(), token) {
		t.Fatal("connected log exposes credential-bearing URL")
	}
}
