package websocket

// lifecycle.go holds the connection-generation bookkeeping behind
// BaseWebSocket.Connect/Close.
//
// A generation is one Connect() call: its context, every goroutine it starts,
// and the channel Close() waits on. Keeping them in one value makes three
// properties hold that the old bare (cancel, done) pair could not:
//
//   - Close() after a FAILED first dial returns immediately: the failure path
//     ends the generation itself (no goroutines were started), so done is
//     closed and Close never waits on a channel nobody will close.
//   - Close() is idempotent and generation-safe: the field holding the
//     generation is cleared under b.mu, so a second Close sees nothing to do
//     and a Close racing a Connect cannot cancel a newer generation.
//   - The close handshake never runs under b.mu: Close detaches the connection
//     under the lock and performs the bounded handshake after unlocking.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	coderws "github.com/coder/websocket"
)

const (
	// closeGraceTimeout bounds Close()'s graceful close handshake. It only has
	// to cover writing the close frame out: a co-operative peer echoes it in
	// milliseconds, and when our own read loop is still running it is the one
	// that consumes the echo, so coder/websocket's own wait (5s, up to 10s in
	// the worst case) is never something Close should sit through. Shutdown
	// must not depend on the peer's courtesy.
	closeGraceTimeout = 250 * time.Millisecond

	// closeDrainTimeout bounds how long Close() waits for the generation's
	// goroutines after cancelling them. Every loop selects on the generation
	// context, so this only expires if user code (an OnConnect/OnDisconnect
	// hook) is itself stuck; Close then logs and returns instead of hanging.
	closeDrainTimeout = 5 * time.Second
)

// wsLife is one connection generation.
type wsLife struct {
	// ctx is the root context of every goroutine of this generation.
	ctx    context.Context
	cancel context.CancelFunc

	// done is closed once every tracked goroutine has exited.
	done chan struct{}

	// drop is signalled by the read loop when a connection that outlived
	// MinStableConnectionMs drops, so the reconnect loop stops waiting out the
	// poll interval. Buffered(1): only "something dropped" matters.
	drop chan struct{}

	// closing is set by Close before it detaches the connection. The read
	// loop must not wake the reconnect loop for a connection Close is tearing
	// down, and the reconnect loop/dialSync must not start a new dial on a
	// generation that is being closed (Close's own close handshake makes the
	// read loop exit normally, which would otherwise look like a drop).
	closing atomic.Bool

	wg   sync.WaitGroup
	once sync.Once
}

func newWSLife(parent context.Context) *wsLife {
	ctx, cancel := context.WithCancel(parent)
	return &wsLife{
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
		drop:   make(chan struct{}, 1),
	}
}

// goRun starts f as part of the generation. Every goroutine this package
// starts must go through it, so Close() can wait for all of them.
func (l *wsLife) goRun(f func()) {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		f()
	}()
}

// finish closes done once every tracked goroutine has exited. It is safe to
// call from several goroutines: once.Do admits one and blocks the rest until
// the close has happened.
func (l *wsLife) finish() {
	l.once.Do(func() {
		l.wg.Wait()
		close(l.done)
	})
}

// notifyDrop wakes the reconnect loop without blocking.
func (l *wsLife) notifyDrop() {
	select {
	case l.drop <- struct{}{}:
	default:
	}
}

// closeGracefully performs the WebSocket close handshake for at most timeout.
//
// coder/websocket's Close has no context parameter: it writes the close frame
// with a 5s budget and then waits up to 5s for the peer's echo. Against a peer
// that never answers, that stalls shutdown for up to 10s. So the handshake
// runs in its own goroutine and is abandoned after timeout; CloseNow then
// tears the socket down (it is a no-op for a handshake already in progress, so
// it never blocks the caller either way).
func closeGracefully(conn *coderws.Conn, timeout time.Duration) {
	handshakeDone := make(chan struct{})
	go func() {
		defer close(handshakeDone)
		_ = conn.Close(coderws.StatusNormalClosure, "shutdown")
	}()

	select {
	case <-handshakeDone:
	case <-time.After(timeout):
		go func() { _ = conn.CloseNow() }()
	}
}
