package websocket

import (
	"testing"
	"time"
)

func TestNextBackoff(t *testing.T) {
	max := 30 * time.Second

	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"base doubles", 5 * time.Second, 10 * time.Second},
		{"second double", 10 * time.Second, 20 * time.Second},
		{"capped at max", 20 * time.Second, 30 * time.Second},
		{"stays at max", 30 * time.Second, 30 * time.Second},
	}
	for _, c := range cases {
		if got := nextBackoff(c.in, max); got != c.want {
			t.Errorf("%s: nextBackoff(%v) = %v, want %v", c.name, c.in, got, c.want)
		}
	}
}
