package testkit

import (
	"context"
	"testing"
	"time"
)

// SourceOf emits the provided values then closes the channel.
func SourceOf[T any](xs ...T) <-chan T {
	ch := make(chan T, len(xs))
	go func() {
		defer close(ch)
		for _, v := range xs {
			ch <- v
		}
	}()
	return ch
}

// Collect drains the channel or fails the test if the context expires.
func Collect[T any](t *testing.T, ctx context.Context, ch <-chan T) []T {
	t.Helper()
	var out []T
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("collect timeout: %v", ctx.Err())
		case v, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, v)
		}
	}
}

// Within returns a context with timeout for coordinating test execution.
func Within(d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = time.Second
	}
	return context.WithTimeout(context.Background(), d)
}
