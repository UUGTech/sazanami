package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UUGTech/sazanami"
	"github.com/UUGTech/sazanami/testkit"
)

func TestBasicFlow(t *testing.T) {
	ctx, cancel := testkit.Within(500 * time.Millisecond)
	defer cancel()

	b := sazanami.From(testkit.SourceOf(1, 2, 3, 4))
	b = sazanami.AddStage(b, "double", func(ctx context.Context, in <-chan int, out chan<- int) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v * 2:
				}
			}
		}
	})
	b = sazanami.AddStage(b, "filter", func(ctx context.Context, in <-chan int, out chan<- int) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				if v%4 != 0 {
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v:
				}
			}
		}
	})
	out := b.Run(ctx)

	got := testkit.Collect(t, ctx, out)
	want := []int{4, 8}
	if len(got) != len(want) {
		t.Fatalf("unexpected output length: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected output at %d: got %d want %d", i, got[i], want[i])
		}
	}
}

func TestRetryPolicy(t *testing.T) {
	ctx, cancel := testkit.Within(500 * time.Millisecond)
	defer cancel()

	var attempts int32
	handler := func(ctx context.Context, in <-chan int, out chan<- int) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				_ = v
				atomic.AddInt32(&attempts, 1)
				return errors.New("boom")
			}
		}
	}

	b := sazanami.From(testkit.SourceOf(7))
	b = sazanami.AddStage(b, "fail-once", handler)
	b = b.OnError(sazanami.Retry(1, sazanami.ConstantBackoff(0)))
	out := b.Run(ctx)

	select {
	case <-ctx.Done():
		t.Fatalf("pipeline deadline while waiting for close: %v", ctx.Err())
	case _, ok := <-out:
		if ok {
			t.Fatalf("expected pipeline to close on retry failure")
		}
	}
	if atomic.LoadInt32(&attempts) != 1 {
		t.Fatalf("expected single attempt, have %d", attempts)
	}
}

func TestContextCancel(t *testing.T) {
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()

	b := sazanami.From(testkit.SourceOf(1, 2, 3, 4, 5))
	b = sazanami.AddStage(b, "", func(ctx context.Context, in <-chan int, out chan<- int) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				time.Sleep(5 * time.Millisecond)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v:
				}
			}
		}
	})
	out := b.Run(baseCtx)

	time.AfterFunc(15*time.Millisecond, baseCancel)

	waitCtx, waitCancel := testkit.Within(300 * time.Millisecond)
	defer waitCancel()

	for {
		select {
		case _, ok := <-out:
			if !ok {
				return
			}
		case <-waitCtx.Done():
			t.Fatalf("pipeline did not close after cancel: %v", waitCtx.Err())
		}
	}
}
