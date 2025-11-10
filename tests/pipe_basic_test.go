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
	if atomic.LoadInt32(&attempts) != 2 {
		t.Fatalf("expected 2 attempts (initial + retry), have %d", attempts)
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

func TestDropPolicy(t *testing.T) {
	ctx, cancel := testkit.Within(500 * time.Millisecond)
	defer cancel()

	handler := func(ctx context.Context, in <-chan int, out chan<- int) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				if v == 2 {
					return errors.New("boom")
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v:
				}
			}
		}
	}

	pipeline := sazanami.AddStage(sazanami.From(testkit.SourceOf(1, 2, 3)), "dropper", handler,
		sazanami.WithErrorPolicy(sazanami.Drop()),
	)

	got := testkit.Collect(t, ctx, pipeline.Run(ctx))
	want := []int{1, 3}
	if len(got) != len(want) {
		t.Fatalf("unexpected length: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected element at %d: got %d want %d", i, got[i], want[i])
		}
	}
}

func TestCollectFailuresFunc(t *testing.T) {
	ctx, cancel := testkit.Within(500 * time.Millisecond)
	defer cancel()

	var collected []int
	handler := func(ctx context.Context, in <-chan int, out chan<- int) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				if v%2 == 0 {
					return errors.New("even drop")
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v:
				}
			}
		}
	}

	pipeline := sazanami.AddStage(sazanami.From(testkit.SourceOf(1, 2, 3, 4, 5)), "collect", handler,
		sazanami.WithErrorPolicy(
			sazanami.Chain(
				sazanami.CollectFailuresFuncAs(func(val int) {
					collected = append(collected, val)
				}),
				sazanami.Drop(),
			),
		),
	)

	got := testkit.Collect(t, ctx, pipeline.Run(ctx))
	if len(got) != 3 {
		t.Fatalf("unexpected output: %v", got)
	}
	if len(collected) != 2 || collected[0] != 2 || collected[1] != 4 {
		t.Fatalf("unexpected collected failures: %v", collected)
	}
}

func TestDrainFailuresFunc(t *testing.T) {
	ctx, cancel := testkit.Within(500 * time.Millisecond)
	defer cancel()

	var drained []int
	handler := func(ctx context.Context, in <-chan int, out chan<- int) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				if v == 3 {
					return errors.New("drain rest")
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v:
				}
			}
		}
	}

	pipeline := sazanami.AddStage(sazanami.From(testkit.SourceOf(1, 2, 3, 4, 5)), "drain", handler,
		sazanami.WithErrorPolicy(
			sazanami.DrainFailuresFuncAs(func(v int) {
				drained = append(drained, v)
			}),
		),
	)

	got := testkit.Collect(t, ctx, pipeline.Run(ctx))
	if len(got) != 2 {
		t.Fatalf("unexpected output: %v", got)
	}
	if len(drained) == 0 || drained[0] != 3 {
		t.Fatalf("expected drained items, got %v", drained)
	}
}

func TestStageRecorder(t *testing.T) {
	ctx, cancel := testkit.Within(500 * time.Millisecond)
	defer cancel()

	recorder := testkit.NewStageRecorder()

	pipeline := sazanami.AddStage(sazanami.From(testkit.SourceOf(1, 2)), "double",
		func(ctx context.Context, in <-chan int, out chan<- int) error {
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
		},
	)
	pipeline = pipeline.WithHooks(recorder.Hooks())

	_ = testkit.Collect(t, ctx, pipeline.Run(ctx))

	snapshot := recorder.Snapshot()
	if len(snapshot) == 0 || snapshot[0].Name != "double" {
		t.Fatalf("expected recorded stage, got %v", snapshot)
	}
}

func TestStreamingStage(t *testing.T) {
	ctx, cancel := testkit.Within(500 * time.Millisecond)
	defer cancel()

	streamStage := func(ctx context.Context, in <-chan int, out chan<- int) error {
		var waiting bool
		var prev int
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					if waiting {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case out <- prev:
						}
					}
					return nil
				}
				if !waiting {
					waiting = true
					prev = v
					continue
				}
				sum := prev + v
				waiting = false
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- sum:
				}
			}
		}
	}

	pipeline := sazanami.AddStage(sazanami.From(testkit.SourceOf(1, 2, 3, 4, 5)), "pair-sum", streamStage,
		sazanami.WithStreaming(),
	)

	got := testkit.Collect(t, ctx, pipeline.Run(ctx))
	want := []int{3, 7, 5}

	if len(got) != len(want) {
		t.Fatalf("unexpected output length: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected output at %d: got %d want %d", i, got[i], want[i])
		}
	}
}
