package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/UUGTech/sazanami"
	"github.com/UUGTech/sazanami/testkit"
)

func TestBuiltinMapFilterForEach(t *testing.T) {
	ctx, cancel := testkit.Within(500 * time.Millisecond)
	defer cancel()

	var (
		seenMu sync.Mutex
		seen   []int
	)

	pipeline := sazanami.From(testkit.SourceOf(1, 2, 3, 4, 5))
	pipeline = sazanami.AddStage(pipeline, "map", sazanami.Map(func(_ context.Context, v int) (int, error) {
		return v * 3, nil
	}))
	pipeline = sazanami.AddStage(pipeline, "filter", sazanami.Filter(func(_ context.Context, v int) (bool, error) {
		return v%2 == 0, nil
	}))
	pipeline = sazanami.AddStage(pipeline, "tap", sazanami.ForEach(func(_ context.Context, v int) error {
		seenMu.Lock()
		seen = append(seen, v)
		seenMu.Unlock()
		return nil
	}))

	got := testkit.Collect(t, ctx, pipeline.Run(ctx))
	want := []int{6, 12}
	if len(got) != len(want) {
		t.Fatalf("unexpected output length: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected value at %d: got %d want %d", i, got[i], want[i])
		}
	}

	seenMu.Lock()
	defer seenMu.Unlock()
	if len(seen) != len(want) {
		t.Fatalf("tap saw %d items, want %d", len(seen), len(want))
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("tap mismatch at %d: got %d want %d", i, seen[i], want[i])
		}
	}
}

func TestBuiltinReduce(t *testing.T) {
	ctx, cancel := testkit.Within(500 * time.Millisecond)
	defer cancel()

	pipeline := sazanami.AddStage(
		sazanami.From(testkit.SourceOf(1, 2, 3, 4)),
		"sum",
		sazanami.Reduce(
			func() int { return 0 },
			func(_ context.Context, acc, v int) (int, error) {
				return acc + v, nil
			},
		),
	)

	got := testkit.Collect(t, ctx, pipeline.Run(ctx))
	if len(got) != 1 || got[0] != 10 {
		t.Fatalf("expected single reduce result 10, got %v", got)
	}
}
