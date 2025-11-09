package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UUGTech/sazanami"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	src := make(chan int)
	go func() {
		defer close(src)
		for _, v := range []int{1, 2, 3, 4, 5, 6} {
			select {
			case <-ctx.Done():
				return
			case src <- v:
			}
		}
	}()

	unstable := &flakyStore{
		failures: map[int]int{
			2: 3,
			4: 1,
			5: 3,
		},
	}

	pipeline := sazanami.AddStage(sazanami.From(src), "unstable-store", unstable.handler(),
		sazanami.WithTags("sink"),
		sazanami.WithParallel(4),
		sazanami.WithErrorPolicy(sazanami.Chain(
			sazanami.Retry(2, sazanami.ConstantBackoff(50*time.Millisecond)),
			sazanami.CollectFailuresFunc(func(f sazanami.Failure) {
				fmt.Printf("failed to store %v: %v\n", f.Item, f.Err)
			}),
		)),
	)

	for v := range pipeline.Run(ctx) {
		fmt.Printf("stored: %d\n", v)
	}
}

type flakyStore struct {
	mu       sync.Mutex
	failures map[int]int
	attempts atomic.Int32
}

func (f *flakyStore) handler() sazanami.Handler[int, int] {
	return sazanami.Map(func(_ context.Context, v int) (int, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.failures[v] > 0 {
			f.failures[v]--
			f.attempts.Add(1)
			fmt.Printf("attempt %d failed for %d\n", f.attempts.Load(), v)
			return 0, errors.New("demonstration failure")
		}
		fmt.Printf("persisted %d\n", v)
		return v, nil
	})
}
