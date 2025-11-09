package main

import (
	"context"
	"fmt"
	"time"

	"github.com/UUGTech/sazanami"
)

type stats struct {
	Count int
	Sum   int
}

func (s stats) Avg() float64 {
	if s.Count == 0 {
		return 0
	}
	return float64(s.Sum) / float64(s.Count)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	src := make(chan int, 5)
	go func() {
		defer close(src)
		for _, v := range []int{3, 5, 7, 9, 11} {
			src <- v
		}
	}()

	pipeline := sazanami.From(src)
	pipeline = sazanami.AddStage(pipeline, "double",
		sazanami.Map(func(_ context.Context, v int) (int, error) {
			return v * 2, nil
		}),
		sazanami.WithParallel(3),
	)

	statsPipeline := sazanami.AddStage(pipeline, "stats",
		sazanami.Reduce(
			func() stats { return stats{} },
			func(_ context.Context, acc stats, v int) (stats, error) {
				acc.Count++
				acc.Sum += v
				return acc, nil
			},
		),
	)

	select {
	case <-ctx.Done():
		fmt.Printf("pipeline stopped: %v\n", ctx.Err())
	case result, ok := <-statsPipeline.Run(ctx):
		if !ok {
			fmt.Println("stats not produced")
			return
		}
		fmt.Printf("processed %d items, sum=%d, avg=%.1f\n", result.Count, result.Sum, result.Avg())
	}
}
