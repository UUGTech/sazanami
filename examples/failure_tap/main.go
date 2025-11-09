package main

import (
	"context"
	"errors"
	"fmt"
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
		for i := 1; i <= 6; i++ {
			select {
			case <-ctx.Done():
				return
			case src <- i:
			}
		}
	}()

	var dropped atomic.Int32
	dropped.Store(0)

	pipeline := sazanami.AddStage(sazanami.From(src), "filter-odd",
		sazanami.Map(func(_ context.Context, v int) (int, error) {
			if v%2 == 0 {
				return 0, errors.New("even number")
			}
			return v, nil
		}),
		sazanami.WithErrorPolicy(
			sazanami.Chain(
				sazanami.CollectFailuresFuncAs(func(val int) {
					dropped.Add(int32(val))
				}),
				sazanami.Drop(),
			),
		),
	)

	for v := range pipeline.Run(ctx) {
		fmt.Printf("accepted: %d\n", v)
	}

	fmt.Printf("sum of dropped values: %d\n", dropped.Load())
}
