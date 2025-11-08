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

	pipeline := sazanami.AddStage(sazanami.From(src), "filter-odd", func(ctx context.Context, in <-chan int, out chan<- int) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				if v%2 == 0 {
					return errors.New("even number")
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v:
				}
			}
		}
	},
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
