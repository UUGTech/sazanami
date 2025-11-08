package main

import (
	"context"
	"fmt"
	"time"

	"github.com/UUGTech/sazanami"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	src := make(chan int)
	go func() {
		defer close(src)
		for i := 1; i <= 5; i++ {
			select {
			case <-ctx.Done():
				return
			case src <- i:
			}
		}
	}()

	pipeline := sazanami.AddStage(sazanami.From(src), "slow-worker", sazanami.Map(func(ctx context.Context, v int) (int, error) {
		fmt.Printf("processing %d...\n", v)
		time.Sleep(300 * time.Millisecond * time.Duration(v))
		return v, nil
	}),
		sazanami.WithTimeout(1000*time.Millisecond),
		sazanami.WithErrorPolicy(sazanami.Chain(
			sazanami.CollectFailuresFuncAs(func(v int) {
				fmt.Printf("dropped item %d due to timeout\n", v)
			}),
			sazanami.Drop(),
		)),
	)

	for v := range pipeline.Run(ctx) {
		fmt.Printf("completed item %d\n", v)
	}
}
