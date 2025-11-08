package main

import (
	"context"
	"fmt"
	"time"

	"github.com/UUGTech/sazanami"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

	pipeline := sazanami.AddStage(sazanami.From(src), "slow-worker", slowWorker,
		sazanami.WithTimeout(80*time.Millisecond),
		sazanami.WithErrorPolicy(sazanami.Drop()),
	)

	for v := range pipeline.Run(ctx) {
		fmt.Printf("completed item %d\n", v)
	}

	fmt.Println("pipeline finished (items exceeding the timeout were dropped)")
}

func slowWorker(ctx context.Context, in <-chan int, out chan<- int) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-in:
			if !ok {
				return nil
			}
			fmt.Printf("processing %d...\n", v)
			time.Sleep(120 * time.Millisecond)
			select {
			case <-ctx.Done():
				fmt.Printf("timeout while handling %d (%v)\n", v, ctx.Err())
				return ctx.Err()
			case out <- v:
			}
		}
	}
}
