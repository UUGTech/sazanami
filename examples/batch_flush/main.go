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

	events := make(chan string)
	go func() {
		defer close(events)
		inputs := []struct {
			value string
			delay time.Duration
		}{
			{"alpha", 0},
			{"beta", 50 * time.Millisecond},
			{"gamma", 50 * time.Millisecond},
			{"delta", 10 * time.Millisecond},
			{"epsilon", 400 * time.Millisecond}, // timer-driven flush will emit before handling this item
		}
		for _, item := range inputs {
			select {
			case <-ctx.Done():
				return
			case <-time.After(item.delay):
			}
			select {
			case <-ctx.Done():
				return
			case events <- item.value:
			}
		}
	}()

	pipeline := sazanami.AddStage(sazanami.From(events), "batch", sazanami.Batch[string](3, 200*time.Millisecond),
		sazanami.WithTags("batch"),
		sazanami.WithAttr("size", "3"),
		sazanami.WithBuffer(1),
	)
	pipeline = sazanami.AddStage(pipeline, "sink", sazanami.ForEach(func(_ context.Context, batch []string) error {
		fmt.Printf("batch ready: %v\n", batch)
		return nil
	}),
		sazanami.WithTags("sink"),
	)

	for batch := range pipeline.Run(ctx) {
		fmt.Printf("drained batch with %d items\n", len(batch))
	}
}
