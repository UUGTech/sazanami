package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/UUGTech/sazanami"
)

type logRecord struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

type entry struct {
	Level     string
	Message   string
	Timestamp time.Time
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src := make(chan string)
	go func() {
		defer close(src)
		lines := []string{
			`{"level":"info","message":"startup"}`,
			`{"level":"warn","message":"cache warm slow"}`,
			`{"level":"error","message":"disk pressure"}`,
			`{"level":"debug","message":"heartbeat"}`,
			`{"level":"warn","message":"throttle"}`,
		}
		for _, line := range lines {
			select {
			case <-ctx.Done():
				return
			case src <- line:
			}
		}
	}()

	var storeFailures int32 = 1

	events := sazanami.AddStage(sazanami.From(src), "parse", parseLogs,
		sazanami.WithTags("ingest", "json"),
		sazanami.WithParallel(2),
	)
	events = sazanami.AddStage(events, "filter", filterLevels("warn", "error"),
		sazanami.WithTags("filter"),
		sazanami.WithBuffer(4),
	)
	batchesBuilder := sazanami.AddStage(events, "batch", sazanami.Batch[entry](2, time.Second),
		sazanami.WithTags("batch"),
		sazanami.WithAttr("size", "2"),
		sazanami.WithBuffer(1),
	)
	pipeline := sazanami.AddStage(batchesBuilder, "store", storeBatches(&storeFailures),
		sazanami.WithTags("sink"),
		sazanami.WithErrorPolicy(sazanami.Retry(2, func(i int) time.Duration {
			if i < 0 {
				i = 0
			}
			return time.Duration(1<<i) * 200 * time.Millisecond
		})),
		sazanami.WithBuffer(1),
	)
	batches := pipeline.Run(ctx)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("pipeline stopped: %v\n", ctx.Err())
			return
		case batch, ok := <-batches:
			if !ok {
				fmt.Println("pipeline drained")
				return
			}
			fmt.Printf("stored batch (%d entries):\n", len(batch))
			for _, item := range batch {
				fmt.Printf("  [%s] %s\n", item.Level, item.Message)
			}
		}
	}
}

func parseLogs(ctx context.Context, in <-chan string, out chan<- entry) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case line, ok := <-in:
			if !ok {
				return nil
			}
			var raw logRecord
			if err := json.Unmarshal([]byte(line), &raw); err != nil {
				continue
			}
			evt := entry{Level: raw.Level, Message: raw.Message, Timestamp: time.Now()}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- evt:
			}
		}
	}
}

func filterLevels(levels ...string) sazanami.Handler[entry, entry] {
	allowed := make(map[string]struct{}, len(levels))
	for _, lvl := range levels {
		allowed[lvl] = struct{}{}
	}
	return func(ctx context.Context, in <-chan entry, out chan<- entry) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case e, ok := <-in:
				if !ok {
					return nil
				}
				if _, ok := allowed[e.Level]; !ok {
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- e:
				}
			}
		}
	}
}

func storeBatches(pending *int32) sazanami.Handler[[]entry, []entry] {
	return func(ctx context.Context, in <-chan []entry, out chan<- []entry) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case batch, ok := <-in:
				if !ok {
					return nil
				}
				attempt := 0
				for {
					if err := ctx.Err(); err != nil {
						return err
					}
					if atomic.CompareAndSwapInt32(pending, 1, 0) {
						delay := time.Duration(1<<attempt) * 200 * time.Millisecond
						if delay > 0 {
							timer := time.NewTimer(delay)
							select {
							case <-ctx.Done():
								if !timer.Stop() {
									<-timer.C
								}
								return ctx.Err()
							case <-timer.C:
							}
						}
						attempt++
						fmt.Printf("store retry %d for batch (%d entries)\n", attempt, len(batch))
						continue
					}
					time.Sleep(50 * time.Millisecond)
					fmt.Printf("persisted %d entries\n", len(batch))
					select {
					case <-ctx.Done():
						return ctx.Err()
					case out <- batch:
					}
					break
				}
			}
		}
	}
}
