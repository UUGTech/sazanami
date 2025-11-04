package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/UUGTech/sazanami"
)

type logRecord struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

type entry struct {
	Level   string
	Message string
	When    time.Time
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	src := make(chan string)
	go func() {
		defer close(src)
		lines := []string{
			`{"level":"info","message":"start"}`,
			`{"level":"warn","message":"slow path"}`,
			`{"level":"error","message":"disk"}`,
		}
		for _, line := range lines {
			select {
			case <-ctx.Done():
				return
			case src <- line:
			}
		}
	}()

	hooks := sazanami.Hooks{
		StageStart: func(info sazanami.StageInfo) {
			fmt.Printf("stage start: %s tags=%v\n", info.Name, info.Tags)
		},
		StageComplete: func(info sazanami.StageInfo) {
			fmt.Printf("stage complete: %s\n", info.Name)
		},
		StageError: func(info sazanami.StageInfo, err error) {
			fmt.Printf("stage error: %s: %v\n", info.Name, err)
		},
	}

	pipeline := sazanami.AddStage(sazanami.From(src), "parse", parseLogs,
		sazanami.WithTags("ingest"),
		sazanami.WithParallel(2),
	)
	pipeline = sazanami.AddStage(pipeline, "filter", filterLevels("warn", "error"),
		sazanami.WithTags("filter"),
	)
	pipeline = sazanami.AddStage(pipeline, "store", storeEntries,
		sazanami.WithTags("sink"),
	)
	pipeline = pipeline.WithHooks(hooks)

	for entry := range pipeline.Run(ctx) {
		fmt.Printf("output: [%s] %s\n", entry.Level, entry.Message)
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
				return err
			}
			e := entry{Level: raw.Level, Message: raw.Message, When: time.Now()}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- e:
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

func storeEntries(ctx context.Context, in <-chan entry, out chan<- entry) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e, ok := <-in:
			if !ok {
				return nil
			}
			fmt.Printf("store: [%s] %s\n", e.Level, e.Message)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- e:
			}
		}
	}
}
