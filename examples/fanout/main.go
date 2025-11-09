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
		for _, v := range []int{1, 2, 3, 4, 5, 6} {
			select {
			case <-ctx.Done():
				return
			case src <- v:
			}
		}
	}()

	branches := sazanami.FanOutBy(src, func(v int) string {
		if v%2 == 0 {
			return "even"
		}
		return "odd"
	}, "even", "odd")

	evenValues := sazanami.AddStage(branches["even"], "double",
		sazanami.Map(func(_ context.Context, v int) (int, error) {
			return v * 2, nil
		}),
	)

	evenFormatted := sazanami.AddStage(evenValues, "format-even",
		sazanami.Map(func(_ context.Context, v int) (string, error) {
			return fmt.Sprintf("even branch -> %d", v), nil
		}),
	)

	oddValues := sazanami.AddStage(branches["odd"], "square",
		sazanami.Map(func(_ context.Context, v int) (int, error) {
			return v * v, nil
		}),
	)

	oddFormatted := sazanami.AddStage(oddValues, "format-odd",
		sazanami.Map(func(_ context.Context, v int) (string, error) {
			return fmt.Sprintf("odd branch -> %d", v), nil
		}),
	)

	merged := sazanami.FanIn(ctx, evenFormatted.Run(ctx), oddFormatted.Run(ctx))

	for msg := range merged {
		fmt.Println(msg)
	}
}
