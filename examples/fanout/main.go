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

	evenValues := sazanami.AddStage(branches["even"], "double", func(ctx context.Context, in <-chan int, out chan<- int) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v * 2:
				}
			}
		}
	})

	evenFormatted := sazanami.AddStage(evenValues, "format-even", func(ctx context.Context, in <-chan int, out chan<- string) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				msg := fmt.Sprintf("even branch -> %d", v)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- msg:
				}
			}
		}
	})

	oddValues := sazanami.AddStage(branches["odd"], "square", func(ctx context.Context, in <-chan int, out chan<- int) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v * v:
				}
			}
		}
	})

	oddFormatted := sazanami.AddStage(oddValues, "format-odd", func(ctx context.Context, in <-chan int, out chan<- string) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				msg := fmt.Sprintf("odd branch -> %d", v)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- msg:
				}
			}
		}
	})

	merged := sazanami.FanIn(ctx, evenFormatted.Run(ctx), oddFormatted.Run(ctx))

	for msg := range merged {
		fmt.Println(msg)
	}
}
