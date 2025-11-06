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

	evenBuilder := sazanami.AddStage(branches["even"], "double", func(ctx context.Context, in <-chan int, out chan<- int) error {
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

	oddBuilder := sazanami.AddStage(branches["odd"], "square", func(ctx context.Context, in <-chan int, out chan<- int) error {
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

	evenOut := evenBuilder.Run(ctx)
	oddOut := oddBuilder.Run(ctx)

	for evenOut != nil || oddOut != nil {
		select {
		case v, ok := <-evenOut:
			if !ok {
				evenOut = nil
				continue
			}
			fmt.Printf("even branch -> %d\n", v)
		case v, ok := <-oddOut:
			if !ok {
				oddOut = nil
				continue
			}
			fmt.Printf("odd branch -> %d\n", v)
		}
	}
}
