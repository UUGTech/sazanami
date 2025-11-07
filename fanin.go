package sazanami

import (
	"context"
	"sync"
)

// FanIn merges multiple channels into a single output channel.
func FanIn[T any](ctx context.Context, inputs ...<-chan T) <-chan T {
	out := make(chan T)
	if len(inputs) == 0 {
		close(out)
		return out
	}

	var wg sync.WaitGroup
	wg.Add(len(inputs))

	forward := func(ch <-chan T) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-ch:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- v:
				}
			}
		}
	}

	for _, ch := range inputs {
		go forward(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
