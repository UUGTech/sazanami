package sazanami

import (
	"context"
	"sync"
)

// Map wraps a simple transform function into a streaming handler.
// The provided fn is invoked for every item and its result is forwarded downstream.
func Map[I any, O any](fn func(context.Context, I) (O, error)) Handler[I, O] {
	if fn == nil {
		panic("sazanami: Map requires a non-nil function")
	}
	return func(ctx context.Context, in <-chan I, out chan<- O) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				next, err := fn(ctx, v)
				if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- next:
				}
			}
		}
	}
}

// Filter keeps only the items for which fn returns true.
func Filter[T any](fn func(context.Context, T) (bool, error)) Handler[T, T] {
	if fn == nil {
		panic("sazanami: Filter requires a non-nil function")
	}
	return func(ctx context.Context, in <-chan T, out chan<- T) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				keep, err := fn(ctx, v)
				if err != nil {
					return err
				}
				if !keep {
					continue
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v:
				}
			}
		}
	}
}

// Reduce accumulates items using fn and emits the final accumulator once the input closes.
func Reduce[I any, O any](seed func() O, fn func(context.Context, O, I) (O, error)) Handler[I, O] {
	if seed == nil {
		panic("sazanami: Reduce requires a non-nil seed function")
	}
	if fn == nil {
		panic("sazanami: Reduce requires a non-nil reducer")
	}
	var (
		mu          sync.Mutex
		acc         O
		initialized bool
	)
	reset := func() O {
		return seed()
	}
	return func(ctx context.Context, in <-chan I, out chan<- O) error {
		processed := false
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					if !processed {
						mu.Lock()
						if !initialized {
							acc = reset()
							initialized = true
						}
						result := acc
						acc = reset()
						mu.Unlock()
						select {
						case <-ctx.Done():
							return ctx.Err()
						case out <- result:
						}
					}
					return nil
				}
				mu.Lock()
				if !initialized {
					acc = reset()
					initialized = true
				}
				next, err := fn(ctx, acc, v)
				if err != nil {
					mu.Unlock()
					return err
				}
				acc = next
				mu.Unlock()
				processed = true
			}
		}
	}
}

// ForEach runs fn for every item and forwards the original item downstream.
func ForEach[T any](fn func(context.Context, T) error) Handler[T, T] {
	if fn == nil {
		panic("sazanami: ForEach requires a non-nil function")
	}
	return func(ctx context.Context, in <-chan T, out chan<- T) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return nil
				}
				if err := fn(ctx, v); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- v:
				}
			}
		}
	}
}
