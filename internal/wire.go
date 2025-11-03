package internal

import (
	"context"
	"fmt"
)

// ToAny forwards typed values onto an any channel while respecting context cancellation.
func ToAny[T any](ctx context.Context, src <-chan T) <-chan any {
	out := make(chan any)
	if src == nil {
		close(out)
		return out
	}
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-src:
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
	}()
	return out
}

// FromAny converts an any channel into a typed channel, invoking onErr on mismatches.
func FromAny[T any](ctx context.Context, src <-chan any, onErr func(error)) <-chan T {
	out := make(chan T)
	if src == nil {
		close(out)
		return out
	}
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-src:
				if !ok {
					return
				}
				val, ok := v.(T)
				if !ok {
					if onErr != nil {
						onErr(fmt.Errorf("sazanami: type mismatch - have %T", v))
					}
					continue
				}
				select {
				case <-ctx.Done():
					return
				case out <- val:
				}
			}
		}
	}()
	return out
}
