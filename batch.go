package sazanami

import (
	"context"
	"sync"
	"time"
)

// Batch groups items into slices of up to size or until dur elapses, whichever comes first.
func Batch[T any](size int, dur time.Duration) Handler[T, []T] {
	if size <= 0 {
		size = 1
	}
	if dur < 0 {
		dur = 0
	}

	var (
		mu     sync.Mutex
		buf    = make([]T, 0, size)
		timer  *time.Timer
		ctxRef context.Context
		outRef chan<- []T
	)

	emit := func(batch []T) error {
		if len(batch) == 0 || outRef == nil || ctxRef == nil {
			return nil
		}
		select {
		case <-ctxRef.Done():
			return ctxRef.Err()
		case outRef <- batch:
			return nil
		}
	}

	stopTimerLocked := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
	}

	flushLocked := func() []T {
		if len(buf) == 0 {
			return nil
		}
		batch := make([]T, len(buf))
		copy(batch, buf)
		buf = buf[:0]
		stopTimerLocked()
		return batch
	}

	scheduleTimerLocked := func() {
		if dur <= 0 {
			return
		}
		stopTimerLocked()
		timer = time.AfterFunc(dur, func() {
			mu.Lock()
			batch := flushLocked()
			mu.Unlock()
			_ = emit(batch)
		})
	}

	return func(ctx context.Context, in <-chan T, out chan<- []T) error {
		ctxRef = ctx
		outRef = out

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					mu.Lock()
					batch := flushLocked()
					mu.Unlock()
					return emit(batch)
				}

				mu.Lock()
				buf = append(buf, v)
				if len(buf) == 1 {
					scheduleTimerLocked()
				}
				if len(buf) >= size {
					batch := flushLocked()
					mu.Unlock()
					if err := emit(batch); err != nil {
						return err
					}
					continue
				}
				mu.Unlock()
				return nil
			}
		}
	}
}
