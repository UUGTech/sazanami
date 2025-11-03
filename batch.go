package sazanami

import (
	"context"
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
	return func(ctx context.Context, in <-chan T, out chan<- []T) error {
		buf := make([]T, 0, size)
		var timer *time.Timer
		var timerCh <-chan time.Time
		if dur > 0 {
			timer = time.NewTimer(dur)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		startTimer := func() {
			if timer == nil {
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(dur)
			timerCh = timer.C
		}
		stopTimer := func() {
			if timer == nil {
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerCh = nil
		}

		flush := func() error {
			if len(buf) == 0 {
				return nil
			}
			batch := make([]T, len(buf))
			copy(batch, buf)
			buf = buf[:0]
			stopTimer()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- batch:
				return nil
			}
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-in:
				if !ok {
					return flush()
				}
				buf = append(buf, v)
				if len(buf) == 1 {
					startTimer()
				}
				if len(buf) >= size {
					if err := flush(); err != nil {
						return err
					}
				}
			case <-timerCh:
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}
