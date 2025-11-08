package testkit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/UUGTech/sazanami"
)

// SourceOf emits the provided values then closes the channel.
func SourceOf[T any](xs ...T) <-chan T {
	ch := make(chan T, len(xs))
	go func() {
		defer close(ch)
		for _, v := range xs {
			ch <- v
		}
	}()
	return ch
}

// Collect drains the channel or fails the test if the context expires.
func Collect[T any](t *testing.T, ctx context.Context, ch <-chan T) []T {
	t.Helper()
	var out []T
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("collect timeout: %v", ctx.Err())
		case v, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, v)
		}
	}
}

// Within returns a context with timeout for coordinating test execution.
func Within(d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = time.Second
	}
	return context.WithTimeout(context.Background(), d)
}

// StageRecorder records StageInfo events emitted by Hooks for inspection in tests.
type StageRecorder struct {
	mu      sync.Mutex
	stages  []sazanami.StageInfo
	started []time.Time
}

// NewStageRecorder constructs a StageRecorder.
func NewStageRecorder() *StageRecorder {
	return &StageRecorder{}
}

// Hooks returns a Hooks struct that records stage lifecycle events.
func (r *StageRecorder) Hooks() sazanami.Hooks {
	return sazanami.Hooks{
		StageStart: func(info sazanami.StageInfo) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.stages = append(r.stages, info)
			r.started = append(r.started, time.Now())
		},
		StageComplete: func(info sazanami.StageInfo) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.stages = append(r.stages, info)
		},
	}
}

// Snapshot returns a copy of the recorded StageInfo sequence.
func (r *StageRecorder) Snapshot() []sazanami.StageInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]sazanami.StageInfo, len(r.stages))
	copy(cp, r.stages)
	return cp
}
