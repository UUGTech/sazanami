package sazanami

import (
	"context"
	"errors"
	"time"
)

// Failure describes a handler failure for a specific item.
type Failure struct {
	Stage    StageInfo
	Item     any
	Err      error
	Attempt  int
	ItemMeta ItemInfo
}

// Result indicates how a policy wants to handle a failure.
type Result struct {
	action actionKind
	delay  time.Duration
	target chan<- Failure
}

type actionKind int

const (
	actionPass actionKind = iota
	actionDrop
	actionRetry
	actionCollect
	actionDrain
	actionFail
)

// Policy decides how to react when a handler reports a failure.
type Policy interface {
	Decide(ctx context.Context, failure Failure) Result
}

type policyFunc func(context.Context, Failure) Result

func (p policyFunc) Decide(ctx context.Context, failure Failure) Result {
	return p(ctx, failure)
}

// Drop ignores the failing item and continues processing.
func Drop() Policy {
	return policyFunc(func(context.Context, Failure) Result {
		return Result{action: actionDrop}
	})
}

// Retry retries the failing item up to max attempts using backoff schedule.
func Retry(max int, backoff Backoff) Policy {
	if max < 0 {
		max = 0
	}
	if backoff == nil {
		backoff = func(int) time.Duration { return 0 }
	}
	return policyFunc(func(_ context.Context, failure Failure) Result {
		if failure.Attempt < max {
			return Result{action: actionRetry, delay: backoff(failure.Attempt)}
		}
		return Result{action: actionPass}
	})
}

// CollectFailures routes failed items to the provided channel while continuing.
func CollectFailures(ch chan<- Failure) Policy {
	return policyFunc(func(context.Context, Failure) Result {
		return Result{action: actionCollect, target: ch}
	})
}

// DrainFailures routes the failing item and all remaining input to the channel and stops the stage.
func DrainFailures(ch chan<- Failure) Policy {
	return policyFunc(func(context.Context, Failure) Result {
		return Result{action: actionDrain, target: ch}
	})
}

// Chain evaluates policies in order until one returns a definitive action.
func Chain(policies ...Policy) Policy {
	filtered := make([]Policy, 0, len(policies))
	for _, p := range policies {
		if p != nil {
			filtered = append(filtered, p)
		}
	}
	if len(filtered) == 0 {
		return Drop()
	}
	return policyFunc(func(ctx context.Context, failure Failure) Result {
		for _, p := range filtered {
			res := p.Decide(ctx, failure)
			if res.action == actionPass {
				continue
			}
			return res
		}
		return Result{action: actionDrop}
	})
}

// Backoff computes the delay before the next retry attempt.
type Backoff func(attempt int) time.Duration

// ConstantBackoff waits for a fixed duration between retries.
func ConstantBackoff(d time.Duration) Backoff {
	if d < 0 {
		d = 0
	}
	return func(int) time.Duration { return d }
}

// ExponentialBackoff doubles the delay each attempt starting from base.
func ExponentialBackoff(base time.Duration) Backoff {
	if base < 0 {
		base = 0
	}
	return func(attempt int) time.Duration {
		if attempt <= 0 {
			return base
		}
		return base << uint(attempt)
	}
}

var ErrDrained = errors.New("sazanami: item drained by policy")
