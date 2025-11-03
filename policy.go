package sazanami

import (
	"context"
	"time"
)

// DecisionAction communicates how the pipeline should react to a handler error.
type DecisionAction int

const (
	DecisionPass DecisionAction = iota
	DecisionContinue
	DecisionRetry
	DecisionFail
)

// Decision is the result of evaluating a policy.
type Decision struct {
	Action DecisionAction
	Delay  time.Duration
}

// Policy decides how to respond to handler errors for a given stage item.
type Policy interface {
	Decide(ctx context.Context, stage StageInfo, item ItemInfo, err error) Decision
}

// Backoff computes the delay before the next retry attempt.
type Backoff func(attempt int) time.Duration

type dropPolicy struct{}

// Drop ignores errors and advances to the next item.
func Drop() Policy {
	return dropPolicy{}
}

func (dropPolicy) Decide(context.Context, StageInfo, ItemInfo, error) Decision {
	return Decision{Action: DecisionContinue}
}

type retryPolicy struct {
	max     int
	backoff Backoff
}

// Retry retries a failing handler up to max times using the provided backoff.
func Retry(max int, backoff Backoff) Policy {
	if max < 0 {
		max = 0
	}
	if backoff == nil {
		backoff = func(int) time.Duration { return 0 }
	}
	return retryPolicy{max: max, backoff: backoff}
}

func (p retryPolicy) Decide(_ context.Context, _ StageInfo, item ItemInfo, _ error) Decision {
	if item.Attempt < p.max {
		return Decision{Action: DecisionRetry, Delay: p.backoff(item.Attempt)}
	}
	return Decision{Action: DecisionFail}
}

type chainPolicy []Policy

// Chain evaluates policies in order until one returns a non-pass decision.
func Chain(policies ...Policy) Policy {
	filtered := chainPolicy{}
	for _, p := range policies {
		if p != nil {
			filtered = append(filtered, p)
		}
	}
	if len(filtered) == 0 {
		return Drop()
	}
	return filtered
}

func (c chainPolicy) Decide(ctx context.Context, stage StageInfo, item ItemInfo, err error) Decision {
	for _, p := range c {
		d := p.Decide(ctx, stage, item, err)
		if d.Action == DecisionPass {
			continue
		}
		return d
	}
	return Decision{Action: DecisionContinue}
}

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
