package sazanami

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UUGTech/sazanami/internal"
)

func makeStageRunner[In, Out any](cfg *stageConfig, h Handler[In, Out]) stageRunner {
	if cfg.stream {
		return makeStreamingStageRunner(cfg, h)
	}
	return func(ctx context.Context, cancel context.CancelFunc, info StageInfo, hooks Hooks, upstream <-chan any) <-chan any {
		workerCtx, workerCancel := context.WithCancel(ctx)

		typedIn := internal.FromAny[In](workerCtx, upstream, func(err error) {
			if hooks.StageError != nil {
				hooks.StageError(info, err)
			}
			workerCancel()
			cancel()
		})

		typedOut := make(chan Out, cfg.buffer)
		outAny := make(chan any, cfg.buffer)

		if hooks.StageStart != nil {
			hooks.StageStart(info)
		}

		policy := cfg.policy
		if policy == nil {
			policy = Drop()
		}

		var wg sync.WaitGroup
		var sequence uint64
		flushOnce := sync.Once{}

		wg.Add(cfg.parallel)
		for i := 0; i < cfg.parallel; i++ {
			workerIndex := i
			go func() {
				defer wg.Done()
				runWorker(workerCtx, cancel, workerCancel, info, hooks, typedIn, typedOut, h, policy, &flushOnce, &sequence, workerIndex, cfg.timeout)
			}()
		}

		go func() {
			wg.Wait()
			close(typedOut)
		}()

		go func() {
			defer close(outAny)
			defer func() {
				if hooks.StageComplete != nil {
					hooks.StageComplete(info)
				}
			}()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-typedOut:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case outAny <- v:
					}
				}
			}
		}()

		return outAny
	}
}

func makeStreamingStageRunner[In, Out any](cfg *stageConfig, h Handler[In, Out]) stageRunner {
	if cfg.parallel > 1 {
		panic("sazanami: streaming stages do not support Parallel > 1")
	}
	return func(ctx context.Context, cancel context.CancelFunc, info StageInfo, hooks Hooks, upstream <-chan any) <-chan any {
		workerCtx, workerCancel := context.WithCancel(ctx)

		typedIn := internal.FromAny[In](workerCtx, upstream, func(err error) {
			if hooks.StageError != nil {
				hooks.StageError(info, err)
			}
			workerCancel()
			cancel()
		})

		typedOut := make(chan Out, cfg.buffer)
		outAny := make(chan any, cfg.buffer)

		if hooks.StageStart != nil {
			hooks.StageStart(info)
		}

		policy := cfg.policy
		if policy == nil {
			policy = Drop()
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			runStreamingWorker(workerCtx, cancel, workerCancel, info, hooks, typedIn, typedOut, h, policy, cfg.timeout)
		}()

		go func() {
			wg.Wait()
			close(typedOut)
		}()

		go func() {
			defer close(outAny)
			defer func() {
				if hooks.StageComplete != nil {
					hooks.StageComplete(info)
				}
			}()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-typedOut:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case outAny <- v:
					}
				}
			}
		}()

		return outAny
	}
}

func runWorker[In, Out any](ctx context.Context, cancel context.CancelFunc, workerCancel context.CancelFunc, info StageInfo, hooks Hooks, typedIn <-chan In, typedOut chan<- Out, h Handler[In, Out], policy Policy, flushOnce *sync.Once, sequence *uint64, worker int, timeout time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-typedIn:
			if !ok {
				flushOnce.Do(func() {
					if err := invokeHandler(ctx, timeout, h, typedOut, nil); err != nil {
						if hooks.StageError != nil {
							hooks.StageError(info, err)
						}
						cancel()
						workerCancel()
					}
				})
				return
			}

			seq := atomic.AddUint64(sequence, 1) - 1
			attempt := 0
			current := item

		attemptLoop:
			for {
				itemInfo := ItemInfo{Sequence: seq, Worker: worker, Attempt: attempt}
				if hooks.ItemStart != nil {
					hooks.ItemStart(info, itemInfo)
				}

				err := invokeHandler(ctx, timeout, h, typedOut, &current)
				if err == nil {
					if hooks.ItemComplete != nil {
						hooks.ItemComplete(info, itemInfo)
					}
					break
				}

				if hooks.ItemError != nil {
					hooks.ItemError(info, itemInfo, err)
				}

				sf := Failure{
					Stage:    info,
					Item:     current,
					Err:      err,
					Attempt:  attempt,
					ItemMeta: itemInfo,
				}

				if isFatalError(err) {
					if hooks.StageError != nil {
						hooks.StageError(info, err)
					}
					cancel()
					workerCancel()
					return
				}

				result := policy.Decide(ctx, sf)
				switch result.action {
				case actionPass:
					fallthrough
				case actionDrop:
					break attemptLoop
				case actionRetry:
					if result.delay > 0 {
						timer := time.NewTimer(result.delay)
						select {
						case <-ctx.Done():
							timer.Stop()
							return
						case <-timer.C:
						}
					}
					attempt++
					continue attemptLoop
				case actionCollect:
					if result.target != nil {
						sfCopy := sf
						select {
						case <-ctx.Done():
							return
						case result.target <- sfCopy:
						}
					}
					break attemptLoop
				case actionDrain:
					if result.target != nil {
						sfCopy := sf
						select {
						case <-ctx.Done():
							return
						case result.target <- sfCopy:
						}
					}
					drainRemaining(ctx, typedIn, result.target, info)
					flushOnce.Do(func() {
						if err := invokeHandler(ctx, timeout, h, typedOut, nil); err != nil {
							if hooks.StageError != nil {
								hooks.StageError(info, err)
							}
							cancel()
							workerCancel()
						}
					})
					return
				case actionFail:
					if hooks.StageError != nil {
						hooks.StageError(info, err)
					}
					cancel()
					workerCancel()
					return
				}

				break attemptLoop
			}
		}
	}
}

func runStreamingWorker[In, Out any](ctx context.Context, cancel context.CancelFunc, workerCancel context.CancelFunc, info StageInfo, hooks Hooks, typedIn <-chan In, typedOut chan<- Out, h Handler[In, Out], policy Policy, timeout time.Duration) {
	for {
		if ctx.Err() != nil {
			return
		}
		err := invokeStreamingHandler(ctx, timeout, h, typedIn, typedOut)
		if err == nil {
			return
		}
		if isFatalError(err) {
			if hooks.StageError != nil {
				hooks.StageError(info, err)
			}
			cancel()
			workerCancel()
			return
		}
		if hooks.StageError != nil {
			hooks.StageError(info, err)
		}
		failure := Failure{Stage: info, Err: err}
		res := policy.Decide(ctx, failure)
		switch res.action {
		case actionRetry:
			if res.delay > 0 {
				timer := time.NewTimer(res.delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			continue
		case actionCollect:
			if res.target != nil {
				select {
				case <-ctx.Done():
					return
				case res.target <- failure:
				}
			}
			continue
		case actionDrain:
			drainRemaining(ctx, typedIn, res.target, info)
			cancel()
			workerCancel()
			return
		case actionFail:
			cancel()
			workerCancel()
			return
		case actionPass, actionDrop:
			continue
		default:
			return
		}
	}
}

func invokeHandler[In, Out any](ctx context.Context, timeout time.Duration, h Handler[In, Out], out chan<- Out, value *In) error {
	callCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	itemIn := make(chan In, 1)
	if value != nil {
		itemIn <- *value
	}
	close(itemIn)
	return h(callCtx, itemIn, out)
}

func invokeStreamingHandler[In, Out any](ctx context.Context, timeout time.Duration, h Handler[In, Out], in <-chan In, out chan<- Out) error {
	callCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return h(callCtx, in, out)
}

func drainRemaining[In any](ctx context.Context, typedIn <-chan In, target chan<- Failure, stage StageInfo) {
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-typedIn:
			if !ok {
				return
			}
			if target != nil {
				failure := Failure{Stage: stage, Item: v, Err: ErrDrained}
				select {
				case <-ctx.Done():
					return
				case target <- failure:
				}
			}
		}
	}
}
