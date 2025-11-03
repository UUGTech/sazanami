package sazanami

import (
	"context"
	"fmt"
	"sync"

	"github.com/yourorg/sazanami/internal"
)

// Handler represents a stage function that consumes values from in and emits to out.
// The handler should return when the input channel is closed or the context is done.
type Handler[I any, O any] func(ctx context.Context, in <-chan I, out chan<- O) error

// Builder wires stages together in a fluent fashion.
type Builder[I any, O any] struct {
	source   <-chan I
	state    *pipelineState
	stageIdx int
}

type pipelineState struct {
	stages []*stageConfig
	hooks  Hooks
}

type stageConfig struct {
	name     string
	tags     []string
	attrs    map[string]string
	parallel int
	buffer   int
	policy   Policy
	runner   stageRunner
}

type stageRunner func(ctx context.Context, cancel context.CancelFunc, info StageInfo, hooks Hooks, in <-chan any) <-chan any

// StageOption mutates stage configuration during declarative assembly.
type StageOption func(*stageConfig)

// From bootstraps a builder from a source channel.
func From[I any](src <-chan I) *Builder[I, I] {
	return &Builder[I, I]{
		source:   src,
		state:    &pipelineState{},
		stageIdx: -1,
	}
}

// Stage appends an anonymous stage to the pipeline.
func Stage[I any, O any, N any](b *Builder[I, O], h Handler[O, N]) *Builder[I, N] {
	return stageNamed(b, "", h)
}

// StageNamed appends a named stage to the pipeline.
func StageNamed[I any, O any, N any](b *Builder[I, O], name string, h Handler[O, N]) *Builder[I, N] {
	return stageNamed(b, name, h)
}

// StageWith appends a stage and applies StageOptions to its configuration.
func StageWith[I any, O any, N any](b *Builder[I, O], name string, h Handler[O, N], opts ...StageOption) *Builder[I, N] {
	nb := StageNamed(b, name, h)
	if nb.stageIdx >= 0 && len(opts) > 0 {
		cfg := nb.state.stages[nb.stageIdx]
		for _, opt := range opts {
			if opt == nil {
				continue
			}
			opt(cfg)
		}
	}
	return nb
}

func stageNamed[I any, O any, N any](b *Builder[I, O], name string, h Handler[O, N]) *Builder[I, N] {
	if b.state == nil {
		b.state = &pipelineState{}
	}
	cfg := &stageConfig{
		name:     name,
		parallel: 1,
		buffer:   64,
		policy:   Drop(),
	}
	if cfg.name == "" {
		cfg.name = fmt.Sprintf("stage-%d", len(b.state.stages)+1)
	}
	cfg.runner = makeStageRunner[O, N](cfg, h)
	b.state.stages = append(b.state.stages, cfg)
	idx := len(b.state.stages) - 1
	return &Builder[I, N]{
		source:   b.source,
		state:    b.state,
		stageIdx: idx,
	}
}

// WithTags attaches tags to the most recent stage.
func (b *Builder[I, O]) WithTags(tags ...string) *Builder[I, O] {
	if b.stageIdx < 0 || len(tags) == 0 {
		return b
	}
	cfg := b.state.stages[b.stageIdx]
	cfg.tags = append(cfg.tags, tags...)
	return b
}

// WithAttr attaches a single attribute key/value pair to the most recent stage.
func (b *Builder[I, O]) WithAttr(k, v string) *Builder[I, O] {
	if b.stageIdx < 0 || k == "" {
		return b
	}
	cfg := b.state.stages[b.stageIdx]
	if cfg.attrs == nil {
		cfg.attrs = make(map[string]string)
	}
	cfg.attrs[k] = v
	return b
}

// Parallel configures worker concurrency for the most recent stage.
func (b *Builder[I, O]) Parallel(n int) *Builder[I, O] {
	if b.stageIdx < 0 {
		return b
	}
	if n < 1 {
		n = 1
	}
	cfg := b.state.stages[b.stageIdx]
	cfg.parallel = n
	return b
}

// Buffer configures the buffer size between this stage and the next.
func (b *Builder[I, O]) Buffer(size int) *Builder[I, O] {
	if b.stageIdx < 0 {
		return b
	}
	if size < 0 {
		size = 0
	}
	cfg := b.state.stages[b.stageIdx]
	cfg.buffer = size
	return b
}

// OnError sets the error policy for the most recent stage.
func (b *Builder[I, O]) OnError(p Policy) *Builder[I, O] {
	if b.stageIdx < 0 {
		return b
	}
	if p == nil {
		p = Drop()
	}
	cfg := b.state.stages[b.stageIdx]
	cfg.policy = p
	return b
}

// WithMetrics installs hooks that observe pipeline execution.
func (b *Builder[I, O]) WithMetrics(h Hooks) *Builder[I, O] {
	if b.state == nil {
		b.state = &pipelineState{}
	}
	b.state.hooks = h
	return b
}

// WithTags returns a StageOption that appends tags to the stage configuration.
func WithTags(tags ...string) StageOption {
	copied := append([]string(nil), tags...)
	return func(c *stageConfig) {
		if len(copied) == 0 {
			return
		}
		c.tags = append(c.tags, copied...)
	}
}

// WithAttr returns a StageOption that sets a single attribute key/value.
func WithAttr(k, v string) StageOption {
	return func(c *stageConfig) {
		if k == "" {
			return
		}
		if c.attrs == nil {
			c.attrs = make(map[string]string)
		}
		c.attrs[k] = v
	}
}

// WithParallel returns a StageOption that configures worker concurrency.
func WithParallel(n int) StageOption {
	return func(c *stageConfig) {
		if n < 1 {
			n = 1
		}
		c.parallel = n
	}
}

// WithBuffer returns a StageOption that configures the channel buffer size.
func WithBuffer(size int) StageOption {
	return func(c *stageConfig) {
		if size < 0 {
			size = 0
		}
		c.buffer = size
	}
}

// WithErrorPolicy returns a StageOption that sets the stage error policy.
func WithErrorPolicy(p Policy) StageOption {
	if p == nil {
		p = Drop()
	}
	return func(c *stageConfig) {
		c.policy = p
	}
}

// Run executes the pipeline and returns a channel streaming the terminal stage outputs.
func (b *Builder[I, O]) Run(ctx context.Context) <-chan O {
	if b.state == nil {
		b.state = &pipelineState{}
	}
	runCtx, cancel := context.WithCancel(ctx)

	var current <-chan any = internal.ToAny(runCtx, b.source)
	for idx, cfg := range b.state.stages {
		info := StageInfo{
			Index:      idx,
			Name:       cfg.name,
			Tags:       append([]string(nil), cfg.tags...),
			Attributes: cloneAttrs(cfg.attrs),
		}
		if cfg.runner == nil {
			panic("sazanami: stage runner missing")
		}
		current = cfg.runner(runCtx, cancel, info, b.state.hooks, current)
	}

	out := make(chan O)
	go func() {
		defer close(out)
		defer cancel()
		typed := internal.FromAny[O](runCtx, current, func(err error) {
			if b.state.hooks.StageError != nil {
				b.state.hooks.StageError(StageInfo{Name: "terminal"}, err)
			}
			cancel()
		})
		for {
			select {
			case <-runCtx.Done():
				return
			case v, ok := <-typed:
				if !ok {
					return
				}
				select {
				case <-runCtx.Done():
					return
				case out <- v:
				}
			}
		}
	}()

	return out
}

func cloneAttrs(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func makeStageRunner[In, Out any](cfg *stageConfig, h Handler[In, Out]) stageRunner {
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

		var wg sync.WaitGroup
		wg.Add(cfg.parallel)
		errCh := make(chan error, cfg.parallel)

		for i := 0; i < cfg.parallel; i++ {
			go func() {
				defer wg.Done()
				if err := h(workerCtx, typedIn, typedOut); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}()
		}

		go func() {
			wg.Wait()
			close(errCh)
			close(typedOut)
		}()

		go func() {
			for err := range errCh {
				if err == nil {
					continue
				}
				if hooks.StageError != nil {
					hooks.StageError(info, err)
				}
				policy := cfg.policy
				if policy == nil {
					policy = Drop()
				}
				decision := policy.Decide(workerCtx, info, ItemInfo{}, err)
				switch decision.Action {
				case DecisionRetry, DecisionFail:
					workerCancel()
					cancel()
				}
			}
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
