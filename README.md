# Sazanami 🌊

[![Go Reference](https://pkg.go.dev/badge/github.com/UUGTech/sazanami.svg)](https://pkg.go.dev/github.com/UUGTech/sazanami)
[![Go Report Card](https://goreportcard.com/badge/github.com/UUGTech/sazanami)](https://goreportcard.com/report/github.com/UUGTech/sazanami)
![CI](https://github.com/UUGTech/sazanami/actions/workflows/ci.yml/badge.svg)

## Overview
Sazanami (“ripples” in Japanese) is a minimal pipeline library for Go. It lets you compose concurrent stages with type-safe handlers, buffering, and error policies—ideal for ETL flows, streaming jobs, or any workload that benefits from backpressure-aware pipelines. The design favors explicit, readable code: generics for safety, zero external dependencies, and a fluent builder that feels at home in Go.

> ⚠️ Status: Early-stage (v0.1.x)
> The core pipeline features are stable, but APIs may change before v1.0.0.
> Feedback and contributions are welcome!


## Installation
```bash
go get github.com/UUGTech/sazanami
```

## Quickstart
```go
src := make(chan int, 4)
go func() {
	defer close(src)
	for _, v := range []int{1, 2, 3, 4} {
		src <- v
	}
}()

ctx := context.Background()

p := sazanami.From(src)
p = sazanami.AddStage(p, "double",
	sazanami.Map(func(_ context.Context, v int) (int, error) {
		return v * 2, nil
	}),
)
p = sazanami.AddStage(p, "filter-multiples",
	sazanami.Filter(func(_ context.Context, v int) (bool, error) {
		return v%4 == 0, nil
	}),
)

for v := range p.Run(ctx) {
	fmt.Println(v) // 4, 8
}
```

## Features
- Fluent builder with typed handlers (`From`, `AddStage`, `Run`)
- Concurrency controls: per-stage `Parallel` and `Buffer`
- Error handling via `Drop`, `Retry`, `Collect/Drain` (channel or handler variants)
- Hooks for stage/item lifecycle metrics; `testkit.StageRecorder` for assertions
- Built-in stage helpers: `Map`, `Filter`, `Reduce`, `ForEach`, `Batch`, plus lightweight `testkit`
- Fan-out / Fan-in helpers (`FanOutBy`, `FanIn`)
- Zero external dependencies; standard library only

## Built-in Handlers
Sazanami ships with adapters for the most common stage shapes so you rarely touch raw channels:

- `Map(func(ctx, in) (out, error))` – transform each item (returning an error drops into the policy path)
- `Filter(func(ctx, in) (keep bool, error))` – keep a subset without manual `continue`
- `Reduce(seed, func(ctx, acc, in) (acc, error))` – accumulate until upstream closes
- `ForEach(func(ctx, in) error)` – side effects while forwarding the original item
- `Batch(size, dur)` – emit slices when capacity or deadline hits first

Each helper returns a standard `Handler`, so you can mix them with custom stages freely.

Need a long-lived stage that ranges over the channel? Mark it with `sazanami.WithStreaming()` when you add the stage—the handler will receive the real input/output streams (parallelism is forced to 1, and retries operate at the stage level).

## Example: ETL Pipeline
```go
ctx := context.Background()
var storeFailures int32 = 1

p := sazanami.From(lines)

p = sazanami.AddStage(p, "parse",  parseLogs,
    sazanami.WithTags("ingest","json"),
    sazanami.WithParallel(2),
)

p = sazanami.AddStage(p, "filter",
    sazanami.Filter(func(_ context.Context, e entry) (bool, error) {
        return e.Level == "warn" || e.Level == "error", nil
    }),
    sazanami.WithTags("filter"),
    sazanami.WithBuffer(4),
)

p = sazanami.AddStage(p, "batch",  sazanami.Batch[entry](2, time.Second),
    sazanami.WithTags("batch"),
    sazanami.WithAttr("size","2"),
    sazanami.WithBuffer(1),
)

p = sazanami.AddStage(p, "store",  storeBatches(&storeFailures),
    sazanami.WithTags("sink"),
    sazanami.WithErrorPolicy(sazanami.Retry(2, func(i int) time.Duration {
        if i < 0 {
            i = 0
        }
        return time.Duration(1<<i) * 200 * time.Millisecond
    })),
    sazanami.WithTimeout(500*time.Millisecond),
    sazanami.WithBuffer(1),
)

out := p.Run(ctx)
```

## Philosophy
Keep the surface area small, the behavior predictable, and the code idiomatic. Sazanami embraces Go’s bias toward explicit composition, relies only on the standard library, and aims to stay approachable for both quick scripts and production services.

## License
MIT

PRs welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for more details.
