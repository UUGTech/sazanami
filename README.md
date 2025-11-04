# Sazanami 🌊

## Overview
Sazanami is a minimal pipeline library for Go. It lets you compose concurrent stages with type-safe handlers, buffering, and error policies—ideal for ETL flows, streaming jobs, or any workload that benefits from backpressure-aware pipelines. The design favors explicit, readable code: generics for safety, zero external dependencies, and a fluent builder that feels at home in Go.

## Installation
```bash
go get github.com/UUGTech/sazanami
```

## Quickstart
```go
src := make(chan int)
go func() {
	defer close(src)
	for _, v := range []int{1, 2, 3, 4} {
		src <- v
	}
}()

ctx := context.Background()

pipeline := sazanami.AddStage(
    sazanami.From(src),
    "",
    func(ctx context.Context, in <-chan int, out chan<- int) error {
        for v := range in {
            select {
            case <-ctx.Done():
                return ctx.Err()
            case out <- v * 2:
            }
        }
        return nil
    },
)

for v := range pipeline.Run(ctx) {
	fmt.Println(v) // 2, 4, 6, 8
}
```

## Features
- Fluent builder with typed handlers (`From`, `AddStage`, `Run`)
- Concurrency controls: per-stage `Parallel` and `Buffer`
- Error handling via `Drop`, `Retry`, and `Chain` policies
- Hooks for stage/item lifecycle metrics
- `Batch(size, duration)` helper and lightweight `testkit`
- Zero external dependencies; standard library only

## Example: ETL Pipeline
```go
ctx := context.Background()
var storeFailures int32 = 1

p := sazanami.From(lines)

p = sazanami.AddStage(p, "parse",  parseLogs,
    sazanami.WithTags("ingest","json"),
    sazanami.WithParallel(2),
)

p = sazanami.AddStage(p, "filter", filterLevels("warn","error"),
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
        return time.Duration(1<<i) * 200 * time.Millisecond
    })),
    sazanami.WithBuffer(1),
)

out := p.Run(ctx)
```

## Philosophy
Keep the surface area small, the behavior predictable, and the code idiomatic. Sazanami embraces Go’s bias toward explicit composition, relies only on the standard library, and aims to stay approachable for both quick scripts and production services.

## License
MIT

PRs welcome.
