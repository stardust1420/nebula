# nebula

Nebula is a small, generic worker pool for Go. It runs a fixed number of
workers that process jobs from a buffered queue, with per-job context support,
panic recovery, optional failure tracking, and a context-aware graceful
shutdown.

The library has no third-party dependencies and targets Go 1.24 or later
(generics and `sync/atomic` types are used).

## Features

- **Generic jobs.** `Nebula[T]` works with any job type `T`; no `interface{}`
  casting is required in your processing function.
- **Fixed worker count.** A configurable number of goroutines drain a shared
  buffered channel, bounding concurrency.
- **Bounded queue.** The job queue is a buffered channel with a user-defined
  capacity, providing back-pressure when workers fall behind.
- **Per-job context.** Each `Submit` call carries its own `context.Context`.
  The context is honored both while the job waits in the queue and while it is
  being processed.
- **Panic recovery.** A panic inside the processing function is recovered,
  converted into an `error`, and reported through the failure tracker rather
  than crashing the pool.
- **Failure tracking.** An optional callback is invoked for every job that
  returns an error, panics, or is skipped because its context expired.
- **Graceful, context-aware shutdown.** `Shutdown` stops new submissions,
  drains in-flight work, and returns the context's error if draining exceeds a
  deadline.
- **Leveled logging.** Built-in logging at `None`, `Error`, `Info`, and `Debug`
  levels, routed through the standard library `log` package.

## Installation

```sh
go get github.com/stardust1420/nebula
```

```go
import "github.com/stardust1420/nebula"
```

## API overview

| Function | Description |
| --- | --- |
| `New[T](process, numWorkers, queueSize, logLevel)` | Constructs a pool. `process` has signature `func(ctx context.Context, job T) error`. |
| `(*Nebula[T]).WithFailureTracker(fn)` | Registers a `func(job T, err error)` callback for failed, panicked, or skipped jobs. Returns the pool for chaining. |
| `(*Nebula[T]).Start()` | Launches the worker goroutines. |
| `(*Nebula[T]).Submit(ctx, job) bool` | Enqueues a job. Returns `false` if the pool is closed or the context expires before the job is queued. |
| `(*Nebula[T]).Shutdown(ctx) error` | Stops submissions and drains the queue. Returns `ctx.Err()` if the deadline is reached before workers finish. |

### Log levels

| Level | Value | Behavior |
| --- | --- | --- |
| `LogLevelNone` | 0 | Silent. Recommended for production. |
| `LogLevelError` | 1 | Panics and critical issues only. |
| `LogLevelInfo` | 2 | Startup, shutdown, and major state changes. |
| `LogLevelDebug` | 3 | Per-worker and per-job activity. |

## Usage

### Basic pool

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/stardust1420/nebula"
)

func main() {
	// process is called by each worker for every job.
	process := func(ctx context.Context, job int) error {
		fmt.Printf("processing %d\n", job)
		return nil
	}

	// 4 workers, queue capacity of 100, silent logging.
	pool := nebula.New(process, 4, 100, nebula.LogLevelNone)
	pool.Start()

	for i := 0; i < 10; i++ {
		pool.Submit(context.Background(), i)
	}

	// Allow up to 5 seconds for in-flight jobs to drain.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Shutdown(ctx); err != nil {
		fmt.Printf("shutdown did not complete cleanly: %v\n", err)
	}
}
```

### Returning errors and tracking failures

The processing function returns an `error`. Any non-nil error, recovered panic,
or context expiration is delivered to the failure tracker, which is a good place
to record metrics, log, or push jobs onto a retry/dead-letter queue.

```go
process := func(ctx context.Context, job string) error {
	if job == "" {
		return fmt.Errorf("empty job")
	}
	return doWork(ctx, job)
}

pool := nebula.New(process, 8, 256, nebula.LogLevelError).
	WithFailureTracker(func(job string, err error) {
		log.Printf("job %q failed: %v", job, err)
	})

pool.Start()
```

The tracker runs on the worker goroutine. If it touches shared state (counters,
slices, maps), guard that state with appropriate synchronization.

### Per-job context and timeouts

Each job carries its own context. It is checked twice: once when a worker picks
the job up (so jobs that expired while queued are skipped), and again by your
processing function while the job runs.

```go
ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
defer cancel()

// Returns false if the queue stays full until the context expires.
if ok := pool.Submit(ctx, job); !ok {
	// Job was not accepted; handle back-pressure here.
}
```

If `Submit` is called with a full queue, it blocks until a slot is free, the
context is cancelled, or the pool begins shutting down — whichever happens
first. The boolean return value reports whether the job was accepted.

## Lifecycle and semantics

- **Start once.** Call `Start` a single time per pool. Submitting before
  `Start` is safe — jobs buffer in the queue up to its capacity — but workers
  only begin consuming after `Start` is called.
- **Submit return value.** `true` means the job entered the queue. `false`
  means the pool was closed, was shutting down, or the job's context expired
  before it could be enqueued.
- **Shutdown is one-way.** `Shutdown` marks the pool closed, rejects new
  submissions, closes the queue, and waits for workers to drain remaining jobs.
  A pool cannot be reused after shutdown; create a new one if needed.
- **Shutdown deadline.** Pass a context with a timeout or deadline to bound how
  long `Shutdown` waits. If workers do not finish in time, it returns
  `ctx.Err()`; the workers continue running in the background until the queue
  drains.
- **Queued-but-expired jobs.** Jobs whose context expires while waiting in the
  queue are skipped (not processed) and reported to the failure tracker.

## Best practices

- **Size the queue for back-pressure, not buffering.** A very large queue hides
  the fact that workers cannot keep up and increases memory use and shutdown
  time. Prefer a bounded queue that lets `Submit` apply back-pressure.
- **Match worker count to the workload.** Use a higher worker count for
  I/O-bound jobs and a count closer to the number of CPUs for CPU-bound jobs.
- **Always give Shutdown a deadline.** Use `context.WithTimeout` so a stuck or
  slow job cannot block shutdown indefinitely, and inspect the returned error.
- **Honor the context in `process`.** Check `ctx.Err()` or pass `ctx` to
  downstream calls so long-running jobs can be cancelled, especially during
  shutdown.
- **Keep the failure tracker fast and thread-safe.** It runs inline on the
  worker. Offload heavy work and protect shared state with synchronization.
- **Don't rely on `Submit` after `Shutdown`.** Once shutdown begins, `Submit`
  returns `false`. Check the return value if late submissions are possible.
- **Use `LogLevelNone` in production.** Higher levels (especially `Debug`) are
  intended for development and can be noisy under load.

## Concurrency notes

Internally the pool uses a buffered channel for jobs, separate wait groups to
track active workers and in-flight `Submit` calls, and an atomic flag to mark
closure. This ordering lets `Shutdown` close the job channel only after all
pending sends have completed, avoiding sends on a closed channel.
