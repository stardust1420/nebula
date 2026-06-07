package nebula

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type Nebula[T any] struct {
	numWorkers uint64
	jobs       chan jobWrapper[T] // Now holds the wrapper
	done       chan struct{}
	process    func(ctx context.Context, job T) error // Now takes ctx and returns error
	onFail     func(job T, err error)                 // Callback for failed/panicked jobs
	wg         sync.WaitGroup                         // Tracks active workers
	senderWg   sync.WaitGroup                         // Tracks active Submit calls
	closed     atomic.Bool
	logLevel   LogLevel
	startOnce  sync.Once // NEW: Ensures Start() only spawns workers once
}

// jobWrapper pairs the user's job with its specific execution context.
type jobWrapper[T any] struct {
	ctx context.Context
	job T
}

// LogLevel defines the verbosity of the worker pool.
type LogLevel int

const (
	LogLevelNone  LogLevel = iota // 0: Silent (Default/Recommended for Prod)
	LogLevelError                 // 1: Only log panics and critical issues
	LogLevelInfo                  // 2: Log startup, shutdown, and major state changes
	LogLevelDebug                 // 3: Log every worker start/stop and job submission
)

func New[T any](
	process func(ctx context.Context, job T) error, // Updated signature
	numWorkers uint64,
	queueSize uint64,
	logLevel LogLevel,
) *Nebula[T] {

	jobsChan := make(chan jobWrapper[T], queueSize)
	doneChan := make(chan struct{})

	return &Nebula[T]{
		numWorkers: numWorkers,
		process:    process,
		jobs:       jobsChan,
		done:       doneChan,
		wg:         sync.WaitGroup{},
		logLevel:   logLevel,
		onFail:     func(job T, err error) {}, // Safe default (no-op)
	}
}

// WithFailureTracker allows users to attach a callback for failed or panicked jobs.
func (n *Nebula[T]) WithFailureTracker(tracker func(job T, err error)) *Nebula[T] {
	n.onFail = tracker
	return n
}

func (n *Nebula[T]) Start() {
	// The code inside Do() is guaranteed to only run once per Nebula instance
	n.startOnce.Do(func() {
		n.logf(LogLevelInfo, "Starting worker pool with %d workers", n.numWorkers)

		for i := uint64(0); i < n.numWorkers; i++ {
			n.wg.Add(1)

			go func(id uint64) {
				defer func() {
					n.wg.Done()
					n.logf(LogLevelDebug, "Worker %d stopped", id)
				}()

				n.logf(LogLevelDebug, "Worker %d started", id)

				// A range loop blocks waiting for jobs, and automatically
				// exits once n.jobs is closed AND fully drained.
				for wrapper := range n.jobs {
					// 1. Skip the job if it timed out while waiting in the queue!
					if wrapper.ctx.Err() != nil {
						n.logf(LogLevelDebug, "Worker %d skipped job (context expired in queue): %v", id, wrapper.job)
						// Trigger failure tracker for the skipped job
						n.onFail(wrapper.job, wrapper.ctx.Err())
						continue
					}

					n.logf(LogLevelDebug, "Worker %d starting job: %v", id, wrapper.job)

					var err error // Capture the result of the process

					func() {
						defer func() {
							if r := recover(); r != nil {
								// Convert the panic payload into a standard error
								err = fmt.Errorf("worker panicked: %v", r)
								n.logf(LogLevelError, "Worker %d panicked: %v\n%s", id, r, debug.Stack())
							}
						}()

						// 2. Pass the context down to the user's function and capture any explicit error
						err = n.process(wrapper.ctx, wrapper.job)
					}()

					// 3. Trigger the failure tracker if anything went wrong
					if err != nil {
						n.logf(LogLevelDebug, "Worker %d failed job: %v, err: %v", id, wrapper.job, err)
						n.onFail(wrapper.job, err)
					} else {
						n.logf(LogLevelDebug, "Worker %d finished job: %v", id, wrapper.job)
					}
				}
			}(i)
		}
	})
}

func (n *Nebula[T]) Submit(ctx context.Context, job T) bool {
	// Fast fail if we are already closed
	if n.closed.Load() {
		n.logf(LogLevelDebug, "Submit rejected: worker pool is already closed")
		return false
	}

	// Register this active send operation
	n.senderWg.Add(1)
	defer n.senderWg.Done()

	// Double-check in case Shutdown was called between step 1 and 2
	if n.closed.Load() {
		n.logf(LogLevelDebug, "Submit aborted: pool closed during submission")
		return false
	}

	// Package the job and context together
	wrapper := jobWrapper[T]{ctx: ctx, job: job}

	// Try to send
	select {
	case <-n.done:
		// Pool is shutting down, unblock and cancel send
		n.logf(LogLevelDebug, "Submit canceled: pool is shutting down")
		return false
	case <-ctx.Done():
		// If the queue is full and the user's context times out
		// before we can push it to the channel, abort the send.
		n.logf(LogLevelDebug, "Submit canceled: context expired before entering queue")
		return false
	case n.jobs <- wrapper:
		n.logf(LogLevelDebug, "Job successfully submitted: %v", job)
		return true
	}
}

func (n *Nebula[T]) Shutdown(ctx context.Context) error {
	// Atomically check and update the closed state.
	// If it was already true, CompareAndSwap returns false, meaning
	// another goroutine already called Shutdown.
	n.logf(LogLevelDebug, "Stopping new submissions...")

	if !n.closed.CompareAndSwap(false, true) {
		n.logf(LogLevelDebug, "Shutdown rejected: already shutting down or closed")
		return errors.New("worker pool is already closed")
	}
	n.logf(LogLevelInfo, "Initiating graceful shutdown...")

	n.logf(LogLevelDebug, "Unblocking any pending Submit calls...")
	close(n.done)

	n.logf(LogLevelDebug, "Waiting for active Submits to exit...")
	n.senderWg.Wait()

	n.logf(LogLevelDebug, "Safely closing the jobs channel...")
	close(n.jobs)

	// 1. Create a channel to signal when workers are naturally finished
	workersDone := make(chan struct{})

	// 2. Run wg.Wait() in the background so it doesn't block our select statement
	go func() {
		n.wg.Wait()
		close(workersDone)
	}()

	n.logf(LogLevelDebug, "Waiting for workers to drain the queue...")

	// 3. Race the workers against the user's context timeout
	select {
	case <-workersDone:
		n.logf(LogLevelInfo, "Shutdown complete: all workers finished gracefully!")
		return nil
	case <-ctx.Done():
		// The context expired before the workers finished
		n.logf(LogLevelError, "Shutdown forced: workers did not finish in time")
		return ctx.Err()
	}
}

func (n *Nebula[T]) logf(level LogLevel, format string, args ...any) {
	// If the pool's configured level is lower than the message's level, ignore it.
	if n.logLevel < level {
		return
	}

	prefix := ""
	switch level {
	case LogLevelError:
		prefix = "[NEBULA ERROR] "
	case LogLevelInfo:
		prefix = "[NEBULA INFO] "
	case LogLevelDebug:
		prefix = "[NEBULA DEBUG] "
	}

	log.Printf(prefix+format, args...)
}
