package nebula

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// NebulaWithConsistentHashing is a worker pool that gives every worker its own
// job channel and routes each job to a worker using a consistent-hash ring keyed
// by a caller-supplied string ID. All jobs sharing an ID are therefore handled
// by the same worker goroutine, which provides per-key ordering and affinity
// (useful for stateful processing, in-order event streams, cache locality, etc.).
//
// It mirrors the plain Nebula pool: same lifecycle (New -> optional
// WithFailureTracker -> Start -> Submit -> Shutdown), same panic recovery,
// failure tracking, leveled logging, and context-aware graceful shutdown. The
// only difference is that Submit additionally requires an ID used for routing.
type NebulaWithConsistentHashing[T any] struct {
	numWorkers uint64
	jobChans   []chan JobWithCtx[T] // one buffered channel per worker
	done       chan struct{}
	process    func(ctx context.Context, job T) error
	onFail     func(job T, err error)
	wg         sync.WaitGroup // Tracks active workers
	mu         sync.RWMutex   // Guards sends against closing the job channels
	closed     atomic.Bool
	logLevel   LogLevel
	startOnce  sync.Once
	ring       *hashRing
}

// NewWithConsistentHashing constructs a consistent-hashing pool. queueSize is
// the buffer capacity of each individual worker's channel, so total buffered
// capacity is numWorkers * queueSize.
func NewWithConsistentHashing[T any](
	process func(ctx context.Context, job T) error,
	numWorkers uint64,
	queueSize uint64,
	logLevel LogLevel,
) (*NebulaWithConsistentHashing[T], error) {

	if numWorkers == 0 {
		return nil, ErrNoWorkers
	}

	chans := make([]chan JobWithCtx[T], numWorkers)
	for i := range chans {
		chans[i] = make(chan JobWithCtx[T], queueSize)
	}

	return &NebulaWithConsistentHashing[T]{
		numWorkers: numWorkers,
		jobChans:   chans,
		done:       make(chan struct{}),
		process:    process,
		onFail:     func(job T, err error) {}, // Safe default (no-op)
		logLevel:   logLevel,
		ring:       newHashRing(numWorkers, defaultVirtualNodes),
	}, nil
}

// WithFailureTracker attaches a callback for failed, panicked, or queue-expired
// jobs. It returns the pool for chaining.
func (n *NebulaWithConsistentHashing[T]) WithFailureTracker(tracker func(job T, err error)) *NebulaWithConsistentHashing[T] {
	n.onFail = tracker
	return n
}

// WorkerFor reports the worker index that Submit will route the given ID to.
// It is deterministic for the lifetime of the pool and is exposed mainly for
// testing, metrics, and debugging.
func (n *NebulaWithConsistentHashing[T]) WorkerFor(id string) uint64 {
	return n.ring.get(id)
}

// Start launches the worker goroutines. It is safe to call multiple times; only
// the first call spawns workers.
func (n *NebulaWithConsistentHashing[T]) Start() {
	n.startOnce.Do(func() {
		n.logf(LogLevelInfo, "Starting consistent-hashing pool with %d workers", n.numWorkers)

		for i := uint64(0); i < n.numWorkers; i++ {
			n.wg.Add(1)

			go func(id uint64) {
				defer n.wg.Done()
				runWorker(id, n.jobChans[id], n.process, n.onFail, n.logf)
			}(i)
		}
	})
}

// Submit routes job to the worker responsible for id and enqueues it. It returns
// false if the pool is closed/closing, has no workers, or the context expires
// before the job can be enqueued.
func (n *NebulaWithConsistentHashing[T]) Submit(ctx context.Context, id string, job T) bool {
	// Fast fail if we are already closed
	if n.closed.Load() {
		n.logf(LogLevelDebug, "Submit rejected: worker pool is already closed")
		return false
	}

	// Hold the read lock for the duration of the send. Shutdown takes the write
	// lock before closing the channels, so while we hold this lock no channel
	// can be closed underneath us. Many Submits can proceed concurrently.
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Re-check under the lock in case Shutdown flipped closed before we acquired it.
	if n.closed.Load() {
		n.logf(LogLevelDebug, "Submit aborted: pool closed during submission")
		return false
	}

	idx := n.ring.get(id)
	jobWithCtx := JobWithCtx[T]{ctx: ctx, job: job}

	select {
	case <-n.done:
		n.logf(LogLevelDebug, "Submit canceled: pool is shutting down")
		return false
	case <-ctx.Done():
		n.logf(LogLevelDebug, "Submit canceled: context expired before entering queue")
		return false
	case n.jobChans[idx] <- jobWithCtx:
		n.logf(LogLevelDebug, "Job with id %q routed to worker %d", id, idx)
		return true
	}
}

// Shutdown stops new submissions, closes every worker channel, and waits for the
// workers to drain. It returns ctx.Err() if the workers do not finish before the
// context's deadline.
func (n *NebulaWithConsistentHashing[T]) Shutdown(ctx context.Context) error {
	if !n.closed.CompareAndSwap(false, true) {
		n.logf(LogLevelDebug, "Shutdown rejected: already shutting down or closed")
		return errors.New("worker pool is already closed")
	}
	n.logf(LogLevelInfo, "Initiating graceful shutdown...")

	n.logf(LogLevelDebug, "Unblocking any pending Submit calls...")
	close(n.done)

	// Acquire the write lock so that every in-flight Submit (each holding the
	// read lock) has returned before we close the channels.
	n.logf(LogLevelDebug, "Waiting for active Submits to exit, then closing channels...")
	n.mu.Lock()
	for _, ch := range n.jobChans {
		close(ch)
	}
	n.mu.Unlock()

	workersDone := make(chan struct{})
	go func() {
		n.wg.Wait()
		close(workersDone)
	}()

	n.logf(LogLevelDebug, "Waiting for workers to drain their queues...")

	select {
	case <-workersDone:
		n.logf(LogLevelInfo, "Shutdown complete: all workers finished gracefully!")
		return nil
	case <-ctx.Done():
		n.logf(LogLevelError, "Shutdown forced: workers did not finish in time")
		return ctx.Err()
	}
}

func (n *NebulaWithConsistentHashing[T]) logf(level LogLevel, format string, args ...any) {
	logAt(n.logLevel, level, format, args...)
}
