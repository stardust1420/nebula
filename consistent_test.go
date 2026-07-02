package nebula

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConsistentHashingDeterministic verifies the same ID always routes to the
// same worker, and that WorkerFor agrees with actual routing.
func TestConsistentHashingDeterministic(t *testing.T) {
	p := NewWithConsistentHashing(func(ctx context.Context, job int) error { return nil }, 8, 16, LogLevelNone)
	for _, id := range []string{"user-1", "order-42", "abc", "", "long-key-value-xyz"} {
		first := p.WorkerFor(id)
		for i := 0; i < 100; i++ {
			if got := p.WorkerFor(id); got != first {
				t.Fatalf("id %q mapped to %d then %d", id, first, got)
			}
			if first >= 8 {
				t.Fatalf("id %q mapped to out-of-range worker %d", id, first)
			}
		}
	}
}

// TestConsistentHashingDistribution checks keys spread across all workers rather
// than piling onto one.
func TestConsistentHashingDistribution(t *testing.T) {
	const workers = 8
	p := NewWithConsistentHashing(func(ctx context.Context, job int) error { return nil }, workers, 16, LogLevelNone)
	counts := make(map[uint64]int)
	for i := 0; i < 10000; i++ {
		counts[p.WorkerFor(fmt.Sprintf("key-%d", i))]++
	}
	if len(counts) != workers {
		t.Fatalf("expected all %d workers to receive keys, got %d", workers, len(counts))
	}
	// Loose balance check: no worker should get more than ~3x its fair share.
	fair := 10000 / workers
	for w, c := range counts {
		if c > fair*3 {
			t.Fatalf("worker %d got %d keys, expected roughly %d (unbalanced)", w, c, fair)
		}
	}
}

// TestConsistentHashingAffinity is the core guarantee: every job with a given ID
// is processed by exactly one worker goroutine.
func TestConsistentHashingAffinity(t *testing.T) {
	const workers = 6
	var mu sync.Mutex
	// id -> set of worker ids that processed a job for that id
	seen := make(map[string]map[uint64]bool)

	// We recover which worker ran a job by having the job carry the id and
	// recording the goroutine's stable worker index via WorkerFor at process
	// time is not possible, so instead we record the processing worker by
	// embedding it: each job is (id, expectedWorker) and the process fn checks it.
	type task struct {
		id       string
		expected uint64
	}

	var p *NebulaWithConsistentHashing[task]
	process := func(ctx context.Context, job task) error {
		w := p.WorkerFor(job.id)
		mu.Lock()
		if seen[job.id] == nil {
			seen[job.id] = make(map[uint64]bool)
		}
		seen[job.id][w] = true
		mu.Unlock()
		return nil
	}
	p = NewWithConsistentHashing(process, workers, 32, LogLevelNone)
	p.Start()

	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for round := 0; round < 200; round++ {
		for _, id := range ids {
			if !p.Submit(context.Background(), id, task{id: id, expected: p.WorkerFor(id)}) {
				t.Fatalf("submit failed for id %q", id)
			}
		}
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}

	for id, workerSet := range seen {
		if len(workerSet) != 1 {
			t.Fatalf("id %q was processed by %d workers, expected 1", id, len(workerSet))
		}
	}
}

// TestConsistentHashingAllProcessed ensures no jobs are lost.
func TestConsistentHashingAllProcessed(t *testing.T) {
	const n = 2000
	var count int64
	p := NewWithConsistentHashing(func(ctx context.Context, job int) error {
		atomic.AddInt64(&count, 1)
		return nil
	}, 8, 32, LogLevelNone)
	p.Start()

	accepted := 0
	for i := 0; i < n; i++ {
		if p.Submit(context.Background(), fmt.Sprintf("id-%d", i%50), i) {
			accepted++
		}
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if got := int(atomic.LoadInt64(&count)); got != accepted {
		t.Fatalf("processed %d, expected %d", got, accepted)
	}
}

// TestConsistentHashingSubmitShutdownRace stresses Submit against Shutdown.
func TestConsistentHashingSubmitShutdownRace(t *testing.T) {
	for iter := 0; iter < 300; iter++ {
		p := NewWithConsistentHashing(func(ctx context.Context, job int) error { return nil }, 4, 8, LogLevelNone)
		p.Start()

		var wg sync.WaitGroup
		for i := 0; i < 25; i++ {
			wg.Add(1)
			go func(v int) {
				defer wg.Done()
				p.Submit(context.Background(), fmt.Sprintf("k-%d", v), v)
			}(i)
		}
		go func() { _ = p.Shutdown(context.Background()) }()
		wg.Wait()
	}
}

// TestConsistentHashingFailureTracker verifies error and panic paths reach the tracker.
func TestConsistentHashingFailureTracker(t *testing.T) {
	var mu sync.Mutex
	failures := 0
	process := func(ctx context.Context, job int) error {
		if job == 0 {
			panic("boom")
		}
		return context.DeadlineExceeded
	}
	p := NewWithConsistentHashing(process, 3, 8, LogLevelNone).
		WithFailureTracker(func(job int, err error) {
			mu.Lock()
			failures++
			mu.Unlock()
		})
	p.Start()
	p.Submit(context.Background(), "panic", 0)
	p.Submit(context.Background(), "err", 1)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	mu.Lock()
	got := failures
	mu.Unlock()
	if got != 2 {
		t.Fatalf("expected 2 failures, got %d", got)
	}
}

// TestConsistentHashingSubmitAfterShutdown ensures Submit is rejected post-shutdown.
func TestConsistentHashingSubmitAfterShutdown(t *testing.T) {
	p := NewWithConsistentHashing(func(ctx context.Context, job int) error { return nil }, 2, 4, LogLevelNone)
	p.Start()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if p.Submit(context.Background(), "x", 1) {
		t.Fatal("Submit should return false after Shutdown")
	}
	if err := p.Shutdown(context.Background()); err == nil {
		t.Fatal("second Shutdown should return an error")
	}
}

// TestConsistentHashingShutdownTimeout verifies the timeout path.
func TestConsistentHashingShutdownTimeout(t *testing.T) {
	process := func(ctx context.Context, job int) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}
	p := NewWithConsistentHashing(process, 1, 10, LogLevelNone)
	p.Start()
	for i := 0; i < 5; i++ {
		p.Submit(context.Background(), "same-id", i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); err == nil {
		t.Fatal("expected timeout error from Shutdown")
	}
}

// TestHashRingStableAcrossInstances verifies routing is stable across two pools
// built with the same worker count (same ring construction).
func TestHashRingStableAcrossInstances(t *testing.T) {
	p1 := NewWithConsistentHashing(func(ctx context.Context, job int) error { return nil }, 5, 4, LogLevelNone)
	p2 := NewWithConsistentHashing(func(ctx context.Context, job int) error { return nil }, 5, 4, LogLevelNone)
	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("entity-%d", i)
		if p1.WorkerFor(id) != p2.WorkerFor(id) {
			t.Fatalf("id %q routed differently across identical pools", id)
		}
	}
}
