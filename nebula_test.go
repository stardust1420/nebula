package nebula

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSubmitShutdownRace hammers Submit while Shutdown races, which under the
// race detector catches send-on-closed-channel / WaitGroup misuse.
func TestSubmitShutdownRace(t *testing.T) {
	for iter := 0; iter < 300; iter++ {
		process := func(ctx context.Context, job int) error { return nil }
		p := New(process, 4, 8, LogLevelNone)
		p.Start()

		var wg sync.WaitGroup
		for i := 0; i < 25; i++ {
			wg.Add(1)
			go func(v int) {
				defer wg.Done()
				p.Submit(context.Background(), v)
			}(i)
		}
		go func() { _ = p.Shutdown(context.Background()) }()
		wg.Wait()
	}
}

// TestAllJobsProcessed verifies every accepted job is processed exactly once
// when Shutdown is given enough time to drain.
func TestAllJobsProcessed(t *testing.T) {
	const n = 1000
	var count int64
	process := func(ctx context.Context, job int) error {
		atomic.AddInt64(&count, 1)
		return nil
	}
	p := New(process, 8, 64, LogLevelNone)
	p.Start()

	accepted := 0
	for i := 0; i < n; i++ {
		if p.Submit(context.Background(), i) {
			accepted++
		}
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
	if got := int(atomic.LoadInt64(&count)); got != accepted {
		t.Fatalf("processed %d jobs, expected %d", got, accepted)
	}
}

// TestSubmitAfterShutdown ensures Submit is rejected after shutdown.
func TestSubmitAfterShutdown(t *testing.T) {
	p := New(func(ctx context.Context, job int) error { return nil }, 2, 4, LogLevelNone)
	p.Start()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
	if p.Submit(context.Background(), 1) {
		t.Fatal("Submit should return false after Shutdown")
	}
}

// TestDoubleShutdown ensures a second Shutdown reports an error and does not
// panic (e.g. double close of channels).
func TestDoubleShutdown(t *testing.T) {
	p := New(func(ctx context.Context, job int) error { return nil }, 2, 4, LogLevelNone)
	p.Start()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("first shutdown returned error: %v", err)
	}
	if err := p.Shutdown(context.Background()); err == nil {
		t.Fatal("second Shutdown should return an error")
	}
}

// TestFailureTracker checks that errors, panics, and queue-expired jobs all
// reach the failure tracker.
func TestFailureTracker(t *testing.T) {
	var mu sync.Mutex
	failures := 0
	process := func(ctx context.Context, job int) error {
		if job == 0 {
			panic("boom")
		}
		return context.DeadlineExceeded
	}
	p := New(process, 1, 8, LogLevelNone).
		WithFailureTracker(func(job int, err error) {
			mu.Lock()
			failures++
			mu.Unlock()
		})
	p.Start()
	p.Submit(context.Background(), 0) // panics
	p.Submit(context.Background(), 1) // returns error
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
	mu.Lock()
	got := failures
	mu.Unlock()
	if got != 2 {
		t.Fatalf("expected 2 failures, got %d", got)
	}
}

// TestShutdownTimeout verifies Shutdown returns ctx.Err() when workers cannot
// drain in time.
func TestShutdownTimeout(t *testing.T) {
	process := func(ctx context.Context, job int) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}
	p := New(process, 1, 10, LogLevelNone)
	p.Start()
	for i := 0; i < 5; i++ {
		p.Submit(context.Background(), i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); err == nil {
		t.Fatal("expected timeout error from Shutdown")
	}
}
