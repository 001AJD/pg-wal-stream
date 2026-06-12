package postgres

import (
	"context"
	"testing"
	"time"
)

func TestBackoffProgression(t *testing.T) {
	bo := newBackoff(100*time.Millisecond, 2*time.Second)

	// Verify delays increase over attempts and are capped.
	for i := 0; i < 10; i++ {
		delay := bo.nextDelay()
		bo.attempt++

		if delay < 100*time.Millisecond {
			t.Errorf("attempt %d: delay %v is below base 100ms", i, delay)
		}
		if delay > 2*time.Second {
			t.Errorf("attempt %d: delay %v exceeds cap 2s", i, delay)
		}
	}
}

func TestBackoffJitter(t *testing.T) {
	// Run multiple backoffs at the same attempt and ensure they're not all identical
	// (would indicate jitter is missing).
	const trials = 20
	delays := make(map[time.Duration]struct{}, trials)
	for i := 0; i < trials; i++ {
		bo := newBackoff(100*time.Millisecond, 10*time.Second)
		bo.attempt = 5 // fixed attempt for a wide jitter range
		delays[bo.nextDelay()] = struct{}{}
	}
	if len(delays) < 2 {
		t.Errorf("expected jitter to produce varied delays, got %d unique values out of %d", len(delays), trials)
	}
}

func TestBackoffReset(t *testing.T) {
	bo := newBackoff(100*time.Millisecond, 10*time.Second)

	// Advance several attempts.
	for i := 0; i < 6; i++ {
		bo.nextDelay()
		bo.attempt++
	}

	bo.Reset()

	if bo.attempt != 0 {
		t.Errorf("expected attempt to be 0 after Reset(), got %d", bo.attempt)
	}

	// After reset, delay should be in the base range.
	delay := bo.nextDelay()
	if delay > 200*time.Millisecond {
		t.Errorf("after Reset(), delay %v is unexpectedly large (expected near 100ms base)", delay)
	}
}

func TestBackoffWaitContextCancel(t *testing.T) {
	bo := newBackoff(10*time.Second, 60*time.Second) // long delay to ensure we cancel first

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- bo.Wait(ctx)
	}()

	// Cancel immediately.
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after context cancellation")
	}
}

func TestBackoffWaitCompletes(t *testing.T) {
	bo := newBackoff(10*time.Millisecond, 50*time.Millisecond)

	start := time.Now()
	err := bo.Wait(context.Background())
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("Wait took too long: %v", elapsed)
	}
}
