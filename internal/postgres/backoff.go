package postgres

import (
	"context"
	"math"
	"math/rand"
	"time"
)

type backoff struct {
	base    time.Duration
	cap     time.Duration
	attempt int
}

func newBackoff(base, cap time.Duration) *backoff {
	return &backoff{base: base, cap: cap}
}

// Wait blocks until the backoff delay elapses or ctx is cancelled.
// Returns ctx.Err() if cancelled, nil otherwise.
func (b *backoff) Wait(ctx context.Context) error {
	delay := b.nextDelay()
	b.attempt++

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (b *backoff) nextDelay() time.Duration {
	// Exponential: base * 2^attempt, capped at max
	exp := math.Min(
		float64(b.base)*math.Pow(2, float64(b.attempt)),
		float64(b.cap),
	)
	// Full jitter: uniform [0, exp)
	jittered := time.Duration(rand.Int63n(int64(exp)))
	if jittered < b.base {
		jittered = b.base
	}
	return jittered
}

// Reset resets the attempt counter. Call after a successful connection.
func (b *backoff) Reset() {
	b.attempt = 0
}
