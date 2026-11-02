package qnview

import (
	"time"

	"github.com/cenkalti/backoff/v4"
)

const resourceRetryMaxDelay = 5 * time.Second

func newResourceRetryBackoff(initial time.Duration) *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = initial
	b.Multiplier = 2
	b.RandomizationFactor = 0.2
	b.MaxInterval = resourceRetryMaxDelay
	// Live requirements keep retrying until fulfilled or released.
	b.MaxElapsedTime = 0
	b.Reset()
	return b
}

func nextResourceRetryDelay(b *backoff.ExponentialBackOff) time.Duration {
	// MaxInterval limits the base interval, not the randomized delay.
	return min(b.NextBackOff(), resourceRetryMaxDelay)
}

func (b *segmentBinding) backoffLocked() {
	if b.retryBackoff == nil {
		b.retryBackoff = newResourceRetryBackoff(200 * time.Millisecond)
	}
	b.retryAfter = time.Now().Add(nextResourceRetryDelay(b.retryBackoff))
}

func (b *segmentBinding) resetRetryLocked() {
	b.retryBackoff = nil
	b.retryAfter = time.Time{}
}
