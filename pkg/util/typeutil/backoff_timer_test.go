package typeutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoffTimer(t *testing.T) {
	b := NewBackoffTimer(BackoffTimerConfig{
		Default: time.Second,
		Backoff: BackoffConfig{
			InitialInterval: 50 * time.Millisecond,
			Multiplier:      2,
			MaxInterval:     200 * time.Millisecond,
		},
	})

	for i := 0; i < 2; i++ {
		assert.Equal(t, time.Second, b.NextInterval())
		assert.Equal(t, time.Second, b.NextInterval())
		assert.Equal(t, time.Second, b.NextInterval())

		b.EnableBackoff()
		timer, backoff := b.NextTimer()
		assert.Less(t, backoff, 200*time.Millisecond)
		<-timer
		_, backoff = b.NextTimer()
		assert.NotZero(t, backoff)
		b.DisableBackoff()
		assert.Equal(t, time.Second, b.NextInterval())
	}
}
