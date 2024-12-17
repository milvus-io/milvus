package typeutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLifetime(t *testing.T) {
	l := NewLifetime()
	assert.True(t, l.Add(LifetimeStateWorking))
	assert.False(t, l.Add(LifetimeStateStopped))
	assert.Equal(t, l.GetState(), LifetimeStateWorking)
	done := make(chan struct{})
	go func() {
		l.Wait()
		close(done)
	}()
	select {
	case <-time.After(10 * time.Millisecond):
	case <-done:
		assert.Fail(t, "lifetime should not be stopped")
	}
	l.SetState(LifetimeStateStopped)
	assert.Equal(t, l.GetState(), LifetimeStateStopped)
	assert.False(t, l.Add(LifetimeStateWorking))
	select {
	case <-time.After(10 * time.Millisecond):
	case <-done:
		assert.Fail(t, "lifetime should not be stopped")
	}
	l.Done()
	<-done
	l.Wait()
}
