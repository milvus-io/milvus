package syncutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestContextCond(t *testing.T) {
	cv := NewContextCond(&sync.Mutex{})
	cv.L.Lock()
	go func() {
		time.Sleep(10 * time.Millisecond)
		cv.LockAndBroadcast()
		cv.L.Unlock()
	}()
	// Acquire lock before wait.
	assert.NoError(t, cv.Wait(context.Background()))
	cv.L.Unlock()

	cv.L.Lock()
	go func() {
		time.Sleep(20 * time.Millisecond)
		cv.LockAndBroadcast()
		cv.L.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Acquire no lock if wait returns error.
	assert.Error(t, cv.Wait(ctx))

	cv.L.Lock()
	assert.NoError(t, cv.Wait(context.Background()))
	cv.L.Unlock()
}
