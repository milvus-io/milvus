package syncutil

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContextCond(t *testing.T) {
	t.Run("broadcast_wakes_waiter", func(t *testing.T) {
		cv := NewContextCond(&sync.Mutex{})

		waitEntered := make(chan struct{})
		waitDone := make(chan error, 1)

		go func() {
			cv.L.Lock()
			close(waitEntered)
			err := cv.Wait(context.Background())
			cv.L.Unlock()
			waitDone <- err
		}()

		// Wait for goroutine to enter Wait (which releases the lock internally)
		<-waitEntered

		// Now broadcast - goroutine is waiting on channel
		cv.LockAndBroadcast()
		cv.L.Unlock()

		assert.NoError(t, <-waitDone)
	})

	t.Run("context_cancel_returns_error", func(t *testing.T) {
		cv := NewContextCond(&sync.Mutex{})
		ctx, cancel := context.WithCancel(context.Background())

		waitEntered := make(chan struct{})
		waitDone := make(chan error, 1)

		go func() {
			cv.L.Lock()
			close(waitEntered)
			err := cv.Wait(ctx)
			// Note: Wait returns without holding lock when context is canceled
			waitDone <- err
		}()

		<-waitEntered

		// Cancel context to trigger Wait to return error
		cancel()

		err := <-waitDone
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("wait_after_cancel_still_works", func(t *testing.T) {
		cv := NewContextCond(&sync.Mutex{})
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Pre-cancel

		// First wait with canceled context should return error immediately
		cv.L.Lock()
		err := cv.Wait(ctx)
		assert.Error(t, err)
		// Note: after error, lock is not held

		// Second wait should still work when broadcast happens
		waitEntered := make(chan struct{})
		waitDone := make(chan error, 1)

		go func() {
			cv.L.Lock()
			close(waitEntered)
			err := cv.Wait(context.Background())
			cv.L.Unlock()
			waitDone <- err
		}()

		<-waitEntered

		cv.LockAndBroadcast()
		cv.L.Unlock()

		assert.NoError(t, <-waitDone)
	})
}
