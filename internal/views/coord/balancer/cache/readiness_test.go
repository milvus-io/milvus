package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitForReadyCancellation(t *testing.T) {
	c := New(nil)
	require.False(t, c.Ready())
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, c.WaitForReady(ctx), context.DeadlineExceeded)
	require.False(t, c.Ready(), "canceling a waiter does not change readiness")
	c.MarkReady()
	require.True(t, c.Ready())
	require.NoError(t, c.WaitForReady(t.Context()))
	canceled, stop := context.WithCancel(t.Context())
	stop()
	require.ErrorIs(t, c.WaitForReady(canceled), context.Canceled, "cancellation is honored even after initialization")
}

func TestMarkReadyWakesAllWaiters(t *testing.T) {
	c := New(nil)
	const waiters = 16
	results := make(chan error, waiters)
	var started sync.WaitGroup
	started.Add(waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			started.Done()
			results <- c.WaitForReady(t.Context())
		}()
	}
	started.Wait()
	select {
	case err := <-results:
		t.Fatalf("waiter returned before readiness: %v", err)
	default:
	}
	var publishers sync.WaitGroup
	publishers.Add(waiters)
	for i := 0; i < waiters; i++ {
		go func() { defer publishers.Done(); c.MarkReady() }()
	}
	publishers.Wait()
	for i := 0; i < waiters; i++ {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("readiness did not wake every waiter")
		}
	}
	require.True(t, c.Ready())
	require.NoError(t, c.WaitForReady(t.Context()), "readiness remains observable after the broadcast")
}
