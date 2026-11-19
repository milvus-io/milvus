package resource

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSplitLifecycleGuardCancellationAndCollectionIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	owner, err := AcquireSplitLifecycle(ctx, 10)
	require.NoError(t, err)
	defer owner()
	other, err := AcquireSplitLifecycle(ctx, 11)
	require.NoError(t, err)
	other()

	waitCtx, stopWait := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		release, err := AcquireSplitLifecycle(waitCtx, 10)
		if release != nil {
			release()
		}
		done <- err
	}()
	require.Eventually(t, func() bool {
		splitLifecycle.Lock()
		defer splitLifecycle.Unlock()
		return splitLifecycle.entries[10].refs == 2
	}, time.Second, time.Millisecond)
	stopWait()
	require.ErrorIs(t, <-done, context.Canceled)
	owner()
	owner() // Repeated cleanup must not release another owner's token.
	second, err := AcquireSplitLifecycle(ctx, 10)
	require.NoError(t, err)
	defer second()
	blockedCtx, stopBlocked := context.WithCancel(ctx)
	stopBlocked()
	_, err = AcquireSplitLifecycle(blockedCtx, 10)
	require.ErrorIs(t, err, context.Canceled)
	second()
	splitLifecycle.Lock()
	defer splitLifecycle.Unlock()
	require.NotContains(t, splitLifecycle.entries, int64(10))
	require.NotContains(t, splitLifecycle.entries, int64(11))
}

func TestSplitLifecycleGuardHandsOffToWaitingAdoption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	schema, err := AcquireSplitLifecycle(ctx, 20)
	require.NoError(t, err)
	defer schema()
	acquired := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		release, err := AcquireSplitLifecycle(ctx, 20)
		if err != nil {
			return
		}
		defer release()
		close(acquired)
	}()
	select {
	case <-acquired:
		t.Fatal("adoption entered while schema handoff held the guard")
	case <-time.After(10 * time.Millisecond):
	}
	schema()
	select {
	case <-acquired:
	case <-ctx.Done():
		t.Fatal("adoption did not resume after schema publication")
	}
	<-done
}
