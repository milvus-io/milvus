package qvresource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestTransformReleaseWakesWaitersWithError(t *testing.T) {
	segment := &fakeQVSegment{id: 10}
	releaseErr := merr.WrapErrServiceInternalMsg("release failed")
	patch := mockey.Mock((*fakeQVSegment).Release).Return(releaseErr).Build()
	defer patch.UnPatch()
	wrapped := newQueryViewTransformSegment(segment, "v1", 50)
	results := make(chan error, 3)
	for _, tick := range []uint64{60, 60, 80} {
		go func() { results <- wrapped.WaitTransformApplied(context.Background(), tick) }()
	}
	require.Eventually(t, func() bool {
		wrapped.mu.Lock()
		defer wrapped.mu.Unlock()
		return len(wrapped.waiters[60]) == 2 && len(wrapped.waiters[80]) == 1
	}, time.Second, time.Millisecond)
	require.ErrorIs(t, wrapped.Release(context.Background()), releaseErr)
	for range 3 {
		select {
		case err := <-results:
			require.ErrorIs(t, err, merr.ErrSegmentNotLoaded)
		case <-time.After(time.Second):
			t.Fatal("release left a Transform waiter blocked")
		}
	}
	for _, tick := range []uint64{0, 50, 99} {
		require.ErrorIs(t, wrapped.WaitTransformApplied(context.Background(), tick), merr.ErrSegmentNotLoaded)
	}
	require.ErrorIs(t, wrapped.Release(context.Background()), releaseErr)
	require.Equal(t, 1, patch.Times())
	require.Empty(t, wrapped.waiters)
}

func TestTransformReleaseRacesProgressAndCancellation(t *testing.T) {
	patch := mockey.Mock((*fakeQVSegment).Release).Return(nil).Build()
	defer patch.UnPatch()
	for range 20 {
		wrapped := newQueryViewTransformSegment(&fakeQVSegment{id: 10}, "v1", 50)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- wrapped.WaitTransformApplied(ctx, 99) }()
		require.Eventually(t, func() bool {
			wrapped.mu.Lock()
			defer wrapped.mu.Unlock()
			return len(wrapped.waiters[99]) == 1
		}, time.Second, time.Millisecond)
		var group sync.WaitGroup
		group.Add(3)
		go func() { defer group.Done(); wrapped.markTransformApplied(99) }()
		go func() { defer group.Done(); _ = wrapped.Release(context.Background()) }()
		go func() { defer group.Done(); cancel() }()
		group.Wait()
		select {
		case <-result: // Progress, cancellation, or release may win.
		case <-time.After(time.Second):
			t.Fatal("concurrent completion left a waiter blocked")
		}
		require.Empty(t, wrapped.waiters)
	}
}
