package qnview

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
)

func requireDropPending(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("reported Dropped before segment release obligations completed")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestSegmentResourceDroppedWaitsForPhysicalRelease(t *testing.T) {
	mockey.PatchConvey("neither query completion nor starting native destruction is Dropped", t, func() {
		f := setupResourceTest(t, nil)
		entered, finish := make(chan struct{}), make(chan struct{})
		f.releaseMock.To(func(*fakeTransformSegment, context.Context) error {
			close(entered)
			<-finish
			return nil
		})
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, 0)
		require.NoError(t, err)
		dropped := f.beginDrop(req)
		requireDropPending(t, dropped)
		handles[0].Release()
		awaitShutdown(t, entered)
		requireDropPending(t, dropped)
		close(finish)
		awaitShutdown(t, dropped)
		f.manager.mu.Lock()
		require.Empty(t, f.manager.resources)
		f.manager.mu.Unlock()
	})
}

func TestSegmentResourceDroppedAllowsSharedTakeover(t *testing.T) {
	mockey.PatchConvey("a successor may retain the segment while the old query finishes", t, func() {
		f := setupResourceTest(t, nil)
		first := f.acquire(1, 1, 1)
		f.await(t, 1)
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), first.Key, first.View, 0)
		require.NoError(t, err)
		dropped := f.beginDrop(first)
		requireDropPending(t, dropped)
		next := f.acquire(2, 1, 1)
		f.await(t, 1)
		handles[0].Release()
		awaitShutdown(t, dropped)
		require.EqualValues(t, 1, f.loads.Load())
		require.Zero(t, f.releases.Load(), "a shared segment must survive the old View's Dropped report")
		query, err := f.manager.AcquireSealedSegmentHandles(t.Context(), next.Key, next.View, 0)
		require.NoError(t, err)
		query[0].Release()
		f.drop(t, next)
		require.EqualValues(t, 1, f.releases.Load())
	})
}

func TestSegmentResourceDroppedJoinsAlreadyClosingOwner(t *testing.T) {
	mockey.PatchConvey("a binding withdrawn during an existing close must join that concrete owner", t, func() {
		f := setupResourceTest(t, nil)
		entered, finish := make(chan struct{}), make(chan struct{})
		f.releaseMock.To(func(*fakeTransformSegment, context.Context) error {
			close(entered)
			<-finish
			return nil
		})
		first := f.acquire(1, 1, 1)
		f.await(t, 1)
		droppedFirst := f.beginDrop(first)
		awaitShutdown(t, entered)
		next := f.acquire(2, 2, 2) // This binding waits for the Closing incarnation.
		droppedNext := f.beginDrop(next)
		requireDropPending(t, droppedNext)
		requireDropPending(t, droppedFirst)
		close(finish)
		awaitShutdown(t, droppedNext)
		awaitShutdown(t, droppedFirst)
		require.EqualValues(t, 1, f.loads.Load(), "withdrawn successor must not reconstruct the segment")
	})
}

func TestSegmentResourceDroppedDoesNotWaitForAnotherViewsQuery(t *testing.T) {
	mockey.PatchConvey("one View's Dropped does not wait for a shared owner retained by another query", t, func() {
		f := setupResourceTest(t, nil)
		first := f.acquire(1, 1, 1)
		f.await(t, 1)
		other := f.acquire(2, 1, 1)
		f.await(t, 1)
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), other.Key, other.View, 0)
		require.NoError(t, err)
		otherDropped := f.beginDrop(other)
		f.drop(t, first)
		requireDropPending(t, otherDropped)
		// With only the other query remaining, a new View may reuse the same owner.
		next := f.acquire(3, 1, 1)
		f.await(t, 1)
		handles[0].Release()
		awaitShutdown(t, otherDropped)
		require.EqualValues(t, 1, f.loads.Load())
		require.Zero(t, f.releases.Load())
		f.drop(t, next)
		require.EqualValues(t, 1, f.releases.Load())
	})
}
