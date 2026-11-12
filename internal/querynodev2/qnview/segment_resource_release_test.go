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
		f.manager.mu.Lock()
		require.True(t, f.manager.resources[1000].closing, "the last binding fences the owner despite its active query")
		f.manager.mu.Unlock()
		requireDropPending(t, dropped)
		requireDropPending(t, entered)
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

func TestSegmentResourceClosingPreservesQueryMVCCWait(t *testing.T) {
	mockey.PatchConvey("Closing cancels preparation but keeps the registration until the pinned query finishes", t, func() {
		f := setupResourceTest(t, nil)
		caughtUp := make(chan struct{})
		close(caughtUp)
		f.registerMock.Return(&fakeTransformRegistration{waitCh: caughtUp}, nil)
		unregistered := make(chan struct{})
		mockey.Mock((*fakeTransformRegistration).Unregister).To(func(*fakeTransformRegistration) {
			close(unregistered)
		}).Build()
		entered, visible := make(chan struct{}), make(chan struct{})
		mockey.Mock((*fakeTransformRegistration).WaitTransformVisible).To(func(_ *fakeTransformRegistration, ctx context.Context, _ uint64) error {
			close(entered)
			select {
			case <-visible:
				return nil
			case <-unregistered:
				return context.Canceled
			case <-ctx.Done():
				return ctx.Err()
			}
		}).Build()
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		acquired := make(chan struct{})
		var handles []SealedSegmentHandle
		var err error
		go func() {
			handles, err = f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, 100)
			close(acquired)
		}()
		awaitShutdown(t, entered)
		dropped := f.beginDrop(req)
		f.manager.mu.Lock()
		preparationCtx := f.manager.resources[1000].bases[0].ctx
		f.manager.mu.Unlock()
		require.ErrorIs(t, preparationCtx.Err(), context.Canceled)
		requireDropPending(t, unregistered)
		close(visible)
		awaitShutdown(t, acquired)
		require.NoError(t, err)
		requireDropPending(t, dropped)
		requireDropPending(t, unregistered)
		handles[0].Release()
		awaitShutdown(t, dropped)
		awaitShutdown(t, unregistered)
	})
}

func TestSegmentResourceDroppedAllowsSharedTakeover(t *testing.T) {
	mockey.PatchConvey("a successor may retain the segment while the old query finishes", t, func() {
		f := setupResourceTest(t, nil)
		first := f.acquire(1, 1, 1)
		f.await(t, 1)
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), first.Key, first.View, 0)
		require.NoError(t, err)
		next := f.acquire(2, 1, 1)
		f.await(t, 1)
		dropped := f.beginDrop(first)
		requireDropPending(t, dropped)
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

func TestSegmentResourceLastBindingWaitsForDetachedQuery(t *testing.T) {
	mockey.PatchConvey("the last binding closes the owner even while another View's query is draining", t, func() {
		f := setupResourceTest(t, nil)
		first := f.acquire(1, 1, 1)
		f.await(t, 1)
		other := f.acquire(2, 1, 1)
		f.await(t, 1)
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), other.Key, other.View, 0)
		require.NoError(t, err)
		otherDropped := f.beginDrop(other)
		firstDropped := f.beginDrop(first)
		requireDropPending(t, firstDropped)
		requireDropPending(t, otherDropped)
		// A query keeps the old incarnation alive, but cannot admit new bindings.
		f.manager.mu.Lock()
		old := f.manager.resources[1000]
		require.True(t, old.closing)
		f.manager.mu.Unlock()
		next := f.acquire(3, 1, 1)
		select {
		case <-f.ready:
			t.Fatal("new View reused an incarnation whose last binding has left")
		case <-time.After(20 * time.Millisecond):
		}
		require.EqualValues(t, 1, f.loads.Load())
		require.Zero(t, f.releases.Load())
		handles[0].Release()
		awaitShutdown(t, otherDropped)
		awaitShutdown(t, firstDropped)
		f.await(t, 1)
		f.manager.mu.Lock()
		require.NotSame(t, old, f.manager.resources[1000])
		f.manager.mu.Unlock()
		require.EqualValues(t, 2, f.loads.Load())
		require.EqualValues(t, 1, f.releases.Load())
		f.drop(t, next)
		require.EqualValues(t, 2, f.releases.Load())
	})
}
