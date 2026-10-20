package qnview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func awaitShutdown(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish")
	}
}

func shutdownViewState(h *QNQueryViewHandler, key qviews.QueryViewKey) qviews.QueryViewState {
	h.mu.Lock()
	shard := h.shards[key.ShardID]
	h.mu.Unlock()
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return shard.views[key.QueryViewVersion].sm.State()
}

func TestQueryViewStoppingPreservesReadyAndRejectsPreparation(t *testing.T) {
	mockey.PatchConvey("stopping preserves serving views and cancels preparation exactly once", t, func() {
		acquired := make(chan AcquireSegments, 4)
		released := make(chan ReleaseSegments, 4)
		mockey.Mock((*mockSegmentManager).Acquire).To(func(_ *mockSegmentManager, req AcquireSegments) { acquired <- req }).Build()
		mockey.Mock((*mockSegmentManager).Release).To(func(_ *mockSegmentManager, req ReleaseSegments) { released <- req }).Build()
		mockey.Mock((*mockSegmentManager).Close).Return().Build()
		h := NewQNQueryViewHandler(&mockSegmentManager{})
		reports := make(chan qviews.QueryViewState, 20)
		report := func(view qviews.QueryViewAtWorkNode) { reports <- view.State() }
		ready, preparing := newPreparingQNView(1, 1), newPreparingQNView(1, 2)
		h.ApplyViews([]handler.ApplyView{{View: ready, OnReport: report}, {View: preparing, OnReport: report}})
		first, second := <-acquired, <-acquired
		first.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})
		require.Equal(t, qviews.QueryViewStateReady, <-reports)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- h.Close(ctx) }()
		require.Equal(t, qviews.QueryViewStateUnrecoverable, <-reports)
		release := <-released
		require.Equal(t, second.Key, release.Key)
		release.OnDropped() // Early cleanup must not erase Unrecoverable before Coord sees it.
		second.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})
		require.Equal(t, qviews.QueryViewStateReady, shutdownViewState(h, first.Key))
		lease, err := h.AcquireReadyView(t.Context(), first.Key.ShardID, first.Key.QueryViewVersion)
		require.NoError(t, err)
		h.ApplyViews([]handler.ApplyView{{View: ready, OnReport: report}, {View: preparing, OnReport: report}, {View: newPreparingQNView(1, 3), OnReport: report}})
		require.Equal(t, qviews.QueryViewStateReady, <-reports)
		require.Equal(t, qviews.QueryViewStateUnrecoverable, <-reports)
		require.Equal(t, qviews.QueryViewStateUnrecoverable, <-reports)
		require.Empty(t, acquired)
		require.Empty(t, released)

		closed := make(chan struct{})
		go func() { require.NoError(t, h.Close(t.Context())); close(closed) }()
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		awaitShutdown(t, h.Closing())
		require.Eventually(t, func() bool {
			return shutdownViewState(h, first.Key) == qviews.QueryViewStateDropping
		}, time.Second, time.Millisecond)
		require.Empty(t, released) // Active lease still pins the Ready view.
		select {
		case <-closed:
			t.Fatal("closed with an active lease")
		default:
		}
		lease.Release()
		release = <-released
		require.Equal(t, first.Key, release.Key)
		release.OnDropped()
		awaitShutdown(t, closed)
		require.NoError(t, h.Close(t.Context()))
		require.Empty(t, released) // Preparing cleanup was not repeated.
		h.ApplyViews([]handler.ApplyView{{View: newPreparingQNView(1, 4), OnReport: report}})
		require.Empty(t, acquired)
	})
}

func TestQueryViewStoppingSerializesWithReadyCallback(t *testing.T) {
	mockey.PatchConvey("readiness either wins before stopping or is rejected permanently", t, func() {
		acquired := make(chan AcquireSegments, 1)
		mockey.Mock((*mockSegmentManager).Acquire).To(func(_ *mockSegmentManager, req AcquireSegments) { acquired <- req }).Build()
		mockey.Mock((*mockSegmentManager).Release).To(func(_ *mockSegmentManager, req ReleaseSegments) { go req.OnDropped() }).Build()
		mockey.Mock((*mockSegmentManager).Close).Return().Build()
		for i := 0; i < 50; i++ {
			h := NewQNQueryViewHandler(&mockSegmentManager{})
			h.ApplyViews([]handler.ApplyView{{View: newPreparingQNView(1, 1)}})
			req := <-acquired
			done := make(chan struct{})
			go func() { req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}}); close(done) }()
			h.beginStopping()
			awaitShutdown(t, done)
			before := shutdownViewState(h, req.Key)
			req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})
			require.Equal(t, before, shutdownViewState(h, req.Key))
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_ = h.Close(ctx)
			require.NoError(t, h.Close(t.Context()))
		}
	})
}

func TestQueryViewCloseWaitsForNonReadyViews(t *testing.T) {
	for _, state := range []string{"preparing", "unrecoverable", "dropping"} {
		t.Run(state, func(t *testing.T) {
			mockey.PatchConvey("no Ready views does not mean all Views have finished Dropped", t, func() {
				acquired := make(chan AcquireSegments, 1)
				released := make(chan ReleaseSegments, 1)
				mockey.Mock((*mockSegmentManager).Acquire).To(func(_ *mockSegmentManager, req AcquireSegments) { acquired <- req }).Build()
				mockey.Mock((*mockSegmentManager).Release).To(func(_ *mockSegmentManager, req ReleaseSegments) { released <- req }).Build()
				mockey.Mock((*mockSegmentManager).Close).Return().Build()
				h := NewQNQueryViewHandler(&mockSegmentManager{})
				h.ApplyViews([]handler.ApplyView{{View: newPreparingQNView(1, 1)}})
				req := <-acquired
				switch state {
				case "unrecoverable":
					req.OnUnrecoverable()
				case "dropping":
					h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1)}})
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- h.Close(ctx) }()
				if state == "preparing" {
					// Stopping releases preparation early, but must retain its
					// Unrecoverable report until Coord explicitly withdraws it.
					release := <-released
					release.OnDropped()
					require.Equal(t, qviews.QueryViewStateUnrecoverable, shutdownViewState(h, req.Key))
				}
				requireDropPending(t, h.Closing())
				if state != "dropping" {
					h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1)}})
				}
				if state != "preparing" {
					release := <-released
					requireDropPending(t, h.Closing())
					release.OnDropped()
				}
				require.NoError(t, <-result)
				require.Empty(t, released)
			})
		})
	}
}

func TestSegmentResourceCloseWaitsForDetachedQueryHandles(t *testing.T) {
	mockey.PatchConvey("both Dropped and Close wait for existing query handles", t, func() {
		f := setupResourceTest(t, nil)
		var buffersClosed, collectionsReleased atomic.Int32
		mockey.Mock((*fakeTransformLogBuffer).Close).To(func(*fakeTransformLogBuffer) { buffersClosed.Add(1) }).Build()
		mockey.Mock((*fakeCollectionRuntimeGuard).Release).To(func(*fakeCollectionRuntimeGuard) { collectionsReleased.Add(1) }).Build()
		req := f.acquire(1, 1, 1)
		f.await(t, 1)
		handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, 0)
		require.NoError(t, err)
		require.Len(t, handles, 1)
		dropped := f.beginDrop(req) // Admission is withdrawn, but Dropped must wait for the query.
		done := make(chan struct{})
		go func() { f.manager.Close(); close(done) }()
		require.Eventually(t, func() bool { f.manager.mu.Lock(); defer f.manager.mu.Unlock(); return f.manager.closed }, time.Second, time.Millisecond)
		select {
		case <-done:
			t.Fatal("closed while native query handle is live")
		default:
		}
		require.Zero(t, collectionsReleased.Load())
		require.Zero(t, buffersClosed.Load())
		handles[0].Release()
		awaitShutdown(t, dropped)
		awaitShutdown(t, done)
		require.EqualValues(t, 1, f.releases.Load())
		require.EqualValues(t, 1, collectionsReleased.Load())
		require.EqualValues(t, 1, buffersClosed.Load())
		f.manager.Close()
		rejected := make(chan struct{})
		next := f.request(2, 1, 1)
		next.OnUnrecoverable = func() { close(rejected) }
		bind := mockey.Mock((*fakeTransformLogBuffer).Bind).Return(nil, nil).Build()
		f.manager.Acquire(next)
		awaitShutdown(t, rejected)
		require.Zero(t, bind.Times())
		require.EqualValues(t, 1, buffersClosed.Load())
	})
}

func TestSegmentResourceCloseWaitsForLateLoad(t *testing.T) {
	mockey.PatchConvey("a canceled native load must finish and release its result before Close returns", t, func() {
		f := setupResourceTest(t, nil)
		started, canceled, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
		f.loadMock.To(func(_ *resourceTestLoader, ctx context.Context, info *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-finish
			return &fakeTransformSegment{id: info.SegmentID, partitionID: info.PartitionID}, nil
		})
		f.acquire(1, 1, 1)
		awaitShutdown(t, started)
		done := make(chan struct{})
		go func() { f.manager.Close(); close(done) }()
		awaitShutdown(t, canceled)
		select {
		case <-done:
			t.Fatal("closed before native load returned")
		default:
		}
		close(finish)
		awaitShutdown(t, done)
		require.EqualValues(t, 1, f.releases.Load())
		require.Empty(t, f.ready)
	})
}

func TestSegmentResourceCloseWaitsForEmptyViewDependencies(t *testing.T) {
	mockey.PatchConvey("even an empty assignment retains dependencies until late acquisition returns", t, func() {
		f := setupResourceTest(t, nil)
		started, canceled, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var collectionsReleased, guardsReleased atomic.Int32
		mockey.Mock((*fakeCollectionRuntimeGuard).Release).To(func(*fakeCollectionRuntimeGuard) { collectionsReleased.Add(1) }).Build()
		mockey.Mock((*fakeTransformLogGuard).Release).To(func(*fakeTransformLogGuard) { guardsReleased.Add(1) }).Build()
		f.dependencyMock.To(func(_ *fakeQueryViewCollectionRuntimeManager, ctx context.Context, _ *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-finish
			return &fakeCollectionRuntimeGuard{}, false, nil
		})
		req := f.request(1, 1, 1)
		req.View.Partitions = nil
		f.manager.Acquire(req)
		awaitShutdown(t, started)
		done := make(chan struct{})
		go func() { f.manager.Close(); close(done) }()
		awaitShutdown(t, canceled)
		select {
		case <-done:
			t.Fatal("closed with dependency acquisition in flight")
		default:
		}
		close(finish)
		awaitShutdown(t, done)
		require.EqualValues(t, 1, collectionsReleased.Load())
		require.EqualValues(t, 1, guardsReleased.Load())
		require.Empty(t, f.ready)
	})
}

func TestQueryViewStoppingWaitsForAdmittedAcquire(t *testing.T) {
	mockey.PatchConvey("a synchronous lazy manager initialization cannot escape stopping", t, func() {
		entered, finish := make(chan struct{}), make(chan struct{})
		var releases atomic.Int32
		mockey.Mock((*mockSegmentManager).Close).Return().Build()
		mockey.Mock((*mockSegmentManager).Acquire).To(func(*mockSegmentManager, AcquireSegments) { close(entered); <-finish }).Build()
		mockey.Mock((*mockSegmentManager).Release).To(func(_ *mockSegmentManager, req ReleaseSegments) { releases.Add(1); go req.OnDropped() }).Build()
		h := NewQNQueryViewHandler(&mockSegmentManager{})
		applied := make(chan struct{})
		go func() { h.ApplyViews([]handler.ApplyView{{View: newPreparingQNView(1, 1)}}); close(applied) }()
		awaitShutdown(t, entered)
		stopped := make(chan struct{})
		go func() { h.beginStopping(); close(stopped) }()
		require.Eventually(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.stopping }, time.Second, time.Millisecond)
		select {
		case <-stopped:
			t.Fatal("stopping escaped an admitted Acquire")
		default:
		}
		close(finish)
		awaitShutdown(t, applied)
		awaitShutdown(t, stopped)
		h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1)}})
		require.NoError(t, h.Close(t.Context()))
		require.EqualValues(t, 1, releases.Load())
	})
}

func TestQueryViewCloseOwnsMigrationAndResourceDrain(t *testing.T) {
	for _, mode := range []string{"coord_drop", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			mockey.PatchConvey("Close success requires the last task handle, including after a timed-out wait", t, func() {
				f := setupResourceTest(t, nil)
				h := NewQNQueryViewHandler(f.manager)
				req := f.request(1, 1, 1)
				view := qviews.NewQueryViewAtQueryNode(req.Meta, req.View)
				reports := make(chan qviews.QueryViewState, 4)
				h.ApplyViews([]handler.ApplyView{{View: view, OnReport: func(v qviews.QueryViewAtWorkNode) { reports <- v.State() }}})
				require.Equal(t, qviews.QueryViewStateReady, <-reports)
				handles, err := f.manager.AcquireSealedSegmentHandles(t.Context(), req.Key, req.View, 0)
				require.NoError(t, err)
				require.Len(t, handles, 1)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if mode == "deadline" {
					cancel()
					ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
					defer cancel()
				}
				result := make(chan error, 1)
				go func() { result <- h.Close(ctx) }()
				if mode == "coord_drop" {
					require.Eventually(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.stopping }, time.Second, time.Millisecond)
					require.Equal(t, qviews.QueryViewStateReady, shutdownViewState(h, req.Key))
					select {
					case <-h.Closing():
						t.Fatal("closed before migration")
					default:
					}
					dropped := qviews.NewQueryViewAtQueryNode(buildHandlerTestMeta(1), req.View)
					pb := dropped.IntoProto()
					pb.Meta.State = viewpb.QueryViewState_QueryViewStateDropped
					h.ApplyViews([]handler.ApplyView{{View: qviews.NewQueryViewAtWorkNodeFromProto(pb)}})
				} else {
					require.ErrorIs(t, <-result, context.DeadlineExceeded)
				}
				if mode == "deadline" {
					awaitShutdown(t, h.Closing())
				} else {
					require.Equal(t, qviews.QueryViewStateDropping, shutdownViewState(h, req.Key))
					select {
					case <-h.Closing():
						t.Fatal("ViewSync closed before Dropped could be reported")
					default:
					}
				}
				select {
				case <-h.closed:
					t.Fatal("Close finished with a live task handle")
				default:
				}
				require.Zero(t, f.releases.Load())
				handles[0].Release()
				require.NoError(t, h.Close(t.Context()))
				if mode == "coord_drop" {
					require.NoError(t, <-result)
				}
				require.EqualValues(t, 1, f.releases.Load())
			})
		})
	}
}
