package qnview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type lifetimeSubscription struct{ SegmentLoadInfoSubscription }

func (*lifetimeSubscription) Close() { panic("mockey") }

type lifetimeRegistration struct{ TransformRegistration }

func (*lifetimeRegistration) WaitCatchup(context.Context) error { panic("mockey") }
func (*lifetimeRegistration) Unregister()                       { panic("mockey") }

func patchLifetime(t *testing.T, mock *mockey.Mocker) {
	t.Helper()
	t.Cleanup(func() { mock.UnPatch() })
}

func releaseLifetimeView(t *testing.T, mgr *QueryViewSegmentManager, key qviews.QueryViewKey) {
	t.Helper()
	done := make(chan struct{})
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { close(done) }})
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("view release timed out")
	}
}

func TestSharedSegmentRetainsPhysicalSubscription(t *testing.T) {
	for _, catchingUp := range []bool{false, true} {
		name := "loaded"
		if catchingUp {
			name = "catching-up"
		}
		t.Run(name, func(t *testing.T) {
			var closed atomic.Bool
			var loads atomic.Int32
			catchup := make(chan struct{})
			registered := make(chan struct{}, 1)
			updated := make(chan SegmentLoadInfoSnapshot, 1)
			patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).Acquire).Return(instantTransformGuard{}, nil).Build())
			patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).Return(&lifetimeRegistration{}, nil).Build())
			patchLifetime(t, mockey.Mock((*lifetimeRegistration).WaitCatchup).To(func(_ *lifetimeRegistration, ctx context.Context) error {
				registered <- struct{}{}
				select {
				case <-catchup:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}).Build())
			patchLifetime(t, mockey.Mock((*lifetimeRegistration).Unregister).To(func(*lifetimeRegistration) {}).Build())
			patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Load).To(func(*fakePhysicalLoader, context.Context, *querypb.SegmentLoadInfo, CollectionRuntime) (TransformSegment, error) {
				loads.Add(1)
				return &fakeTransformSegment{id: 1000, partitionID: 10}, nil
			}).Build())
			patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Update).To(func(_ *fakePhysicalLoader, _ context.Context, _ TransformSegment, _ CollectionRuntime, snapshot SegmentLoadInfoSnapshot, _ SegmentUpdateAction) error {
				updated <- snapshot
				return nil
			}).Build())
			subscription := make(chan SegmentLoadInfoSubscriptionOption, 1)
			patchLifetime(t, mockey.Mock((*fakeSegmentLoadInfoStream).Subscribe).To(func(_ *fakeSegmentLoadInfoStream, opt SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
				subscription <- opt
				return &lifetimeSubscription{}
			}).Build())
			patchLifetime(t, mockey.Mock((*lifetimeSubscription).Close).To(func(*lifetimeSubscription) { closed.Store(true) }).Build())
			patchLifetime(t, mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(*fakeQueryViewCollectionRuntimeManager, context.Context, *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
				return &fakeCollectionRuntimeGuard{collectionID: testCollectionID}, false, nil
			}).Build())
			scheduler := nodescheduler.New(4)
			t.Cleanup(scheduler.Close)
			physical := newTestSegmentPreparerWithStream(scheduler, &fakePhysicalLoader{}, &fakeSegmentLoadInfoStream{})
			mgr := newTestManagerWithPreparation(t, scheduler, physical, &fakeTransformLogBuffer{}, 1, &fakeQueryViewCollectionRuntimeManager{})
			view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
			acquire := func(version int64) (qviews.QueryViewKey, chan struct{}) {
				meta := buildHandlerTestMeta(version)
				meta.Version.DataVersion = &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}
				key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
				ready := make(chan struct{}, 1)
				mgr.Acquire(AcquireSegments{Key: key, Meta: meta, View: view, OnReady: func(map[int64][]int64) { ready <- struct{}{} }})
				return key, ready
			}
			first, readyFirst := acquire(1)
			opt := <-subscription
			snapshot := SegmentLoadInfoSnapshot{DataVersion: opt.DataVersion, CollectionID: testCollectionID, SegmentID: 1000, Revision: SegmentLoadInfoRevision{Revision: 1}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: 1000}}
			require.NoError(t, opt.Handler.Handle(snapshot))
			<-registered
			if !catchingUp {
				close(catchup)
				<-readyFirst
			}
			second, readySecond := acquire(2)
			require.Eventually(t, func() bool {
				physical.mu.Lock()
				defer physical.mu.Unlock()
				return physical.views[second] != nil
			}, time.Second, time.Millisecond)
			releaseLifetimeView(t, mgr, first)
			require.False(t, closed.Load(), "replacement view still needs metadata updates")
			if catchingUp {
				close(catchup)
			}
			<-readySecond
			snapshot.Revision.Revision = 2
			require.NoError(t, opt.Handler.Handle(snapshot))
			select {
			case got := <-updated:
				require.Equal(t, uint64(2), got.Revision.Revision)
			case <-time.After(3 * time.Second):
				t.Fatal("replacement view lost metadata updates")
			}
			releaseLifetimeView(t, mgr, second)
			require.True(t, closed.Load())
			require.Equal(t, int32(1), loads.Load(), "shared segment must only load once")
		})
	}
}

func TestReleaseCancelsPendingPreparation(t *testing.T) {
	entered, finished := make(chan struct{}), make(chan struct{})
	patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).Acquire).Return(instantTransformGuard{}, nil).Build())
	patchLifetime(t, mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(_ *fakeQueryViewCollectionRuntimeManager, ctx context.Context, _ *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
		close(entered)
		<-ctx.Done()
		close(finished)
		return nil, true, ctx.Err()
	}).Build())
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	mgr := NewQueryViewSegmentManager(QueryViewSegmentManagerConfig{Scheduler: scheduler, Buffer: &fakeTransformLogBuffer{}, Collections: &fakeQueryViewCollectionRuntimeManager{}, CatchupConcurrency: 1})
	meta, view := buildHandlerTestMeta(1), buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	mgr.Acquire(AcquireSegments{Key: key, Meta: meta, View: view})
	<-entered
	releaseLifetimeView(t, mgr, key)
	waitGenerationEvent(t, finished)
	mgr.mu.Lock()
	require.Empty(t, mgr.views)
	require.Empty(t, mgr.segments)
	mgr.mu.Unlock()
}

// A pending view must own the same instance before collection metadata returns.
// This is the handoff that two independently populated reference tables lost.
func TestPendingViewOwnsSharedSegment(t *testing.T) {
	for _, stage := range []string{"loaded", "loading", "metadata-failure", "canceled"} {
		t.Run(stage, func(t *testing.T) {
			var loads, releases, closes atomic.Int32
			segment := &fakeTransformSegment{id: 1000, partitionID: 10}
			loadEntered, finishLoad := make(chan struct{}), make(chan struct{})
			metadataEntered, finishMetadata := make(chan struct{}), make(chan struct{})
			patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Load).To(func(_ *fakePhysicalLoader, ctx context.Context, _ *querypb.SegmentLoadInfo, _ CollectionRuntime) (TransformSegment, error) {
				loads.Add(1)
				close(loadEntered)
				if stage == "loading" {
					select {
					case <-finishLoad:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return segment, nil
			}).Build())
			patchLifetime(t, mockey.Mock((*fakeTransformSegment).Release).To(func(*fakeTransformSegment, context.Context) error { releases.Add(1); return nil }).Build())
			patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).Acquire).Return(instantTransformGuard{}, nil).Build())
			patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).Return(instantTransformRegistration{}, nil).Build())
			patchLifetime(t, mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(_ *fakeQueryViewCollectionRuntimeManager, ctx context.Context, view *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
				if view.QueryViewKey().QueryViewVersion.QueryVersion == 2 {
					close(metadataEntered)
					select {
					case <-finishMetadata:
					case <-ctx.Done():
						return nil, true, ctx.Err()
					}
					if stage == "metadata-failure" {
						return nil, false, merr.WrapErrServiceInternalMsg("injected metadata failure")
					}
				}
				return &fakeCollectionRuntimeGuard{}, false, nil
			}).Build())
			options := make(chan SegmentLoadInfoSubscriptionOption, 2)
			patchLifetime(t, mockey.Mock((*fakeSegmentLoadInfoStream).Subscribe).To(func(_ *fakeSegmentLoadInfoStream, opt SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
				options <- opt
				return &lifetimeSubscription{}
			}).Build())
			patchLifetime(t, mockey.Mock((*lifetimeSubscription).Close).To(func(*lifetimeSubscription) { closes.Add(1) }).Build())
			scheduler := nodescheduler.New(4)
			t.Cleanup(scheduler.Close)
			manager := NewQueryViewSegmentManager(QueryViewSegmentManagerConfig{Scheduler: scheduler, Loader: &fakePhysicalLoader{}, LoadInfoStream: &fakeSegmentLoadInfoStream{}, Buffer: &fakeTransformLogBuffer{}, Collections: &fakeQueryViewCollectionRuntimeManager{}, CatchupConcurrency: 1})
			view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
			acquire := func(version int64, ready, failed chan struct{}) qviews.QueryViewKey {
				meta := buildHandlerTestMeta(version)
				meta.Version.DataVersion = &viewpb.DataVersion{}
				key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
				manager.Acquire(AcquireSegments{Key: key, Meta: meta, View: view, OnReady: func(map[int64][]int64) { ready <- struct{}{} }, OnUnrecoverable: func() { failed <- struct{}{} }})
				return key
			}
			firstReady, secondReady, failed := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{}, 2)
			first := acquire(1, firstReady, failed)
			var option SegmentLoadInfoSubscriptionOption
			select {
			case option = <-options:
			case <-time.After(time.Second):
				t.Fatal("no subscription")
			}
			require.NoError(t, option.Handler.Handle(SegmentLoadInfoSnapshot{SegmentID: 1000, Revision: SegmentLoadInfoRevision{Revision: 1}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: 1000}}))
			waitGenerationEvent(t, loadEntered)
			if stage != "loading" {
				waitGenerationEvent(t, firstReady)
			}
			second := acquire(2, secondReady, failed)
			waitGenerationEvent(t, metadataEntered)
			manager.mu.Lock()
			state := manager.views[first].states[1000]
			require.Same(t, state, manager.views[second].states[1000])
			require.Len(t, state.refs, 2)
			manager.mu.Unlock()
			dropped := make(chan struct{})
			manager.Release(ReleaseSegments{Key: first, OnDropped: func() { close(dropped) }})
			require.Zero(t, releases.Load())
			require.Zero(t, closes.Load())
			if stage == "loading" {
				close(finishLoad)
			}
			waitGenerationEvent(t, dropped)
			if stage == "canceled" {
				releaseLifetimeView(t, manager, second)
			} else {
				close(finishMetadata)
				if stage == "metadata-failure" {
					waitGenerationEvent(t, failed)
					require.Zero(t, releases.Load(), "failure must not release the view's reference")
				} else {
					waitGenerationEvent(t, secondReady)
					handles, err := manager.AcquireSealedSegmentHandles(context.Background(), second, view)
					require.NoError(t, err)
					require.Same(t, segment, handles[0].Segment())
					handles[0].Release()
				}
				releaseLifetimeView(t, manager, second)
			}
			require.Eventually(t, func() bool { return releases.Load() == 1 }, time.Second, time.Millisecond)
			// The snapshot callback can run before Subscribe returns. If the view
			// is released first, subscribeSegments closes the late subscription.
			require.Eventually(t, func() bool { return closes.Load() == 1 }, time.Second, time.Millisecond)
			require.EqualValues(t, 1, loads.Load())
			manager.mu.Lock()
			require.Empty(t, manager.views)
			require.Empty(t, manager.segments)
			manager.mu.Unlock()
		})
	}
}

func TestPendingSubscriptionRetainsPredecessorGuard(t *testing.T) {
	oldGuard := &fakeTransformLogGuard{}
	var released atomic.Int32
	oldPrepared, subscribing, canceled := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
	patchLifetime(t, mockey.Mock((*fakeTransformLogGuard).Release).To(func(*fakeTransformLogGuard) { released.Add(1) }).Build())
	patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).Acquire).To(func(_ *fakeTransformLogBuffer, ctx context.Context, view *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
		if view.QueryViewKey().QueryViewVersion.QueryVersion == 1 {
			return oldGuard, nil
		}
		close(subscribing)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}).Build())
	patchLifetime(t, mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).To(func(*fakeQueryViewCollectionRuntimeManager, context.Context, *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
		select {
		case oldPrepared <- struct{}{}:
		default:
		}
		return nil, true, merr.WrapErrServiceUnavailableMsg("metadata pending")
	}).Build())
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	manager := NewQueryViewSegmentManager(QueryViewSegmentManagerConfig{Scheduler: scheduler, Buffer: &fakeTransformLogBuffer{}, Collections: &fakeQueryViewCollectionRuntimeManager{}, CatchupConcurrency: 1})
	first, _, _ := acquireGenerationView(manager, 1)
	waitGenerationEvent(t, oldPrepared)
	second, _, _ := acquireGenerationView(manager, 2)
	waitGenerationEvent(t, subscribing)
	releaseLifetimeView(t, manager, first)
	require.Zero(t, released.Load(), "new reference must bridge the asynchronous subscription handoff")
	releaseLifetimeView(t, manager, second)
	waitGenerationEvent(t, canceled)
	require.EqualValues(t, 1, released.Load())
	manager.mu.Lock()
	require.Empty(t, manager.views)
	require.Empty(t, manager.segments)
	manager.mu.Unlock()
}

func TestLastViewReleaseWaitsForNativeReopen(t *testing.T) {
	var released atomic.Int32
	entered, resume := make(chan struct{}), make(chan struct{})
	segment := &fakeTransformSegment{id: 1000, partitionID: 10}
	patchLifetime(t, mockey.Mock((*fakeTransformSegment).Release).To(func(*fakeTransformSegment, context.Context) error { released.Add(1); return nil }).Build())
	patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Load).Return(segment, nil).Build())
	patchLifetime(t, mockey.Mock((*fakePhysicalLoader).Update).To(func(*fakePhysicalLoader, context.Context, TransformSegment, CollectionRuntime, SegmentLoadInfoSnapshot, SegmentUpdateAction) error {
		close(entered)
		<-resume // Native work cannot be interrupted by context cancellation.
		require.Zero(t, released.Load())
		return nil
	}).Build())
	options := make(chan SegmentLoadInfoSubscriptionOption, 1)
	patchLifetime(t, mockey.Mock((*fakeSegmentLoadInfoStream).Subscribe).To(func(_ *fakeSegmentLoadInfoStream, option SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
		options <- option
		return &lifetimeSubscription{}
	}).Build())
	patchLifetime(t, mockey.Mock((*lifetimeSubscription).Close).Return().Build())
	scheduler := nodescheduler.New(2)
	t.Cleanup(scheduler.Close)
	manager := newTestSegmentPreparerWithStream(scheduler, &fakePhysicalLoader{}, &fakeSegmentLoadInfoStream{})
	meta, view := buildHandlerTestMeta(1), &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	ready := make(chan struct{}, 1)
	manager.Acquire(segmentPreparationRequest{Key: key, Meta: meta, View: view, OnLoaded: func([]TransformSegment) { ready <- struct{}{} }})
	option := <-options
	snapshot := SegmentLoadInfoSnapshot{SegmentID: 1000, DataVersion: option.DataVersion, Revision: SegmentLoadInfoRevision{Revision: 1}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: 1000}}
	require.NoError(t, option.Handler.Handle(snapshot))
	waitGenerationEvent(t, ready)
	snapshot.Revision.Revision = 2
	require.NoError(t, option.Handler.Handle(snapshot))
	waitGenerationEvent(t, entered)
	dropped := make(chan struct{})
	manager.Release(ReleaseSegments{Key: key, OnDropped: func() { close(dropped) }})
	require.Zero(t, released.Load())
	select {
	case <-dropped:
		t.Fatal("drop completed while native Reopen still owns the instance")
	default:
	}
	close(resume)
	waitGenerationEvent(t, dropped)
	require.EqualValues(t, 1, released.Load())
}
