package qnview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestPreparationRetainsEarlierFrontierAndReleasesAbandonedRange(t *testing.T) {
	guards := []*fakeTransformLogGuard{{}, {}}
	var releases [2]int
	patchLifetime(t, mockey.Mock((*fakeTransformLogGuard).Release).To(func(g *fakeTransformLogGuard) {
		for i, guard := range guards {
			if g == guard {
				releases[i]++
			}
		}
	}).Build())
	manager := &QueryViewSegmentManager{views: make(map[qviews.QueryViewKey]*queryViewRef), segments: make(map[int64]*segmentState)}
	view := &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	var refs []*queryViewRef
	var keys []qviews.QueryViewKey
	for i, frontier := range []uint64{100, 40} {
		meta := buildHandlerTestMeta(int64(i + 1))
		meta.TransformStartAfterTimetick = frontier
		req := AcquireSegments{Key: qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(), Meta: meta, View: view}
		ref, ok := manager.recordPendingAcquire(context.Background(), req, func() {})
		require.True(t, ok)
		if ref.transformGuard != nil {
			ref.transformGuard.Release()
		}
		ref.transformGuard = newRetainedTransformGuard(guards[i], frontier)
		_, _, ok = manager.activateAcquire(req, ref, nil)
		require.True(t, ok)
		refs, keys = append(refs, ref), append(keys, req.Key)
	}
	// A new async acquisition must bridge the minimum retained frontier, even
	// when another live view already has a later frontier.
	meta := buildHandlerTestMeta(3)
	meta.TransformStartAfterTimetick = 80
	req := AcquireSegments{Key: qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(), Meta: meta, View: view}
	pending, ok := manager.recordPendingAcquire(context.Background(), req, func() {})
	require.True(t, ok)
	require.Same(t, refs[1].transformGuard, pending.transformGuard)
	manager.mu.Lock()
	detachedPending := manager.detachViewLocked(req.Key)
	manager.mu.Unlock()
	detachedPending.releaseTransform()
	detachedPending.unregister()
	state := manager.segments[1000]
	require.EqualValues(t, 40, state.replayStart)
	require.Same(t, refs[1].transformGuard, state.replayGuard)
	// The higher guard now belongs only to its view; the lower one also pins
	// preparation, even after its view is removed.
	require.EqualValues(t, 1, refs[0].transformGuard.refs.Load())
	require.EqualValues(t, 2, refs[1].transformGuard.refs.Load())
	for i := 1; i >= 0; i-- {
		manager.mu.Lock()
		detached := manager.detachViewLocked(keys[i])
		manager.mu.Unlock()
		detached.releaseTransform()
		detached.unregister()
		if i == 1 {
			require.Zero(t, releases[1], "surviving shared preparation still needs the earlier range")
		}
	}
	require.Equal(t, [2]int{1, 1}, releases)
	require.Empty(t, manager.segments)
}

func TestQueuedLoadRetainsReplayRangeAfterOriginatingViewDrops(t *testing.T) {
	for _, failRegistration := range []bool{false, true} {
		name := "registered"
		if failRegistration {
			name = "registration-failed"
		}
		t.Run(name, func(t *testing.T) {
			oldGuard, newGuard := &fakeTransformLogGuard{}, &fakeTransformLogGuard{}
			var oldReleased, newReleased atomic.Bool
			patchLifetime(t, mockey.Mock((*fakeTransformLogGuard).Release).To(func(g *fakeTransformLogGuard) {
				if g == oldGuard {
					oldReleased.Store(true)
				} else {
					newReleased.Store(true)
				}
			}).Build())
			patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).Acquire).To(func(_ *fakeTransformLogBuffer, _ context.Context, view *qviews.QueryViewAtQueryNode) (TransformLogGuard, error) {
				if view.IntoProto().GetMeta().GetTransformStartAfterTimetick() == 0 {
					return oldGuard, nil
				}
				return newGuard, nil
			}).Build())
			patchLifetime(t, mockey.Mock((*fakeQueryViewCollectionRuntimeManager).Acquire).Return(&fakeCollectionRuntimeGuard{}, false, nil).Build())
			options := make(chan SegmentLoadInfoSubscriptionOption, 2)
			patchLifetime(t, mockey.Mock((*fakeSegmentLoadInfoStream).Subscribe).To(func(_ *fakeSegmentLoadInfoStream, option SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
				options <- option
				return &lifetimeSubscription{}
			}).Build())
			patchLifetime(t, mockey.Mock((*lifetimeSubscription).Close).Return().Build())
			entered, proceed := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			t.Cleanup(func() { unblock.Do(func() { close(proceed) }) })
			segment := &fakeTransformSegment{id: 1000, partitionID: 10}
			patchLifetime(t, mockey.Mock((*fakeTransformSegment).Release).Return(nil).Build())
			patchLifetime(t, mockey.Mock((*plannedTestLoader).LoadWithPlan).To(func(_ *plannedTestLoader, ctx context.Context, plan SegmentLoadPlan) (TransformSegment, error) {
				require.Zero(t, plan.TransformStartAfterTimeTick)
				close(entered)
				select {
				case <-proceed:
					return segment, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}).Build())
			patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).RegisterSegment).To(func(_ *fakeTransformLogBuffer, _ context.Context, loaded TransformSegment) (TransformRegistration, error) {
				require.False(t, oldReleased.Load(), "load completion must not leave a gap before registration pins history")
				require.Zero(t, loaded.TransformStartAfterTimeTick())
				if failRegistration {
					return nil, merr.WrapErrServiceUnavailableMsg("registration failed")
				}
				return &lifetimeRegistration{}, nil
			}).Build())
			patchLifetime(t, mockey.Mock((*lifetimeRegistration).Catchup).To(func(_ *lifetimeRegistration, _ context.Context, done func(error)) { done(nil) }).Build())
			patchLifetime(t, mockey.Mock((*lifetimeRegistration).Unregister).Return().Build())
			scheduler := nodescheduler.New(3)
			t.Cleanup(scheduler.Close)
			physical := newTestSegmentPreparerWithStream(scheduler, &plannedTestLoader{}, &fakeSegmentLoadInfoStream{})
			manager := newTestManagerWithPreparation(t, scheduler, physical, &fakeTransformLogBuffer{}, &fakeQueryViewCollectionRuntimeManager{})
			view := &viewpb.QueryViewOfQueryNode{Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
			finished := make(chan bool, 2)
			acquire := func(version int64, frontier uint64) qviews.QueryViewKey {
				meta := buildHandlerTestMeta(version)
				meta.Version.DataVersion = &viewpb.DataVersion{}
				meta.TransformStartAfterTimetick = frontier
				key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
				manager.Acquire(AcquireSegments{Key: key, Meta: meta, View: view, OnReady: func(map[int64][]int64) { finished <- true }, OnUnrecoverable: func() { finished <- false }})
				return key
			}
			first := acquire(1, 0)
			option := <-options
			require.NoError(t, option.Handler.Handle(testSegmentLoadSnapshot(1000, 10)))
			waitGenerationEvent(t, entered)
			second := acquire(2, 50)
			require.Eventually(t, func() bool {
				physical.mu.Lock()
				defer physical.mu.Unlock()
				return physical.views[second] != nil
			}, time.Second, time.Millisecond)
			dropped := make(chan struct{})
			manager.Release(ReleaseSegments{Key: first, OnDropped: func() { close(dropped) }})
			require.False(t, oldReleased.Load())
			unblock.Do(func() { close(proceed) })
			select {
			case ready := <-finished:
				require.Equal(t, !failRegistration, ready)
			case <-time.After(time.Second):
				t.Fatal("surviving view did not complete preparation")
			}
			waitGenerationEvent(t, dropped)
			require.True(t, oldReleased.Load())
			require.False(t, newReleased.Load())
			releaseLifetimeView(t, manager, second)
			require.True(t, newReleased.Load())
		})
	}
}
