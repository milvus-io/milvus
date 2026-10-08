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

func releaseLifetimeView(t *testing.T, mgr *QueryViewSegmentReadinessManager, key qviews.QueryViewKey) {
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
			physical := NewViewScopedPhysicalSegmentManagerWithNodeSchedulerAndStream(scheduler, &fakePhysicalLoader{}, &fakeSegmentLoadInfoStream{})
			mgr := NewQueryViewSegmentReadinessManagerWithScheduler(scheduler, physical, &fakeTransformLogBuffer{}, 1, &fakeQueryViewCollectionRuntimeManager{})
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

func TestReleaseWaitsForPhysicalReferenceRegistration(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var physicalRegistered atomic.Bool
	patchLifetime(t, mockey.Mock(fakePhysicalSegmentManager.Acquire).To(func(fakePhysicalSegmentManager, AcquirePhysicalSegments) {
		close(entered)
		<-resume
		physicalRegistered.Store(true)
	}).Build())
	patchLifetime(t, mockey.Mock(fakePhysicalSegmentManager.Release).To(func(_ fakePhysicalSegmentManager, req ReleaseSegments) {
		require.True(t, physicalRegistered.Load(), "release must follow physical acquire")
		req.OnDropped()
	}).Build())
	patchLifetime(t, mockey.Mock((*fakeTransformLogBuffer).Acquire).Return(instantTransformGuard{}, nil).Build())
	scheduler := nodescheduler.New(1)
	t.Cleanup(scheduler.Close)
	mgr := NewQueryViewSegmentReadinessManagerWithScheduler(scheduler, fakePhysicalSegmentManager{}, &fakeTransformLogBuffer{}, 1)
	meta, view := buildHandlerTestMeta(1), buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	mgr.Acquire(AcquireSegments{Key: key, Meta: meta, View: view})
	<-entered
	dropped := make(chan struct{})
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { close(dropped) }})
	select {
	case <-dropped:
		t.Fatal("released before physical registration completed")
	default:
	}
	close(resume)
	select {
	case <-dropped:
	case <-time.After(3 * time.Second):
		t.Fatal("deferred release did not complete")
	}
	mgr.mu.Lock()
	require.Empty(t, mgr.views)
	require.Empty(t, mgr.segments)
	mgr.mu.Unlock()
}
