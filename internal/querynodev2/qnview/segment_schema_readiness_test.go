package qnview

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestPhysicalSegmentSchemaHandoffKeepsSubscriptionAndWaitsForReopen(t *testing.T) {
	scheduler := &fakeNodeScheduler{}
	stream := &fakeSegmentLoadInfoStream{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler, stream)
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	ready := make(chan uint64, 10)
	acquire := func(version int64, schema int32, load uint64) AcquirePhysicalSegments {
		meta := buildHandlerTestMeta(version)
		meta.SchemaVersion, meta.LoadInfoVersion = schema, load
		req := AcquirePhysicalSegments{Key: qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(), Meta: meta, View: view, Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID}, OnLoaded: func([]TransformSegment) { ready <- load }}
		mgr.Acquire(req)
		return req
	}
	first := acquire(1, 1, 1)
	snapshot := SegmentLoadInfoSnapshot{CollectionID: testCollectionID, SegmentID: 1000, Revision: SegmentLoadInfoRevision{Revision: 10}, LoadInfo: &querypb.SegmentLoadInfo{CollectionID: testCollectionID, PartitionID: 10, SegmentID: 1000}}
	require.NoError(t, stream.Emit(snapshot))
	scheduler.Tasks()[0].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})
	require.Equal(t, uint64(1), <-ready)
	second := acquire(2, 2, 1)
	require.Empty(t, ready)
	require.Len(t, scheduler.Updates(), 1)
	update := scheduler.Updates()[0]
	require.True(t, update.ForceReopen, "same snapshot still needs schema preparation")
	attempts := 0
	patch := mockey.Mock((*fakePhysicalLoader).Update).To(func(_ *fakePhysicalLoader, _ context.Context, _ TransformSegment, _ CollectionRuntime, _ SegmentLoadInfoSnapshot, action SegmentUpdateAction) error {
		require.True(t, action.Has(SegmentUpdateReopen))
		attempts++
		if attempts == 1 {
			return merr.WrapErrServiceNotReadyMsg("retry reopen")
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, update.Execute(t.Context()), nodescheduler.ErrDelay)
	require.Empty(t, ready, "failed reopen must not acknowledge the view")
	// A load-fields change at the same schema arrives while reopen is in flight.
	third := acquire(3, 2, 2)
	require.Len(t, scheduler.Updates(), 1)
	mgr.Release(ReleaseSegments{Key: first.Key})
	mgr.Release(ReleaseSegments{Key: second.Key})
	require.False(t, stream.subscriptions[0].closed)
	require.NoError(t, update.Execute(t.Context()))
	require.Empty(t, ready, "the earlier reopen cannot acknowledge newer load fields")
	require.Len(t, scheduler.Updates(), 2)
	require.NoError(t, scheduler.Updates()[1].Execute(t.Context()))
	require.Equal(t, uint64(2), <-ready)
	// Metadata publication after old views drop still reaches the remaining view.
	snapshot.Revision.Revision = 11
	require.NoError(t, stream.Emit(snapshot))
	require.Len(t, scheduler.Updates(), 3)
	require.NoError(t, scheduler.Updates()[2].Execute(t.Context()))
	require.Empty(t, ready, "ready callbacks are deduplicated per view and segment")
	require.Len(t, stream.subscriptions, 1)
	mgr.Release(ReleaseSegments{Key: third.Key})
	require.True(t, stream.subscriptions[0].closed)
}

func TestPhysicalSegmentNewSchemaDuringInitialLoad(t *testing.T) {
	scheduler := &fakeNodeScheduler{}
	stream := &fakeSegmentLoadInfoStream{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler, stream)
	meta := buildHandlerTestMeta(1)
	meta.SchemaVersion = 1
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	ready := make(chan int32, 4)
	acquire := func(meta *viewpb.QueryViewMeta) {
		mgr.Acquire(AcquirePhysicalSegments{Key: qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(), Meta: meta, View: view, Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID}, OnLoaded: func([]TransformSegment) { ready <- meta.SchemaVersion }})
	}
	acquire(meta)
	snapshot := SegmentLoadInfoSnapshot{CollectionID: testCollectionID, SegmentID: 1000, Revision: SegmentLoadInfoRevision{Revision: 10}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID, PartitionID: 10}}
	require.NoError(t, stream.Emit(snapshot))
	newer := proto.Clone(meta).(*viewpb.QueryViewMeta)
	newer.Version.QueryVersion++
	newer.SchemaVersion = 2
	acquire(newer)
	scheduler.Tasks()[0].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})
	require.Empty(t, ready)
	require.Len(t, scheduler.Updates(), 1)
	require.True(t, scheduler.Updates()[0].ForceReopen)
	scheduler.Updates()[0].OnUpdated(snapshot.Revision)
	require.Len(t, ready, 2)
}

func TestPendingViewRetainsPhysicalWatchBeforeCollectionPreparation(t *testing.T) {
	catchup := mockey.Mock((*fakeTransformRegistration).WaitCatchup).Return(nil).Build()
	defer catchup.UnPatch()
	physicalScheduler := &fakeNodeScheduler{}
	stream := &fakeSegmentLoadInfoStream{}
	physical := newTestViewScopedPhysicalSegmentManager(t, physicalScheduler, stream)
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	runtime := &fakeQueryViewCollectionRuntimeManager{}
	manager := NewQueryViewSegmentReadinessManagerWithScheduler(scheduler, physical, &fakeTransformLogBuffer{}, 1, runtime)
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	first := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	ready := make(chan struct{}, 4)
	onReady := func(map[int64][]int64) { ready <- struct{}{} }
	manager.Acquire(AcquireSegments{Key: first, Meta: meta, View: view, OnReady: onReady})
	// A scheduler barrier waits for collection preparation/subscription creation.
	require.NoError(t, scheduler.Submit(qnTaskFunc(func(context.Context) error { return nil })).Wait(t.Context()))
	snapshot := SegmentLoadInfoSnapshot{CollectionID: testCollectionID, SegmentID: 1000, Revision: SegmentLoadInfoRevision{Revision: 10}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID, PartitionID: 10}}
	require.NoError(t, stream.Emit(snapshot))
	physicalScheduler.Tasks()[0].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})
	<-ready
	entered, resume := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(resume) })
	scheduler.Submit(qnTaskFunc(func(context.Context) error { close(entered); <-resume; return nil }))
	<-entered
	next := proto.Clone(meta).(*viewpb.QueryViewMeta)
	next.Version.QueryVersion++
	second := qviews.NewQueryViewAtQueryNode(next, view).QueryViewKey()
	manager.Acquire(AcquireSegments{Key: second, Meta: next, View: view, OnReady: onReady})
	manager.Release(ReleaseSegments{Key: first})
	require.False(t, stream.subscriptions[0].closed, "pending replacement owns the watch before its collection task runs")
	unblock.Do(func() { close(resume) })
	<-ready
	require.Len(t, physicalScheduler.Tasks(), 1, "handoff must reuse the original physical segment")
	snapshot.Revision.Revision++
	require.NoError(t, stream.Emit(snapshot))
	require.Len(t, physicalScheduler.Updates(), 1, "later publications still reach the reused segment")
	physicalScheduler.Updates()[0].OnUpdated(snapshot.Revision)
	manager.Release(ReleaseSegments{Key: second})
	require.True(t, stream.subscriptions[0].closed)
}
