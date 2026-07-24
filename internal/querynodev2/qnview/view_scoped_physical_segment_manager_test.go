//go:build test && dynamic

package qnview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type channelNodeScheduler struct {
	tasks chan *SegmentLoadTask
}

func (s *channelNodeScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	switch task := task.(type) {
	case schedulerTaskFunc:
		_ = task.Execute(context.Background())
	case *SegmentLoadTask:
		s.tasks <- task
	}
	return noopNodeTaskHandle{}
}

func TestViewScopedPhysicalSegmentManager_SubmitsSegmentLoadTasks(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	meta.TransformStartAfterTimetick = 99
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	scheduler := &fakeNodeScheduler{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler)

	loadedCh := make(chan []TransformSegment, 2)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view, Collection: runtime,
		OnLoaded:        func(loaded []TransformSegment) { loadedCh <- loaded },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})

	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 2
	}, time.Second, 10*time.Millisecond)
	taskBySegment := make(map[int64]*SegmentLoadTask, len(scheduler.tasks))
	for _, task := range scheduler.tasks {
		taskBySegment[task.SegmentID] = task
	}
	for _, segmentID := range []int64{1000, 1001} {
		task := taskBySegment[segmentID]
		assert.Same(t, runtime, task.Collection)
		assert.Equal(t, uint64(99), task.TransformStartAfterTimeTick)
	}

	taskBySegment[1000].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})
	taskBySegment[1001].OnLoaded(&fakeTransformSegment{id: 1001, partitionID: 10})

	require.Eventually(t, func() bool {
		return len(loadedCh) == 2
	}, time.Second, 10*time.Millisecond)
}

func TestViewScopedPhysicalSegmentManager_ReleaseCompletesForQueuedCanceledLoad(t *testing.T) {
	nodeScheduler := nodescheduler.New(1)
	t.Cleanup(nodeScheduler.Close)

	blockStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	blocker := nodeScheduler.Submit(qnTaskFunc(func(context.Context) error {
		close(blockStarted)
		<-releaseBlocker
		return nil
	}))
	<-blockStarted

	mgr := NewViewScopedPhysicalSegmentManagerWithNodeScheduler(nodeScheduler, &fakePhysicalLoader{})
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	mgr.Acquire(AcquirePhysicalSegments{
		Key:        key,
		Meta:       meta,
		View:       view,
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		OnLoaded: func([]TransformSegment) {
			t.Fatal("unexpected loaded segment")
		},
		OnUnrecoverable: func() {
			t.Fatal("unexpected unrecoverable notification")
		},
	})

	dropped := make(chan struct{})
	mgr.Release(ReleaseSegments{
		Key:       key,
		OnDropped: func() { close(dropped) },
	})
	close(releaseBlocker)
	require.NoError(t, blocker.Wait(context.Background()))

	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued load cancellation")
	}
}

func TestViewScopedPhysicalSegmentManager_ReleaseCompletesFromTaskFinishedCallback(t *testing.T) {
	segmentScheduler := &channelNodeScheduler{tasks: make(chan *SegmentLoadTask, 1)}
	mgr := NewViewScopedPhysicalSegmentManagerWithNodeScheduler(segmentScheduler, &fakePhysicalLoader{})

	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		Collection:      &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		OnLoaded:        func([]TransformSegment) { t.Fatal("unexpected loaded") },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	var task *SegmentLoadTask
	select {
	case task = <-segmentScheduler.tasks:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for segment load task")
	}

	dropped := make(chan struct{})
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { close(dropped) }})
	select {
	case <-dropped:
		t.Fatal("release completed before segment task finished")
	case <-time.After(20 * time.Millisecond):
	}
	task.OnFinished()
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("release did not complete from task callback")
	}
}

func TestViewScopedPhysicalSegmentManager_PendsResourceFailureWhileOtherSegmentLoads(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	scheduler := &fakeNodeScheduler{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler)

	loadedCh := make(chan []TransformSegment, 2)
	failedCh := make(chan int64, 1)
	unrecoverableCh := make(chan struct{}, 1)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		Collection:             &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		OnLoaded:               func(loaded []TransformSegment) { loadedCh <- loaded },
		OnSegmentUnrecoverable: func(segmentID int64, err error) { failedCh <- segmentID },
		OnUnrecoverable:        func() { unrecoverableCh <- struct{}{} },
	})

	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 2
	}, time.Second, 10*time.Millisecond)
	taskBySegment := make(map[int64]*SegmentLoadTask, len(scheduler.tasks))
	for _, task := range scheduler.tasks {
		taskBySegment[task.SegmentID] = task
	}

	taskBySegment[1000].OnUnrecoverable(merr.WrapErrSegmentRequestResourceFailed("Memory"))
	select {
	case got := <-failedCh:
		t.Fatalf("resource failure should pend while another segment is loading, got failed segment %d", got)
	case <-unrecoverableCh:
		t.Fatal("resource failure should not mark view unrecoverable while another segment is loading")
	case <-time.After(20 * time.Millisecond):
	}

	taskBySegment[1001].OnLoaded(&fakeTransformSegment{id: 1001, partitionID: 10})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 3
	}, time.Second, 10*time.Millisecond)
	retry := scheduler.tasks[2]
	require.Equal(t, int64(1000), retry.SegmentID)
	retry.OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})

	require.Eventually(t, func() bool {
		return len(loadedCh) == 2
	}, time.Second, 10*time.Millisecond)
	select {
	case got := <-failedCh:
		t.Fatalf("resource failure should be recovered by retry, got failed segment %d", got)
	case <-unrecoverableCh:
		t.Fatal("resource failure should be recovered by retry")
	default:
	}
}

func TestViewScopedPhysicalSegmentManager_ReleaseWaitsForPendingRetryCallback(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	scheduler := &fakeNodeScheduler{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler)

	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:               func([]TransformSegment) {},
		OnSegmentUnrecoverable: func(segmentID int64, err error) { t.Fatalf("unexpected failed segment %d: %v", segmentID, err) },
		OnUnrecoverable:        func() { t.Fatal("unexpected unrecoverable") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 2
	}, time.Second, 10*time.Millisecond)
	taskBySegment := make(map[int64]*SegmentLoadTask, len(scheduler.tasks))
	for _, task := range scheduler.tasks {
		taskBySegment[task.SegmentID] = task
	}
	taskBySegment[1000].OnUnrecoverable(merr.WrapErrSegmentRequestResourceFailed("Memory"))
	taskBySegment[1001].OnLoaded(&fakeTransformSegment{id: 1001, partitionID: 10})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 3
	}, time.Second, 10*time.Millisecond)

	dropped := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { dropped <- struct{}{} }})
	require.ErrorIs(t, scheduler.tasks[2].Context.Err(), context.Canceled)
	select {
	case <-dropped:
		t.Fatal("release should wait for pending retry callback")
	case <-time.After(20 * time.Millisecond):
	}

	scheduler.tasks[2].OnUnrecoverable(context.Canceled)
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dropped after pending retry callback")
	}
}

func TestViewScopedPhysicalSegmentManager_FailsResourceFailureWithoutOtherSegmentLoads(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	scheduler := &fakeNodeScheduler{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler)

	failedCh := make(chan int64, 1)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:               func([]TransformSegment) { t.Fatal("unexpected loaded") },
		OnSegmentUnrecoverable: func(segmentID int64, err error) { failedCh <- segmentID },
		OnUnrecoverable:        func() { t.Fatal("unexpected view unrecoverable") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)

	scheduler.tasks[0].OnUnrecoverable(merr.WrapErrSegmentRequestResourceFailed("Memory"))
	select {
	case got := <-failedCh:
		assert.Equal(t, int64(1000), got)
	case <-time.After(time.Second):
		t.Fatal("resource failure should fail when no other segment is loading")
	}
}

func TestViewScopedPhysicalSegmentManager_FailsNonResourceErrorWhileOtherSegmentLoads(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	scheduler := &fakeNodeScheduler{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler)

	failedCh := make(chan int64, 1)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:               func([]TransformSegment) { t.Fatal("unexpected loaded") },
		OnSegmentUnrecoverable: func(segmentID int64, err error) { failedCh <- segmentID },
		OnUnrecoverable:        func() { t.Fatal("unexpected view unrecoverable") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 2
	}, time.Second, 10*time.Millisecond)
	taskBySegment := make(map[int64]*SegmentLoadTask, len(scheduler.tasks))
	for _, task := range scheduler.tasks {
		taskBySegment[task.SegmentID] = task
	}

	taskBySegment[1000].OnUnrecoverable(assert.AnError)
	select {
	case got := <-failedCh:
		assert.Equal(t, int64(1000), got)
	case <-time.After(time.Second):
		t.Fatal("non-resource failure should fail immediately")
	}
}

func TestViewScopedPhysicalSegmentManager_CancelsLoadingSegmentAfterLastViewRelease(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	scheduler := &fakeNodeScheduler{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler)

	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:        func([]TransformSegment) { t.Fatal("unexpected loaded") },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)

	dropped := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { dropped <- struct{}{} }})

	require.ErrorIs(t, scheduler.tasks[0].Context.Err(), context.Canceled)
	select {
	case <-dropped:
		t.Fatal("release should wait for canceled loading task callback")
	case <-time.After(20 * time.Millisecond):
	}
	scheduler.tasks[0].OnUnrecoverable(assert.AnError)
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dropped after canceled task callback")
	}
}

func TestViewScopedPhysicalSegmentManager_ReleaseWaitsForInFlightLoadCallback(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	scheduler := &fakeNodeScheduler{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler)

	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:        func([]TransformSegment) { t.Fatal("unexpected loaded after release") },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)

	dropped := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { dropped <- struct{}{} }})
	select {
	case <-dropped:
		t.Fatal("release dropped before in-flight load callback completed")
	case <-time.After(20 * time.Millisecond):
	}

	segment := &fakeTransformSegment{id: 1000, partitionID: 10}
	scheduler.tasks[0].OnLoaded(segment)
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dropped after load callback completed")
	}
	assert.True(t, segment.released)
}

func TestViewScopedPhysicalSegmentManager_AppliesLoadInfoSnapshotAndCoalescesUpdates(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	scheduler := &fakeNodeScheduler{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler)
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}

	mgr.Acquire(AcquirePhysicalSegments{
		Key:        key,
		Meta:       meta,
		View:       view,
		Collection: runtime,
		OnLoaded:   func([]TransformSegment) {},
		OnUnrecoverable: func() {
			t.Fatal("unexpected unrecoverable")
		},
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)
	scheduler.tasks[0].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})

	mgr.ApplyLoadInfoSnapshot(context.Background(), SegmentLoadInfoSnapshot{
		CollectionID: testCollectionID,
		SegmentID:    1000,
		Revision:     SegmentLoadInfoRevision{Revision: 10},
		LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID},
	})
	mgr.ApplyLoadInfoSnapshot(context.Background(), SegmentLoadInfoSnapshot{
		CollectionID: testCollectionID,
		SegmentID:    1000,
		Revision:     SegmentLoadInfoRevision{Revision: 11},
		LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID},
	})

	require.Eventually(t, func() bool {
		return len(scheduler.updates) == 1
	}, time.Second, 10*time.Millisecond)
	scheduler.updates[0].OnUpdated(SegmentLoadInfoRevision{Revision: 10})
	require.Eventually(t, func() bool {
		return len(scheduler.updates) == 2
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(11), scheduler.updates[1].Snapshot.Revision.Revision)
}

func TestViewScopedPhysicalSegmentManager_KeepsNewerSnapshotPendingWhileUpdateTaskRetries(t *testing.T) {
	nodeScheduler := &capturedNodeScheduler{}
	var attempts atomic.Int32
	loader := &fakePhysicalLoader{
		updateFn: func(TransformSegment, CollectionRuntime, SegmentLoadInfoSnapshot, SegmentUpdateAction) error {
			if attempts.Add(1) == 1 {
				return errors.New("update failed")
			}
			return nil
		},
	}
	mgr := NewViewScopedPhysicalSegmentManagerWithNodeScheduler(nodeScheduler, loader)

	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}}}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	mgr.views[key] = &viewRef{segments: map[int64]int64{1000: 10}}
	mgr.segments[1000] = &physicalSegmentState{
		segment:      &fakeTransformSegment{id: 1000, partitionID: 10},
		refs:         map[qviews.QueryViewKey]struct{}{key: {}},
		requests:     map[qviews.QueryViewKey]segmentLoadRequest{key: {meta: meta, collection: runtime}},
		revision:     SegmentLoadInfoRevision{Revision: 1},
		collectionID: testCollectionID,
	}

	mgr.ApplyLoadInfoSnapshot(context.Background(), SegmentLoadInfoSnapshot{SegmentID: 1000, Revision: SegmentLoadInfoRevision{Revision: 10}})
	mgr.ApplyLoadInfoSnapshot(context.Background(), SegmentLoadInfoSnapshot{SegmentID: 1000, Revision: SegmentLoadInfoRevision{Revision: 11}})
	require.Len(t, nodeScheduler.tasks, 1)

	current := nodeScheduler.tasks[0]
	require.ErrorIs(t, current.Execute(context.Background()), nodescheduler.ErrDelay)
	require.Len(t, nodeScheduler.tasks, 1)

	require.NoError(t, current.Execute(context.Background()))
	require.Len(t, nodeScheduler.tasks, 2)
	next := nodeScheduler.tasks[1].(*SegmentUpdateTask)
	assert.Equal(t, uint64(11), next.Snapshot.Revision.Revision)
}

func TestViewScopedPhysicalSegmentManager_ReleaseStopsRetryingSegmentUpdate(t *testing.T) {
	nodeScheduler := nodescheduler.New(1)
	t.Cleanup(nodeScheduler.Close)

	var attempts atomic.Int32
	loader := &fakePhysicalLoader{
		updateFn: func(TransformSegment, CollectionRuntime, SegmentLoadInfoSnapshot, SegmentUpdateAction) error {
			attempts.Add(1)
			return errors.New("update failed")
		},
	}
	mgr := NewViewScopedPhysicalSegmentManagerWithNodeScheduler(nodeScheduler, loader)

	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	runtime := &fakeCollectionRuntimeGuard{collectionID: testCollectionID}
	mgr.views[key] = &viewRef{segments: map[int64]int64{1000: 10}}
	mgr.segments[1000] = &physicalSegmentState{
		segment:      &fakeTransformSegment{id: 1000, partitionID: 10},
		refs:         map[qviews.QueryViewKey]struct{}{key: {}},
		requests:     map[qviews.QueryViewKey]segmentLoadRequest{key: {meta: meta, collection: runtime}},
		revision:     SegmentLoadInfoRevision{Revision: 1},
		collectionID: testCollectionID,
	}

	mgr.ApplyLoadInfoSnapshot(context.Background(), SegmentLoadInfoSnapshot{
		CollectionID: testCollectionID,
		SegmentID:    1000,
		Revision:     SegmentLoadInfoRevision{Revision: 10},
		LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, CollectionID: testCollectionID},
	})
	require.Eventually(t, func() bool {
		return attempts.Load() >= 2
	}, time.Second, time.Millisecond)

	dropped := make(chan struct{})
	mgr.Release(ReleaseSegments{
		Key:       key,
		OnDropped: func() { close(dropped) },
	})
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for view release")
	}

	time.Sleep(20 * time.Millisecond)
	settled := attempts.Load()
	assert.Never(t, func() bool {
		return attempts.Load() > settled
	}, 100*time.Millisecond, time.Millisecond)
}

func TestViewScopedPhysicalSegmentManager_WatchesInitialSnapshotUntilLastRelease(t *testing.T) {
	scheduler := &fakeNodeScheduler{}
	watcher := &fakeSegmentLoadInfoWatcher{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler, watcher)

	loadedCh := make(chan []TransformSegment, 1)
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	req := AcquirePhysicalSegments{
		Key:        qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey(),
		Meta:       meta,
		View:       view,
		Collection: &fakeCollectionRuntimeGuard{collectionID: testCollectionID},
		OnLoaded: func(segments []TransformSegment) {
			loadedCh <- segments
		},
		OnUnrecoverable: func() {
			t.Fatal("unexpected unrecoverable")
		},
	}

	mgr.Acquire(req)
	require.Empty(t, scheduler.tasks)
	require.Len(t, watcher.subscriptions, 1)
	assert.Equal(t, SegmentLoadInfoSubscription{
		CollectionID: testCollectionID,
		SegmentID:    1000,
	}, watcher.subscriptions[0])

	revision := SegmentLoadInfoRevision{Revision: 10}
	mgr.ApplyLoadInfoSnapshot(context.Background(), SegmentLoadInfoSnapshot{
		CollectionID: testCollectionID,
		SegmentID:    1000,
		Revision:     revision,
		LoadInfo:     &querypb.SegmentLoadInfo{SegmentID: 1000, PartitionID: 10, CollectionID: testCollectionID},
	})

	require.Len(t, scheduler.tasks, 1)
	assert.Equal(t, revision, scheduler.tasks[0].Snapshot.Revision)
	scheduler.tasks[0].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})

	select {
	case loaded := <-loadedCh:
		require.Len(t, loaded, 1)
		assert.Equal(t, int64(1000), loaded[0].ID())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watched segment load")
	}
	require.Len(t, watcher.subscriptions, 2)
	assert.Equal(t, revision, watcher.subscriptions[1].Revision)

	droppedCh := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{
		Key:       req.Key,
		OnDropped: func() { droppedCh <- struct{}{} },
	})
	select {
	case <-droppedCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for release")
	}
	require.Len(t, watcher.unsubscriptions, 1)
	assert.Equal(t, SegmentLoadInfoSubscription{CollectionID: testCollectionID, SegmentID: 1000}, watcher.unsubscriptions[0])
}

func TestViewScopedPhysicalSegmentManager_SharedInFlightLoadSurvivesSubmitterRelease(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view).QueryViewKey()
	meta2 := buildHandlerTestMeta(2)
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view).QueryViewKey()

	scheduler := &fakeNodeScheduler{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler)

	loaded1 := make(chan []TransformSegment, 1)
	loaded2 := make(chan []TransformSegment, 1)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key1, Meta: meta1, View: view,
		OnLoaded:        func(loaded []TransformSegment) { loaded1 <- loaded },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for first view") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key2, Meta: meta2, View: view,
		OnLoaded:        func(loaded []TransformSegment) { loaded2 <- loaded },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for second view") },
	})
	time.Sleep(20 * time.Millisecond)
	require.Len(t, scheduler.tasks, 1, "shared in-flight segment should not submit another load task")

	dropped1 := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key1, OnDropped: func() { dropped1 <- struct{}{} }})
	require.Eventually(t, func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		return mgr.views[key1] == nil
	}, time.Second, 10*time.Millisecond)
	assert.NoError(t, scheduler.tasks[0].Context.Err())

	segment := &fakeTransformSegment{id: 1000, partitionID: 10}
	scheduler.tasks[0].OnLoaded(segment)
	select {
	case <-dropped1:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first view release")
	}
	select {
	case <-loaded1:
		t.Fatal("released submitter view should not receive loaded callback")
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case got := <-loaded2:
		require.Len(t, got, 1)
		assert.Equal(t, int64(1000), got[0].ID())
	case <-time.After(time.Second):
		t.Fatal("shared live view did not receive loaded callback")
	}
	assert.False(t, segment.released)
}

func TestViewScopedPhysicalSegmentManager_CancelsOnlyLastRefTasksOnMixedRelease(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	view1 := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}}},
	}
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view1).QueryViewKey()
	meta2 := buildHandlerTestMeta(2)
	view2 := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view2).QueryViewKey()

	scheduler := &fakeNodeScheduler{}
	mgr := newTestViewScopedPhysicalSegmentManager(t, scheduler)

	mgr.Acquire(AcquirePhysicalSegments{
		Key: key1, Meta: meta1, View: view1,
		OnLoaded:        func([]TransformSegment) { t.Fatal("unexpected loaded for first view") },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for first view") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 2
	}, time.Second, 10*time.Millisecond)
	taskBySegment := make(map[int64]*SegmentLoadTask, len(scheduler.tasks))
	for _, task := range scheduler.tasks {
		taskBySegment[task.SegmentID] = task
	}
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key2, Meta: meta2, View: view2,
		OnLoaded:        func([]TransformSegment) {},
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for second view") },
	})
	time.Sleep(20 * time.Millisecond)
	require.Len(t, scheduler.tasks, 2, "shared in-flight segment should not submit another load task")

	dropped1 := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key1, OnDropped: func() { dropped1 <- struct{}{} }})
	assert.NoError(t, taskBySegment[1000].Context.Err())
	assert.ErrorIs(t, taskBySegment[1001].Context.Err(), context.Canceled)

	taskBySegment[1001].OnUnrecoverable(context.Canceled)
	taskBySegment[1000].OnLoaded(&fakeTransformSegment{id: 1000, partitionID: 10})
	select {
	case <-dropped1:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first view release")
	}
}

func TestViewScopedPhysicalSegmentManager_AcquireWatchesSnapshotsLoadsAndReports(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	watcher := &fakeSegmentLoadInfoWatcher{}
	mgr := newTestViewScopedPhysicalSegmentManagerWithLoader(t, loader, watcher)

	loadedCh := make(chan []TransformSegment, 3)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:        func(loaded []TransformSegment) { loadedCh <- loaded },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	assert.ElementsMatch(t, []SegmentLoadInfoSubscription{
		{CollectionID: testCollectionID, SegmentID: 1000},
		{CollectionID: testCollectionID, SegmentID: 1001},
		{CollectionID: testCollectionID, SegmentID: 2000},
	}, watcher.subscriptions)
	applySegmentLoadSnapshots(mgr, map[int64]int64{1000: 10, 1001: 10, 2000: 20})

	require.Eventually(t, func() bool {
		return len(loadedCh) == 3
	}, time.Second, 10*time.Millisecond)
	require.Len(t, loader.loadInfos, 3)
}

func TestViewScopedPhysicalSegmentManager_LoadsMissingSegmentsIndependently(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	watcher := &fakeSegmentLoadInfoWatcher{}
	mgr := newTestViewScopedPhysicalSegmentManagerWithLoader(t, loader, watcher)

	loadedCh := make(chan []TransformSegment, 2)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:        func(loaded []TransformSegment) { loadedCh <- loaded },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	applySegmentLoadSnapshots(mgr, map[int64]int64{1000: 10, 1001: 10})

	require.Eventually(t, func() bool {
		return len(loadedCh) == 2
	}, time.Second, 10*time.Millisecond)
	require.Len(t, loader.loadInfos, 2)
	loadedSegments := []int64{loader.loadInfos[0].GetSegmentID(), loader.loadInfos[1].GetSegmentID()}
	assert.ElementsMatch(t, []int64{1000, 1001}, loadedSegments)
}

func TestViewScopedPhysicalSegmentManager_ReleaseAfterLastView(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	view1 := buildHandlerTestQNView(1)
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view1).QueryViewKey()
	meta2 := buildHandlerTestMeta(2)
	view2 := buildHandlerTestQNView(1)
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view2).QueryViewKey()
	loader := &fakePhysicalLoader{
		loadFn: func(info *querypb.SegmentLoadInfo, collection CollectionRuntime) (TransformSegment, error) {
			return &fakeTransformSegment{id: info.GetSegmentID(), partitionID: info.GetPartitionID()}, nil
		},
	}
	watcher := &fakeSegmentLoadInfoWatcher{}
	mgr := newTestViewScopedPhysicalSegmentManagerWithLoader(t, loader, watcher)

	ready1 := make(chan []TransformSegment, 3)
	ready2 := make(chan []TransformSegment, 4)
	mgr.Acquire(AcquirePhysicalSegments{Key: key1, Meta: meta1, View: view1, OnLoaded: func(loaded []TransformSegment) { ready1 <- loaded }, OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") }})
	applySegmentLoadSnapshots(mgr, map[int64]int64{1000: 10, 1001: 10, 2000: 20})
	require.Eventually(t, func() bool {
		return len(ready1) == 3
	}, time.Second, 10*time.Millisecond)
	mgr.Acquire(AcquirePhysicalSegments{Key: key2, Meta: meta2, View: view2, OnLoaded: func(loaded []TransformSegment) { ready2 <- loaded }, OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") }})
	<-ready2

	dropped1 := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key1, OnDropped: func() { dropped1 <- struct{}{} }})
	<-dropped1
	assert.Empty(t, loader.released)

	dropped2 := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key2, OnDropped: func() { dropped2 <- struct{}{} }})
	<-dropped2
	assert.Empty(t, loader.released)
	assert.Len(t, loader.loadInfos, 3)
}

func TestViewScopedPhysicalSegmentManager_MissingIndexDoesNotBlockAcquire(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	loader := &fakePhysicalLoader{loaded: &fakeTransformSegment{id: 1000, partitionID: 10}}
	watcher := &fakeSegmentLoadInfoWatcher{}
	mgr := newTestViewScopedPhysicalSegmentManagerWithLoader(t, loader, watcher)

	loadedCh := make(chan []TransformSegment, 1)
	unrecoverableCh := make(chan struct{}, 1)
	mgr.Acquire(AcquirePhysicalSegments{
		Key: key, Meta: meta, View: view,
		OnLoaded:        func(loaded []TransformSegment) { loadedCh <- loaded },
		OnUnrecoverable: func() { unrecoverableCh <- struct{}{} },
	})
	applySegmentLoadSnapshots(mgr, map[int64]int64{1000: 10})

	select {
	case got := <-loadedCh:
		require.Len(t, got, 1)
		assert.Equal(t, int64(1000), got[0].ID())
	case <-unrecoverableCh:
		t.Fatal("missing index should not make query view unrecoverable")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for load callback")
	}
}

func applySegmentLoadSnapshots(mgr *ViewScopedPhysicalSegmentManager, segments map[int64]int64) {
	for segmentID, partitionID := range segments {
		mgr.ApplyLoadInfoSnapshot(context.Background(), SegmentLoadInfoSnapshot{
			CollectionID: testCollectionID,
			SegmentID:    segmentID,
			Revision:     SegmentLoadInfoRevision{Revision: uint64(segmentID)},
			LoadInfo: &querypb.SegmentLoadInfo{
				SegmentID:    segmentID,
				PartitionID:  partitionID,
				CollectionID: testCollectionID,
			},
		})
	}
}
