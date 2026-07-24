//go:build test && dynamic

package qnview

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestQueryViewSegmentReadinessManager_AcquireUsesNodeScheduler(t *testing.T) {
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

	meta := buildHandlerTestMeta(1)
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	collections := &fakeQueryViewCollectionRuntimeManager{}
	physicalCalled := make(chan struct{}, 1)
	physical := fakePhysicalSegmentManager{
		acquire: func(AcquirePhysicalSegments) { physicalCalled <- struct{}{} },
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	mgr := NewQueryViewSegmentReadinessManagerWithScheduler(nodeScheduler, physical, &fakeTransformLogBuffer{}, collections)

	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(map[int64][]int64) { t.Fatal("unexpected ready") },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	select {
	case <-physicalCalled:
		t.Fatal("acquire bypassed node scheduler")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseBlocker)
	require.NoError(t, blocker.Wait(context.Background()))
	select {
	case <-physicalCalled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduled acquire")
	}
}

func TestQueryViewSegmentReadinessManager_WaitsForCatchupBeforeReady(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	meta.TransformStartAfterTimetick = 100
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()

	loaded := []TransformSegment{
		&fakeTransformSegment{id: 1000, partitionID: 10},
		&fakeTransformSegment{id: 1001, partitionID: 10},
	}

	var physicalReq AcquirePhysicalSegments
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			physicalReq = req
			req.OnLoaded(loaded)
		},
		release: func(req ReleaseSegments) {
			req.OnDropped()
		},
	}
	buffer := &fakeTransformLogBuffer{}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	readyCh := make(chan map[int64][]int64, 1)
	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(ready map[int64][]int64) { readyCh <- ready },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})

	require.Eventually(t, func() bool {
		return physicalReq.Key == key
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, meta, physicalReq.Meta)
	require.Equal(t, view, physicalReq.View)

	select {
	case <-readyCh:
		t.Fatal("ready reported before transform catch-up")
	case <-time.After(20 * time.Millisecond):
	}

	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.regs) == 2
	}, time.Second, 10*time.Millisecond)

	buffer.mu.Lock()
	regs := append([]*fakeTransformRegistration(nil), buffer.regs...)
	buffer.mu.Unlock()
	for _, reg := range regs {
		close(reg.waitCh)
	}

	ready := <-readyCh
	mergeReadyByPartition(ready, <-readyCh)
	require.ElementsMatch(t, []int64{1000, 1001}, ready[10])
	assert.Equal(t, testVChannel, buffer.acquireView.IntoProto().GetMeta().GetVchannel())
	assert.Equal(t, uint64(100), buffer.acquireView.IntoProto().GetMeta().GetTransformStartAfterTimetick())
	assert.ElementsMatch(t, []int64{1000, 1001}, buffer.registerSegments)
}

func mergeReadyByPartition(dst map[int64][]int64, src map[int64][]int64) {
	for partitionID, segmentIDs := range src {
		dst[partitionID] = append(dst[partitionID], segmentIDs...)
	}
}

func TestQueryViewSegmentReadinessManager_AcquiresTransformGuardBeforePhysicalAcquire(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()

	buffer := &fakeTransformLogBuffer{}
	physicalCalled := make(chan bool, 1)
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			buffer.mu.Lock()
			acquired := buffer.acquireView != nil
			buffer.mu.Unlock()
			physicalCalled <- acquired
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(map[int64][]int64) { t.Fatal("unexpected ready") },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})

	select {
	case acquired := <-physicalCalled:
		assert.True(t, acquired, "TransformLogBuffer.Acquire must happen before physical segment acquire")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for physical acquire")
	}
}

func TestQueryViewSegmentReadinessManager_WaitTransformVisibleUsesTransformGuard(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	meta.TransformStartAfterTimetick = 100
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()

	segment := &fakeTransformSegment{id: 1000, partitionID: 10}
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			req.OnLoaded([]TransformSegment{segment})
		},
		release: func(req ReleaseSegments) {
			req.OnDropped()
		},
	}
	buffer := &fakeTransformLogBuffer{}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	readyCh := make(chan map[int64][]int64, 1)
	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(ready map[int64][]int64) { readyCh <- ready },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})

	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.regs) == 1
	}, time.Second, 10*time.Millisecond)
	buffer.mu.Lock()
	close(buffer.regs[0].waitCh)
	buffer.mu.Unlock()
	require.Eventually(t, func() bool {
		select {
		case <-readyCh:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, mgr.WaitTransformVisible(context.Background(), key, 120))
	assert.False(t, segment.waitCalled)
	assert.True(t, buffer.guard.waitCalled)
	assert.Equal(t, uint64(120), buffer.guard.waitTimetick)
}

func TestQueryViewSegmentReadinessManager_AcquiresCollectionGuardBeforePhysicalAcquire(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()

	buffer := &fakeTransformLogBuffer{}
	collections := &fakeQueryViewCollectionRuntimeManager{}
	physicalCalled := make(chan bool, 1)
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			collections.mu.Lock()
			acquired := collections.acquireView != nil
			collections.mu.Unlock()
			physicalCalled <- acquired
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer, collections)

	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(map[int64][]int64) { t.Fatal("unexpected ready") },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})

	select {
	case acquired := <-physicalCalled:
		assert.True(t, acquired, "QueryViewCollectionRuntimeManager.Acquire must happen before physical segment acquire")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for physical acquire")
	}
	assert.Equal(t, testVChannel, collections.acquireView.IntoProto().GetMeta().GetVchannel())
}

func TestQueryViewSegmentReadinessManager_CollectionGuardFailureStopsPhysicalAcquire(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()

	buffer := &fakeTransformLogBuffer{}
	collections := &fakeQueryViewCollectionRuntimeManager{acquireErr: errors.New("collection unavailable")}
	physicalCalled := make(chan struct{}, 1)
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			physicalCalled <- struct{}{}
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer, collections)

	unrecoverable := make(chan struct{}, 1)
	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(map[int64][]int64) { t.Fatal("unexpected ready") },
		OnUnrecoverable: func() { unrecoverable <- struct{}{} },
	})

	<-unrecoverable
	select {
	case <-physicalCalled:
		t.Fatal("physical acquire should not run after collection guard failure")
	case <-time.After(20 * time.Millisecond):
	}
	require.NotNil(t, buffer.guard)
	buffer.guard.mu.Lock()
	assert.True(t, buffer.guard.released)
	buffer.guard.mu.Unlock()
}

type blockingQueryViewCollectionRuntimeManager struct {
	entered chan struct{}
	done    chan struct{}
}

func (m *blockingQueryViewCollectionRuntimeManager) Acquire(ctx context.Context, _ *qviews.QueryViewAtQueryNode) (CollectionRuntimeGuard, bool, error) {
	close(m.entered)
	<-ctx.Done()
	close(m.done)
	return nil, true, ctx.Err()
}

func TestQueryViewSegmentReadinessManager_RetriesRetryableCollectionAcquireInNodeScheduler(t *testing.T) {
	nodeScheduler := nodescheduler.New(1)
	t.Cleanup(nodeScheduler.Close)

	meta := buildHandlerTestMeta(1)
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	collections := &fakeQueryViewCollectionRuntimeManager{
		acquireErrs: []error{errors.New("collection temporarily unavailable")},
		retryable:   []bool{true},
	}
	physicalCalled := make(chan struct{}, 1)
	physical := fakePhysicalSegmentManager{
		acquire: func(AcquirePhysicalSegments) { physicalCalled <- struct{}{} },
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	mgr := NewQueryViewSegmentReadinessManagerWithScheduler(nodeScheduler, physical, &fakeTransformLogBuffer{}, collections)

	unrecoverable := make(chan struct{}, 1)
	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(map[int64][]int64) { t.Fatal("unexpected ready") },
		OnUnrecoverable: func() { unrecoverable <- struct{}{} },
	})

	select {
	case <-physicalCalled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for collection acquire retry")
	}
	collections.mu.Lock()
	assert.Equal(t, 2, collections.acquireCalls)
	collections.mu.Unlock()
	select {
	case <-unrecoverable:
		t.Fatal("retryable collection acquire must not become unrecoverable")
	default:
	}
}

func TestQueryViewSegmentReadinessManager_ReleaseCancelsPendingCollectionAcquire(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()

	buffer := &fakeTransformLogBuffer{}
	collections := &blockingQueryViewCollectionRuntimeManager{
		entered: make(chan struct{}),
		done:    make(chan struct{}),
	}
	physicalCalled := make(chan struct{}, 1)
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			physicalCalled <- struct{}{}
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer, collections)

	unrecoverable := make(chan struct{}, 1)
	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(map[int64][]int64) { t.Fatal("unexpected ready") },
		OnUnrecoverable: func() { unrecoverable <- struct{}{} },
	})

	select {
	case <-collections.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for collection acquire")
	}

	dropped := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { dropped <- struct{}{} }})

	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for drop")
	}
	select {
	case <-collections.done:
	case <-time.After(time.Second):
		t.Fatal("release should cancel pending collection acquire")
	}
	select {
	case <-physicalCalled:
		t.Fatal("physical acquire should not run after release cancels pending acquire")
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case <-unrecoverable:
		t.Fatal("release cancellation should not report unrecoverable")
	default:
	}
	require.NotNil(t, buffer.guard)
	buffer.guard.mu.Lock()
	assert.True(t, buffer.guard.released)
	buffer.guard.mu.Unlock()
}

func TestQueryViewSegmentReadinessManager_ReleasesLoadedSegmentAfterLastView(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	segment := &fakeTransformSegment{id: 1000, partitionID: 10}

	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			req.OnLoaded([]TransformSegment{segment})
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	buffer := &fakeTransformLogBuffer{}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	readyCh := make(chan map[int64][]int64, 1)
	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(ready map[int64][]int64) { readyCh <- ready },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.regs) == 1
	}, time.Second, 10*time.Millisecond)
	buffer.mu.Lock()
	reg := buffer.regs[0]
	buffer.mu.Unlock()
	close(reg.waitCh)
	require.Equal(t, map[int64][]int64{10: {1000}}, <-readyCh)

	dropped := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { dropped <- struct{}{} }})
	<-dropped

	assert.True(t, reg.unregistered)
	assert.True(t, segment.released)
}

func TestQueryViewSegmentReadinessManager_QueryHandleDefersSegmentRelease(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	segment := &fakeTransformSegment{id: 1000, partitionID: 10}

	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			req.OnLoaded([]TransformSegment{segment})
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	buffer := &fakeTransformLogBuffer{}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	readyCh := make(chan map[int64][]int64, 1)
	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(ready map[int64][]int64) { readyCh <- ready },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.regs) == 1
	}, time.Second, 10*time.Millisecond)
	buffer.mu.Lock()
	reg := buffer.regs[0]
	buffer.mu.Unlock()
	close(reg.waitCh)
	require.Equal(t, map[int64][]int64{10: {1000}}, <-readyCh)

	handles, err := mgr.AcquireSealedSegmentHandles(context.Background(), key, view)
	require.NoError(t, err)
	require.Len(t, handles, 1)
	assert.Equal(t, int64(1000), handles[0].ID())

	dropped := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { dropped <- struct{}{} }})
	<-dropped
	assert.False(t, segment.released)

	handles[0].Release()
	assert.True(t, segment.released)
}

func TestQueryViewSegmentReadinessManager_ReleasesLateLoadedSegmentAfterViewRelease(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	acquireCh := make(chan AcquirePhysicalSegments, 1)
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			acquireCh <- req
		},
		release: func(req ReleaseSegments) {
			req.OnDropped()
		},
	}
	buffer := &fakeTransformLogBuffer{}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	readyCh := make(chan struct{}, 1)
	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(map[int64][]int64) { readyCh <- struct{}{} },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})
	physicalReq := <-acquireCh

	dropped := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key, OnDropped: func() { dropped <- struct{}{} }})
	<-dropped

	lateSegment := &fakeTransformSegment{id: 1000, partitionID: 10}
	physicalReq.OnLoaded([]TransformSegment{lateSegment})

	assert.True(t, lateSegment.released)
	select {
	case <-readyCh:
		t.Fatal("late loaded segment should not report ready after view release")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestQueryViewSegmentReadinessManager_ReportsReadyIncrementallyPerSegment(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()

	loaded := []TransformSegment{
		&fakeTransformSegment{id: 1000, partitionID: 10},
		&fakeTransformSegment{id: 1001, partitionID: 10},
	}
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			req.OnLoaded(loaded)
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	buffer := &fakeTransformLogBuffer{}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	readyCh := make(chan map[int64][]int64, 2)
	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(ready map[int64][]int64) { readyCh <- ready },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") },
	})

	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.regs) == 2
	}, time.Second, 10*time.Millisecond)

	buffer.mu.Lock()
	regBySegment := make(map[int64]*fakeTransformRegistration, len(buffer.regs))
	for i, segmentID := range buffer.registerSegments {
		regBySegment[segmentID] = buffer.regs[i]
	}
	first := regBySegment[1000]
	second := regBySegment[1001]
	buffer.mu.Unlock()
	require.NotNil(t, first)
	require.NotNil(t, second)
	close(first.waitCh)

	select {
	case ready := <-readyCh:
		assert.Equal(t, map[int64][]int64{10: {1000}}, ready)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first segment ready")
	}

	select {
	case ready := <-readyCh:
		t.Fatalf("second segment reported before catch-up: %v", ready)
	case <-time.After(20 * time.Millisecond):
	}

	close(second.waitCh)
	assert.Equal(t, map[int64][]int64{10: {1001}}, <-readyCh)
}

func TestQueryViewSegmentReadinessManager_LoadedSegmentAcquireDoesNotReleaseSharedSegment(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view).QueryViewKey()
	meta2 := buildHandlerTestMeta(2)
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view).QueryViewKey()
	segment := &fakeTransformSegment{id: 1000, partitionID: 10}

	acquireCalls := make(chan AcquirePhysicalSegments, 2)
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			acquireCalls <- req
			req.OnLoaded([]TransformSegment{segment})
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	buffer := &fakeTransformLogBuffer{}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	ready1 := make(chan map[int64][]int64, 1)
	mgr.Acquire(AcquireSegments{
		Key: key1, Meta: meta1, View: view,
		OnReady:         func(ready map[int64][]int64) { ready1 <- ready },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for first view") },
	})
	<-acquireCalls
	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.regs) == 1
	}, time.Second, 10*time.Millisecond)
	buffer.mu.Lock()
	close(buffer.regs[0].waitCh)
	buffer.mu.Unlock()
	assert.Equal(t, map[int64][]int64{10: {1000}}, <-ready1)

	ready2 := make(chan map[int64][]int64, 1)
	mgr.Acquire(AcquireSegments{
		Key: key2, Meta: meta2, View: view,
		OnReady:         func(ready map[int64][]int64) { ready2 <- ready },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for second view") },
	})

	assert.Equal(t, map[int64][]int64{10: {1000}}, <-ready2)
	select {
	case <-acquireCalls:
		t.Fatal("already loaded segment should not trigger another physical acquire")
	case <-time.After(20 * time.Millisecond):
	}
	assert.False(t, segment.released)
}

func TestQueryViewSegmentReadinessManager_RetriesPhysicalLoadAfterRegisterFailure(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view).QueryViewKey()
	meta2 := buildHandlerTestMeta(2)
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view).QueryViewKey()

	scheduler := &fakeNodeScheduler{}
	physical := newTestViewScopedPhysicalSegmentManager(t, scheduler)
	buffer := &fakeTransformLogBuffer{registerErr: errors.New("register failed")}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	unrecoverable1 := make(chan struct{}, 1)
	mgr.Acquire(AcquireSegments{
		Key: key1, Meta: meta1, View: view,
		OnReady:         func(map[int64][]int64) { t.Fatal("unexpected ready for first view") },
		OnUnrecoverable: func() { unrecoverable1 <- struct{}{} },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)
	firstSegment := &fakeTransformSegment{id: 1000, partitionID: 10}
	scheduler.tasks[0].OnLoaded(firstSegment)
	select {
	case <-unrecoverable1:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first view unrecoverable")
	}
	assert.True(t, firstSegment.released)

	buffer.mu.Lock()
	buffer.registerErr = nil
	buffer.mu.Unlock()
	ready2 := make(chan map[int64][]int64, 1)
	mgr.Acquire(AcquireSegments{
		Key: key2, Meta: meta2, View: view,
		OnReady:         func(ready map[int64][]int64) { ready2 <- ready },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for second view") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 2
	}, time.Second, 10*time.Millisecond, "retry after registration failure should submit a new physical load")

	secondSegment := &fakeTransformSegment{id: 1000, partitionID: 10}
	scheduler.tasks[1].OnLoaded(secondSegment)
	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.regs) == 1
	}, time.Second, 10*time.Millisecond)
	buffer.mu.Lock()
	close(buffer.regs[0].waitCh)
	buffer.mu.Unlock()
	assert.Equal(t, map[int64][]int64{10: {1000}}, <-ready2)
	assert.False(t, secondSegment.released)
}

func TestQueryViewSegmentReadinessManager_RetriesPhysicalLoadAfterSchedulerFailure(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view).QueryViewKey()
	meta2 := buildHandlerTestMeta(2)
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view).QueryViewKey()

	scheduler := &fakeNodeScheduler{}
	physical := newTestViewScopedPhysicalSegmentManager(t, scheduler)
	buffer := &fakeTransformLogBuffer{}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	unrecoverable1 := make(chan struct{}, 1)
	mgr.Acquire(AcquireSegments{
		Key: key1, Meta: meta1, View: view,
		OnReady:         func(map[int64][]int64) { t.Fatal("unexpected ready for first view") },
		OnUnrecoverable: func() { unrecoverable1 <- struct{}{} },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)
	scheduler.tasks[0].OnUnrecoverable(errors.New("load failed"))
	select {
	case <-unrecoverable1:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first view unrecoverable")
	}

	ready2 := make(chan map[int64][]int64, 1)
	mgr.Acquire(AcquireSegments{
		Key: key2, Meta: meta2, View: view,
		OnReady:         func(ready map[int64][]int64) { ready2 <- ready },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for second view") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 2
	}, time.Second, 10*time.Millisecond, "retry after scheduler failure should submit a new physical load")

	segment := &fakeTransformSegment{id: 1000, partitionID: 10}
	scheduler.tasks[1].OnLoaded(segment)
	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.regs) == 1
	}, time.Second, 10*time.Millisecond)
	buffer.mu.Lock()
	close(buffer.regs[0].waitCh)
	buffer.mu.Unlock()
	assert.Equal(t, map[int64][]int64{10: {1000}}, <-ready2)
	assert.False(t, segment.released)
}

func TestQueryViewSegmentReadinessManager_SegmentFailureDetachesFailedViewRef(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	view := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view).QueryViewKey()
	meta2 := buildHandlerTestMeta(2)
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view).QueryViewKey()

	scheduler := &fakeNodeScheduler{}
	physical := newTestViewScopedPhysicalSegmentManager(t, scheduler)
	buffer := &fakeTransformLogBuffer{}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	unrecoverable1 := make(chan struct{}, 1)
	mgr.Acquire(AcquireSegments{
		Key: key1, Meta: meta1, View: view,
		OnReady:         func(map[int64][]int64) { t.Fatal("unexpected ready for first view") },
		OnUnrecoverable: func() { unrecoverable1 <- struct{}{} },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 1
	}, time.Second, 10*time.Millisecond)
	scheduler.tasks[0].OnUnrecoverable(errors.New("load failed"))
	select {
	case <-unrecoverable1:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first view unrecoverable")
	}

	ready2 := make(chan map[int64][]int64, 1)
	mgr.Acquire(AcquireSegments{
		Key: key2, Meta: meta2, View: view,
		OnReady:         func(ready map[int64][]int64) { ready2 <- ready },
		OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable for second view") },
	})
	require.Eventually(t, func() bool {
		return len(scheduler.tasks) == 2
	}, time.Second, 10*time.Millisecond)

	segment := &fakeTransformSegment{id: 1000, partitionID: 10}
	scheduler.tasks[1].OnLoaded(segment)
	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.regs) == 1
	}, time.Second, 10*time.Millisecond)
	buffer.mu.Lock()
	close(buffer.regs[0].waitCh)
	buffer.mu.Unlock()
	assert.Equal(t, map[int64][]int64{10: {1000}}, <-ready2)

	dropped2 := make(chan struct{}, 1)
	mgr.Release(ReleaseSegments{Key: key2, OnDropped: func() { dropped2 <- struct{}{} }})
	select {
	case <-dropped2:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second view release")
	}
	assert.True(t, segment.released, "failed first view must not keep a stale ref after segment-scoped failure")
}

func TestQueryViewSegmentReadinessManager_KeepsEnsureRegisterVChannelTogether(t *testing.T) {
	meta1 := buildHandlerTestMeta(1)
	meta1.Vchannel = "vchannel-1"
	view1 := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000}}},
	}
	key1 := qviews.NewQueryViewAtQueryNode(meta1, view1).QueryViewKey()

	meta2 := buildHandlerTestMeta(2)
	meta2.Vchannel = "vchannel-2"
	view2 := &viewpb.QueryViewOfQueryNode{
		NodeId:     1,
		Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 20, SegmentIds: []int64{2000}}},
	}
	key2 := qviews.NewQueryViewAtQueryNode(meta2, view2).QueryViewKey()

	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			switch req.Meta.GetVchannel() {
			case "vchannel-1":
				req.OnLoaded([]TransformSegment{&fakeTransformSegment{id: 1000, vchannel: "vchannel-1", partitionID: 10}})
			case "vchannel-2":
				req.OnLoaded([]TransformSegment{&fakeTransformSegment{id: 2000, vchannel: "vchannel-2", partitionID: 20}})
			}
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	buffer := newInterleavingTransformLogBuffer()
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	readyCh := make(chan struct{}, 2)
	mgr.Acquire(AcquireSegments{Key: key1, Meta: meta1, View: view1, OnReady: func(map[int64][]int64) { readyCh <- struct{}{} }, OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") }})
	mgr.Acquire(AcquireSegments{Key: key2, Meta: meta2, View: view2, OnReady: func(map[int64][]int64) { readyCh <- struct{}{} }, OnUnrecoverable: func() { t.Fatal("unexpected unrecoverable") }})

	require.Eventually(t, func() bool {
		return len(readyCh) == 2
	}, time.Second, 10*time.Millisecond)

	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	assert.Equal(t, "vchannel-1", buffer.registeredChannel[1000])
	assert.Equal(t, "vchannel-2", buffer.registeredChannel[2000])
}
func TestQueryViewSegmentReadinessManager_FailureReportsUnrecoverable(t *testing.T) {
	meta := buildHandlerTestMeta(1)
	view := buildHandlerTestQNView(1)
	key := qviews.NewQueryViewAtQueryNode(meta, view).QueryViewKey()
	physicalCalled := make(chan struct{}, 1)
	physical := fakePhysicalSegmentManager{
		acquire: func(req AcquirePhysicalSegments) {
			physicalCalled <- struct{}{}
			req.OnLoaded([]TransformSegment{&fakeTransformSegment{id: 1000, partitionID: 10}})
		},
		release: func(req ReleaseSegments) { req.OnDropped() },
	}
	buffer := &fakeTransformLogBuffer{acquireErr: errors.New("truncated")}
	mgr := newTestQueryViewSegmentReadinessManager(t, physical, buffer)

	unrecoverable := make(chan struct{}, 1)
	mgr.Acquire(AcquireSegments{
		Key: key, Meta: meta, View: view,
		OnReady:         func(map[int64][]int64) { t.Fatal("unexpected ready") },
		OnUnrecoverable: func() { unrecoverable <- struct{}{} },
	})

	<-unrecoverable
	select {
	case <-physicalCalled:
		t.Fatal("physical acquire should not run after transform guard failure")
	case <-time.After(20 * time.Millisecond):
	}
}
