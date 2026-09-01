//go:build test && dynamic

package qnview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// ---------------------------------------------------------------------------
// Mock SegmentManager
// ---------------------------------------------------------------------------

type mockSegmentManager struct {
	mu              sync.Mutex
	acquired        map[qviews.QueryViewKey]AcquireSegments
	released        []qviews.QueryViewKey
	releaseCallback map[qviews.QueryViewKey]func() // captured onDropped callbacks
	queryHandles    []SealedSegmentHandle
	queryErr        error
	queryKey        qviews.QueryViewKey
	queryView       *viewpb.QueryViewOfQueryNode
	beforeQuery     func()
	waitKey         qviews.QueryViewKey
	waitTimetick    uint64
	waitErr         error
	waitCalled      bool
}

func newMockSegmentManager() *mockSegmentManager {
	return &mockSegmentManager{
		acquired:        make(map[qviews.QueryViewKey]AcquireSegments),
		releaseCallback: make(map[qviews.QueryViewKey]func()),
	}
}

func (m *mockSegmentManager) Acquire(req AcquireSegments) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acquired[req.Key] = req
}

func (m *mockSegmentManager) Release(req ReleaseSegments) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.acquired, req.Key)
	m.released = append(m.released, req.Key)
	m.releaseCallback[req.Key] = req.OnDropped
}

func (m *mockSegmentManager) AcquireSealedSegmentHandles(_ context.Context, key qviews.QueryViewKey, view *viewpb.QueryViewOfQueryNode) ([]SealedSegmentHandle, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.beforeQuery != nil {
		m.beforeQuery()
	}
	m.queryKey = key
	m.queryView = proto.Clone(view).(*viewpb.QueryViewOfQueryNode)
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	return append([]SealedSegmentHandle(nil), m.queryHandles...), nil
}

func (m *mockSegmentManager) WaitTransformVisible(_ context.Context, key qviews.QueryViewKey, timetick uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitCalled = true
	m.waitKey = key
	m.waitTimetick = timetick
	return m.waitErr
}

func (m *mockSegmentManager) invokeReleaseCallback(key qviews.QueryViewKey) {
	m.mu.Lock()
	cb := m.releaseCallback[key]
	delete(m.releaseCallback, key)
	m.mu.Unlock()
	if cb != nil {
		cb()
	}
}

func (m *mockSegmentManager) getAcquired(key qviews.QueryViewKey) (AcquireSegments, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	req, ok := m.acquired[key]
	return req, ok
}

func (m *mockSegmentManager) acquiredCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.acquired)
}

func (m *mockSegmentManager) releasedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.released)
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func buildHandlerTestMeta(version int64) *viewpb.QueryViewMeta {
	return &viewpb.QueryViewMeta{
		CollectionId: testCollectionID,
		ReplicaId:    testReplicaID,
		Vchannel:     testVChannel,
		Version: &viewpb.QueryViewVersion{
			DataVersion:  &viewpb.DataVersion{StreamingVersion: version, CompactVersion: 1},
			QueryVersion: version,
		},
		State: viewpb.QueryViewState_QueryViewStatePreparing,
	}
}

func buildHandlerTestQNView(nodeID int64) *viewpb.QueryViewOfQueryNode {
	return &viewpb.QueryViewOfQueryNode{
		NodeId: nodeID,
		Partitions: []*viewpb.QueryViewOfPartition{
			{PartitionId: 10, SegmentIds: []int64{1000, 1001}},
			{PartitionId: 20, SegmentIds: []int64{2000}},
		},
	}
}

func newPreparingQNView(nodeID int64, version int64) qviews.QueryViewAtWorkNode {
	return qviews.NewQueryViewAtQueryNode(buildHandlerTestMeta(version), buildHandlerTestQNView(nodeID))
}

func newDroppedQNView(nodeID int64, version int64) qviews.QueryViewAtWorkNode {
	meta := buildHandlerTestMeta(version)
	meta.State = viewpb.QueryViewState_QueryViewStateDropped
	return qviews.NewQueryViewAtQueryNode(meta, buildHandlerTestQNView(nodeID))
}

func newQNViewWithState(nodeID int64, version int64, state viewpb.QueryViewState) qviews.QueryViewAtWorkNode {
	meta := buildHandlerTestMeta(version)
	meta.State = state
	return qviews.NewQueryViewAtQueryNode(meta, buildHandlerTestQNView(nodeID))
}

type reportCollector struct {
	mu      sync.Mutex
	reports []qviews.QueryViewAtWorkNode
}

func (c *reportCollector) onReport(report qviews.QueryViewAtWorkNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reports = append(c.reports, report)
}

func (c *reportCollector) get() []qviews.QueryViewAtWorkNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]qviews.QueryViewAtWorkNode{}, c.reports...)
}

func (c *reportCollector) last() qviews.QueryViewAtWorkNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reports) == 0 {
		return nil
	}
	return c.reports[len(c.reports)-1]
}

func (c *reportCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.reports)
}

// ---------------------------------------------------------------------------
// 1. ApplyViews — new Preparing view triggers Acquire
// ---------------------------------------------------------------------------

func TestQNHandler_ApplyViews_NewPreparing(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	// QN SM does not generate report on construction.
	assert.Equal(t, 0, rc.count())

	// SegmentManager should have been called with Acquire.
	key := view.QueryViewKey()
	req, ok := mgr.getAcquired(key)
	require.True(t, ok)
	assert.True(t, proto.Equal(buildHandlerTestMeta(1), req.Meta))
	assert.True(t, proto.Equal(buildHandlerTestQNView(1), req.View))
	assert.NotNil(t, req.OnReady)
	assert.NotNil(t, req.OnUnrecoverable)
}

func TestQNHandler_ApplyViews_UnknownViewReportsUnrecoverable(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	// Non-Preparing, non-Dropped state for unknown view → Unrecoverable.
	meta := buildHandlerTestMeta(1)
	meta.State = viewpb.QueryViewState_QueryViewStateReady
	view := qviews.NewQueryViewAtQueryNode(meta, buildHandlerTestQNView(1))

	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, rc.last().State())
	assert.Equal(t, 0, mgr.acquiredCount())
}

func TestQNHandler_ApplyViews_DroppedOnUnknownViewReportsBack(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	// Coord pushes Dropped for a view QN doesn't know (e.g., after restart).
	// QN must report Dropped back so Coord can finish cleanup.
	rc := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newDroppedQNView(1, 1), OnReport: rc.onReport},
	})

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())
	assert.Equal(t, 0, mgr.acquiredCount())
}

func TestQNHandler_ApplyViews_PrioritizesPreparingAndUpInBatch(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	var reportStates []qviews.QueryViewState
	onReport := func(report qviews.QueryViewAtWorkNode) {
		reportStates = append(reportStates, report.State())
	}

	h.ApplyViews([]handler.ApplyView{
		{View: newDroppedQNView(1, 1), OnReport: onReport},
		{View: newQNViewWithState(1, 2, viewpb.QueryViewState_QueryViewStateUp), OnReport: onReport},
	})

	require.Equal(t, []qviews.QueryViewState{
		qviews.QueryViewStateUnrecoverable,
		qviews.QueryViewStateDropped,
	}, reportStates)
}

// ---------------------------------------------------------------------------
// 2. SegmentManager callback → Ready
// ---------------------------------------------------------------------------

func TestQNHandler_SegmentManagerCallback_TransitionToReady(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)

	// SegmentManager calls OnReady with all segments.
	req.OnReady(map[int64][]int64{
		10: {1000, 1001},
		20: {2000},
	})

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc.last().State())
}

func TestQNHandler_SegmentManagerCallback_IncrementalProgress(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)

	// Partial: still Preparing.
	req.OnReady(map[int64][]int64{10: {1000}})
	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStatePreparing, rc.last().State())

	// Complete: Ready.
	req.OnReady(map[int64][]int64{
		10: {1001},
		20: {2000},
	})
	require.Equal(t, 2, rc.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc.last().State())
}

func TestQNHandler_SegmentManagerCallback_StaleCallbackIgnored(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)

	// Drop the view → SM enters Dropping, Release called.
	h.ApplyViews([]handler.ApplyView{
		{View: newDroppedQNView(1, 1), OnReport: rc.onReport},
	})

	// Complete Release → SM transitions to Dropped, entry removed.
	mgr.invokeReleaseCallback(key)
	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())

	// Stale OnReady callback after Dropped — should be a no-op (entry removed).
	req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})
	assert.Equal(t, 1, rc.count()) // no extra report
}

// ---------------------------------------------------------------------------
// 3. SegmentManager callback → Unrecoverable
// ---------------------------------------------------------------------------

func TestQNHandler_SegmentManagerCallback_Unrecoverable(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)

	req.OnUnrecoverable()

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateUnrecoverable, rc.last().State())
}

// ---------------------------------------------------------------------------
// 4. ApplyViews — coord Dropped triggers Release
// ---------------------------------------------------------------------------

func TestQNHandler_ApplyViews_CoordDropped(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})
	assert.Equal(t, 1, mgr.acquiredCount())

	// Push Dropped → SM enters Dropping, Release called.
	h.ApplyViews([]handler.ApplyView{
		{View: newDroppedQNView(1, 1), OnReport: rc.onReport},
	})

	// No report yet — SM is in Dropping, waiting for Release callback.
	assert.Equal(t, 0, rc.count())
	assert.Equal(t, 1, mgr.releasedCount())

	// SegmentManager completes release → SM transitions Dropping → Dropped.
	mgr.invokeReleaseCallback(key)

	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())
}

func TestQNHandler_ApplyViews_CoordDroppedWhileDropping(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})

	// Push Dropped → SM enters Dropping.
	h.ApplyViews([]handler.ApplyView{
		{View: newDroppedQNView(1, 1), OnReport: rc.onReport},
	})
	assert.Equal(t, 0, rc.count()) // still Dropping
	assert.Equal(t, 1, mgr.releasedCount())

	// Coord re-pushes Dropped while SM is Dropping → no additional Release.
	h.ApplyViews([]handler.ApplyView{
		{View: newDroppedQNView(1, 1), OnReport: rc.onReport},
	})
	assert.Equal(t, 0, rc.count())
	assert.Equal(t, 1, mgr.releasedCount()) // no double Release

	// Release callback completes → Dropped report.
	mgr.invokeReleaseCallback(key)
	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())
}

// ---------------------------------------------------------------------------
// 5. ApplyViews — callback replacement on re-apply
// ---------------------------------------------------------------------------

func TestQNHandler_ApplyViews_CallbackReplacement(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	rc1 := &reportCollector{}
	view := newPreparingQNView(1, 1)
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc1.onReport},
	})

	// Make SM Ready via SegmentManager callback.
	key := view.QueryViewKey()
	req, _ := mgr.getAcquired(key)
	req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})
	assert.Equal(t, 1, rc1.count())

	// Re-apply with new callback — should get re-report of current state.
	rc2 := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: newPreparingQNView(1, 1), OnReport: rc2.onReport},
	})

	// rc1 should NOT get a new report (callback replaced).
	assert.Equal(t, 1, rc1.count())
	// rc2 should get the Ready re-report (coord re-push Preparing, SM is Ready).
	require.Equal(t, 1, rc2.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc2.last().State())
}

// ---------------------------------------------------------------------------
// 6. Multiple shards
// ---------------------------------------------------------------------------

func TestQNHandler_MultipleShards(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	// Shard 1: replica 1, vchannel v0_c0
	meta1 := buildHandlerTestMeta(1)
	meta1.ReplicaId = 1
	qnV1 := buildHandlerTestQNView(1)
	view1 := qviews.NewQueryViewAtQueryNode(meta1, qnV1)

	// Shard 2: replica 2, vchannel v0_c0
	meta2 := buildHandlerTestMeta(1)
	meta2.ReplicaId = 2
	qnV2 := buildHandlerTestQNView(1)
	view2 := qviews.NewQueryViewAtQueryNode(meta2, qnV2)

	rc1 := &reportCollector{}
	rc2 := &reportCollector{}
	h.ApplyViews([]handler.ApplyView{
		{View: view1, OnReport: rc1.onReport},
		{View: view2, OnReport: rc2.onReport},
	})

	assert.Equal(t, 2, mgr.acquiredCount())

	// Only complete shard 1 via callback.
	key1 := view1.QueryViewKey()
	req1, _ := mgr.getAcquired(key1)
	req1.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})

	require.Equal(t, 1, rc1.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc1.last().State())
	assert.Equal(t, 0, rc2.count()) // shard 2 unaffected
}

// ---------------------------------------------------------------------------
// 7. Multiple versions in same shard
// ---------------------------------------------------------------------------

func TestQNHandler_MultipleVersions(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	view1 := newPreparingQNView(1, 1)
	view2 := newPreparingQNView(1, 2)
	rc1 := &reportCollector{}
	rc2 := &reportCollector{}

	h.ApplyViews([]handler.ApplyView{
		{View: view1, OnReport: rc1.onReport},
		{View: view2, OnReport: rc2.onReport},
	})

	assert.Equal(t, 2, mgr.acquiredCount())

	// Complete version 1 only.
	key1 := view1.QueryViewKey()
	req1, _ := mgr.getAcquired(key1)
	req1.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})

	assert.Equal(t, 1, rc1.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc1.last().State())
	assert.Equal(t, 0, rc2.count()) // version 2 unaffected
}

// ---------------------------------------------------------------------------
// 8. Concurrency safety
// ---------------------------------------------------------------------------

func TestQNHandler_ConcurrentApplyAndCallback(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	const numViews = 20
	var wg sync.WaitGroup
	var readyCount atomic.Int32

	for i := int64(1); i <= numViews; i++ {
		view := newPreparingQNView(1, i)
		h.ApplyViews([]handler.ApplyView{
			{View: view, OnReport: func(report qviews.QueryViewAtWorkNode) {
				if report.State() == qviews.QueryViewStateReady {
					readyCount.Add(1)
				}
			}},
		})
	}

	// Invoke all callbacks concurrently.
	for i := int64(1); i <= numViews; i++ {
		wg.Add(1)
		go func(version int64) {
			defer wg.Done()
			view := newPreparingQNView(1, version)
			key := view.QueryViewKey()
			req, ok := mgr.getAcquired(key)
			if ok {
				req.OnReady(map[int64][]int64{
					10: {1000, 1001}, 20: {2000},
				})
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int32(numViews), readyCount.Load())
}

// ---------------------------------------------------------------------------
// 10. Full lifecycle
// ---------------------------------------------------------------------------

func TestQNHandler_FullLifecycle(t *testing.T) {
	mgr := newMockSegmentManager()
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	key := view.QueryViewKey()

	// 1. Apply Preparing → Acquire called.
	h.ApplyViews([]handler.ApplyView{
		{View: view, OnReport: rc.onReport},
	})
	assert.Equal(t, 0, rc.count())
	assert.Equal(t, 1, mgr.acquiredCount())

	req, _ := mgr.getAcquired(key)

	// 2. Incremental segment loading via callback.
	req.OnReady(map[int64][]int64{10: {1000}})
	require.Equal(t, 1, rc.count())
	assert.Equal(t, qviews.QueryViewStatePreparing, rc.last().State())

	// 3. Complete segments → Ready.
	req.OnReady(map[int64][]int64{
		10: {1001}, 20: {2000},
	})
	require.Equal(t, 2, rc.count())
	assert.Equal(t, qviews.QueryViewStateReady, rc.last().State())

	// 4. Coord pushes Dropped → Dropping, Release called.
	h.ApplyViews([]handler.ApplyView{
		{View: newDroppedQNView(1, 1), OnReport: rc.onReport},
	})
	assert.Equal(t, 2, rc.count()) // no report yet (Dropping)
	assert.Equal(t, 1, mgr.releasedCount())

	// 5. Release callback → Dropped.
	mgr.invokeReleaseCallback(key)
	require.Equal(t, 3, rc.count())
	assert.Equal(t, qviews.QueryViewStateDropped, rc.last().State())

	// 6. Further callbacks are no-op (entry removed).
	req.OnReady(map[int64][]int64{10: {1000}})
	assert.Equal(t, 3, rc.count())
}
