//go:build test && dynamic

package snview

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var _ viewquery.QuerySegmentTask = (*SNQuerySegmentTask)(nil)

type mockGrowingSegmentHandle struct {
	id           int64
	partitionID  int64
	releaseCount int
}

func (h *mockGrowingSegmentHandle) ID() int64 {
	return h.id
}

func (h *mockGrowingSegmentHandle) PartitionID() int64 {
	return h.partitionID
}

func (h *mockGrowingSegmentHandle) Collection() *segcore.CCollection {
	return nil
}

func (h *mockGrowingSegmentHandle) Segment() segcore.CSegment {
	return nil
}

func (h *mockGrowingSegmentHandle) Release() {
	h.releaseCount++
}

func TestSNHandler_AcquireSearchSegmentTasksWaitsRuntimeAndReleasesViewRef(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	handle := &mockGrowingSegmentHandle{id: 1000, partitionID: 10}
	runtime := &mockQueryRuntime{handles: []GrowingSegmentHandle{handle}}
	mgr.runtime = runtime
	optimizer := &fakeSNLocalOptimizer{}
	h := recoverSNQueryViewHandler(testPChannel, cat, mgr, nil)
	h.localOptimizer = optimizer

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
	acquired, ok := mgr.getAcquired(view.QueryViewKey())
	require.True(t, ok)
	acquired.OnReady()
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport}})

	tasks, err := h.AcquireSearchSegmentTasks(
		context.Background(),
		view.ShardID(),
		view.QueryViewKey().QueryViewVersion,
		&viewpb.QueryPlanMVCC{GrowingTimetick: 11, TransformingTimetick: 10},
		&internalpb.SearchRequest{CollectionID: testCollectionID, PartitionIDs: []int64{10}},
	)

	require.NoError(t, err)
	assert.True(t, optimizer.searchCalled)
	assert.Equal(t, uint64(11), runtime.growingTimetick)
	assert.Equal(t, uint64(10), runtime.transformingTimetick)
	assert.Equal(t, []int64{10}, runtime.partitionIDs)
	require.Len(t, tasks.Tasks(), 1)

	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport}})
	assert.Equal(t, 1, mgr.releasedCount())
	assert.Equal(t, 0, handle.releaseCount)

	tasks.Release()
	assert.Equal(t, 1, handle.releaseCount)
}

func TestSNHandler_AcquireQuerySegmentTasksWaitsRuntimeAndReleasesViewRef(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	handle := &mockGrowingSegmentHandle{id: 2000, partitionID: 20}
	runtime := &mockQueryRuntime{handles: []GrowingSegmentHandle{handle}}
	mgr.runtime = runtime
	optimizer := &fakeSNLocalOptimizer{}
	h := recoverSNQueryViewHandler(testPChannel, cat, mgr, nil)
	h.localOptimizer = optimizer

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
	acquired, ok := mgr.getAcquired(view.QueryViewKey())
	require.True(t, ok)
	acquired.OnReady()
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport}})

	tasks, err := h.AcquireQuerySegmentTasks(
		context.Background(),
		view.ShardID(),
		view.QueryViewKey().QueryViewVersion,
		&viewpb.QueryPlanMVCC{GrowingTimetick: 11, TransformingTimetick: 10},
		&internalpb.RetrieveRequest{CollectionID: testCollectionID, PartitionIDs: []int64{20}},
	)

	require.NoError(t, err)
	assert.True(t, optimizer.queryCalled)
	assert.Equal(t, uint64(11), runtime.growingTimetick)
	assert.Equal(t, uint64(10), runtime.transformingTimetick)
	assert.Equal(t, []int64{20}, runtime.partitionIDs)
	require.Len(t, tasks.Tasks(), 1)

	tasks.Release()
	assert.Equal(t, 1, handle.releaseCount)
}

func TestSNHandler_AcquireSearchSegmentTasksReleasesViewRefOnRuntimeError(t *testing.T) {
	cat := newMockCatalog()
	mgr := newMockResourceManager()
	runtimeErr := errors.New("runtime not ready")
	mgr.runtime = &mockQueryRuntime{waitErr: runtimeErr}
	h := recoverSNQueryViewHandler(testPChannel, cat, mgr, nil)

	rc := &reportCollector{}
	view := newPreparingSNView(1)
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
	acquired, ok := mgr.getAcquired(view.QueryViewKey())
	require.True(t, ok)
	acquired.OnReady()
	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateUp), OnReport: rc.onReport}})

	_, err := h.AcquireSearchSegmentTasks(
		context.Background(),
		view.ShardID(),
		view.QueryViewKey().QueryViewVersion,
		&viewpb.QueryPlanMVCC{GrowingTimetick: 11, TransformingTimetick: 10},
		&internalpb.SearchRequest{CollectionID: testCollectionID},
	)
	require.ErrorIs(t, err, runtimeErr)

	h.ApplyViews([]handler.ApplyView{{View: newSNViewWithState(1, viewpb.QueryViewState_QueryViewStateDropped), OnReport: rc.onReport}})
	assert.Equal(t, 1, mgr.releasedCount())
}

type mockQueryRuntime struct {
	growingTimetick      uint64
	transformingTimetick uint64
	partitionIDs         []int64
	handles              []GrowingSegmentHandle
	waitErr              error
	handleErr            error
}

func (r *mockQueryRuntime) WaitMVCCVisible(_ context.Context, growingTimetick uint64, transformingTimetick uint64) error {
	r.growingTimetick = growingTimetick
	r.transformingTimetick = transformingTimetick
	return r.waitErr
}

func (r *mockQueryRuntime) AcquireGrowingSegmentHandles(_ context.Context, partitionIDs []int64) ([]GrowingSegmentHandle, error) {
	r.partitionIDs = append([]int64(nil), partitionIDs...)
	return r.handles, r.handleErr
}

type fakeSNLocalOptimizer struct {
	searchCalled bool
	queryCalled  bool
	searchErr    error
	queryErr     error
}

func (o *fakeSNLocalOptimizer) OptimizeSearch(context.Context, *internalpb.SearchRequest) error {
	o.searchCalled = true
	return o.searchErr
}

func (o *fakeSNLocalOptimizer) OptimizeRetrieve(context.Context, *internalpb.RetrieveRequest) error {
	o.queryCalled = true
	return o.queryErr
}
