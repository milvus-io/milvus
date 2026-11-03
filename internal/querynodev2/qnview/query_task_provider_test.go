//go:build test && dynamic

package qnview

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var _ viewquery.QuerySegmentTask = (*QNQuerySegmentTask)(nil)

func TestQNHandler_RetainedQueryKeepsSchemaAndMVCC(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version int32
	}{{"schema zero", 0}, {"versioned schema", 3}} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := newMockSegmentManager()
			segment := &mockSealedSegmentHandle{id: 1000, partitionID: 10}
			mgr.queryHandles = []SealedSegmentHandle{segment}
			h := NewQNQueryViewHandler(mgr)
			meta := buildHandlerTestMeta(1)
			meta.SchemaVersion = tc.version
			view := qviews.NewQueryViewAtQueryNode(meta, buildHandlerTestQNView(1))
			key := view.QueryViewKey()
			rc := &reportCollector{}
			h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
			preparation, ok := mgr.getAcquired(key)
			require.True(t, ok)
			preparation.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})
			mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 10}

			// No Phase 1 field allowlist is needed, including for version zero.
			search, err := h.AcquireSearchSegmentTasks(t.Context(), view.ShardID(), key.QueryViewVersion, mvcc,
				&internalpb.SearchRequest{CollectionID: testCollectionID, SchemaVersion: tc.version, OutputFieldsId: []int64{100}}, nil)
			require.NoError(t, err)
			search.Release()
			query, err := h.AcquireQuerySegmentTasks(t.Context(), view.ShardID(), key.QueryViewVersion, mvcc,
				&internalpb.RetrieveRequest{CollectionID: testCollectionID, SchemaVersion: tc.version, OutputFieldsId: []int64{100, 101}}, nil)
			require.NoError(t, err)
			require.Len(t, query.Tasks(), 1)
			task := query.Tasks()[0].(*QNQuerySegmentTask)
			require.Equal(t, []int64{100, 101}, task.Request.GetOutputFieldsId())
			require.Equal(t, tc.version, task.Request.GetSchemaVersion())
			require.Same(t, mvcc, task.MVCC)
			require.Equal(t, key, mgr.queryKey)
			require.Equal(t, uint64(10), mgr.waitTimetick, "QN must not wait for GrowingMVCC")
			query.Release()
			require.Equal(t, 2, segment.releaseCount)

			mgr.waitCalled = false
			_, err = h.AcquireSearchSegmentTasks(t.Context(), view.ShardID(), key.QueryViewVersion, mvcc,
				&internalpb.SearchRequest{CollectionID: testCollectionID, SchemaVersion: tc.version + 1}, nil)
			require.Error(t, err)
			require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
			_, err = h.AcquireQuerySegmentTasks(t.Context(), view.ShardID(), key.QueryViewVersion, mvcc,
				&internalpb.RetrieveRequest{CollectionID: testCollectionID, SchemaVersion: tc.version + 1}, nil)
			require.Error(t, err)
			require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
			require.False(t, mgr.waitCalled, "schema mismatch must fail before acquiring resources")
		})
	}
}

type mockSealedSegmentHandle struct {
	id           int64
	partitionID  int64
	releaseCount int
}

func (h *mockSealedSegmentHandle) ID() int64 {
	return h.id
}

func (h *mockSealedSegmentHandle) PartitionID() int64 {
	return h.partitionID
}

func (h *mockSealedSegmentHandle) Segment() TransformSegment {
	return nil
}

func (h *mockSealedSegmentHandle) Release() {
	h.releaseCount++
}

func TestQNHandler_AcquireQuerySegmentTasksReleasesViewRefAfterHandles(t *testing.T) {
	mgr := newMockSegmentManager()
	handles := []*mockSealedSegmentHandle{{id: 1000, partitionID: 10}}
	mgr.queryHandles = []SealedSegmentHandle{handles[0]}
	optimizer := &fakeLocalOptimizer{}
	mgr.beforeQuery = func() {
		assert.True(t, optimizer.queryCalled)
		assert.True(t, mgr.waitCalled)
	}
	h := NewQNQueryViewHandler(mgr)
	h.localOptimizer = optimizer

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
	req, _ := mgr.getAcquired(key)
	req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})

	tasks, err := h.AcquireQuerySegmentTasks(
		context.Background(),
		view.ShardID(),
		key.QueryViewVersion,
		&viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 10},
		&internalpb.RetrieveRequest{CollectionID: testCollectionID, PartitionIDs: []int64{10}}, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, testCollectionID, optimizer.queryReq.GetCollectionID())
	assert.Equal(t, key, mgr.waitKey)
	assert.Equal(t, uint64(10), mgr.waitTimetick)
	require.Len(t, tasks.Tasks(), 1)
	assert.Equal(t, key, mgr.queryKey)
	require.Len(t, mgr.queryView.GetPartitions(), 1)
	assert.Equal(t, int64(10), mgr.queryView.GetPartitions()[0].GetPartitionId())

	h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1), OnReport: rc.onReport}})
	assert.Equal(t, 1, mgr.releasedCount())
	assert.Equal(t, 0, handles[0].releaseCount)

	tasks.Release()
	assert.Equal(t, 1, handles[0].releaseCount)
}

func TestQNHandler_AcquireSearchSegmentTasksReleasesViewRefAfterHandles(t *testing.T) {
	mgr := newMockSegmentManager()
	handles := []*mockSealedSegmentHandle{{id: 2000, partitionID: 20}}
	mgr.queryHandles = []SealedSegmentHandle{handles[0]}
	optimizer := &fakeLocalOptimizer{}
	mgr.beforeQuery = func() {
		assert.True(t, optimizer.searchCalled)
		assert.True(t, mgr.waitCalled)
	}
	h := NewQNQueryViewHandler(mgr)
	h.localOptimizer = optimizer

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
	req, _ := mgr.getAcquired(key)
	req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})

	tasks, err := h.AcquireSearchSegmentTasks(
		context.Background(),
		view.ShardID(),
		key.QueryViewVersion,
		&viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 10},
		&internalpb.SearchRequest{CollectionID: testCollectionID, PartitionIDs: []int64{20}}, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, testCollectionID, optimizer.searchReq.GetCollectionID())
	assert.Equal(t, key, mgr.waitKey)
	assert.Equal(t, uint64(10), mgr.waitTimetick)
	require.Len(t, tasks.Tasks(), 1)
	assert.Equal(t, key, mgr.queryKey)
	require.Len(t, mgr.queryView.GetPartitions(), 1)
	assert.Equal(t, int64(20), mgr.queryView.GetPartitions()[0].GetPartitionId())

	h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1), OnReport: rc.onReport}})
	assert.Equal(t, 1, mgr.releasedCount())
	assert.Equal(t, 0, handles[0].releaseCount)

	tasks.Release()
	assert.Equal(t, 1, handles[0].releaseCount)
}

func TestQNHandler_AcquireQuerySegmentTasksReleasesViewRefOnHandleError(t *testing.T) {
	mgr := newMockSegmentManager()
	mgr.queryErr = errors.New("segment handle unavailable")
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
	req, _ := mgr.getAcquired(key)
	req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})

	_, err := h.AcquireQuerySegmentTasks(
		context.Background(),
		view.ShardID(),
		key.QueryViewVersion,
		&viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 10},
		&internalpb.RetrieveRequest{CollectionID: testCollectionID}, nil,
	)
	require.Error(t, err)

	h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1), OnReport: rc.onReport}})
	assert.Equal(t, 1, mgr.releasedCount())
}

func TestQNHandler_AcquireSearchSegmentTasksReleasesViewRefOnOptimizerError(t *testing.T) {
	mgr := newMockSegmentManager()
	optimizerErr := errors.New("optimizer failed")
	h := NewQNQueryViewHandler(mgr)
	h.localOptimizer = &fakeLocalOptimizer{searchErr: optimizerErr}

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
	req, _ := mgr.getAcquired(key)
	req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})

	_, err := h.AcquireSearchSegmentTasks(
		context.Background(),
		view.ShardID(),
		key.QueryViewVersion,
		&viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 10},
		&internalpb.SearchRequest{CollectionID: testCollectionID}, nil,
	)
	require.ErrorIs(t, err, optimizerErr)
	assert.Nil(t, mgr.queryView)

	h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1), OnReport: rc.onReport}})
	assert.Equal(t, 1, mgr.releasedCount())
}

func TestQNHandler_AcquireQuerySegmentTasksReleasesViewRefOnTransformWaitError(t *testing.T) {
	mgr := newMockSegmentManager()
	waitErr := errors.New("transform visibility wait failed")
	mgr.waitErr = waitErr
	h := NewQNQueryViewHandler(mgr)

	rc := &reportCollector{}
	view := newPreparingQNView(1, 1)
	key := view.QueryViewKey()
	h.ApplyViews([]handler.ApplyView{{View: view, OnReport: rc.onReport}})
	req, _ := mgr.getAcquired(key)
	req.OnReady(map[int64][]int64{10: {1000, 1001}, 20: {2000}})

	_, err := h.AcquireQuerySegmentTasks(
		context.Background(),
		view.ShardID(),
		key.QueryViewVersion,
		&viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 10},
		&internalpb.RetrieveRequest{CollectionID: testCollectionID}, nil,
	)
	require.ErrorIs(t, err, waitErr)
	assert.True(t, mgr.waitCalled)
	assert.Nil(t, mgr.queryView)

	h.ApplyViews([]handler.ApplyView{{View: newDroppedQNView(1, 1), OnReport: rc.onReport}})
	assert.Equal(t, 1, mgr.releasedCount())
}

type fakeLocalOptimizer struct {
	searchCalled bool
	queryCalled  bool
	searchReq    *internalpb.SearchRequest
	queryReq     *internalpb.RetrieveRequest
	searchErr    error
	queryErr     error
}

func (o *fakeLocalOptimizer) OptimizeSearch(_ context.Context, req *internalpb.SearchRequest) error {
	o.searchCalled = true
	o.searchReq = req
	return o.searchErr
}

func (o *fakeLocalOptimizer) OptimizeRetrieve(_ context.Context, req *internalpb.RetrieveRequest) error {
	o.queryCalled = true
	o.queryReq = req
	return o.queryErr
}
