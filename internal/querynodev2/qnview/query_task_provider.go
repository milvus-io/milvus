package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

var _ viewquery.TaskProvider = (*QNQueryViewHandler)(nil)

func (h *QNQueryViewHandler) AcquireSearchSegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.SearchRequest,
	selection *viewpb.SegmentSelection,
) (viewquery.SearchSegmentTasks, error) {
	leaseTimer := queryLease.Begin()
	lease, err := h.AcquireReadyView(ctx, shardID, version)
	leaseTimer.End(err)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	if req.GetSchemaVersion() != lease.Meta.GetSchemaVersion() {
		return nil, viewerror.NewViewInvalidated("request schema %d does not match QueryView schema %d", req.GetSchemaVersion(), lease.Meta.GetSchemaVersion())
	}

	view := qviews.FilterSelectedSegments(filterQueryNodeViewByPartitions(lease.View, req.GetPartitionIDs()), selection)
	queryOptimizeTimer := queryOptimize.Begin()
	queryOptimizeErr := h.localOptimizer.OptimizeSearch(ctx, req)
	queryOptimizeTimer.End(queryOptimizeErr)
	if err := queryOptimizeErr; err != nil {
		return nil, err
	}
	key := qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version}
	queryVisibleTimer := queryVisible.Begin()
	queryVisibleErr := h.segMgr.WaitTransformVisible(ctx, key, mvcc.GetTransformingTimetick())
	queryVisibleTimer.End(queryVisibleErr)
	if err := queryVisibleErr; err != nil {
		return nil, err
	}
	handlesTimer := queryHandles.Begin()
	handles, err := h.segMgr.AcquireSealedSegmentHandles(ctx, key, view, mvcc.GetTransformingTimetick())
	handlesTimer.End(err)
	if err != nil {
		return nil, err
	}
	tasks := make([]QNSearchSegmentTask, 0, len(handles))
	for _, handle := range handles {
		tasks = append(tasks, QNSearchSegmentTask{
			Handle:  handle,
			Request: req,
			MVCC:    mvcc,
		})
	}
	return NewQNSearchSegmentTasks(tasks), nil
}

func (h *QNQueryViewHandler) AcquireQuerySegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.RetrieveRequest,
	selection *viewpb.SegmentSelection,
) (viewquery.QuerySegmentTasks, error) {
	leaseTimer := queryLease.Begin()
	lease, err := h.AcquireReadyView(ctx, shardID, version)
	leaseTimer.End(err)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	if req.GetSchemaVersion() != lease.Meta.GetSchemaVersion() {
		return nil, viewerror.NewViewInvalidated("request schema %d does not match QueryView schema %d", req.GetSchemaVersion(), lease.Meta.GetSchemaVersion())
	}

	view := qviews.FilterSelectedSegments(filterQueryNodeViewByPartitions(lease.View, req.GetPartitionIDs()), selection)
	queryOptimizeTimer := queryOptimize.Begin()
	queryOptimizeErr := h.localOptimizer.OptimizeRetrieve(ctx, req)
	queryOptimizeTimer.End(queryOptimizeErr)
	if err := queryOptimizeErr; err != nil {
		return nil, err
	}
	key := qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version}
	queryVisibleTimer := queryVisible.Begin()
	queryVisibleErr := h.segMgr.WaitTransformVisible(ctx, key, mvcc.GetTransformingTimetick())
	queryVisibleTimer.End(queryVisibleErr)
	if err := queryVisibleErr; err != nil {
		return nil, err
	}
	handlesTimer := queryHandles.Begin()
	handles, err := h.segMgr.AcquireSealedSegmentHandles(ctx, key, view, mvcc.GetTransformingTimetick())
	handlesTimer.End(err)
	if err != nil {
		return nil, err
	}
	tasks := make([]QNQuerySegmentTask, 0, len(handles))
	for _, handle := range handles {
		tasks = append(tasks, QNQuerySegmentTask{
			Handle:  handle,
			Request: req,
			MVCC:    mvcc,
		})
	}
	return NewQNQuerySegmentTasks(tasks), nil
}

var (
	queryLease    = stage.New("queryNode", "query_acquire", "lease")
	queryOptimize = stage.New("queryNode", "query_acquire", "local_optimize")
	queryVisible  = stage.New("queryNode", "query_acquire", "visibility_wait")
	queryHandles  = stage.New("queryNode", "query_acquire", "segment_handles")
)
