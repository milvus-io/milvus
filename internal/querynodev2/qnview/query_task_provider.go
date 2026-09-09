package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var _ viewquery.TaskProvider = (*QNQueryViewHandler)(nil)

func (h *QNQueryViewHandler) AcquireSearchSegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.SearchRequest,
) (viewquery.SearchSegmentTasks, error) {
	lease, err := h.AcquireReadyView(ctx, shardID, version)
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	view := filterQueryNodeViewByPartitions(lease.View, req.GetPartitionIDs())
	if err := h.localOptimizer.OptimizeSearch(ctx, req); err != nil {
		return nil, err
	}
	key := qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version}
	if err := h.segMgr.WaitTransformVisible(ctx, key, mvcc.GetTransformingTimetick()); err != nil {
		return nil, err
	}
	handles, err := h.segMgr.AcquireSealedSegmentHandles(ctx, key, view)
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
) (viewquery.QuerySegmentTasks, error) {
	lease, err := h.AcquireReadyView(ctx, shardID, version)
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	view := filterQueryNodeViewByPartitions(lease.View, req.GetPartitionIDs())
	if err := h.localOptimizer.OptimizeRetrieve(ctx, req); err != nil {
		return nil, err
	}
	key := qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version}
	if err := h.segMgr.WaitTransformVisible(ctx, key, mvcc.GetTransformingTimetick()); err != nil {
		return nil, err
	}
	handles, err := h.segMgr.AcquireSealedSegmentHandles(ctx, key, view)
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
